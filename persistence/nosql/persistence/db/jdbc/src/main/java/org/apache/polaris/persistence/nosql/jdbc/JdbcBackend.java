/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.persistence.nosql.jdbc;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.deserialize;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.serialize;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.BIGINT;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.INT;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.NAME;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.OBJ_ID;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.VARBINARY;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcColumnType.VARCHAR;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.ADD_REFERENCE;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.ALL_OBJ_COLS_LIST;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.ALL_OBJ_KEY_LIST;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.BULK_DELETE_OBJS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.BULK_DELETE_REFS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.DELETE_OBJ;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.DELETE_OBJ_CONDITIONAL;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.FIND_OBJS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.FIND_REFERENCES;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.INSERT_OBJ;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.MAX_BATCH_SIZE;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.PURGE_REALMS_OBJS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.PURGE_REALMS_REFS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.SCAN_OBJS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.SCAN_REFS;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.UPDATE_OBJ;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.UPDATE_OBJ_CONDITIONAL;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConstants.UPDATE_REFERENCE_POINTER;

import com.google.common.collect.Maps;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntHashMap;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.ImmutableReference;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;

final class JdbcBackend implements Backend {

  private static final int MAX_CREATE_TABLE_RECURSION_DEPTH = 3;

  private final DatabaseSpecific databaseSpecific;
  private final DataSource dataSource;
  private final boolean closeDataSource;
  private final String createTableRefsSql;
  private final String createTableObjsSql;

  JdbcBackend(JdbcBackendConfig backendConfig, DatabaseSpecific databaseSpecific) {
    this.dataSource = backendConfig.dataSource();
    this.closeDataSource = backendConfig.closeClient();
    this.databaseSpecific = databaseSpecific;
    createTableRefsSql = buildCreateTableRefsSql(databaseSpecific);
    createTableObjsSql = buildCreateTableObjsSql(databaseSpecific);
  }

  // Columns ordered to minimize column padding in PostgreSQL. A column is padded to "have the same
  // length" as the _next_ column. "Bigger" columns should appear before "smaller" columns.
  private String buildCreateTableRefsSql(DatabaseSpecific databaseSpecific) {
    var columnTypes = databaseSpecific.columnTypes();
    return "CREATE TABLE "
        + TABLE_REFS
        + "\n  (\n    "
        + COL_REALM
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_REF_NAME
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_REF_POINTER
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_REF_PREVIOUS
        + " "
        + columnTypes.get(VARBINARY)
        + ",\n    "
        + COL_REF_CREATED_AT
        + " "
        + columnTypes.get(BIGINT)
        + ",\n    PRIMARY KEY ("
        + COL_REALM
        + ", "
        + COL_REF_NAME
        + ")\n  )";
  }

  private String buildCreateTableObjsSql(DatabaseSpecific databaseSpecific) {
    var columnTypes = databaseSpecific.columnTypes();
    return "CREATE TABLE "
        + TABLE_OBJS
        + "\n  (\n    "
        + COL_REALM
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_OBJ_ID
        + " "
        + columnTypes.get(OBJ_ID)
        + ",\n    "
        + COL_OBJ_TYPE
        + " "
        + columnTypes.get(NAME)
        + ",\n    "
        + COL_OBJ_VERSION
        + " "
        + columnTypes.get(VARCHAR)
        + ",\n    "
        + COL_OBJ_VALUE
        + " "
        + columnTypes.get(VARBINARY)
        + ",\n    "
        + COL_OBJ_CREATED_AT
        + " "
        + columnTypes.get(BIGINT)
        + ",\n    "
        + COL_OBJ_REAL_PART_NUM
        + " "
        + columnTypes.get(INT)
        + ",\n    PRIMARY KEY ("
        + COL_REALM
        + ", "
        + databaseSpecific.primaryKeyCol(COL_OBJ_ID, OBJ_ID)
        + ")\n  )";
  }

  @Override
  @Nonnull
  public String type() {
    return JdbcBackendFactory.NAME;
  }

  @Nonnull
  @Override
  public Persistence newPersistence(
      Function<Backend, Backend> backendWrapper,
      @Nonnull PersistenceParams persistenceParams,
      String realmId,
      MonotonicClock monotonicClock,
      IdGenerator idGenerator) {
    return new PersistenceImplementation(
        backendWrapper.apply(this), persistenceParams, realmId, monotonicClock, idGenerator);
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    if (realmIds.isEmpty()) {
      return;
    }

    try (var conn = borrowConnection()) {
      var sql = sqlInMultipleMultiple(PURGE_REALMS_OBJS, realmIds.size());
      try (var ps = conn.prepareStatement(sql)) {
        var idx = 1;
        for (var realmId : realmIds) {
          ps.setString(idx++, realmId);
        }

        ps.executeUpdate();
      }

      sql = sqlInMultipleMultiple(PURGE_REALMS_REFS, realmIds.size());
      try (var ps = conn.prepareStatement(sql)) {
        var idx = 1;
        for (var realmId : realmIds) {
          ps.setString(idx++, realmId);
        }

        ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean supportsRealmDeletion() {
    return true;
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    try (var conn = borrowConnection()) {
      for (var r : realmRefs.entrySet()) {
        var realmId = r.getKey();
        var refs = r.getValue();

        var sql = sqlInMultipleMultiple(BULK_DELETE_REFS, refs.size());
        try (var ps = conn.prepareStatement(sql)) {
          var idx = 1;
          ps.setString(idx++, realmId);
          for (var ref : refs) {
            ps.setString(idx++, ref);
          }

          ps.executeUpdate();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    try (var conn = borrowConnection()) {
      for (var r : realmObjs.entrySet()) {
        var realmId = r.getKey();
        var objs = r.getValue();

        var sql = sqlInMultipleMultiple(BULK_DELETE_OBJS, objs.size());
        try (var ps = conn.prepareStatement(sql)) {
          var idx = 1;
          ps.setString(idx++, realmId);
          for (var pid : objs) {
            serializePersistId(ps, idx++, pid, databaseSpecific);
          }

          ps.executeUpdate();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    try (var conn = borrowConnection()) {
      try (var ps = conn.prepareStatement(SCAN_REFS)) {
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            var realm = rs.getString(COL_REALM);
            var ref = rs.getString(COL_REF_NAME);
            var cr = rs.getLong(COL_REF_CREATED_AT);
            referenceConsumer.call(realm, ref, cr);
          }
        }
      }
      try (var ps = conn.prepareStatement(SCAN_OBJS)) {
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            var realm = rs.getString(COL_REALM);
            var i = deserializePersistId(rs);
            String t = rs.getString(COL_OBJ_TYPE);
            var cr = rs.getLong(COL_OBJ_CREATED_AT);
            objConsumer.call(realm, t, persistId(i.id(), i.part()), cr);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    return withConnectionException(
        false,
        conn -> {
          var sql = databaseSpecific.wrapInsert(ADD_REFERENCE);
          try (var ps = conn.prepareStatement(sql)) {
            var idx = 1;
            ps.setString(idx++, realmId);
            ps.setString(idx++, newRef.name());
            serializeObjId(ps, idx++, newRef.pointer(), databaseSpecific);
            ps.setLong(idx++, newRef.createdAtMicros());
            ps.setBytes(idx, serialize(newRef.previousPointers()));

            return ps.executeUpdate() == 1;
          } catch (SQLException e) {
            if (databaseSpecific.isConstraintViolation(e)) {
              return false;
            }
            throw unhandledSQLException(e);
          }
        });
  }

  @Override
  public void createReferences(@Nonnull String realmId, @Nonnull List<Reference> newRefs) {
    for (Reference newRef : newRefs) {
      createReference(realmId, newRef);
    }
  }

  @Override
  public boolean updateReference(
      @Nonnull String realmId,
      @Nonnull Reference updatedRef,
      @Nonnull Optional<ObjRef> expectedPointer) {
    return withConnectionException(
        false,
        conn -> {
          var sql =
              UPDATE_REFERENCE_POINTER.replace(
                  "*PTR*", expectedPointer.isEmpty() ? " IS NULL" : "=?");

          try (PreparedStatement ps = conn.prepareStatement(sql)) {
            var idx = 1;
            serializeObjId(ps, idx++, updatedRef.pointer(), databaseSpecific);
            ps.setBytes(idx++, serialize(updatedRef.previousPointers()));

            ps.setString(idx++, realmId);
            ps.setString(idx++, updatedRef.name());
            if (expectedPointer.isPresent()) {
              serializeObjId(ps, idx++, expectedPointer, databaseSpecific);
            }
            ps.setLong(idx, updatedRef.createdAtMicros());

            if (ps.executeUpdate() == 1) {
              return true;
            }
            findReference(conn, realmId, updatedRef.name());
            return false;
          }
        });
  }

  @Nonnull
  @Override
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    return withConnectionException(true, conn -> findReference(conn, realmId, name));
  }

  @Nonnull
  Reference findReference(@Nonnull Connection conn, @Nonnull String realmId, @Nonnull String name)
      throws SQLException {
    try (var ps = conn.prepareStatement(sqlInMultipleMultiple(FIND_REFERENCES, 1))) {
      var idx = 1;
      ps.setString(idx++, realmId);
      ps.setString(idx, name);

      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return deserializeReference(rs);
        }
        throw new ReferenceNotFoundException(name);
      }
    }
  }

  @Nonnull
  @Override
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    return withConnectionException(
        true,
        conn -> {
          var numIds = ids.size();
          var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(numIds);

          var sql = sqlInMultipleMultiple(FIND_OBJS, numIds);

          try (var ps = conn.prepareStatement(sql)) {
            var idx = 1;
            ps.setString(idx++, realmId);
            for (var id : ids) {
              serializePersistId(ps, idx++, id, databaseSpecific);
            }

            try (var rs = ps.executeQuery()) {
              while (rs.next()) {
                var id = deserializePersistId(rs);
                r.put(
                    id,
                    new FetchedObj(
                        rs.getString(COL_OBJ_TYPE),
                        rs.getLong(COL_OBJ_CREATED_AT),
                        rs.getString(COL_OBJ_VERSION),
                        rs.getBytes(COL_OBJ_VALUE),
                        rs.getInt(COL_OBJ_REAL_PART_NUM)));
              }

              return r;
            }
          }
        });
  }

  @Override
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    withConnectionVoid(
        conn -> {
          // The following code works for MySQL, MariaDB, PostgreSQL, CockroachDB via their
          // DatabaseSpecific implementations using a _single_ SQL command that performs the UPSERT
          // semantics, for example 'INSERT INTO ... ON CONFLICT ... DO UPDATE'. H2 however does
          // directly not implement such a statement, only the rather complex MERGE INTO, which is
          // not implemented in 'H2DatabaseSpecific' and falls back to 'INSERT ... ON CONFLICT DO
          // NOTHING' plus 'UPDATE', if necessary.

          var numWrites = writes.size();
          var sql = databaseSpecific.wrapUpsert(INSERT_OBJ, ALL_OBJ_KEY_LIST, ALL_OBJ_COLS_LIST);
          //noinspection SqlSourceToSinkFlow
          try (var psInsert = conn.prepareStatement(sql)) {
            var inserted = new boolean[numWrites];
            var batchIndexToObjIndex =
                new Int2IntHashMap(numWrites * 2, Hashing.DEFAULT_LOAD_FACTOR, -1);

            var allInserted = true;

            var batchIndex = 0;
            for (var i = 0; ; i++) {
              var write = writes.get(i);

              var idx = bindObjPk(psInsert, realmId, persistId(write.id(), write.part()), 1);
              psInsert.setString(idx++, write.type());
              bindObjForWrite(
                  psInsert,
                  write.createdAtMicros(),
                  null,
                  write.serialized(),
                  write.partNum(),
                  idx);

              batchIndexToObjIndex.put(batchIndex++, i);
              psInsert.addBatch();

              var last = i == numWrites - 1;
              if (batchIndex == MAX_BATCH_SIZE || last) {
                batchIndex = 0;
                var updated = psInsert.executeBatch();

                for (int ri = 0; ri < updated.length; ri++) {
                  var objIndex = batchIndexToObjIndex.get(ri);
                  var ru = updated[ri];
                  if (ru == 1 || ru == 2) { // MariaDB returns '2' if upserted
                    inserted[objIndex] = true;
                  } else if (ru != 0) {
                    throw new IllegalStateException(
                        "driver returned unexpected value for a batch INSERT: " + ru);
                  } else {
                    allInserted = false;
                  }
                }
              }
              if (last) {
                break;
              }
            }

            if (!allInserted) {
              try (var psUpdate = conn.prepareStatement(UPDATE_OBJ)) {
                for (var i = 0; ; i++) {
                  var write = writes.get(i);
                  if (inserted[i]) {
                    continue;
                  }

                  var idx =
                      bindObjForWrite(
                          psUpdate,
                          write.createdAtMicros(),
                          null,
                          write.serialized(),
                          write.partNum(),
                          1);
                  bindObjPk(psUpdate, realmId, persistId(write.id(), write.part()), idx);

                  batchIndex++;
                  psUpdate.addBatch();

                  var last = i == numWrites - 1;
                  if (batchIndex == MAX_BATCH_SIZE || last) {
                    batchIndex = 0;
                    var updated = psUpdate.executeBatch();

                    for (int ru : updated) {
                      if (ru != 1) {
                        throw new IllegalStateException(
                            "driver returned unexpected value for a batch UPDATE after INSERT ('UPSERT') attempt: "
                                + ru);
                      }
                    }
                  }
                  if (last) {
                    break;
                  }
                }
              }
            }
          } catch (SQLException e) {
            if (databaseSpecific.isConstraintViolation(e)) {
              throw new UnsupportedOperationException(
                  "The database should support a functionality like PostgreSQL's "
                      + "'ON CONFLICT DO NOTHING' for INSERT statements. For H2, enable the "
                      + "PostgreSQL Compatibility Mode.");
            }
            throw unhandledSQLException(e);
          }
        });
  }

  @Override
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    if (ids.isEmpty()) {
      return;
    }
    withConnectionVoid(
        conn -> {
          try (var ps = conn.prepareStatement(DELETE_OBJ)) {
            var batchSize = 0;

            for (var id : ids) {
              if (id == null) {
                continue;
              }
              bindObjPk(ps, realmId, id, 1);
              ps.addBatch();

              if (++batchSize == MAX_BATCH_SIZE) {
                batchSize = 0;
                ps.executeBatch();
              }
            }

            if (batchSize > 0) {
              ps.executeBatch();
            }
          }
        });
  }

  @Override
  public boolean conditionalInsert(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    return withConnectionException(
        false,
        conn -> {
          try (var ps = conn.prepareStatement(databaseSpecific.wrapInsert(INSERT_OBJ))) {

            var idx = bindObjPk(ps, realmId, persistId, 1);
            ps.setString(idx++, objTypeId);
            bindObjForWrite(ps, createdAtMicros, versionToken, serializedValue, 1, idx);

            var updated = ps.executeUpdate();
            return switch (updated) {
              case 1 -> true;
              case 0 -> false;
              default ->
                  throw new IllegalStateException(
                      "driver returned unexpected value for single row INSERT: " + updated);
            };
          }
        });
  }

  @Override
  public boolean conditionalUpdate(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String updateToken,
      @Nonnull String expectedToken,
      @Nonnull byte[] serializedValue) {
    return withConnectionException(
        false,
        conn -> {
          try (var ps = conn.prepareStatement(UPDATE_OBJ_CONDITIONAL)) {
            var idx = bindObjForWrite(ps, createdAtMicros, updateToken, serializedValue, 1, 1);
            idx = bindObjPk(ps, realmId, persistId, idx);
            ps.setString(idx, expectedToken);

            var updated = ps.executeUpdate();
            return switch (updated) {
              case 1 -> true;
              case 0 -> false;
              default ->
                  throw new IllegalStateException(
                      "driver returned unexpected value for single row UPDATE: " + updated);
            };
          }
        });
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    return withConnectionException(
        false,
        conn -> {
          try (var ps = conn.prepareStatement(DELETE_OBJ_CONDITIONAL)) {
            var idx = bindObjPk(ps, realmId, persistId, 1);
            ps.setString(idx, expectedToken);
            var updated = ps.executeUpdate();
            return switch (updated) {
              case 1 -> true;
              case 0 -> false;
              default ->
                  throw new IllegalStateException(
                      "driver returned unexpected value for single row DELETE: " + updated);
            };
          }
        });
  }

  @FunctionalInterface
  interface SQLRunnableException<R, E extends Exception> {
    R run(Connection conn) throws E, SQLException;
  }

  @FunctionalInterface
  interface SQLRunnableVoid {
    void run(Connection conn) throws SQLException;
  }

  private void withConnectionVoid(SQLRunnableVoid runnable) {
    withConnectionException(
        false,
        conn -> {
          runnable.run(conn);
          return null;
        });
  }

  private <R, E extends Exception> R withConnectionException(
      boolean readOnly, SQLRunnableException<R, E> runnable) throws E {
    try (var conn = borrowConnection()) {
      var ok = false;
      R r;
      try {
        r = runnable.run(conn);
        ok = true;
      } finally {
        if (!readOnly) {
          if (ok) {
            conn.commit();
          } else {
            conn.rollback();
          }
        }
      }
      return r;
    } catch (SQLException e) {
      throw unhandledSQLException(e);
    }
  }

  static Optional<ObjRef> deserializeObjId(ResultSet rs, String col) throws SQLException {
    return Optional.ofNullable(rs.getBytes(col)).map(b -> deserialize(b, ObjRef.class));
  }

  static PersistId deserializePersistId(ResultSet rs) throws SQLException {
    var s = rs.getBytes(COL_OBJ_ID);
    return deserialize(s, PersistId.class);
  }

  static void serializeObjId(
      PreparedStatement ps, int col, Optional<ObjRef> value, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (value.isEmpty()) {
      ps.setNull(col, databaseSpecific.columnTypeIds().get(JdbcColumnType.OBJ_ID));
    } else {
      ps.setBytes(col, serialize(value.orElse(null)));
    }
  }

  static void serializePersistId(
      PreparedStatement ps, int col, PersistId value, DatabaseSpecific databaseSpecific)
      throws SQLException {
    if (value != null) {
      ps.setBytes(col, serialize(value));
    } else {
      ps.setNull(col, databaseSpecific.columnTypeIds().get(JdbcColumnType.OBJ_ID));
    }
  }

  static Reference deserializeReference(ResultSet rs) throws SQLException {
    var prevBytes = rs.getBytes(COL_REF_PREVIOUS);
    var previousPointers = prevBytes != null ? deserialize(prevBytes, long[].class) : new long[0];
    return ImmutableReference.builder()
        .name(rs.getString(COL_REF_NAME))
        .pointer(deserializeObjId(rs, COL_REF_POINTER))
        .createdAtMicros(rs.getLong(COL_REF_CREATED_AT))
        .previousPointers(previousPointers)
        .build();
  }

  static String sqlInMultipleMultiple(String sql, int count) {
    if (count == 1) {
      return sql;
    }
    var marks = new StringBuilder(sql.length() + 10 + 3 * count);
    var idx = sql.indexOf("(?)");
    checkArgument(idx > 0, "SQL does not contain (?) placeholder: %s", sql);
    marks.append(sql, 0, idx).append("(?");
    for (var i = 1; i < count; i++) {
      marks.append(",?");
    }
    marks.append(')').append(sql, idx + 3, sql.length());
    return marks.toString();
  }

  RuntimeException unhandledSQLException(SQLException e) {
    return JdbcBackend.unhandledSQLException(databaseSpecific, e);
  }

  int bindObjForWrite(
      PreparedStatement ps,
      long createdAtMicros,
      String versionToken,
      byte[] serialized,
      int partNum,
      int idx)
      throws SQLException {
    if (versionToken != null) {
      ps.setString(idx++, versionToken);
    } else {
      ps.setNull(idx++, Types.VARCHAR);
    }
    ps.setBytes(idx++, serialized);
    ps.setLong(idx++, createdAtMicros);
    ps.setInt(idx++, partNum);
    return idx;
  }

  int bindObjPk(PreparedStatement ps, String realmId, PersistId id, int idx) throws SQLException {
    ps.setString(idx++, realmId);
    serializePersistId(ps, idx++, id, databaseSpecific);
    return idx;
  }

  @Override
  public void close() {
    if (closeDataSource) {
      try {
        if (dataSource instanceof AutoCloseable ac) {
          ac.close();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  Connection borrowConnection() throws SQLException {
    var c = dataSource.getConnection();
    c.setAutoCommit(false);
    return c;
  }

  @Override
  public Optional<String> setupSchema() {
    try (var conn = borrowConnection()) {
      var nameTypeId = databaseSpecific.columnTypeIds().get(NAME);
      var objIdTypeId = databaseSpecific.columnTypeIds().get(OBJ_ID);

      var info = new StringBuilder();
      var s = conn.getCatalog();
      if (s != null && !s.isEmpty()) {
        info.append("catalog: ").append(s);
      }
      s = conn.getSchema();
      if (s != null && !s.isEmpty()) {
        if (!info.isEmpty()) {
          info.append(", ");
        }
        info.append("schema: ").append(s);
      }

      info.append(", ")
          .append(
              createTableIfNotExists(
                  0,
                  conn,
                  TABLE_REFS,
                  createTableRefsSql,
                  Stream.of(
                          COL_REALM,
                          COL_REF_NAME,
                          COL_REF_POINTER,
                          COL_REF_CREATED_AT,
                          COL_REF_PREVIOUS)
                      .collect(Collectors.toSet()),
                  Map.of(COL_REALM, nameTypeId, COL_REF_NAME, nameTypeId)));

      info.append(", ")
          .append(
              createTableIfNotExists(
                  0,
                  conn,
                  TABLE_OBJS,
                  createTableObjsSql,
                  Stream.of(
                          COL_REALM,
                          COL_OBJ_ID,
                          COL_OBJ_TYPE,
                          COL_OBJ_VERSION,
                          COL_OBJ_CREATED_AT,
                          COL_OBJ_VALUE)
                      .collect(Collectors.toSet()),
                  Map.of(COL_REALM, nameTypeId, COL_OBJ_ID, objIdTypeId)));

      // Need to commit to get DDL changes into some databases (Postgres for example)
      conn.commit();

      return Optional.of(info.toString());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String createTableIfNotExists(
      int depth,
      Connection conn,
      String tableName,
      String createTable,
      Set<String> expectedColumns,
      Map<String, Integer> expectedPrimaryKey)
      throws SQLException {

    try (var st = conn.createStatement()) {
      if (conn.getMetaData().storesLowerCaseIdentifiers()) {
        tableName = tableName.toLowerCase(Locale.ROOT);
      } else if (conn.getMetaData().storesUpperCaseIdentifiers()) {
        tableName = tableName.toUpperCase(Locale.ROOT);
      }

      var catalog = conn.getCatalog();
      var schema = conn.getSchema();

      try (var rs = conn.getMetaData().getTables(catalog, schema, tableName, null)) {
        if (rs.next()) {
          // table already exists

          var primaryKey = new LinkedHashMap<String, Integer>();
          var columns = new LinkedHashMap<String, Integer>();

          try (var cols = conn.getMetaData().getColumns(catalog, schema, tableName, null)) {
            while (cols.next()) {
              var colName = cols.getString("COLUMN_NAME").toLowerCase(Locale.ROOT);
              columns.put(colName, cols.getInt("DATA_TYPE"));
            }
          }
          try (var cols = conn.getMetaData().getPrimaryKeys(catalog, schema, tableName)) {
            while (cols.next()) {
              var colName = cols.getString("COLUMN_NAME").toLowerCase(Locale.ROOT);
              var colType = columns.get(colName);
              primaryKey.put(colName.toLowerCase(Locale.ROOT), colType);
            }
          }

          checkState(
              primaryKey.equals(expectedPrimaryKey),
              "Expected primary key columns %s do not match existing primary key columns %s for table '%s' (type names and ordinals from java.sql.Types). DDL template:\n%s",
              withSqlTypesNames(expectedPrimaryKey),
              withSqlTypesNames(primaryKey),
              tableName,
              createTable);
          var missingColumns = new HashSet<>(expectedColumns);
          missingColumns.removeAll(columns.keySet());
          if (!missingColumns.isEmpty()) {
            throw new IllegalStateException(
                format(
                    "The database table %s is missing mandatory columns %s.%nFound columns : %s%nExpected columns : %s%nDDL template:\n%s",
                    tableName,
                    sortedColumnNames(missingColumns),
                    sortedColumnNames(columns.keySet()),
                    sortedColumnNames(expectedColumns),
                    createTable));
          }

          // Existing table looks compatible
          return format("table '%s' looks compatible", tableName);
        }
      }

      try {
        st.executeUpdate(createTable);

        return format("table '%s' created", tableName);
      } catch (SQLException e) {
        if (!databaseSpecific.isAlreadyExists(e)) {
          throw e;
        }

        if (depth >= MAX_CREATE_TABLE_RECURSION_DEPTH) {
          throw e;
        }

        // table was created by another process, try again to check the schema
        return createTableIfNotExists(
            depth + 1, conn, tableName, createTable, expectedColumns, expectedPrimaryKey);
      }
    }
  }

  private String withSqlTypesNames(Map<String, Integer> primaryKey) {
    StringBuilder sb = new StringBuilder().append('[');
    primaryKey.forEach(
        (col, type) -> {
          String typeName = sqlTypeName(type);
          if (sb.length() > 1) {
            sb.append(", ");
          }
          sb.append(col).append(" ").append(typeName).append("(JDBC id:").append(type).append(')');
        });
    return sb.append(']').toString();
  }

  private String sqlTypeName(int type) {
    return Arrays.stream(Types.class.getDeclaredFields())
        .filter(
            f ->
                Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers())
                    && f.getType() == int.class)
        .filter(
            f -> {
              try {
                return f.getInt(null) == type;
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            })
        .map(Field::getName)
        .findFirst()
        .orElseThrow();
  }

  private static String sortedColumnNames(Collection<?> input) {
    return input.stream().map(Object::toString).sorted().collect(Collectors.joining(","));
  }

  static RuntimeException unhandledSQLException(DatabaseSpecific databaseSpecific, SQLException e) {
    if (databaseSpecific.isRetryTransaction(e)) {
      return new UnknownOperationResultException("Unhandled SQL exception", e);
    }
    return new RuntimeException("Unhandled SQL exception", e);
  }
}

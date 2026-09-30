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
package org.apache.polaris.persistence.nosql.bigtable;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import static com.google.common.base.Preconditions.checkState;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.CELL_TIMESTAMP;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.FAMILY_OBJS;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.FAMILY_REFS;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.MAX_PARALLEL_READS;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_REF_POINTER;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.QUALIFIER_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.UnknownException;
import com.google.api.gax.rpc.WatchdogTimeoutException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.MutationApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import jakarta.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableBackend implements Backend {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigtableBackend.class);

  private final boolean closeClient;
  final BigtableDataClient dataClient;
  private final BigtableTableAdminClient tableAdminClient;
  final long apiTimeoutMillis;

  final String tableRefs;
  final String tableObjs;
  final TableId tableRefsId;
  final TableId tableObjsId;

  public BigtableBackend(BigtableBackendConfig config) {
    this.closeClient = config.closeClient();
    this.dataClient = config.dataClient();
    this.apiTimeoutMillis = config.apiTimeoutMillis();
    this.tableAdminClient = config.tableAdminClient();
    this.tableRefs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_REFS).orElse(TABLE_REFS);
    this.tableObjs =
        config.tablePrefix().map(prefix -> prefix + '_' + TABLE_OBJS).orElse(TABLE_OBJS);
    this.tableRefsId = TableId.of(tableRefs);
    this.tableObjsId = TableId.of(tableObjs);
  }

  @Override
  @Nonnull
  public String type() {
    return BigtableBackendFactory.NAME;
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
  public Optional<String> setupSchema() {
    if (tableAdminClient == null) {
      // If BigTable admin client is not available, check at least that the required tables exist.
      boolean refs = checkTableNoAdmin(tableRefsId);
      boolean objs = checkTableNoAdmin(tableObjsId);
      checkState(
          refs && objs,
          "Not all required tables (%s and %s) are available in BigTable, cannot start.",
          tableRefs,
          tableObjs);
      LOGGER.info("No Bigtable admin client available, skipping schema setup");
    } else {
      checkTable(tableRefs, FAMILY_REFS);
      checkTable(tableObjs, FAMILY_OBJS);
    }
    return tableAdminClient != null ? Optional.empty() : Optional.of("no admin client");
  }

  private boolean checkTableNoAdmin(TableId table) {
    try {
      dataClient.readRow(table, "dummy");
      return true;
    } catch (NotFoundException nf) {
      LOGGER.error("Table '{}' does not exist in Google Bigtable", table);
    }
    return false;
  }

  private void checkTable(String table, String family) {
    BigtableTableAdminClient client = requireNonNull(tableAdminClient, "tableAdminClient");
    try {
      client.getTable(table);
    } catch (NotFoundException nf) {
      LOGGER.info("Creating table '{}' in Google Bigtable...", table);
      client.createTable(CreateTableRequest.of(table).addFamily(family));
    }
  }

  @Override
  public boolean supportsRealmDeletion() {
    return tableAdminClient != null;
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    realmIds.forEach(
        realmId -> {
          var realmPrefix = dbKeyRealmPrefix(realmId);
          tableAdminClient.dropRowRange(tableObjs, realmPrefix);
          tableAdminClient.dropRowRange(tableRefs, realmPrefix);
        });
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(tableRefsId)) {
      realmRefs.forEach(
          (realm, refs) -> {
            for (var ref : refs) {
              var key = dbKey(realm, ref);
              var mutation = RowMutationEntry.create(key).deleteRow();
              batcher.add(mutation);
            }
          });
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(tableObjsId)) {
      realmObjs.forEach(
          (realm, obis) -> {
            for (var id : obis) {
              var key = dbKey(realm, id);
              var mutation = RowMutationEntry.create(key).deleteRow();
              batcher.add(mutation);
            }
          });
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    scan(
        tableRefsId,
        FAMILY_REFS,
        row -> {
          try {
            var key = row.getKey();
            var keyBytes = key.newCodedInput();
            var realm = keyBytes.readStringRequireUtf8();
            var ref = keyBytes.readStringRequireUtf8();
            var createdAtMicros =
                Long.parseLong(
                    row.getCells(FAMILY_REFS, QUALIFIER_REF_CREATED_AT)
                        .getFirst()
                        .getValue()
                        .toStringUtf8());
            referenceConsumer.call(realm, ref, createdAtMicros);
            return key;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });

    scan(
        tableObjsId,
        FAMILY_OBJS,
        row -> {
          try {
            var key = row.getKey();
            var keyBytes = key.newCodedInput();
            var realm = keyBytes.readStringRequireUtf8();
            var persistId = PersistId.fromBytes(keyBytes.readByteArray());
            var type =
                row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_TYPE).getFirst().getValue().toStringUtf8();
            var createdAtMicros =
                Long.parseLong(
                    row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_CREATED_AT)
                        .getFirst()
                        .getValue()
                        .toStringUtf8());
            objConsumer.call(realm, type, persistId, createdAtMicros);
            return key;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void scan(TableId tableId, String family, Function<Row, ByteString> rowConsumer) {
    var q =
        Query.create(tableId).filter(FILTERS.chain().filter(FILTERS.family().exactMatch(family)));
    var paginator = q.createPaginator(100);
    var iter = dataClient.readRows(paginator.getNextQuery()).iterator();
    var lastKey = (ByteString) null;
    while (true) {
      if (iter.hasNext()) {
        var row = iter.next();
        lastKey = rowConsumer.apply(row);
        continue;
      }

      if (lastKey == null) {
        break;
      }
      paginator.advance(lastKey);
      iter = dataClient.readRows(paginator.getNextQuery()).iterator();
      lastKey = null;
    }
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    try {
      var key = dbKey(realmId, newRef.name());

      var mutation = refsMutation(newRef);
      var condition =
          FILTERS
              .chain()
              .filter(FILTERS.family().exactMatch(FAMILY_REFS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REF_CREATED_AT));

      return !dataClient.checkAndMutateRow(
          ConditionalRowMutation.create(tableRefsId, key).condition(condition).otherwise(mutation));
    } catch (ApiException e) {
      throw apiException(e);
    }
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
    try {
      var key = dbKey(realmId, updatedRef.name());

      var filter =
          FILTERS
              .chain()
              .filter(FILTERS.family().exactMatch(FAMILY_REFS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_REF_POINTER))
              .filter(FILTERS.value().exactMatch(refPointerBytes(expectedPointer)));

      if (casReference(key, filter, refsMutation(updatedRef))) {
        return true;
      }

      fetchReference(realmId, updatedRef.name());
      return false;
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  private boolean casReference(ByteString key, Filters.Filter condition, Mutation mutation) {
    return dataClient.checkAndMutateRow(
        ConditionalRowMutation.create(tableRefsId, key).condition(condition).then(mutation));
  }

  @Nonnull
  @Override
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    try {
      var key = dbKey(realmId, name);
      var row = dataClient.readRow(tableRefsId, key);
      if (row != null) {
        return refFromRow(name, row);
      }
      throw new ReferenceNotFoundException(name);
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Nonnull
  @Override
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    try {
      var r = new HashMap<PersistId, FetchedObj>();

      bulkFetch(
          tableObjsId,
          ids,
          id -> dbKey(realmId, id),
          row -> {
            var obj = objFromRow(row);

            try {
              var key = row.getKey();
              var keyBytes = key.newCodedInput();
              /* var realm = */ keyBytes.readStringRequireUtf8();
              var persistId = PersistId.fromBytes(keyBytes.readByteArray());
              r.put(persistId, obj);

            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });

      return r;
    } catch (ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private <ID> void bulkFetch(
      TableId tableId, Set<ID> ids, Function<ID, ByteString> keyGen, Consumer<Row> resultConsumer)
      throws InterruptedException, ExecutionException, TimeoutException {
    var idsList = new ArrayList<>(ids);

    var num = idsList.size();
    if (num == 0) {
      return;
    }

    ApiFuture<Row>[] handles;
    if (num <= MAX_PARALLEL_READS) {
      handles = doBulkFetch(idsList, keyGen, key -> dataClient.readRowAsync(tableId, key));
    } else {
      try (Batcher<ByteString, Row> batcher = dataClient.newBulkReadRowsBatcher(tableId)) {
        handles = doBulkFetch(idsList, keyGen, batcher::add);
      }
    }

    for (int idx = 0; idx < num; idx++) {
      ApiFuture<Row> handle = handles[idx];
      if (handle != null) {
        Row row = handle.get(apiTimeoutMillis, MILLISECONDS);
        if (row != null) {
          resultConsumer.accept(row);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <ID> ApiFuture<Row>[] doBulkFetch(
      List<ID> ids,
      Function<ID, ByteString> keyGen,
      Function<ByteString, ApiFuture<Row>> handleGen) {
    var num = ids.size();
    var handles = new ApiFuture[num];
    for (var idx = 0; idx < num; idx++) {
      var id = ids.get(idx);
      if (id != null) {
        var key = keyGen.apply(id);
        handles[idx] = handleGen.apply(key);
      }
    }
    return handles;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    if (writes.isEmpty()) {
      return;
    }

    try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(tableObjsId)) {
      for (var obj : writes) {
        if (obj == null) {
          continue;
        }

        var key = dbKey(realmId, PersistId.persistId(obj.id(), obj.part()));

        batcher.add(
            objToMutation(
                RowMutationEntry.create(key),
                obj.type(),
                obj.serialized(),
                obj.partNum(),
                obj.createdAtMicros(),
                null));
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(tableObjsId)) {
      for (var id : ids) {
        if (id == null) {
          continue;
        }

        var key = dbKey(realmId, id);
        var mutation = RowMutationEntry.create(key).deleteRow();
        batcher.add(mutation);
      }
    } catch (ApiException e) {
      throw apiException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean conditionalInsert(
      @Nonnull String realmId,
      String objTypeId,
      @Nonnull PersistId persistId,
      long createdAtMicros,
      @Nonnull String versionToken,
      @Nonnull byte[] serializedValue) {
    try {
      var key = dbKey(realmId, persistId);

      var mutation =
          objToMutation(
              Mutation.create(), objTypeId, serializedValue, 1, createdAtMicros, versionToken);
      var condition =
          FILTERS
              .chain()
              .filter(FILTERS.key().exactMatch(key))
              .filter(FILTERS.family().exactMatch(FAMILY_OBJS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_CREATED_AT));

      return !dataClient.checkAndMutateRow(
          ConditionalRowMutation.create(tableObjsId, key).condition(condition).otherwise(mutation));
    } catch (ApiException e) {
      throw apiException(e);
    }
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
    try {
      var key = dbKey(realmId, persistId);

      var mutation =
          objToMutation(
              Mutation.create(), objTypeId, serializedValue, 1, createdAtMicros, updateToken);
      var condition =
          FILTERS
              .chain()
              .filter(FILTERS.key().exactMatch(key))
              .filter(FILTERS.family().exactMatch(FAMILY_OBJS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_VERSION))
              .filter(FILTERS.value().exactMatch(expectedToken));

      return dataClient.checkAndMutateRow(
          ConditionalRowMutation.create(tableObjsId, key).condition(condition).then(mutation));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    try {
      var key = dbKey(realmId, persistId);

      var mutation = Mutation.create().deleteRow();
      var condition =
          FILTERS
              .chain()
              .filter(FILTERS.key().exactMatch(key))
              .filter(FILTERS.family().exactMatch(FAMILY_OBJS))
              .filter(FILTERS.qualifier().exactMatch(QUALIFIER_OBJ_VERSION))
              .filter(FILTERS.value().exactMatch(expectedToken));

      return dataClient.checkAndMutateRow(
          ConditionalRowMutation.create(tableObjsId, key).condition(condition).then(mutation));
    } catch (ApiException e) {
      throw apiException(e);
    }
  }

  static <M extends MutationApi<M>> M objToMutation(
      M mutation,
      String objTypeId,
      byte[] serialized,
      int partNum,
      long createdAtMicros,
      String versionToken) {
    mutation
        .setCell(FAMILY_OBJS, QUALIFIER_OBJ_VALUE, CELL_TIMESTAMP, unsafeWrap(serialized))
        .setCell(FAMILY_OBJS, QUALIFIER_OBJ_TYPE, CELL_TIMESTAMP, copyFromUtf8(objTypeId))
        .setCell(
            FAMILY_OBJS,
            QUALIFIER_OBJ_REAL_PART_NUM,
            CELL_TIMESTAMP,
            copyFromUtf8(Integer.toString(partNum)))
        .setCell(
            FAMILY_OBJS,
            QUALIFIER_OBJ_CREATED_AT,
            CELL_TIMESTAMP,
            copyFromUtf8(Long.toString(createdAtMicros)));
    if (versionToken != null) {
      mutation.setCell(
          FAMILY_OBJS, QUALIFIER_OBJ_VERSION, CELL_TIMESTAMP, copyFromUtf8(versionToken));
    }
    return mutation;
  }

  @SuppressWarnings("ByteBufferBackingArray")
  private FetchedObj objFromRow(Row row) {
    var createdAtMicros =
        Long.parseLong(
            row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_CREATED_AT)
                .getFirst()
                .getValue()
                .toStringUtf8());

    var serialized =
        row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_VALUE).getFirst().getValue().asReadOnlyByteBuffer();

    var objVersionCells = row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_VERSION);
    var versionToken =
        objVersionCells.isEmpty() ? null : objVersionCells.getFirst().getValue().toStringUtf8();

    String objTypeId =
        row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_TYPE).getFirst().getValue().toStringUtf8();

    var realPartNum =
        Integer.parseInt(
            row.getCells(FAMILY_OBJS, QUALIFIER_OBJ_REAL_PART_NUM)
                .getFirst()
                .getValue()
                .toStringUtf8());

    if (serialized.position() == 0
        && serialized.remaining() == serialized.capacity()
        && serialized.hasArray()) {
      // fast path, can use the byte[] of the ByteBuffer
      return new FetchedObj(
          objTypeId, createdAtMicros, versionToken, serialized.array(), realPartNum);
    }
    // slow path, must copy
    var bin = new byte[serialized.remaining()];
    serialized.get(bin);
    return new FetchedObj(objTypeId, createdAtMicros, versionToken, bin, realPartNum);
  }

  private Reference refFromRow(String refName, Row row) {
    var ref = Reference.builder().name(refName);

    ref.createdAtMicros(
        Long.parseLong(
            row.getCells(FAMILY_REFS, QUALIFIER_REF_CREATED_AT)
                .getFirst()
                .getValue()
                .toStringUtf8()));

    var pointer =
        row.getCells(FAMILY_REFS, QUALIFIER_REF_POINTER).getFirst().getValue().toByteArray();
    if (pointer.length > 0) {
      var p = ObjRef.fromBytes(pointer);
      if (p != null) {
        ref.pointer(p);
      }
    }

    try {
      var ci =
          row.getCells(FAMILY_REFS, QUALIFIER_REF_PREVIOUS).getFirst().getValue().newCodedInput();
      var numPrevious = ci.readUInt32();
      var previous = new long[numPrevious];
      for (var i = 0; i < numPrevious; i++) {
        previous[i] = ci.readFixed64();
      }
      ref.previousPointers(previous);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return ref.build();
  }

  private static Mutation refsMutation(@Nonnull Reference reference) {
    var mutation =
        Mutation.create()
            .setCell(
                FAMILY_REFS,
                QUALIFIER_REF_CREATED_AT,
                // Note: must use a constant timestamp, otherwise BigTable will pile up historic
                // values,
                // which would also break our CAS conditions, because historic values could match.
                CELL_TIMESTAMP,
                copyFromUtf8(Long.toString(reference.createdAtMicros())));
    mutation.setCell(
        FAMILY_REFS, QUALIFIER_REF_POINTER, CELL_TIMESTAMP, refPointerBytes(reference.pointer()));

    return mutation.setCell(
        FAMILY_REFS,
        QUALIFIER_REF_PREVIOUS,
        CELL_TIMESTAMP,
        unsafeWrap(previousPointerBytes(reference.previousPointers())));
  }

  private static ByteString refPointerBytes(Optional<ObjRef> pointer) {
    return unsafeWrap(pointer.map(ObjRef::toBytes).orElse(new byte[0]));
  }

  static byte[] previousPointerBytes(long[] pointers) {
    try (var previousOut = new ByteArrayOutputStream()) {
      var cos = CodedOutputStream.newInstance(previousOut);
      cos.writeUInt32NoTag(pointers.length);
      for (var p : pointers) {
        cos.writeFixed64NoTag(p);
      }
      cos.flush();
      previousOut.flush();
      return previousOut.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (closeClient) {
      RuntimeException ex = null;
      try {
        dataClient.close();
      } catch (Exception e) {
        ex = new RuntimeException(e);
      }
      try {
        if (tableAdminClient != null) {
          tableAdminClient.close();
        }
      } catch (Exception e) {
        if (ex == null) {
          ex = new RuntimeException(e);
        } else {
          ex.addSuppressed(e);
        }
      }
      if (ex != null) {
        throw ex;
      }
    }
  }

  static ByteString dbKeyRealmPrefix(String realm) {
    try {
      var out = ByteString.newOutput();
      var codedOut = CodedOutputStream.newInstance(out);
      codedOut.writeStringNoTag(realm);
      codedOut.flush();
      return out.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static ByteString dbKey(String realm, byte[] bytes) {
    try {
      var out = ByteString.newOutput();
      var codedOut = CodedOutputStream.newInstance(out);
      codedOut.writeStringNoTag(realm);
      codedOut.writeByteArrayNoTag(bytes);
      codedOut.flush();
      return out.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static ByteString dbKey(String realm, PersistId id) {
    var bytes = PersistId.serializeAsBytes(id);
    return dbKey(realm, bytes);
  }

  static ByteString dbKey(String realm, String ref) {
    try {
      var out = ByteString.newOutput();
      var codedOut = CodedOutputStream.newInstance(out);
      codedOut.writeStringNoTag(realm);
      codedOut.writeStringNoTag(ref);
      codedOut.flush();
      return out.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static RuntimeException apiException(ApiException e) {
    if (e instanceof DeadlineExceededException
        || e instanceof WatchdogTimeoutException
        || e instanceof UnknownException
        || e instanceof AbortedException) {
      throw new UnknownOperationResultException("Unhandled BigTable exception", e);
    }
    throw new RuntimeException("Unhandled BigTable exception", e);
  }
}

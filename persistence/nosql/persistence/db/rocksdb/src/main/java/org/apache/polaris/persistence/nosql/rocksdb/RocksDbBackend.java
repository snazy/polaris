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
package org.apache.polaris.persistence.nosql.rocksdb;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;
import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.deserialize;
import static org.apache.polaris.persistence.nosql.impl.PersistenceImplementation.serialize;
import static org.apache.polaris.persistence.nosql.rocksdb.ObjKey.objKey;
import static org.apache.polaris.persistence.nosql.rocksdb.RefKey.refKey;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.FetchedObj;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.apache.polaris.persistence.nosql.api.backend.WriteObj;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.impl.PersistenceImplementation;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;

@SuppressWarnings("NullableProblems")
final class RocksDbBackend implements Backend {
  public static final String CF_REFERENCES = "refs";
  public static final String CF_OBJECTS = "objs";

  private static final List<String> CF_ALL = asList(CF_REFERENCES, CF_OBJECTS);

  private static final int STRIPES = 16;

  private final RocksDbBackendConfig config;

  private ColumnFamilyOptions columnFamilyOptions;
  private TransactionDB db;
  private ColumnFamilyHandle cfReferences;
  private ColumnFamilyHandle cfObjects;

  private final Striped<Lock> referencesLocks = Striped.lock(STRIPES);

  private final Striped<Lock> objLocks = Striped.lock(STRIPES);

  Lock referencesLock(String referenceName) {
    var l = referencesLocks.get(referenceName);
    l.lock();
    return l;
  }

  Lock objLock(ObjKey id) {
    var l = objLocks.get(id);
    l.lock();
    return l;
  }

  RocksDbBackend(RocksDbBackendConfig config) {
    this.config = config;
  }

  @Override
  @Nonnull
  public String type() {
    return RocksDbBackendFactory.NAME;
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
    initialize();
    return Optional.of("database path: " + config.databasePath());
  }

  private synchronized void initialize() {
    if (db == null) {
      var dbPath = config.databasePath();

      checkState(dbPath != null, "RocksDB instance is missing the databasePath option.");
      checkState(
          !Files.exists(dbPath) || Files.isDirectory(dbPath),
          "RocksDB cannot use databasePath %s.",
          dbPath);

      var columnFamilies = new ArrayList<byte[]>();
      columnFamilies.add(DEFAULT_COLUMN_FAMILY);
      CF_ALL.stream().map(s -> s.getBytes(StandardCharsets.UTF_8)).forEach(columnFamilies::add);

      columnFamilyOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
      var columnFamilyDescriptors =
          columnFamilies.stream()
              .map(c -> new ColumnFamilyDescriptor(c, columnFamilyOptions))
              .collect(Collectors.toList());

      try (final var dbOptions =
          new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
        // TODO: Consider setting WAL limits.
        var columnFamilyHandles = new ArrayList<ColumnFamilyHandle>();
        db =
            TransactionDB.open(
                dbOptions,
                new TransactionDBOptions(),
                dbPath.toString(),
                columnFamilyDescriptors,
                columnFamilyHandles);

        var columnFamilyHandleMap = new HashMap<String, ColumnFamilyHandle>();
        for (var i = 0; i < CF_ALL.size(); i++) {
          String cf = CF_ALL.get(i);
          columnFamilyHandleMap.put(cf, columnFamilyHandles.get(i + 1));
        }

        cfReferences = columnFamilyHandleMap.get(CF_REFERENCES);
        cfObjects = columnFamilyHandleMap.get(CF_OBJECTS);
      } catch (RocksDBException e) {
        throw new RuntimeException("RocksDB failed to start", e);
      }
    }
  }

  @Override
  public void deleteRealms(Set<String> realmIds) {
    // TODO could implement this using RocksDb.deleteRange(), but would have to implement a custom
    //  serialization for RefKey + ObjKey that has the realmId at the beginning.
    throw new UnsupportedOperationException("Backend does not support deletion of realms.");
  }

  @Override
  public boolean supportsRealmDeletion() {
    return false;
  }

  @Override
  public void batchDeleteRefs(Map<String, Set<String>> realmRefs) {
    try {
      for (var r : realmRefs.entrySet()) {
        var realmId = r.getKey();
        for (var ref : r.getValue()) {
          var k = serialize(refKey(realmId, ref));
          db.delete(cfReferences, k);
        }
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("RocksDB failed to start", e);
    }
  }

  @Override
  public void batchDeleteObjs(Map<String, Set<PersistId>> realmObjs) {
    try {
      for (var r : realmObjs.entrySet()) {
        var realmId = r.getKey();
        for (var obj : r.getValue()) {
          var k = serialize(objKey(realmId, obj.id(), obj.part()));
          db.delete(cfObjects, k);
        }
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("RocksDB failed to start", e);
    }
  }

  @Override
  public void scanBackend(
      @Nonnull ReferenceScanCallback referenceConsumer, @Nonnull ObjScanCallback objConsumer) {
    var first = true;
    var lastKey = (byte[]) null;
    try (var iter = db.newIterator(cfReferences)) {
      iter.seekToFirst();
      while (iter.isValid()) {
        if (first) {
          first = false;
        } else {
          iter.next();
        }

        var k = iter.key();
        if (lastKey != null && Arrays.equals(lastKey, k)) {
          // RocksDB sometimes tends to return the same key twice
          continue;
        }
        lastKey = k;
        var refKey = deserialize(k, RefKey.class);
        var ser = db.get(cfReferences, k);
        var ref = deserialize(ser, Reference.class);
        referenceConsumer.call(refKey.realmId(), refKey.name(), ref.createdAtMicros());
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("RocksDB failed to start", e);
    }

    first = true;
    try (var iter = db.newIterator(cfObjects)) {
      iter.seekToFirst();
      while (iter.isValid()) {
        if (first) {
          first = false;
        } else {
          iter.next();
        }

        var k = iter.key();
        if (lastKey != null && Arrays.equals(lastKey, k)) {
          // RocksDB sometimes tends to return the same key twice
          continue;
        }
        lastKey = k;
        var objKey = deserialize(k, ObjKey.class);
        var ser = db.get(cfObjects, k);
        var obj = deserialize(ser, WrappedObj.class);
        objConsumer.call(
            objKey.realmId(),
            obj.type(),
            persistId(objKey.id(), objKey.part()),
            obj.createdAtMicros());
      }
    } catch (RocksDBException e) {
      throw new RuntimeException("RocksDB failed to start", e);
    }
  }

  @Override
  public boolean createReference(@Nonnull String realmId, @Nonnull Reference newRef) {
    var l = referencesLock(newRef.name());
    try {
      var cf = cfReferences;
      var key = dbKey(realmId, newRef.name());

      byte[] existing = db.get(cf, key);
      if (existing != null) {
        return false;
      }

      db.put(cf, key, serialize(newRef));

      return true;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
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
    var l = referencesLock(updatedRef.name());
    try {
      var cf = cfReferences;
      var key = dbKey(realmId, updatedRef.name());

      var existing = db.get(cf, key);
      if (existing == null) {
        throw new ReferenceNotFoundException(updatedRef.name());
      }
      var ref = deserialize(existing, Reference.class);
      if (!ref.pointer().equals(expectedPointer)) {
        return false;
      }

      db.put(cf, key, serialize(updatedRef));
      return true;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    } finally {
      l.unlock();
    }
  }

  @Nonnull
  @Override
  public Reference fetchReference(@Nonnull String realmId, @Nonnull String name) {
    try {
      var cf = cfReferences;
      var key = dbKey(realmId, name);

      var reference = db.get(cf, key);
      var ref = deserialize(reference, Reference.class);
      if (ref == null) {
        throw new ReferenceNotFoundException(name);
      }
      return ref;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  private byte[] dbKey(@Nonnull String realmId, @Nonnull String name) {
    var refKey = refKey(realmId, name);
    return serialize(refKey);
  }

  @Nonnull
  @Override
  public Map<PersistId, FetchedObj> fetch(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    try {
      var cf = cfObjects;

      var r = Maps.<PersistId, FetchedObj>newHashMapWithExpectedSize(ids.size());
      for (var id : ids) {
        var key = objKey(realmId, id);
        var keyBytes = serialize(key);
        byte[] obj = db.get(cf, keyBytes);
        if (obj != null) {
          var wrapped = deserialize(obj, WrappedObj.class);
          r.put(
              id,
              new FetchedObj(
                  wrapped.type(),
                  wrapped.createdAtMicros(),
                  wrapped.versionToken(),
                  wrapped.serialized(),
                  wrapped.realPartNum()));
        }
      }
      return r;
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public void write(@Nonnull String realmId, @Nonnull List<WriteObj> writes) {
    try {
      var cf = cfObjects;

      for (WriteObj write : writes) {
        var key = objKey(realmId, write.id(), write.part());
        var keyBytes = serialize(key);
        var wrapped =
            new WrappedObj(
                write.type(), write.createdAtMicros(), null, write.serialized(), write.partNum());
        var serialized = serialize(wrapped);
        db.put(cf, keyBytes, serialized);
      }
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public void delete(@Nonnull String realmId, @Nonnull Set<PersistId> ids) {
    try {
      var cf = cfObjects;

      for (var id : ids) {
        var key = objKey(realmId, id);
        var keyBytes = serialize(key);
        db.delete(cf, keyBytes);
      }
    } catch (RocksDBException e) {
      throw rocksDbException(e);
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
      var cf = cfObjects;

      var key = objKey(realmId, persistId);
      var keyBytes = serialize(key);
      var l = objLock(key);
      try {
        if (db.get(cf, keyBytes) != null) {
          return false;
        }

        var wrapped = new WrappedObj(objTypeId, createdAtMicros, versionToken, serializedValue, 1);
        var serialized = serialize(wrapped);
        db.put(cf, keyBytes, serialized);
        return true;
      } finally {
        l.unlock();
      }
    } catch (RocksDBException e) {
      throw rocksDbException(e);
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
      var cf = cfObjects;

      var key = objKey(realmId, persistId);
      var keyBytes = serialize(key);
      var l = objLock(key);
      try {
        var existingBytes = db.get(cf, keyBytes);
        if (existingBytes == null) {
          return false;
        }
        var existing = deserialize(existingBytes, WrappedObj.class);
        if (!expectedToken.equals(existing.versionToken())) {
          return false;
        }

        var wrapped = new WrappedObj(objTypeId, createdAtMicros, updateToken, serializedValue, 1);
        var serialized = serialize(wrapped);
        db.put(cf, keyBytes, serialized);
        return true;
      } finally {
        l.unlock();
      }
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public boolean conditionalDelete(
      @Nonnull String realmId, @Nonnull PersistId persistId, @Nonnull String expectedToken) {
    try {
      var cf = cfObjects;

      var key = objKey(realmId, persistId);
      var keyBytes = serialize(key);
      var l = objLock(key);
      try {
        var existingBytes = db.get(cf, keyBytes);
        if (existingBytes == null) {
          return false;
        }
        var existing = deserialize(existingBytes, WrappedObj.class);
        if (!expectedToken.equals(existing.versionToken())) {
          return false;
        }

        db.delete(cf, keyBytes);
        return true;
      } finally {
        l.unlock();
      }
    } catch (RocksDBException e) {
      throw rocksDbException(e);
    }
  }

  @Override
  public synchronized void close() {
    if (db != null) {
      try {
        Exception ex = null;
        for (var closeable : List.of(cfObjects, cfReferences, db, columnFamilyOptions)) {
          try {
            closeable.close();
          } catch (Exception e) {
            if (ex == null) {
              ex = e;
            } else {
              ex.addSuppressed(e);
            }
          }
        }
        if (ex != null) {
          throw new RuntimeException(ex);
        }
      } finally {
        db = null;
        cfReferences = null;
        cfObjects = null;
      }
    }
  }

  static RuntimeException rocksDbException(RocksDBException e) {
    throw new RuntimeException("Unhandled RocksDB exception", e);
  }
}

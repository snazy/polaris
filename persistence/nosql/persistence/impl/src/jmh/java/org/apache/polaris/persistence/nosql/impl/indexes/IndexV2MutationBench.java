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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Writer-only and complete mutation workflows; all fresh promotion/size work is charged. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class IndexV2MutationBench {
  @State(Scope.Thread)
  public static class Data {
    @Param({"BASELINE", "COMPACT", "V2", "V2_BULK"})
    public String variant;

    @Param({"5000"})
    public int entries;

    @Param({"HIERARCHICAL", "WORDS"})
    public String shape;

    @Param({"16"})
    public int changes;

    @Param({"64"})
    public int blockEntries;

    @Param({"FULL"})
    public String directoryEncoding;

    @Param({"false"})
    public boolean uneven;

    @Param({"16384"})
    public int splitTarget;

    V2IndexWriter.Options options;
    ByteBuffer v1;
    ByteBuffer v2;
    List<IndexKey> keys;
    ObjRef[] values;
    IndexKey[] updateKeys;
    IndexKey[] removeKeys;
    IndexKey[] insertKeys;
    IndexKey[] tombstones;
    ObjRef[] replacements;
    ObjRef[] larger;
    IndexSpi<ObjRef> materialized;
    IndexSpi<ObjRef> unchanged;

    @Setup
    public void setup() {
      checkArgument(
          entries >= 1000 && changes > 0 && changes * 17 < entries,
          "Invalid mutation workload size");
      var logical = PrototypeIndexData.create(entries, shape);
      keys = logical.keys();
      v1 =
          FullKeyCheckpoints.rewrite(
              logical.serialized(), OBJ_REF_SERIALIZER, FullKeyCheckpoints.Policy.NONE, 64);
      options =
          new V2IndexWriter.Options(
              blockEntries, uneven, V2IndexWriter.DirectoryEncoding.valueOf(directoryEncoding), 8);
      var original = IndexImpl.deserializeStoreIndex(v1.duplicate(), OBJ_REF_SERIALIZER);
      v2 = V2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
      values = new ObjRef[entries];
      var live = new ArrayList<IndexKey>();
      for (int i = 0; i < entries; i++) {
        values[i] = original.get(keys.get(i));
        if (values[i] != null) live.add(keys.get(i));
      }
      updateKeys = new IndexKey[changes];
      removeKeys = new IndexKey[changes];
      insertKeys = new IndexKey[changes];
      tombstones = new IndexKey[changes];
      replacements = new ObjRef[changes];
      larger = new ObjRef[changes];
      for (int i = 0; i < changes; i++) {
        updateKeys[i] = live.get((i + 1) * live.size() / (changes * 2 + 1));
        removeKeys[i] = live.get((changes + i + 1) * live.size() / (changes * 2 + 1));
        var bytes = updateKeys[i].asByteBuffer();
        var extended = ByteBuffer.allocate(bytes.remaining() + 1).put(bytes).put((byte) 127).flip();
        insertKeys[i] = IndexKey.key(extended);
        checkArgument(!original.containsElement(insertKeys[i]), "Insertion key already exists");
        tombstones[i] = keys.get(i * 17);
        var previous = requireNonNull(original.get(updateKeys[i]));
        replacements[i] = ObjRef.objRef("other", previous.id(), previous.numParts());
        larger[i] =
            ObjRef.objRef("substantially-longer-value-label", previous.id(), previous.numParts());
        checkArgument(
            OBJ_REF_SERIALIZER.serializedSize(original.get(updateKeys[i]))
                == OBJ_REF_SERIALIZER.serializedSize(replacements[i]),
            "Expected same-size replacement");
      }
      materialized = empty();
      populate(materialized);
      encode(materialized);
      unchanged = fresh();
    }

    IndexSpi<ObjRef> empty() {
      return variant.equals("V2")
          ? new V2IndexImpl<>(OBJ_REF_SERIALIZER, options)
          : new IndexImpl<>(OBJ_REF_SERIALIZER);
    }

    IndexSpi<ObjRef> fresh() {
      return switch (variant) {
        case "BASELINE", "V2_BULK" ->
            IndexImpl.deserializeStoreIndex(v1.duplicate(), OBJ_REF_SERIALIZER);
        case "COMPACT" -> new CompactIndexImpl<>(v1, OBJ_REF_SERIALIZER).asMutableIndex();
        case "V2" -> new V2IndexImpl<>(v2, OBJ_REF_SERIALIZER, options);
        default -> throw new IllegalArgumentException(variant);
      };
    }

    void populate(IndexSpi<ObjRef> index) {
      for (int i = 0; i < entries; i++) index.add(indexElement(keys.get(i), values[i]));
    }

    ByteBuffer encode(IndexSpi<ObjRef> index) {
      return variant.equals("V2_BULK")
          ? V2IndexWriter.write(index, OBJ_REF_SERIALIZER, options)
          : index.serialize();
    }

    void force(IndexSpi<ObjRef> index, boolean replan) {
      if (index instanceof V2IndexImpl<?> mutable) {
        if (replan) mutable.invalidatePlan();
        else mutable.setModified();
      } else ((IndexImpl<?>) index).setModified();
    }

    IndexSpi<ObjRef> insert() {
      var index = fresh();
      for (int i = 0; i < changes; i++) index.put(insertKeys[i], replacements[i]);
      return index;
    }

    IndexSpi<ObjRef> update(boolean grow) {
      var index = fresh();
      for (int i = 0; i < changes; i++)
        index.put(updateKeys[i], grow ? larger[i] : replacements[i]);
      return index;
    }

    IndexSpi<ObjRef> remove() {
      var index = fresh();
      for (var key : removeKeys) index.remove(key);
      return index;
    }
  }

  @Benchmark
  public Object build(Data state) {
    var index = state.empty();
    state.populate(index);
    return index;
  }

  @Benchmark
  public Object buildSerialize(Data state) {
    var index = state.empty();
    state.populate(index);
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeMutable(Data state) {
    return state.fresh();
  }

  @Benchmark
  public Object deserializeSerialize(Data state) {
    return state.encode(state.fresh());
  }

  @Benchmark
  public Object serializeUnmodifiedIndex(Data state) {
    return state.encode(state.unchanged);
  }

  @Benchmark
  public Object serializeRepeated(Data state) {
    return state.encode(state.materialized);
  }

  @Benchmark
  public Object serializeModifiedIndex(Data state) {
    state.force(state.materialized, false);
    return state.encode(state.materialized);
  }

  @Benchmark
  public Object serializeReplannedIndex(Data state) {
    state.force(state.materialized, true);
    return state.encode(state.materialized);
  }

  @Benchmark
  public Object deserializeForceSerialize(Data state) {
    var index = state.fresh();
    state.force(index, false);
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeAdd(Data state) {
    return state.insert();
  }

  @Benchmark
  public Object deserializeAddSerialize(Data state) {
    return state.encode(state.insert());
  }

  @Benchmark
  public Object deserializeUpdate(Data state) {
    return state.update(false);
  }

  @Benchmark
  public Object deserializeUpdateSerialize(Data state) {
    return state.encode(state.update(false));
  }

  @Benchmark
  public Object deserializeGrowValueSerialize(Data state) {
    return state.encode(state.update(true));
  }

  @Benchmark
  public Object deserializeRemove(Data state) {
    return state.remove();
  }

  @Benchmark
  public Object deserializeRemoveSerialize(Data state) {
    return state.encode(state.remove());
  }

  @Benchmark
  public Object deserializeTombstoneSerialize(Data state) {
    var index = state.fresh();
    for (var key : state.updateKeys) index.add(indexElement(key, null));
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeResurrectSerialize(Data state) {
    var index = state.fresh();
    for (int i = 0; i < state.changes; i++) index.put(state.tombstones[i], state.replacements[i]);
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeRemoveMissingSerialize(Data state) {
    var index = state.fresh();
    for (var key : state.insertKeys) index.remove(key);
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeMixedSerialize(Data state) {
    var index = state.fresh();
    for (int i = 0; i < state.changes; i++) {
      index.put(state.insertKeys[i], state.replacements[i]);
      index.put(state.updateKeys[i], state.larger[i]);
      index.remove(state.removeKeys[i]);
    }
    return state.encode(index);
  }

  @Benchmark
  public Object deserializeSplit(Data state) {
    return state.fresh().splitByTargetSize(state.splitTarget);
  }

  @Benchmark
  public void deserializeSplitSerialize(Data state, Blackhole sink) {
    for (var part : state.fresh().splitByTargetSize(state.splitTarget))
      sink.consume(state.encode(part));
  }
}

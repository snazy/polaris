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
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/** Equal logical data and complete cold read/spill workflows for the six implementations. */
final class SplitV2Workload {
  enum Variant {
    BASELINE(null),
    V2(null),
    V2_REUSE(null),
    FRONT_ENDS(SplitV2Format.Layout.FRONT_ENDS),
    FRONT_LENGTHS(SplitV2Format.Layout.FRONT_LENGTHS),
    PREFIX_ENDS(SplitV2Format.Layout.PREFIX_ENDS);
    final SplitV2Format.Layout layout;

    Variant(SplitV2Format.Layout layout) {
      this.layout = layout;
    }
  }

  record Outcome(List<ByteBuffer> parts, long observed) {}

  final List<IndexKey> keys;
  final IndexKey[] queries = new IndexKey[1024];
  final List<SplitV2IndexWriter.Change<ObjRef>> changes;
  private final EnumMap<Variant, ByteBuffer> input;
  private final EnumMap<Variant, ByteBuffer> pending;
  private final int blockEntries;
  private final int keyRestart;
  private final V2IndexWriter.DirectoryEncoding directory;

  SplitV2Workload(
      int entries,
      String shape,
      String valueShape,
      int mutations,
      String locality,
      String operation,
      int blockEntries,
      V2IndexWriter.DirectoryEncoding directory) {
    this(entries, shape, valueShape, mutations, locality, operation, blockEntries, directory, 8);
  }

  SplitV2Workload(
      int entries,
      String shape,
      String valueShape,
      int mutations,
      String locality,
      String operation,
      int blockEntries,
      V2IndexWriter.DirectoryEncoding directory,
      int keyRestart) {
    checkArgument(entries > 0 && mutations >= 0 && mutations < entries / 2);
    this.blockEntries = blockEntries;
    this.keyRestart = keyRestart;
    this.directory = directory;
    var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
    keys = PrototypeIndexData.create(entries, shape).keys();
    if (!valueShape.equals("SINGLE_PART") && !valueShape.equals("MIXED_PARTS"))
      throw new IllegalArgumentException(valueShape);
    for (int i = 0; i < entries; i++)
      original.put(
          keys.get(i),
          ObjRef.objRef("value", i + 100L, valueShape.equals("SINGLE_PART") ? 1 : 1 + i % 3));
    input = serialize(original);
    var random = new Random(983);
    for (int from = 0; from < queries.length; from += 16) {
      var selected = new HashSet<IndexKey>();
      for (int i = from; i < from + 16; i++) {
        if (selected.size() == entries) selected.clear();
        IndexKey key;
        do {
          key = keys.get(random.nextInt(entries));
        } while (!selected.add(key));
        queries[i] = key;
      }
    }
    var updates = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < mutations; i++) {
      int ordinal =
          switch (locality) {
            case "CLUSTERED" -> entries / 2 + i;
            case "SCATTERED" -> (int) ((long) (i + 1) * entries / (mutations + 1));
            default -> throw new IllegalArgumentException(locality);
          };
      var key = keys.get(ordinal);
      var old = requireNonNull(original.get(key));
      ObjRef value = ObjRef.objRef("other", old.id(), old.numParts());
      String op =
          operation.equals("MIXED")
              ? switch (i % 3) {
                case 0 -> "REMOVE";
                case 1 -> "UPDATE";
                default -> "INSERT";
              }
              : operation;
      switch (op) {
        case "UPDATE" -> {}
        case "GROW" ->
            value = ObjRef.objRef("substantially-longer-value-label", old.id(), old.numParts());
        case "REMOVE" -> value = null;
        case "INSERT" -> {
          do {
            var raw = key.asByteBuffer();
            key =
                IndexKey.key(
                    ByteBuffer.allocate(raw.remaining() + 1).put(raw).put((byte) 127).flip());
          } while (original.containsElement(key) || updates.containsElement(key));
        }
        default -> throw new IllegalArgumentException(operation);
      }
      updates.add(indexElement(key, value));
    }
    pending = serialize(updates);
    var logical = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
    updates
        .elementIterator()
        .forEachRemaining(element ->
            logical.add(
                new SplitV2IndexWriter.Change<>(
                    element.key(), element.hasValue() ? element : null)));
    changes = List.copyOf(logical);
  }

  SplitV2Format.Options options(Variant variant) {
    return new SplitV2Format.Options(variant.layout, blockEntries, directory, keyRestart);
  }

  private EnumMap<Variant, ByteBuffer> serialize(IndexSpi<ObjRef> index) {
    var result = new EnumMap<Variant, ByteBuffer>(Variant.class);
    result.put(
        Variant.BASELINE,
        FullKeyCheckpoints.rewrite(
            index.serialize(), OBJ_REF_SERIALIZER, FullKeyCheckpoints.Policy.NONE, blockEntries));
    var old =
        V2IndexWriter.write(
            index,
            OBJ_REF_SERIALIZER,
            new V2IndexWriter.Options(blockEntries, false, directory, 8));
    result.put(Variant.V2, old);
    result.put(Variant.V2_REUSE, old);
    for (var variant : Variant.values())
      if (variant.layout != null)
        result.put(variant, SplitV2IndexWriter.write(index, OBJ_REF_SERIALIZER, options(variant)));
    return result;
  }

  int serializedSize(Variant variant) {
    return input.get(variant).remaining();
  }

  IndexSpi<ObjRef> read(Variant variant) {
    return read(variant, input.get(variant));
  }

  private IndexSpi<ObjRef> read(Variant variant, ByteBuffer bytes) {
    if (variant == Variant.BASELINE)
      return IndexImpl.deserializeStoreIndex(bytes.duplicate(), OBJ_REF_SERIALIZER);
    if (variant.layout == null) return new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    return new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
  }

  Outcome spill(Variant variant, int preReads, boolean split, long target) {
    var source = read(variant);
    long observed = 0;
    for (int i = 0; i < preReads; i++) {
      var value =
          source.get(changes.isEmpty() ? queries[i & 1023] : changes.get(i % changes.size()).key());
      if (value != null) observed += value.id();
    }
    var batch = read(variant, pending.get(variant));
    if (variant.layout != null) {
      var merge = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
      for (var iterator = batch.elementIterator(); iterator.hasNext(); ) {
        var element = iterator.next();
        merge.add(
            new SplitV2IndexWriter.Change<>(
                element.key(), element.valueNullable() == null ? null : element));
      }
      var base = (SplitV2IndexReader<ObjRef>) source;
      return new Outcome(
          split
              ? SplitV2IndexWriter.mergeSplit(base, merge, options(variant), target)
              : List.of(SplitV2IndexWriter.merge(base, merge, options(variant))),
          observed);
    }
    IndexSpi<ObjRef> mutable =
        switch (variant) {
          case BASELINE -> source;
          case V2 ->
              new V2IndexImpl<>(
                  (V2IndexReader<ObjRef>) source,
                  OBJ_REF_SERIALIZER,
                  new V2IndexWriter.Options(blockEntries, false, directory, 8));
          case V2_REUSE ->
              new V2BlockReuseIndex<>(
                  (V2IndexReader<ObjRef>) source,
                  OBJ_REF_SERIALIZER,
                  new V2IndexWriter.Options(blockEntries, false, directory, 8));
          default -> throw new IllegalArgumentException(variant.name());
        };
    for (var iterator = batch.elementIterator(); iterator.hasNext(); ) {
      var element = iterator.next();
      if (element.valueNullable() == null) mutable.remove(element.key());
      else mutable.add(element);
    }
    var parts = new ArrayList<ByteBuffer>();
    if (split) for (var part : mutable.splitByTargetSize(target)) parts.add(part.serialize());
    else {
      mutable.estimatedSerializedSize();
      parts.add(mutable.serialize());
    }
    return new Outcome(parts, observed);
  }
}

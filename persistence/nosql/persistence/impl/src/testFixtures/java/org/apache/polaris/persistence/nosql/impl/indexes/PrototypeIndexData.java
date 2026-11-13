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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/** Deterministic, identical v1 inputs for the production reader and both experiments. */
record PrototypeIndexData(ByteBuffer serialized, List<IndexKey> keys) {
  static PrototypeIndexData create(int entries, String shape) {
    var ordered = new TreeSet<IndexKey>();
    var random = new Random(42);
    var words = shape.equals("WORDS") ? KeyIndexTestSet.Words.words() : List.<String>of();
    if (shape.equals("WORDS") && entries > words.size()) {
      throw new IllegalArgumentException("Too many word keys");
    }
    for (int i = 0; ordered.size() < entries; i++) {
      ordered.add(
          switch (shape) {
            case "HIERARCHICAL" ->
                IndexKey.key("catalog/namespace-%03d/table-%06d".formatted(i / 100, i));
            case "WORDS" -> IndexKey.key(words.get((int) ((long) i * words.size() / entries)));
            case "UUID" -> IndexKey.key(new UUID(random.nextLong(), random.nextLong()).toString());
            case "BINARY" -> {
              byte[] bytes = new byte[1 + random.nextInt(60)];
              random.nextBytes(bytes);
              yield IndexKey.key(ByteBuffer.wrap(bytes));
            }
            default -> throw new IllegalArgumentException(shape);
          });
    }
    var keys = new ArrayList<>(ordered);
    var index = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < keys.size(); i++) {
      index.add(
          indexElement(keys.get(i), i % 17 == 0 ? null : objRef("value", i + 100L, 1 + i % 3)));
    }
    return new PrototypeIndexData(index.serialize().asReadOnlyBuffer(), List.copyOf(keys));
  }

  IndexKey liveKeyAt(int ordinal) {
    return keys.get(ordinal % 17 == 0 ? ordinal + 1 : ordinal);
  }

  PrototypeIndexData encodedFor(String variant, int interval) {
    if (!variant.startsWith("OFFSET_")) return this;
    var policy =
        variant.startsWith("OFFSET_BINARY")
            ? FullKeyCheckpoints.Policy.BINARY
            : FullKeyCheckpoints.Policy.PERIODIC;
    return new PrototypeIndexData(
        FullKeyCheckpoints.rewrite(serialized, OBJ_REF_SERIALIZER, policy, interval), keys);
  }

  <V> IndexSpi<V> fresh(
      String variant, ByteBuffer bytes, IndexValueSerializer<V> codec, int interval) {
    if (variant.startsWith("ADAPTIVE_")) {
      return new AdaptiveCheckpointIndexImpl<>(
          bytes,
          codec,
          interval,
          variant.startsWith("ADAPTIVE_PUBLISHED"),
          variant.endsWith("_PAGED"),
          !variant.endsWith("_UNMEMOIZED"));
    }
    if (variant.startsWith("OFFSET_")) {
      return new OffsetCheckpointIndexImpl<>(
          bytes, codec, variant.endsWith("_PAGED"), !variant.endsWith("_UNMEMOIZED"));
    }
    return switch (variant) {
      case "BASELINE" -> IndexImpl.deserializeStoreIndex(bytes.slice(), codec);
      case "CHECKPOINT" -> new CheckpointIndexImpl<>(bytes, codec, interval);
      case "CHECKPOINT_PAGED" -> new CheckpointIndexImpl<>(bytes, codec, interval, true);
      case "COMPACT" -> new CompactIndexImpl<>(bytes, codec);
      case "COMPACT_PAGED" -> new CompactIndexImpl<>(bytes, codec, true);
      case "CHECKPOINT_UNMEMOIZED" ->
          new CheckpointIndexImpl<>(bytes, codec, interval, false, false);
      case "COMPACT_UNMEMOIZED" -> new CompactIndexImpl<>(bytes, codec, false, false);
      default -> throw new IllegalArgumentException(variant);
    };
  }

  IndexSpi<ObjRef> fresh(String variant, int interval) {
    return fresh(variant, serialized, OBJ_REF_SERIALIZER, interval);
  }

  Object prepare(String variant, int interval) {
    if (variant.startsWith("ADAPTIVE_")) {
      return new AdaptiveCheckpointIndexImpl<>(
              serialized,
              OBJ_REF_SERIALIZER,
              interval,
              variant.startsWith("ADAPTIVE_PUBLISHED"),
              variant.endsWith("_PAGED"),
              !variant.endsWith("_UNMEMOIZED"))
          .prepare();
    }
    if (variant.startsWith("OFFSET_")) {
      return new OffsetCheckpointIndexImpl<>(
              serialized,
              OBJ_REF_SERIALIZER,
              variant.endsWith("_PAGED"),
              !variant.endsWith("_UNMEMOIZED"))
          .prepare();
    }
    return switch (variant) {
      case "BASELINE" -> null;
      case "CHECKPOINT", "CHECKPOINT_PAGED", "CHECKPOINT_UNMEMOIZED" ->
          new CheckpointIndexImpl<>(
                  serialized,
                  OBJ_REF_SERIALIZER,
                  interval,
                  variant.endsWith("_PAGED"),
                  !variant.endsWith("_UNMEMOIZED"))
              .prepare();
      case "COMPACT", "COMPACT_PAGED", "COMPACT_UNMEMOIZED" ->
          new CompactIndexImpl<>(
                  serialized,
                  OBJ_REF_SERIALIZER,
                  variant.endsWith("_PAGED"),
                  !variant.endsWith("_UNMEMOIZED"))
              .layout;
      default -> throw new IllegalArgumentException(variant);
    };
  }

  IndexSpi<ObjRef> fromPrepared(String variant, Object prepared, int interval) {
    return fromPrepared(variant, prepared, OBJ_REF_SERIALIZER, interval);
  }

  <V> IndexSpi<V> fromPrepared(
      String variant, Object prepared, IndexValueSerializer<V> codec, int interval) {
    if (variant.startsWith("ADAPTIVE_")) {
      return new AdaptiveCheckpointIndexImpl<>(
          (AdaptiveCheckpointIndexImpl.Snapshot) prepared,
          codec,
          variant.startsWith("ADAPTIVE_PUBLISHED"),
          variant.endsWith("_PAGED"),
          !variant.endsWith("_UNMEMOIZED"));
    }
    if (variant.startsWith("OFFSET_")) {
      return new OffsetCheckpointIndexImpl<>(
          (OffsetCheckpointIndexImpl.Directory) prepared,
          codec,
          variant.endsWith("_PAGED"),
          !variant.endsWith("_UNMEMOIZED"));
    }
    return switch (variant) {
      case "BASELINE" -> fresh(variant, serialized, codec, interval);
      case "CHECKPOINT" ->
          new CheckpointIndexImpl<>((CheckpointIndexImpl.Directory) prepared, codec);
      case "COMPACT" -> new CompactIndexImpl<>((CompactIndexImpl.Layout) prepared, codec);
      case "CHECKPOINT_PAGED" ->
          new CheckpointIndexImpl<>((CheckpointIndexImpl.Directory) prepared, codec, true);
      case "COMPACT_PAGED" ->
          new CompactIndexImpl<>((CompactIndexImpl.Layout) prepared, codec, true);
      case "CHECKPOINT_UNMEMOIZED" ->
          new CheckpointIndexImpl<>((CheckpointIndexImpl.Directory) prepared, codec, false, false);
      case "COMPACT_UNMEMOIZED" ->
          new CompactIndexImpl<>((CompactIndexImpl.Layout) prepared, codec, false, false);
      default -> throw new IllegalArgumentException(variant);
    };
  }
}

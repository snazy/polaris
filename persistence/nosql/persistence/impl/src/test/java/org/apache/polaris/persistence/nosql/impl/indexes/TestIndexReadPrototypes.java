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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexReadPrototypes {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "CHECKPOINT",
        "COMPACT",
        "COMPACT3",
        "OFFSET_PERIODIC",
        "OFFSET_BINARY",
        "ADAPTIVE_LOCKED",
        "ADAPTIVE_PUBLISHED",
        "ADAPTIVE_PUBLISHED_PACKED_KEYS",
        "ADAPTIVE_PUBLISHED_DECODED_KEYS",
        "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
        "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH"
      })
  void memoizesDecodedValuesAcrossElementsWithinEachView(String variant) {
    var data = PrototypeIndexData.create(129, "HIERARCHICAL").encodedFor(variant, 16);
    var codec = new CountingSerializer();
    var view = data.fresh(variant, data.serialized(), codec, 16);
    var key = data.keys().get(64);
    assertThat(view.contains(key)).isTrue();
    assertThat(codec.decodes).isZero();
    var element = requireNonNull(view.getElement(key));
    var value = element.value();
    assertThat(element.value()).isSameAs(value);
    assertThat(codec.decodes).isEqualTo(1);
    assertThat(view.get(key)).isSameAs(value);
    assertThat(view.iterator(key, null, false).next().value()).isSameAs(value);
    assertThat(view.reverseIterator(null, key, false).next().value()).isSameAs(value);
    assertThat(codec.decodes).isEqualTo(1);
    var tombstone = data.keys().getFirst();
    assertThat(view.contains(tombstone)).isFalse();
    assertThat(codec.decodes).isEqualTo(1);
    assertThat(view.get(tombstone)).isNull();
    assertThat(view.get(tombstone)).isNull();
    assertThat(codec.decodes).isEqualTo(2);
    var nullElement = requireNonNull(view.getElement(tombstone));
    assertThat(nullElement.valueNullable()).isNull();
    assertThat(nullElement.valueNullable()).isNull();
    assertThat(codec.decodes).isEqualTo(2);
    var prepared = data.prepare(variant, 16);
    var independent = data.fromPrepared(variant, prepared, codec, 16);
    assertThat(independent.get(key)).isEqualTo(value).isNotSameAs(value);
    assertThat(codec.decodes).isEqualTo(3);
    var second = data.fromPrepared(variant, prepared, codec, 16);
    var secondValue = second.get(key);
    assertThat(secondValue).isEqualTo(value).isNotSameAs(independent.get(key));
    assertThat(second.get(key)).isSameAs(secondValue);
    assertThat(codec.decodes).isEqualTo(4);
  }

  private static final class CountingSerializer implements IndexValueSerializer<ObjRef> {
    int decodes;

    @Override
    public int serializedSize(@Nullable ObjRef value) {
      return OBJ_REF_SERIALIZER.serializedSize(value);
    }

    @Override
    public @NonNull ByteBuffer serialize(@Nullable ObjRef value, @NonNull ByteBuffer target) {
      return OBJ_REF_SERIALIZER.serialize(value, target);
    }

    @Override
    public @Nullable ObjRef deserialize(@NonNull ByteBuffer buffer) {
      decodes++;
      return OBJ_REF_SERIALIZER.deserialize(buffer);
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer buffer) {
      return OBJ_REF_SERIALIZER.isNullSerialized(buffer);
    }

    @Override
    public void skip(@NonNull ByteBuffer buffer) {
      OBJ_REF_SERIALIZER.skip(buffer);
    }
  }

  static Stream<Arguments> variants() {
    return Stream.of(
            "CHECKPOINT",
            "COMPACT",
            "COMPACT3",
            "OFFSET_PERIODIC",
            "OFFSET_BINARY",
            "ADAPTIVE_LOCKED",
            "ADAPTIVE_PUBLISHED",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH")
        .flatMap(variant ->
            Stream.of("HIERARCHICAL", "UUID", "BINARY", "WORDS")
                .flatMap(shape ->
                    Stream.of(0, 1, 33, 257)
                        .flatMap(size ->
                            Stream.of(1, 16, 64)
                                .map(interval -> arguments(variant, shape, size, interval)))));
  }

  @ParameterizedTest
  @MethodSource("variants")
  void matchesProductionV1Reads(String variant, String shape, int size, int interval) {
    var data = PrototypeIndexData.create(size, shape).encodedFor(variant, interval);
    var baseline = data.fresh("BASELINE", interval);
    var candidate = data.fresh(variant, interval);
    assertThat(candidate.hasElements()).isEqualTo(baseline.hasElements());
    assertThat(candidate.first()).isEqualTo(baseline.first());
    if (size > 0) {
      // Exercise forward cursors before fully building a checkpoint directory via last/reverse.
      assertThat(collect(candidate, null, null, false, 10))
          .isEqualTo(collect(baseline, null, null, false, 10));
      assertThat(candidate.last()).isEqualTo(baseline.last());
    }
    assertThat(collect(candidate, null, null, false, Integer.MAX_VALUE))
        .isEqualTo(collect(baseline, null, null, false, Integer.MAX_VALUE));
    assertThat(collect(candidate, null, null, true, Integer.MAX_VALUE))
        .isEqualTo(collect(baseline, null, null, true, Integer.MAX_VALUE));
    for (var key : data.keys()) {
      var expected = baseline.get(key);
      var actual = candidate.get(key);
      assertThat(actual).isEqualTo(expected);
      if (expected != null) {
        assertThat(actual).isNotNull();
        assertThat(actual.numParts()).isEqualTo(expected.numParts());
      }
      assertThat(candidate.contains(key)).isEqualTo(baseline.contains(key));
      assertThat(candidate.containsElement(key)).isTrue();
    }
    for (var key :
        List.of(
            IndexKey.key(""),
            IndexKey.key("not-present"),
            IndexKey.key(ByteBuffer.wrap(new byte[] {-1, -1, -1})))) {
      assertThat(candidate.get(key)).isEqualTo(baseline.get(key));
      assertThat(candidate.containsElement(key)).isEqualTo(baseline.containsElement(key));
    }
    if (size > 1) {
      var lower = data.keys().get(size / 4);
      var higher = data.keys().get(size * 3 / 4);
      assertThat(collect(candidate, lower, higher, false, Integer.MAX_VALUE))
          .isEqualTo(collect(baseline, lower, higher, false, Integer.MAX_VALUE));
      assertThat(collect(candidate, lower, higher, true, Integer.MAX_VALUE))
          .isEqualTo(collect(baseline, lower, higher, true, Integer.MAX_VALUE));
      assertThat(collect(candidate, lower, lower, false, Integer.MAX_VALUE))
          .isEqualTo(collect(baseline, lower, lower, false, Integer.MAX_VALUE));
    }
    var prefix = IndexKey.key("catalog/namespace-001/");
    assertThat(collect(candidate, prefix, prefix, false, Integer.MAX_VALUE))
        .isEqualTo(collect(baseline, prefix, prefix, false, Integer.MAX_VALUE));
    assertThat(candidate.serialize()).isEqualTo(data.serialized());
    // Promotion keeps the production writer and old-reader compatibility.
    var mutable = candidate.asMutableIndex();
    var extra = IndexKey.key("added-after-read");
    var value = ObjRef.objRef("value", 987654321, 3);
    mutable.put(extra, value);
    var encoded = mutable.serialize();
    assertThat(encoded.get(encoded.position())).isEqualTo((byte) 1);
    assertThat(IndexImpl.deserializeStoreIndex(encoded.duplicate(), OBJ_REF_SERIALIZER).get(extra))
        .isEqualTo(value);
  }

  @Test
  void partialCheckpointsAndPreparedLayoutsMatchFreshReads() throws Exception {
    var data = PrototypeIndexData.create(1000, "HIERARCHICAL");
    var checkpoint = new CheckpointIndexImpl<>(data.serialized(), OBJ_REF_SERIALIZER, 64);
    assertThat(checkpoint.directory.size).isZero();
    long initialBytes = checkpoint.metadataBytes();
    collect(checkpoint, null, null, false, 10);
    assertThat(checkpoint.metadataBytes()).isGreaterThan(initialBytes);
    assertThat(checkpoint.directory.size).isEqualTo(1);
    assertThat(checkpoint.directory.complete).isFalse();
    var baseline = data.fresh("BASELINE", 64);
    for (int ordinal : new int[] {150, 1, 300, 65, 700, 501}) {
      var key = data.keys().get(ordinal);
      assertThat(checkpoint.get(key)).isEqualTo(baseline.get(key));
    }
    assertThat(checkpoint.directory.complete).isFalse();
    var expected = collect(data.fresh("BASELINE", 64), null, null, false, Integer.MAX_VALUE);
    for (String variant :
        List.of(
            "CHECKPOINT",
            "COMPACT",
            "COMPACT3",
            "OFFSET_PERIODIC",
            "OFFSET_BINARY",
            "ADAPTIVE_LOCKED",
            "ADAPTIVE_PUBLISHED",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH")) {
      Object layout = data.prepare(variant, 64);
      var sharedView = data.fromPrepared(variant, layout, 64);
      long metadataBytes = ((IndexReadPrototype<?>) sharedView).metadataBytes();
      try (var executor = Executors.newFixedThreadPool(2)) {
        List<Callable<List<Observed>>> jobs = new ArrayList<>();
        for (int i = 0; i < 8; i++)
          jobs.add(() -> {
            var independent = data.fromPrepared(variant, layout, 64);
            assertThat(sharedView.get(data.keys().get(700)))
                .isEqualTo(independent.get(data.keys().get(700)));
            return collect(independent, null, null, false, Integer.MAX_VALUE);
          });
        for (var future : executor.invokeAll(jobs)) assertThat(future.get()).isEqualTo(expected);
      }
      assertThat(((IndexReadPrototype<?>) sharedView).metadataBytes()).isEqualTo(metadataBytes);
    }
  }

  @Test
  void concurrentColdReadersPublishSharedValues() throws Exception {
    var data = PrototypeIndexData.create(1000, "BINARY");
    var baseline = data.fresh("BASELINE", 64);
    for (String variant :
        List.of(
            "CHECKPOINT",
            "COMPACT",
            "COMPACT3",
            "OFFSET_PERIODIC",
            "OFFSET_BINARY",
            "ADAPTIVE_LOCKED",
            "ADAPTIVE_PUBLISHED",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH")) {
      var candidate = data.fresh(variant, 64);
      var sharedKey = data.keys().get(700);
      var start = new CountDownLatch(1);
      try (var executor = Executors.newFixedThreadPool(2)) {
        List<Callable<ObjRef>> jobs = new ArrayList<>();
        for (int ordinal : new int[] {700, 5, 100, 999, 300, 1, 510, 812}) {
          var key = data.keys().get(ordinal);
          jobs.add(() -> {
            start.await();
            var sharedValue = candidate.get(sharedKey);
            var value = candidate.get(key);
            assertThat(value).isEqualTo(baseline.get(key));
            assertThat(candidate.get(key)).isSameAs(value);
            return sharedValue;
          });
        }
        var futures = jobs.stream().map(executor::submit).toList();
        start.countDown();
        for (var future : futures) {
          assertThat(future.get()).isSameAs(candidate.get(sharedKey));
        }
      }
    }
  }

  @Test
  void codecsAndBufferRangesPreserveV1Semantics() {
    var data = PrototypeIndexData.create(100, "BINARY");
    var source = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    for (int i = 0; i < data.keys().size(); i++)
      source.add(
          indexElement(
              data.keys().get(i), i % 7 == 0 ? null : data.keys().get(data.keys().size() - i - 1)));
    var bytes = source.serialize();
    for (String variant :
        List.of(
            "CHECKPOINT",
            "COMPACT",
            "COMPACT3",
            "OFFSET_PERIODIC",
            "OFFSET_BINARY",
            "ADAPTIVE_LOCKED",
            "ADAPTIVE_PUBLISHED",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH")) {
      var encoded =
          variant.startsWith("OFFSET_")
              ? FullKeyCheckpoints.rewrite(
                  bytes,
                  INDEX_KEY_SERIALIZER,
                  variant.startsWith("OFFSET_BINARY")
                      ? FullKeyCheckpoints.Policy.BINARY
                      : FullKeyCheckpoints.Policy.PERIODIC,
                  16)
              : bytes;
      for (String kind : List.of("HEAP", "SLICE", "READ_ONLY", "DIRECT")) {
        var padded =
            kind.equals("DIRECT")
                ? ByteBuffer.allocateDirect(encoded.remaining() + 32)
                : ByteBuffer.allocate(encoded.remaining() + 32);
        padded.position(5);
        if (kind.equals("SLICE")) padded = padded.slice().position(7);
        int start = padded.position();
        padded.put(encoded.duplicate()).flip().position(start);
        if (kind.equals("READ_ONLY")) padded = padded.asReadOnlyBuffer();
        int limit = padded.limit();
        var candidate = data.fresh(variant, padded, INDEX_KEY_SERIALIZER, 16);
        for (var key : data.keys()) assertThat(candidate.get(key)).isEqualTo(source.get(key));
        assertThat(candidate.serialize()).isEqualTo(encoded);
        assertThat(padded.position()).isEqualTo(start);
        assertThat(padded.limit()).isEqualTo(limit);
      }
    }
  }

  @Test
  void escapedEmptyUnicodeAndLongKeysMatchTheProductionReader() {
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var keys =
        List.of(
            IndexKey.key(""),
            IndexKey.key("a"),
            IndexKey.key("a\u0000"),
            IndexKey.key("a\u0001"),
            IndexKey.key("a\u0002"),
            IndexKey.key("漢字"),
            IndexKey.key("😀"),
            IndexKey.key("x".repeat(500)));
    for (int i = 0; i < keys.size(); i++) {
      source.put(keys.get(i), ObjRef.objRef("value", i + 1L));
    }
    var bytes = source.serialize();
    var data = new PrototypeIndexData(bytes, keys);
    var expected = collect(source, null, null, false, Integer.MAX_VALUE);
    for (String variant :
        List.of(
            "CHECKPOINT",
            "COMPACT",
            "COMPACT3",
            "OFFSET_PERIODIC",
            "OFFSET_BINARY",
            "ADAPTIVE_LOCKED",
            "ADAPTIVE_PUBLISHED",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS",
            "ADAPTIVE_PUBLISHED_PACKED_KEYS_VH",
            "ADAPTIVE_PUBLISHED_DECODED_KEYS_VH")) {
      var candidate = data.fresh(variant, 2);
      for (var key : keys) assertThat(candidate.get(key)).isEqualTo(source.get(key));
      assertThat(collect(candidate, null, null, false, Integer.MAX_VALUE)).isEqualTo(expected);
      assertThat(collect(candidate, null, null, true, Integer.MAX_VALUE))
          .isEqualTo(collect(source, null, null, true, Integer.MAX_VALUE));
    }
  }

  private static List<Observed> collect(
      Index<ObjRef> index, IndexKey lower, IndexKey higher, boolean reverse, int limit) {
    var result = new ArrayList<Observed>();
    var iterator =
        reverse
            ? index.reverseIterator(lower, higher, false)
            : index.iterator(lower, higher, false);
    while (result.size() < limit && iterator.hasNext()) {
      var element = iterator.next();
      result.add(new Observed(element.key(), element.value()));
    }
    return result;
  }

  private record Observed(IndexKey key, ObjRef value) {}
}

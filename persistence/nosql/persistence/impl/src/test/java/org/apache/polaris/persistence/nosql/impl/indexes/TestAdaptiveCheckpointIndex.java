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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestAdaptiveCheckpointIndex {
  static Stream<Arguments> modes() {
    return Stream.of(false, true)
        .flatMap(
            published ->
                Stream.of("HIERARCHICAL", "WORDS")
                    .flatMap(
                        shape ->
                            Stream.of("NONE", "PERIODIC", "BINARY", "MIXED")
                                .flatMap(
                                    encoding ->
                                        Stream.of(AdaptiveCheckpointIndexImpl.KeyMemo.values())
                                            .map(
                                                memo ->
                                                    arguments(published, shape, encoding, memo)))));
  }

  private static PrototypeIndexData fixture(int count, String shape, String encoding) {
    var data = PrototypeIndexData.create(count, shape);
    ByteBuffer bytes;
    if (encoding.equals("MIXED")) {
      var anchors = new BitSet();
      for (int ordinal : new int[] {0, 10, 137, 138, 512, 777, 1023}) {
        if (ordinal < count) anchors.set(ordinal);
      }
      bytes = FullKeyCheckpoints.rewrite(data.serialized(), OBJ_REF_SERIALIZER, anchors);
    } else {
      bytes =
          FullKeyCheckpoints.rewrite(
              data.serialized(),
              OBJ_REF_SERIALIZER,
              FullKeyCheckpoints.Policy.valueOf(encoding),
              64);
    }
    return new PrototypeIndexData(bytes, data.keys());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void checkpointsBoundGapsAndSnapshotsKeepTheirPublishedPrefix(
      boolean published, String shape, String encoding, AdaptiveCheckpointIndexImpl.KeyMemo memo) {
    var data = fixture(1025, shape, encoding);
    var codec = new CountingSerializer();
    var view = new AdaptiveCheckpointIndexImpl<>(data.serialized(), codec, 64, published, memo);
    view.get(data.keys().get(1));
    var early = view.directory();
    var ordinals = Arrays.copyOf(early.ordinals(), early.size());
    var references = Arrays.copyOf(early.references(), early.size());
    var keys = Arrays.copyOf(early.keys(), early.used());
    int decoded = codec.decodes.get();
    var complete = view.prepare();
    assertThat(codec.decodes.get()).isEqualTo(decoded);
    assertThat(complete.complete()).isTrue();
    assertThat(Arrays.copyOf(early.ordinals(), early.size())).containsExactly(ordinals);
    assertThat(Arrays.copyOf(early.references(), early.size())).containsExactly(references);
    assertThat(Arrays.copyOf(early.keys(), early.used())).containsExactly(keys);
    assertThat(complete.ordinals()).isNotSameAs(early.ordinals());
    for (int i = 1; i < complete.size(); i++) {
      assertThat(complete.ordinals()[i] - complete.ordinals()[i - 1]).isBetween(1, 64);
    }
    assertThat(1024 - complete.ordinals()[complete.size() - 1]).isLessThan(64);
    if (encoding.equals("PERIODIC")) {
      assertThat(complete.used()).isZero();
    } else if (encoding.equals("NONE") && shape.equals("HIERARCHICAL")) {
      assertThat(complete.used()).isPositive();
      assertThat(
              Arrays.stream(complete.references(), 0, complete.size()).filter(x -> x >= 0).count())
          .isEqualTo(1);
    } else if (encoding.equals("MIXED")) {
      assertThat(complete.used()).isPositive();
      assertThat(Arrays.stream(complete.references(), 0, complete.size()).anyMatch(x -> x >= 0))
          .isTrue();
    }
    var original =
        IndexImpl.deserializeStoreIndex(data.serialized().duplicate(), OBJ_REF_SERIALIZER);
    for (int i : new int[] {1024, 63, 64, 65, 136, 137, 138, 139, 511, 512, 513, 1}) {
      assertThat(view.get(data.keys().get(i))).isEqualTo(original.get(data.keys().get(i)));
    }
    var independentView =
        new AdaptiveCheckpointIndexImpl<>(complete, OBJ_REF_SERIALIZER, published, memo);
    assertThat(independentView.get(data.keys().get(700)))
        .isEqualTo(original.get(data.keys().get(700)));
    assertThat(view.serialize()).isEqualTo(data.serialized());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void concurrentExtensionAndIterationPublishOnlyCompleteCheckpoints(
      boolean published, String shape, String encoding, AdaptiveCheckpointIndexImpl.KeyMemo memo)
      throws Exception {
    var data = fixture(2048, shape, encoding);
    var view =
        new AdaptiveCheckpointIndexImpl<>(
            data.serialized(), OBJ_REF_SERIALIZER, 64, published, memo);
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(4)) {
      var futures = new ArrayList<Future<?>>();
      futures.add(
          executor.submit(
              () -> {
                start.await();
                int ordinal = 1;
                for (var entry : view) {
                  while (ordinal % 17 == 0) ordinal++;
                  assertThat(entry.key()).isEqualTo(data.keys().get(ordinal));
                  assertThat(entry.value()).isEqualTo(expected(ordinal));
                  ordinal++;
                }
                assertThat(ordinal).isEqualTo(2048);
                return null;
              }));
      for (int thread = 1; thread < 4; thread++) {
        int offset = thread * 431;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < 200; i++) {
                    int ordinal = (offset + i * 137) % 2048;
                    assertThat(view.get(data.keys().get(ordinal))).isEqualTo(expected(ordinal));
                    var snapshot = view.directory();
                    for (int p = 0; p < snapshot.size(); p++) {
                      if (p > 0)
                        assertThat(snapshot.ordinals()[p])
                            .isGreaterThan(snapshot.ordinals()[p - 1]);
                      int ref = snapshot.references()[p];
                      if (ref < 0) assertThat(~ref).isLessThan(snapshot.used());
                    }
                  }
                  return null;
                }));
      }
      start.countDown();
      for (var future : futures) future.get(30, TimeUnit.SECONDS);
    }
    assertThat(view.prepare().complete()).isTrue();
    for (int i = 0; i < 2048; i++) assertThat(view.get(data.keys().get(i))).isEqualTo(expected(i));
  }

  @ParameterizedTest
  @MethodSource("modes")
  void blockMemosAreLazyPrivateAndNeverPopulatedByForwardScans(
      boolean published, String shape, String encoding, AdaptiveCheckpointIndexImpl.KeyMemo memo) {
    var data = fixture(1025, shape, encoding);
    var codec = new CountingSerializer();
    var view = new AdaptiveCheckpointIndexImpl<>(data.serialized(), codec, 64, published, memo);
    assertThat(view.memoizedBlockCount()).isZero();
    view.prepare();
    assertThat(view.memoizedBlockCount()).isZero();
    assertThat(codec.decodes.get()).isZero();
    var iterator = view.forward(null);
    while (iterator.hasNext()) iterator.next().key();
    assertThat(view.memoizedBlockCount()).isZero();
    assertThat(codec.decodes.get()).isZero();
    assertThat(requireNonNull(view.getElement(data.keys().get(1))).key())
        .isEqualTo(data.keys().get(1));
    assertThat(view.memoizedBlockCount())
        .isEqualTo(memo == AdaptiveCheckpointIndexImpl.KeyMemo.NONE ? 0 : 1);
    assertThat(codec.decodes.get()).isZero();
    assertThat(requireNonNull(view.getElement(data.keys().get(2))).key())
        .isEqualTo(data.keys().get(2));
    assertThat(view.memoizedBlockCount())
        .isEqualTo(memo == AdaptiveCheckpointIndexImpl.KeyMemo.NONE ? 0 : 1);
    int skips = codec.skips.get();
    view.getElement(data.keys().get(1));
    if (memo != AdaptiveCheckpointIndexImpl.KeyMemo.NONE)
      assertThat(codec.skips.get()).isEqualTo(skips);
    int blocks = view.memoizedBlockCount();
    iterator = view.forward(data.keys().get(2));
    while (iterator.hasNext()) iterator.next().key();
    assertThat(view.memoizedBlockCount()).isEqualTo(blocks);
    var other = new AdaptiveCheckpointIndexImpl<>(view.prepare(), codec, published, memo);
    assertThat(other.memoizedBlockCount()).isZero();
    assertThat(other.get(data.keys().get(1000))).isEqualTo(expected(1000));
    assertThat(view.memoizedBlockCount()).isEqualTo(blocks);
  }

  @Test
  void wordDatasetSpansVocabularyWithoutTheHierarchicalPrefix() {
    var first = PrototypeIndexData.create(5000, "WORDS");
    var second = PrototypeIndexData.create(5000, "WORDS");
    assertThat(first.keys()).containsExactlyElementsOf(second.keys());
    assertThat(first.serialized()).isEqualTo(second.serialized());
    assertThat(first.keys()).hasSize(5000);
    assertThat(first.keys().stream().map(key -> key.toString().charAt(0)).distinct().count())
        .isGreaterThan(20);
    assertThat(first.keys()).noneMatch(key -> key.toString().startsWith("catalog/namespace-"));
  }

  @ParameterizedTest
  @EnumSource(
      value = AdaptiveCheckpointIndexImpl.KeyMemo.class,
      names = {"PACKED_PAGED", "DECODED_PAGED"})
  void pagedMemoSupportsANativeCheckpointAtEveryEntry(AdaptiveCheckpointIndexImpl.KeyMemo memo) {
    var data = PrototypeIndexData.create(257, "HIERARCHICAL");
    var anchors = new BitSet();
    anchors.set(0, 257);
    var bytes = FullKeyCheckpoints.rewrite(data.serialized(), OBJ_REF_SERIALIZER, anchors);
    var view = new AdaptiveCheckpointIndexImpl<>(bytes, OBJ_REF_SERIALIZER, 64, true, memo);
    assertThat(view.prepare().size()).isEqualTo(257);
    assertThat(view.memoizedPageCount()).isZero();
    for (int ordinal : new int[] {0, 63, 64, 127, 128, 255, 256})
      assertThat(view.get(data.keys().get(ordinal))).isEqualTo(expected(ordinal));
    assertThat(view.memoizedBlockCount()).isEqualTo(7);
    assertThat(view.memoizedPageCount()).isEqualTo(5);
    var iterator = view.forward(null);
    while (iterator.hasNext()) iterator.next().key();
    assertThat(view.memoizedBlockCount()).isEqualTo(7);
    assertThat(view.memoizedPageCount()).isEqualTo(5);
  }

  @ParameterizedTest
  @EnumSource(
      value = AdaptiveCheckpointIndexImpl.KeyMemo.class,
      names = {"PACKED_PAGED", "DECODED_PAGED"})
  void concurrentMissesPublishOneCompletePageAndBlock(AdaptiveCheckpointIndexImpl.KeyMemo memo)
      throws Exception {
    var data = fixture(1025, "HIERARCHICAL", "NONE");
    var view =
        new AdaptiveCheckpointIndexImpl<>(data.serialized(), OBJ_REF_SERIALIZER, 64, true, memo);
    view.prepare();
    assertThat(view.memoizedPageCount()).isZero();
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<?>>();
      for (int thread = 0; thread < 8; thread++)
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < 100; i++) {
                    int ordinal = 193 + i % 60;
                    assertThat(view.get(data.keys().get(ordinal))).isEqualTo(expected(ordinal));
                  }
                  return null;
                }));
      start.countDown();
      for (var future : futures) future.get(30, TimeUnit.SECONDS);
    }
    assertThat(view.memoizedPageCount()).isEqualTo(1);
    assertThat(view.memoizedBlockCount()).isEqualTo(1);
  }

  private static @Nullable ObjRef expected(int ordinal) {
    return ordinal % 17 == 0 ? null : objRef("value", ordinal + 100L, 1 + ordinal % 3);
  }

  private static final class CountingSerializer implements IndexValueSerializer<ObjRef> {
    final AtomicInteger decodes = new AtomicInteger();
    final AtomicInteger skips = new AtomicInteger();

    @Override
    public int serializedSize(@Nullable ObjRef value) {
      return OBJ_REF_SERIALIZER.serializedSize(value);
    }

    @Override
    public @NonNull ByteBuffer serialize(@Nullable ObjRef value, @NonNull ByteBuffer target) {
      return OBJ_REF_SERIALIZER.serialize(value, target);
    }

    @Override
    public @Nullable ObjRef deserialize(@NonNull ByteBuffer bytes) {
      decodes.incrementAndGet();
      return OBJ_REF_SERIALIZER.deserialize(bytes);
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer bytes) {
      return OBJ_REF_SERIALIZER.isNullSerialized(bytes);
    }

    @Override
    public void skip(@NonNull ByteBuffer bytes) {
      skips.incrementAndGet();
      OBJ_REF_SERIALIZER.skip(bytes);
    }
  }
}

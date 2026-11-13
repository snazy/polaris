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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestCompactIndexLayouts {
  static Stream<String> modes() {
    return Stream.of("COMPACT");
  }

  static Stream<Arguments> cases() {
    return modes()
        .flatMap(mode ->
            Stream.of("HIERARCHICAL", "WORDS")
                .flatMap(shape ->
                    Stream.of("NONE", "PERIODIC", "BINARY", "MIXED")
                        .map(encoding -> arguments(mode, shape, encoding))));
  }

  private static PrototypeIndexData fixture(String shape, String encoding) {
    var data = PrototypeIndexData.create(257, shape);
    ByteBuffer bytes;
    if (encoding.equals("MIXED")) {
      var anchors = new BitSet();
      for (int i : new int[] {0, 1, 32, 33, 100, 256}) anchors.set(i);
      bytes = FullKeyCheckpoints.rewrite(data.serialized(), OBJ_REF_SERIALIZER, anchors);
    } else
      bytes =
          FullKeyCheckpoints.rewrite(
              data.serialized(),
              OBJ_REF_SERIALIZER,
              FullKeyCheckpoints.Policy.valueOf(encoding),
              64);
    return new PrototypeIndexData(bytes, data.keys());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void preparesOnlyPrimitiveMetadataAndMemoizesOnlySearchProbes(
      String mode, String shape, String encoding) {
    var data = fixture(shape, encoding);
    var codec = new CountingSerializer();
    var view = (CompactIndexImpl<ObjRef>) data.fresh(mode, data.serialized(), codec, 64);
    assertThat(view.layout.entries()).hasSize(257 * 4);
    assertThat(view.memoizedKeyCount()).isZero();
    assertThat(codec.decodes).isZero();
    var iterator = view.forward(null);
    while (iterator.hasNext()) iterator.next().key();
    assertThat(view.memoizedKeyCount()).isZero();
    view.getElement(data.keys().get(255));
    assertThat(view.memoizedKeyCount()).isBetween(1, 10);
    assertThat(codec.decodes).isZero();
    var baseline = data.fresh("BASELINE", 64);
    for (var key : data.keys()) assertThat(view.get(key)).isEqualTo(baseline.get(key));
    assertThat(view.serialize()).isEqualTo(data.serialized());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void sharedLayoutsKeepDecodedKeysPrivateToEachView(String mode, String shape, String encoding) {
    var data = fixture(shape, encoding);
    var prepared = data.prepare(mode, 64);
    var first = (CompactIndexImpl<?>) data.fromPrepared(mode, prepared, 64);
    var second = (CompactIndexImpl<?>) data.fromPrepared(mode, prepared, 64);
    assertThat(first.memoizedKeyCount()).isZero();
    assertThat(second.memoizedKeyCount()).isZero();
    assertThat(first.get(data.keys().get(128))).isEqualTo(second.get(data.keys().get(128)));
    first.get(data.keys().get(255));
    assertThat(first.memoizedKeyCount()).isGreaterThan(second.memoizedKeyCount());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void preservesMaximumEscapedKeysAndLargePrefixLengths(String mode) {
    var keys = new TreeSet<IndexKey>();
    keys.add(IndexKey.key(""));
    keys.add(IndexKey.key("漢字"));
    for (byte fill : new byte[] {1, 2}) {
      for (byte tail : new byte[] {1, 2, 3, 127, -1}) {
        var raw = new byte[IndexKey.MAX_LENGTH];
        Arrays.fill(raw, fill);
        raw[raw.length - 1] = tail;
        keys.add(IndexKey.key(ByteBuffer.wrap(raw)));
      }
    }
    assertThat(keys.stream().mapToInt(IndexKey::serializedSize).max().orElseThrow())
        .isEqualTo(IndexKey.MAX_LENGTH * 2 + 1);
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    int ordinal = 0;
    for (var key : keys) source.put(key, objRef("value", ++ordinal, 1));
    var data = new PrototypeIndexData(source.serialize(), new ArrayList<>(keys));
    var view = data.fresh(mode, 64);
    for (var key : keys) assertThat(view.get(key)).isEqualTo(source.get(key));
    assertThat(view.asKeyList()).containsExactlyElementsOf(keys);
    var actual = new ArrayList<IndexKey>();
    view.reverseIterator().forEachRemaining(entry -> actual.add(entry.key()));
    assertThat(actual).containsExactlyElementsOf(keys.descendingSet());
  }

  @ParameterizedTest
  @MethodSource("modes")
  void concurrentQueriesPublishCompleteKeys(String mode) throws Exception {
    var data = PrototypeIndexData.create(1025, "HIERARCHICAL");
    var view = data.fresh(mode, 64);
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<?>>();
      for (int thread = 0; thread < 8; thread++) {
        int offset = thread * 137;
        futures.add(
            executor.submit(() -> {
              start.await();
              for (int i = 0; i < 100; i++) {
                int at = (offset + i * 71) % 1025;
                assertThat(view.get(data.keys().get(at)))
                    .isEqualTo(at % 17 == 0 ? null : objRef("value", at + 100L, 1 + at % 3));
              }
              return null;
            }));
      }
      start.countDown();
      for (var future : futures) future.get(30, TimeUnit.SECONDS);
    }
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
    public @Nullable ObjRef deserialize(@NonNull ByteBuffer bytes) {
      decodes++;
      return OBJ_REF_SERIALIZER.deserialize(bytes);
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer bytes) {
      return OBJ_REF_SERIALIZER.isNullSerialized(bytes);
    }

    @Override
    public void skip(@NonNull ByteBuffer bytes) {
      OBJ_REF_SERIALIZER.skip(bytes);
    }
  }
}

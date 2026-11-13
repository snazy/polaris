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
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
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
    return Stream.of("COMPACT", "COMPACT3");
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
    var view = (IndexReadPrototype<ObjRef>) data.fresh(mode, data.serialized(), codec, 64);
    assertThat(view.metadataBytes())
        .isEqualTo(48L + 257 * (mode.equals("COMPACT3") ? 3 : 4) * Integer.BYTES);
    assertThat(memoizedKeyCount(view)).isZero();
    assertThat(codec.decodes).isZero();
    var iterator = view.forward(null);
    while (iterator.hasNext()) iterator.next().key();
    assertThat(memoizedKeyCount(view)).isZero();
    view.getElement(data.keys().get(255));
    assertThat(memoizedKeyCount(view)).isBetween(1, 10);
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
    var first = (IndexReadPrototype<?>) data.fromPrepared(mode, prepared, 64);
    var second = (IndexReadPrototype<?>) data.fromPrepared(mode, prepared, 64);
    assertThat(memoizedKeyCount(first)).isZero();
    assertThat(memoizedKeyCount(second)).isZero();
    assertThat(first.get(data.keys().get(128))).isEqualTo(second.get(data.keys().get(128)));
    first.get(data.keys().get(255));
    assertThat(memoizedKeyCount(first)).isGreaterThan(memoizedKeyCount(second));
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

  private static int memoizedKeyCount(IndexReadPrototype<?> view) {
    return switch (view) {
      case CompactIndexImpl<?> compact -> compact.memoizedKeyCount();
      case Compact3IndexImpl<?> compact -> compact.memoizedKeyCount();
      default -> throw new IllegalArgumentException();
    };
  }

  @ParameterizedTest
  @MethodSource("modes")
  void preservesValueBoundariesWithNoncanonicalPrefixVarints(String mode) {
    var data = PrototypeIndexData.create(129, "BINARY");
    var bytes = data.serialized().duplicate();
    var rewritten = ByteBuffer.allocate(bytes.remaining() + data.keys().size() * 9);
    rewritten.put(bytes.get());
    int count = readVarInt(bytes);
    putVarInt(rewritten, count);
    for (int ordinal = 0; ordinal < count; ordinal++) {
      if (ordinal != 0) {
        int delta = readVarInt(bytes);
        int width = 3 + ordinal % 7;
        for (int i = 1; i < width; i++) {
          rewritten.put((byte) ((delta & 0x7f) | 0x80));
          delta >>>= 7;
        }
        rewritten.put((byte) delta);
      }
      int start = bytes.position();
      IndexKey.skip(bytes);
      OBJ_REF_SERIALIZER.skip(bytes);
      rewritten.put(bytes.duplicate().position(start).limit(bytes.position()));
    }
    rewritten.flip();
    var view = data.fresh(mode, rewritten, OBJ_REF_SERIALIZER, 64);
    var baseline = data.fresh("BASELINE", rewritten, OBJ_REF_SERIALIZER, 64);
    for (var key : data.keys()) {
      var expected = requireNonNull(baseline.getElement(key));
      var actual = requireNonNull(view.getElement(key));
      assertThat(view.get(key)).isEqualTo(expected.valueNullable());
      assertThat(actual.contentSerializedSize(OBJ_REF_SERIALIZER))
          .isEqualTo(expected.contentSerializedSize(OBJ_REF_SERIALIZER));
      var expectedContent = ByteBuffer.allocate(expected.contentSerializedSize(OBJ_REF_SERIALIZER));
      var actualContent = ByteBuffer.allocate(actual.contentSerializedSize(OBJ_REF_SERIALIZER));
      expected.serializeContent(OBJ_REF_SERIALIZER, expectedContent);
      actual.serializeContent(OBJ_REF_SERIALIZER, actualContent);
      assertThat(actualContent.flip()).isEqualTo(expectedContent.flip());
    }
    assertThat(view.serialize()).isEqualTo(rewritten);
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

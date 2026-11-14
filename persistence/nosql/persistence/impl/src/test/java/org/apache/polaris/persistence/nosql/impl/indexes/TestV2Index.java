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
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

class TestV2Index {
  static Stream<Arguments> cases() {
    return Stream.of("HIERARCHICAL", "WORDS", "UUID", "BINARY")
        .flatMap(shape ->
            Stream.of(0, 1, 33, 257)
                .flatMap(size ->
                    Stream.of(1, 16, 64)
                        .flatMap(target ->
                            options(target).map(option -> arguments(shape, size, option)))));
  }

  static Stream<V2IndexWriter.Options> options(int target) {
    return Stream.of(false, true)
        .flatMap(uneven ->
            Stream.of(V2IndexWriter.DirectoryEncoding.values())
                .map(encoding -> new V2IndexWriter.Options(target, uneven, encoding, 8)));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void matchesV1ReadsAndPromotion(String shape, int size, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(size, shape);
    var baseline = data.fresh("BASELINE", 64);
    var bytes = V2IndexWriter.write(baseline, OBJ_REF_SERIALIZER, options);
    var view = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    assertThat(view.materializedBlocks()).isZero();
    assertThat(view.hasElements()).isEqualTo(baseline.hasElements());
    assertThat(collect(view, null, null, false)).isEqualTo(collect(baseline, null, null, false));
    assertThat(view.materializedBlocks()).isZero();
    assertThat(view.first()).isEqualTo(baseline.first());
    assertThat(view.last()).isEqualTo(baseline.last());
    assertThat(collect(view, null, null, true)).isEqualTo(collect(baseline, null, null, true));
    for (var key : data.keys()) {
      assertThat(view.get(key)).isEqualTo(baseline.get(key));
      assertThat(view.contains(key)).isEqualTo(baseline.contains(key));
      assertThat(view.containsElement(key)).isTrue();
    }
    assertThat(view.get(IndexKey.key("not-a-present-key"))).isNull();
    assertThat(view.containsElement(IndexKey.key("not-a-present-key"))).isFalse();
    if (size > 1) {
      var lower = data.keys().get(size / 4);
      var upper = data.keys().get(size * 3 / 4);
      assertThat(collect(view, lower, upper, false))
          .isEqualTo(collect(baseline, lower, upper, false));
      assertThat(collect(view, lower, upper, true))
          .isEqualTo(collect(baseline, lower, upper, true));
      assertThat(collect(view, lower, lower, false))
          .isEqualTo(collect(baseline, lower, lower, false));
    }
    var prefix = IndexKey.key("catalog/namespace-001/");
    assertThat(collect(view, prefix, prefix, false))
        .isEqualTo(collect(baseline, prefix, prefix, false));
    assertThat(view.serialize()).isEqualTo(bytes);
    var ref = ObjRef.objRef("index", 1234);
    view.setObjId(ref);
    var mutable = view.asMutableIndex();
    assertThat(mutable.getObjId()).isEqualTo(ref);
    assertThat(mutable.asKeyList()).containsExactlyElementsOf(data.keys());
    assertThat(mutable.serialize().get(0)).isEqualTo((byte) 2);
    var extra = IndexKey.key("added-after-read");
    mutable.add(indexElement(extra, ObjRef.objRef("new", 9999)));
    assertThat(V2IndexReader.read(mutable.serialize(), OBJ_REF_SERIALIZER).get(extra))
        .isEqualTo(ObjRef.objRef("new", 9999));
    assertThat(V2IndexReader.read(data.serialized(), OBJ_REF_SERIALIZER).asKeyList())
        .containsExactlyElementsOf(data.keys());
    assertThat(V2IndexReader.read(bytes, OBJ_REF_SERIALIZER).asKeyList())
        .containsExactlyElementsOf(data.keys());
  }

  @Test
  void valuesRemainLazyMemoizedAndPrivateToEachView() {
    var data = PrototypeIndexData.create(129, "WORDS");
    var bytes =
        V2IndexWriter.write(
            data.fresh("BASELINE", 64), OBJ_REF_SERIALIZER, V2IndexWriter.Options.defaults(16));
    var codec = new CountingSerializer();
    var view = new V2IndexReader<>(bytes, codec);
    var key = data.keys().get(64);
    assertThat(view.contains(key)).isTrue();
    assertThat(codec.decodes).isZero();
    var value = view.get(key);
    assertThat(codec.decodes).isEqualTo(1);
    assertThat(view.get(key)).isSameAs(value);
    assertThat(requireNonNull(view.getElement(key)).value()).isSameAs(value);
    assertThat(view.iterator(key, null, false).next().value()).isSameAs(value);
    assertThat(view.reverseIterator(null, key, false).next().value()).isSameAs(value);
    assertThat(codec.decodes).isEqualTo(1);
    var tombstone = data.keys().getFirst();
    assertThat(view.contains(tombstone)).isFalse();
    assertThat(codec.decodes).isEqualTo(1);
    assertThat(view.get(tombstone)).isNull();
    assertThat(view.get(tombstone)).isNull();
    assertThat(codec.decodes).isEqualTo(2);
    assertThat(new V2IndexReader<>(bytes, codec).get(key)).isEqualTo(value).isNotSameAs(value);
    assertThat(codec.decodes).isEqualTo(3);
  }

  @Test
  void indexKeyCodecAndBufferViewsPreserveCallerState() {
    var data = PrototypeIndexData.create(129, "BINARY");
    var original = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    for (int i = 0; i < data.keys().size(); i++)
      original.add(
          indexElement(
              data.keys().get(i), i % 7 == 0 ? null : data.keys().get(data.keys().size() - i - 1)));
    options(16)
        .forEach(option -> {
          var bytes = V2IndexWriter.write(original, INDEX_KEY_SERIALIZER, option);
          for (var kind : List.of("HEAP", "SLICE", "READ_ONLY", "DIRECT")) {
            var input =
                kind.equals("DIRECT")
                    ? ByteBuffer.allocateDirect(bytes.remaining() + 30)
                    : ByteBuffer.allocate(bytes.remaining() + 30);
            input.position(5);
            if (kind.equals("SLICE")) input = input.slice().position(7);
            int start = input.position();
            input.put(bytes.duplicate()).flip().position(start);
            if (kind.equals("READ_ONLY")) input = input.asReadOnlyBuffer();
            int end = input.limit();
            var view = V2IndexReader.read(input, INDEX_KEY_SERIALIZER);
            for (var key : data.keys()) assertThat(view.get(key)).isEqualTo(original.get(key));
            assertThat(view.serialize()).isEqualTo(bytes);
            assertThat(input.position()).isEqualTo(start);
            assertThat(input.limit()).isEqualTo(end);
          }
        });
  }

  @Test
  void supportsEscapedUnicodeEmptyAndMaximumLengthKeys() {
    var raw = new byte[IndexKey.MAX_LENGTH];
    Arrays.fill(raw, (byte) 1);
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (var key :
        List.of(
            IndexKey.key(""),
            IndexKey.key("a\u0000"),
            IndexKey.key("a\u0001"),
            IndexKey.key("漢字"),
            IndexKey.key(ByteBuffer.wrap(raw))))
      source.add(indexElement(key, ObjRef.objRef("value", 42)));
    options(2)
        .forEach(option -> {
          var view =
              new V2IndexReader<>(
                  V2IndexWriter.write(source, OBJ_REF_SERIALIZER, option), OBJ_REF_SERIALIZER);
          assertThat(collect(view, null, null, false))
              .isEqualTo(collect(source, null, null, false));
          assertThat(collect(view, null, null, true)).isEqualTo(collect(source, null, null, true));
        });
  }

  @Test
  void zeroByteNonNullValuesAllowEqualBlockOffsets() {
    var value = ObjRef.objRef("zero", 7);
    IndexValueSerializer<ObjRef> codec =
        new IndexValueSerializer<>() {
          @Override
          public int serializedSize(@Nullable ObjRef ignored) {
            return 0;
          }

          @Override
          public @NonNull ByteBuffer serialize(
              @Nullable ObjRef ignored, @NonNull ByteBuffer target) {
            return target;
          }

          @Override
          public ObjRef deserialize(@NonNull ByteBuffer input) {
            return value;
          }

          @Override
          public boolean isNullSerialized(@NonNull ByteBuffer input) {
            return false;
          }

          @Override
          public void skip(@NonNull ByteBuffer input) {}
        };
    var source = new IndexImpl<>(codec);
    for (int i = 0; i < 10; i++) source.add(indexElement(IndexKey.key(i), value));
    var view =
        new V2IndexReader<>(
            V2IndexWriter.write(source, codec, V2IndexWriter.Options.defaults(1)), codec);
    for (int i = 0; i < 10; i++) assertThat(view.get(IndexKey.key(i))).isSameAs(value);
    var mutable = view.asMutableIndex();
    assertThat(mutable.remove(IndexKey.key(5))).isTrue();
    mutable.put(IndexKey.key(11), value);
    var after = new V2IndexReader<>(mutable.serialize(), codec);
    assertThat(after.get(IndexKey.key(11))).isSameAs(value);
    assertThat(after.containsElement(IndexKey.key(5))).isFalse();
  }

  @Test
  void concurrentColdReadersPublishCompleteBlocksAndSharedValues() throws Exception {
    var data = PrototypeIndexData.create(1025, "BINARY");
    var baseline = data.fresh("BASELINE", 64);
    var bytes =
        V2IndexWriter.write(
            baseline,
            OBJ_REF_SERIALIZER,
            new V2IndexWriter.Options(64, true, V2IndexWriter.DirectoryEncoding.PREFIX, 8));
    var view = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(8)) {
      var futures = new ArrayList<Future<ObjRef>>();
      for (int thread = 0; thread < 8; thread++) {
        int seed = thread;
        futures.add(
            executor.submit(() -> {
              start.await();
              var shared = view.get(data.keys().get(700));
              for (int i = 0; i < 100; i++) {
                var key = data.keys().get((seed * 137 + i * 71) % 1025);
                assertThat(view.get(key)).isEqualTo(baseline.get(key));
              }
              return shared;
            }));
      }
      start.countDown();
      for (var future : futures)
        assertThat(future.get(30, TimeUnit.SECONDS)).isSameAs(view.get(data.keys().get(700)));
    }
  }

  @Test
  void rejectsMalformedDirectoryAndTruncatedPayloads() {
    var data = PrototypeIndexData.create(129, "WORDS");
    var valid =
        V2IndexWriter.write(
            data.fresh("BASELINE", 64), OBJ_REF_SERIALIZER, V2IndexWriter.Options.defaults(16));
    var header = valid.duplicate();
    header.get();
    readVarInt(header);
    int pointer = header.position();
    int directory = valid.getInt(pointer);
    for (int mutation = 0; mutation < 7; mutation++) {
      var bytes = ByteBuffer.allocate(valid.remaining()).put(valid.duplicate()).flip();
      switch (mutation) {
        case 0 -> bytes.putInt(pointer, -1);
        case 1 -> bytes.putInt(directory, Integer.MAX_VALUE);
        case 2 -> bytes.put(directory + 4, (byte) 42);
        case 3 -> bytes.put(directory + 5, (byte) 0);
        case 4 -> bytes.putInt(directory + 6, -1);
        case 5 -> bytes.putInt(directory + 10, 1);
        case 6 -> bytes.putInt(directory + 14, directory);
        default -> throw new IllegalArgumentException();
      }
      assertThatThrownBy(() -> new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER))
          .isInstanceOf(IllegalArgumentException.class);
    }
    var truncated = valid.duplicate().limit(valid.limit() - 1);
    assertThatThrownBy(
            () -> {
              var view = new V2IndexReader<>(truncated, OBJ_REF_SERIALIZER);
              view.asKeyList().forEach(key -> {});
            })
        .isInstanceOfAny(IllegalArgumentException.class, BufferUnderflowException.class);
    var compressed =
        V2IndexWriter.write(
            data.fresh("BASELINE", 64),
            OBJ_REF_SERIALIZER,
            new V2IndexWriter.Options(16, false, V2IndexWriter.DirectoryEncoding.PREFIX, 8));
    var broken = ByteBuffer.allocate(compressed.remaining()).put(compressed.duplicate()).flip();
    int dir = broken.getInt(pointer);
    broken.put(broken.getInt(dir + 14), (byte) 1);
    assertThatThrownBy(() -> new V2IndexReader<>(broken, OBJ_REF_SERIALIZER).first())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void defaultWriterUsesUncompressedDirectoryAndUnevenBoundariesAreExplicit() {
    var data = PrototypeIndexData.create(257, "WORDS");
    var baseline = data.fresh("BASELINE", 64);
    var bytes =
        V2IndexWriter.write(baseline, OBJ_REF_SERIALIZER, V2IndexWriter.Options.defaults(16));
    var view = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    assertThat(bytes.get(view.directoryOffset + 4)).isZero();
    var uneven =
        new V2IndexReader<>(
            V2IndexWriter.write(
                baseline,
                OBJ_REF_SERIALIZER,
                new V2IndexWriter.Options(16, true, V2IndexWriter.DirectoryEncoding.PREFIX, 3)),
            OBJ_REF_SERIALIZER);
    assertThat(uneven.blockCount).isGreaterThan(view.blockCount);
    for (var key : data.keys()) assertThat(uneven.get(key)).isEqualTo(baseline.get(key));
  }

  private record Observed(IndexKey key, ObjRef value) {}

  private static List<Observed> collect(
      Index<ObjRef> index, IndexKey lower, IndexKey upper, boolean reverse) {
    var result = new ArrayList<Observed>();
    var iterator =
        reverse ? index.reverseIterator(lower, upper, false) : index.iterator(lower, upper, false);
    while (iterator.hasNext()) {
      var entry = iterator.next();
      result.add(new Observed(entry.key(), entry.value()));
    }
    return result;
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
    public ObjRef deserialize(@NonNull ByteBuffer input) {
      decodes++;
      return OBJ_REF_SERIALIZER.deserialize(input);
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer input) {
      return OBJ_REF_SERIALIZER.isNullSerialized(input);
    }

    @Override
    public void skip(@NonNull ByteBuffer input) {
      OBJ_REF_SERIALIZER.skip(input);
    }
  }
}

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
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.index.IndexStripe.indexStripe;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
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

class TestReadOnlyV2IndexImpl {
  // Independent wire fixtures: body begins at 6; directory descriptors point to that body,
  // ordinal zero and the first directory key. Keys a/ab map to x/y.
  private static final String FRONT =
      "020200000011" + "6003016201020478017901" + "0000000100010000000600000000000000236101";
  private static final String PREFIX =
      "020200000012" + "610001026201020478017901" + "0000000100010000000600000000000000246101";
  private static final String HYBRID =
      "020200000013" + "e0000103006201020478017901" + "0000000100010000000600000000000000256101";
  private static final String STRIDE =
      "02020000000f" + "300301620178017901" + "0000000100010000000600000000000000216101";
  private static final String DIRECTORY_PREFIX =
      "020200000011" + "6003016201020478017901" + "00000001010200000006000000000000002300026101";

  @ParameterizedTest
  @ValueSource(strings = {FRONT, PREFIX, HYBRID, STRIDE, DIRECTORY_PREFIX})
  void independentGoldenBytes(String hex) {
    var bytes = ByteBuffer.wrap(HexFormat.of().parseHex(hex));
    var read = deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false, 1);
    assertThat(read).isExactlyInstanceOf(ReadOnlyV2IndexImpl.class);
    assertThat(read.asKeyList()).containsExactly(key("a"), key("ab"));
    assertThat(read.get(key("a"))).isEqualTo(key("x"));
    assertThat(read.get(key("ab"))).isEqualTo(key("y"));
    assertThat(read.get(key("aa"))).isNull();
    assertThat(read.get(key("0"))).isNull();
    assertThat(read.get(key("z"))).isNull();
    assertThat(read.serialize()).isEqualTo(bytes);
    assertThat(read.estimatedSerializedSize()).isEqualTo(bytes.remaining());
    assertThatThrownBy(() -> read.serialize().put((byte) 1))
        .isInstanceOf(ReadOnlyBufferException.class);
    assertThat(deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, true, 1))
        .isInstanceOf(MutableV2IndexImpl.class);
    for (int preference : new int[] {1, 2}) {
      var mutable = deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, true, preference);
      assertThat(mutable.serialize()).isEqualTo(bytes);
      mutable.put(key("ab"), key("updated"));
      mutable.remove(key("a"));
      mutable.put(key("ac"), key("added"));
      var output = mutable.serialize();
      assertThat(output.remaining()).isLessThanOrEqualTo(mutable.estimatedSerializedSize());
      var reload = deserializeStoreIndex(output, INDEX_KEY_SERIALIZER, false, preference);
      assertThat(reload.asKeyList()).containsExactly(key("ab"), key("ac"));
      assertThat(reload.get(key("ab"))).isEqualTo(key("updated"));
      assertThat(reload.get(key("ac"))).isEqualTo(key("added"));
      assertThat(read.get(key("a"))).isEqualTo(key("x"));
      assertThat(read.get(key("ab"))).isEqualTo(key("y"));
      assertThatThrownBy(
              () -> IndexLegacyImpl.deserializeStoreIndex(output.duplicate(), INDEX_KEY_SERIALIZER))
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> read.put(key("b"), key("z")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> read.remove(key("a")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> read.splitByTargetSize(1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void emptyAndVersionDispatch() {
    var empty = ByteBuffer.wrap(HexFormat.of().parseHex("020000000006000000000001"));
    var read = deserializeStoreIndex(empty, INDEX_KEY_SERIALIZER, false, 1);
    assertThat(read.hasElements()).isFalse();
    assertThat(read.first()).isNull();
    assertThat(read.last()).isNull();
    assertThat(read.get(key("absent"))).isNull();
    assertThat(read.elementIterator()).isExhausted();
    assertThat(read.reverseElementIterator()).isExhausted();
    assertThat(read.serialize()).isEqualTo(empty);
    var v1 = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    v1.put(key("a"), key("b"));
    assertThat(deserializeStoreIndex(v1.serialize(), INDEX_KEY_SERIALIZER, false, 1))
        .isInstanceOf(ReadOnlyIndexImpl.class);
    assertThat(deserializeStoreIndex(v1.serialize(), INDEX_KEY_SERIALIZER, true, 1))
        .isInstanceOf(IndexImpl.class);
    assertThatThrownBy(() ->
            deserializeStoreIndex(ByteBuffer.wrap(new byte[] {3}), INDEX_KEY_SERIALIZER, false, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("version");
  }

  static Stream<Arguments> policies() {
    return Stream.of(
            SplitV2Format.Layout.PREFIX_ENDS,
            SplitV2Format.Layout.FRONT_ENDS,
            SplitV2Format.Layout.FRONT_PREFIX_ENDS)
        .flatMap(layout ->
            Stream.of(SplitV2Format.DirectoryEncoding.values())
                .flatMap(directory ->
                    Stream.of("WORDS", "BINARY", "UUID", "HIERARCHICAL")
                        .map(shape ->
                            Arguments.of(
                                shape, new SplitV2Format.Options(layout, 16, directory)))));
  }

  @ParameterizedTest
  @MethodSource("policies")
  void matchesModelAcrossBuffersAndBounds(String shape, SplitV2Format.Options options) {
    for (int size : new int[] {0, 1, 17, 130}) {
      var data = V2TestData.create(size, shape);
      var model = data.mutable();
      var bytes = SplitV2IndexWriter.write(model, OBJ_REF_SERIALIZER, options);
      for (int bufferKind = 0; bufferKind < 4; bufferKind++) {
        var input =
            bufferKind == 1
                ? ByteBuffer.allocateDirect(bytes.remaining() + 11)
                : ByteBuffer.allocate(bytes.remaining() + 11);
        input.position(7).put(bytes.duplicate()).flip().position(7);
        if (bufferKind == 2) input = input.slice().asReadOnlyBuffer();
        if (bufferKind == 3) input.order(ByteOrder.LITTLE_ENDIAN);
        input.mark();
        var start = input.position();
        var end = input.limit();
        var read = deserializeStoreIndex(input, OBJ_REF_SERIALIZER, false, 1);
        assertThat(input.position()).isEqualTo(start);
        assertThat(input.limit()).isEqualTo(end);
        input.reset();
        assertThat(read.asKeyList()).containsExactlyElementsOf(data.keys());
        var reverse = new ArrayList<>(data.keys());
        Collections.reverse(reverse);
        assertThat(read.reverseElementIterator())
            .toIterable()
            .extracting(InternalIndexElement::key)
            .containsExactlyElementsOf(reverse);
        assertThat(read.elementIterator())
            .toIterable()
            .extracting(InternalIndexElement::key)
            .containsExactlyElementsOf(data.keys());
        for (var k : data.keys()) {
          assertThat(read.containsElement(k)).isTrue();
          assertThat(read.get(k)).isEqualTo(model.get(k));
          assertThat(read.contains(k)).isEqualTo(model.contains(k));
        }
        if (size > 16) {
          var lower = data.keys().get(3);
          var upper = data.keys().get(size - 3);
          var expected = data.keys().subList(3, size - 2);
          assertThat(read.elementIterator(lower, upper, false))
              .toIterable()
              .extracting(InternalIndexElement::key)
              .containsExactlyElementsOf(expected);
          var backwards = new ArrayList<>(expected);
          Collections.reverse(backwards);
          assertThat(read.reverseElementIterator(lower, upper, false))
              .toIterable()
              .extracting(InternalIndexElement::key)
              .containsExactlyElementsOf(backwards);
          assertThat(read.elementIterator(lower, lower, false))
              .toIterable()
              .extracting(InternalIndexElement::key)
              .containsExactlyElementsOf(
                  data.keys().stream().filter(k -> k.startsWith(lower)).toList());
        }
        assertThat(read.isMutable()).isFalse();
        assertThat(read.isModified()).isFalse();
        assertThat(read.isLoaded()).isTrue();
        var id = ObjRef.objRef("stripe", 123, 1);
        assertThat(read.setObjId(id)).isSameAs(read);
        assertThat(read.getObjId()).isSameAs(id);
        assertThat(read.serialize()).isEqualTo(bytes);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  void nullChecksStayLazyAndValuesAreMemoized(String shape, SplitV2Format.Options options) {
    var data = V2TestData.create(130, shape);
    var bytes = SplitV2IndexWriter.write(data.mutable(), OBJ_REF_SERIALIZER, options);
    var codec = spy(OBJ_REF_SERIALIZER);
    var read = deserializeStoreIndex(bytes, codec, false, 1);
    var missingValue = data.keys().getFirst();
    var present = data.keys().get(1);
    assertThat(read.containsElement(missingValue)).isTrue();
    assertThat(read.contains(missingValue)).isFalse();
    assertThat(read.contains(present)).isTrue();
    verify(codec, never()).deserialize(any());
    verify(codec, never()).skip(any());
    var element = requireNonNull(read.getElement(present));
    var decoded = read.get(present);
    assertThat(element.valueNullable()).isSameAs(decoded);
    assertThat(read.get(present)).isSameAs(decoded);
    assertThat(read.get(missingValue)).isNull();
    assertThat(read.get(missingValue)).isNull();
    verify(codec, org.mockito.Mockito.times(2)).deserialize(any());
  }

  @ParameterizedTest
  @MethodSource("policies")
  void concurrentReadersPublishSameAndDifferentPages(String shape, SplitV2Format.Options options)
      throws Exception {
    var data = V2TestData.create(193, shape);
    var expected = data.mutable();
    var encoded = SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    for (int mode = 0; mode < 5; mode++) {
      var storage =
          mode == 1
              ? ByteBuffer.allocateDirect(encoded.remaining() + 11)
              : ByteBuffer.allocate(encoded.remaining() + 11);
      storage.position(7).put(encoded.duplicate()).flip().position(7);
      var input =
          mode == 2
              ? storage.asReadOnlyBuffer()
              : mode == 3
                  ? storage.slice()
                  : mode == 4 ? storage.order(ByteOrder.LITTLE_ENDIAN) : storage;
      int position = input.position(), limit = input.limit();
      var read = deserializeStoreIndex(input, OBJ_REF_SERIALIZER, false, 1);
      // Materialize the independent V1 oracle before concurrent access; mutable V1 is exclusive.
      var values = new ArrayList<ObjRef>();
      for (var k : data.keys()) values.add(expected.get(k));
      var barrier = new CyclicBarrier(4);
      try (var executor = Executors.newFixedThreadPool(4)) {
        var tasks = new ArrayList<Callable<ObjRef>>();
        for (int thread = 0; thread < 4; thread++) {
          final int threadSeed = thread;
          tasks.add(() -> {
            var random = new Random(991 + threadSeed);
            barrier.await();
            for (int pass = 0; pass < 3; pass++) {
              for (int ordinal : new int[] {0, 63, 64, 65, 127, 128, 192}) {
                var k = data.keys().get(ordinal);
                assertThat(read.get(k)).isEqualTo(values.get(ordinal));
                assertThat(read.asKeyList().get(ordinal)).isEqualTo(k);
                assertThat(read.contains(k)).isEqualTo(values.get(ordinal) != null);
              }
              for (int access = 0; access < 25; access++) {
                int ordinal = random.nextInt(data.keys().size());
                var k = data.keys().get(ordinal);
                var value = read.get(k);
                if (value != null)
                  assertThat(value.toByteBuffer()).isEqualTo(values.get(ordinal).toByteBuffer());
                else assertThat(values.get(ordinal)).isNull();
                assertThat(read.get(k)).isSameAs(value);
              }
              assertThat(read.elementIterator())
                  .toIterable()
                  .extracting(InternalIndexElement::key)
                  .containsExactlyElementsOf(data.keys());
              var reverse = new ArrayList<>(data.keys());
              Collections.reverse(reverse);
              assertThat(read.reverseElementIterator())
                  .toIterable()
                  .extracting(InternalIndexElement::key)
                  .containsExactlyElementsOf(reverse);
            }
            return read.get(data.keys().get(65));
          });
        }
        var futures = executor.invokeAll(tasks);
        var winner = futures.getFirst().get();
        for (var future : futures) assertThat(future.get()).isSameAs(winner);
      }
      assertThat(input.position()).isEqualTo(position);
      assertThat(input.limit()).isEqualTo(limit);
      assertThat(read.serialize()).isEqualTo(encoded);
    }
  }

  @Test
  void embeddedFactoryReadsV2() {
    var bytes = ByteBuffer.wrap(HexFormat.of().parseHex(FRONT));
    var container = ImmutableIndexContainer.<IndexKey>builder().embedded(bytes).build();
    var read =
        IndexesProvider.buildReadIndex(container, mock(Persistence.class), INDEX_KEY_SERIALIZER);
    assertThat(read.get(key("ab"))).isEqualTo(key("y"));
  }

  @Test
  void singleLazyStripeLoadsOnlyAfterEmbeddedMiss() {
    var bytes = ByteBuffer.wrap(HexFormat.of().parseHex(FRONT));
    var id = ObjRef.objRef("index-stripe", 99, 1);
    var persistence = mock(Persistence.class);
    var stripe = IndexStripeObj.indexStripeObj(99, bytes);
    when(persistence.fetch(id, IndexStripeObj.class)).thenReturn(stripe);
    var embedded = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    embedded.put(key("0"), key("local"));
    var container =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(embedded.serialize())
            .addStripes(indexStripe(key("a"), key("ab"), id))
            .build();
    var read = IndexesProvider.buildReadIndex(container, persistence, INDEX_KEY_SERIALIZER);
    assertThat(read.get(key("0"))).isEqualTo(key("local"));
    verify(persistence, never()).fetch(any(), any());
    assertThat(read.get(key("ab"))).isEqualTo(key("y"));
    verify(persistence).fetch(id, IndexStripeObj.class);
  }

  @Test
  void bulkLazyLoaderDispatchesMixedV1V2Stripes() {
    var bytes = ByteBuffer.wrap(HexFormat.of().parseHex(FRONT));
    var first = ObjRef.objRef("index-stripe", 99, 1);
    var second = ObjRef.objRef("index-stripe", 100, 1);
    var old = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    old.put(key("z"), key("v1"));
    var persistence = mock(Persistence.class);
    when(persistence.fetchMany(eq(IndexStripeObj.class), any(ObjRef[].class)))
        .thenReturn(
            new IndexStripeObj[] {
              IndexStripeObj.indexStripeObj(99, bytes),
              IndexStripeObj.indexStripeObj(100, old.serialize())
            });
    var container =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(new IndexImpl<>(INDEX_KEY_SERIALIZER).serialize())
            .addStripes(
                indexStripe(key("a"), key("ab"), first), indexStripe(key("z"), key("z"), second))
            .build();
    var read = IndexesProvider.buildReadIndex(container, persistence, INDEX_KEY_SERIALIZER);
    verifyNoInteractions(persistence);
    read.prefetchIfNecessary(List.of(key("a"), key("z")));
    assertThat(read.get(key("ab"))).isEqualTo(key("y"));
    assertThat(read.get(key("z"))).isEqualTo(key("v1"));
    verify(persistence, never()).fetch(any(), any());
  }

  static Stream<SplitV2Format.Options> widePolicies() {
    return policies().map(args -> (SplitV2Format.Options) args.get()[1]).distinct();
  }

  @ParameterizedTest
  @MethodSource("widePolicies")
  void supportsWideOffsetsAndZeroByteValues(SplitV2Format.Options options) {
    var codec =
        new IndexValueSerializer<byte[]>() {
          @Override
          public int serializedSize(@Nullable byte[] value) {
            return requireNonNull(value).length;
          }

          @Override
          public @NonNull ByteBuffer serialize(@Nullable byte[] value, @NonNull ByteBuffer target) {
            return target.put(requireNonNull(value));
          }

          @Override
          public byte[] deserialize(@NonNull ByteBuffer input) {
            var value = new byte[input.remaining()];
            input.get(value);
            return value;
          }

          @Override
          public boolean isNullSerialized(@NonNull ByteBuffer input) {
            return false;
          }

          @Override
          public void skip(@NonNull ByteBuffer input) {
            throw new AssertionError("No skip");
          }
        };
    var writer = new IndexImpl<>(codec);
    var random = new Random(19);
    for (int i = 0; i < 256; i++) {
      var bytes = new byte[IndexKey.MAX_LENGTH];
      random.nextBytes(bytes);
      writer.put(key(ByteBuffer.wrap(bytes)), new byte[i == 0 ? 70000 : i]);
    }
    var fixed = new SplitV2Format.Options(options.layout(), 256, options.directory(), 8, false);
    var bytes = SplitV2IndexWriter.write(writer, codec, fixed);
    var read = new ReadOnlyV2IndexImpl<>(bytes, codec);
    assertThat(read.block(0).keyWidth()).isEqualTo(4);
    assertThat(read.block(0).valueWidth()).isEqualTo(4);
    for (var k : writer.asKeyList()) assertThat(read.get(k)).containsExactly(writer.get(k));
    var emptyValues = new IndexImpl<>(codec);
    for (int i = 0; i < 65; i++) emptyValues.put(key(i), new byte[0]);
    var zero =
        deserializeStoreIndex(SplitV2IndexWriter.write(emptyValues, codec, fixed), codec, false, 1);
    for (var k : emptyValues.asKeyList()) assertThat(zero.get(k)).isEmpty();
  }

  @Test
  void retainedKeysAndValuesDoNotBorrowScratch() {
    var model = V2TestData.create(130, "WORDS");
    var policy =
        new SplitV2Format.Options(
            SplitV2Format.Layout.FRONT_PREFIX_ENDS, 64, SplitV2Format.DirectoryEncoding.PREFIX);
    var read =
        deserializeStoreIndex(
            SplitV2IndexWriter.write(model.mutable(), OBJ_REF_SERIALIZER, policy),
            OBJ_REF_SERIALIZER,
            false,
            1);
    var k = model.keys().get(65);
    var retained = requireNonNull(read.getElement(k));
    var value = retained.valueNullable();
    var scan = read.elementIterator();
    var first = scan.next();
    var scratch = IndexKeyBufferPool.acquire();
    try {
      Arrays.fill(
          scratch.array(),
          scratch.arrayOffset(),
          scratch.arrayOffset() + scratch.capacity(),
          (byte) 0x7e);
    } finally {
      IndexKeyBufferPool.release(scratch);
    }
    scan.forEachRemaining(InternalIndexElement::valueNullable);
    assertThat(retained.key()).isEqualTo(k);
    assertThat(retained.valueNullable()).isSameAs(value);
    assertThat(first.key()).isEqualTo(model.keys().getFirst());
    assertThat(read.get(k)).isSameAs(value);
  }

  @Test
  void rejectsEmptyFrontSuffixThroughEveryAccessPath() {
    // The second key record is only prefix=2 with no suffix: it would repeat a.
    var bytes =
        ByteBuffer.wrap(
            HexFormat.of()
                .parseHex(
                    "02020000000f6001020204780179010000000100010000000600000000000000216101"));
    assertThatThrownBy(
            () -> {
              var read = deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false, 1);
              read.elementIterator().forEachRemaining(InternalIndexElement::valueNullable);
            })
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false, 1).asKeyList().get(1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false, 1)
                .reverseElementIterator()
                .next())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedWireIsRejectedWhenAccessed() {
    for (String fixture : List.of(FRONT, PREFIX, HYBRID, STRIDE, DIRECTORY_PREFIX)) {
      var encoded = HexFormat.of().parseHex(fixture);
      for (int end = 0; end < encoded.length; end++) {
        var truncated = ByteBuffer.wrap(encoded, 0, end);
        assertThatThrownBy(
                () -> {
                  var read = deserializeStoreIndex(truncated, INDEX_KEY_SERIALIZER, false, 1);
                  read.elementIterator().forEachRemaining(InternalIndexElement::valueNullable);
                })
            .isInstanceOf(RuntimeException.class);
      }
    }
    for (int flags : new int[] {0x08, 0x18, 0x22, 0x44, 0xe1}) {
      var corrupt = ByteBuffer.wrap(HexFormat.of().parseHex(FRONT));
      corrupt.put(6, (byte) flags);
      assertThatThrownBy(
              () -> deserializeStoreIndex(corrupt, INDEX_KEY_SERIALIZER, false, 1).get(key("ab")))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}

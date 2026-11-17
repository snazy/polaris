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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestReadOnlyIndexImpl {
  enum BufferKind {
    HEAP,
    DIRECT,
    READ_ONLY,
    POSITIONED,
    SLICED,
    POSITIONED_SLICE,
    DIRECT_SLICE,
    READ_ONLY_SLICE
  }

  private static IndexKey entryKey(int i) {
    return key("namespace/shared-prefix/" + String.format("%03d", i));
  }

  private static IndexSpi<IndexKey> writer() {
    var writer = newStoreIndex(INDEX_KEY_SERIALIZER);
    for (var i = 0; i < 130; i++) {
      writer.add(indexElement(entryKey(i), i % 3 == 0 ? null : key("value-" + i)));
    }
    return writer;
  }

  private static byte[] copy(ByteBuffer bytes) {
    var copy = new byte[bytes.remaining()];
    bytes.duplicate().get(copy);
    return copy;
  }

  @ParameterizedTest
  @EnumSource(BufferKind.class)
  void preservesBufferStateAndBorrowedScratchOwnership(BufferKind kind) {
    var writer = writer();
    var expected = copy(writer.serialize());
    var direct = kind == BufferKind.DIRECT || kind == BufferKind.DIRECT_SLICE;
    var plain =
        kind == BufferKind.HEAP || kind == BufferKind.DIRECT || kind == BufferKind.READ_ONLY;
    var parent =
        direct
            ? ByteBuffer.allocateDirect(expected.length + 32)
            : ByteBuffer.allocate(expected.length + 32);
    var start = plain ? 0 : 7;
    parent.position(start).put(expected).limit(start + expected.length).position(start);
    var input =
        switch (kind) {
          case SLICED, DIRECT_SLICE, READ_ONLY_SLICE -> parent.slice();
          case POSITIONED_SLICE -> parent.position(3).slice().position(4);
          default -> parent;
        };
    if (kind == BufferKind.READ_ONLY || kind == BufferKind.READ_ONLY_SLICE) {
      input = input.asReadOnlyBuffer();
    }
    var position = input.position();
    var limit = input.limit();
    input.mark();
    var read = deserializeStoreIndex(input, INDEX_KEY_SERIALIZER, false);
    assertThat(read).isInstanceOf(ReadOnlyIndexImpl.class);
    assertThat(input.position()).isEqualTo(position);
    assertThat(input.limit()).isEqualTo(limit);
    input.reset();

    var retained = read.getElement(entryKey(64));
    var retainedKey = retained.key();
    var retainedValue = retained.valueNullable();
    var forward = read.elementIterator();
    var reverse = read.reverseElementIterator();
    var first = forward.next();
    var last = reverse.next();
    for (var i = 1; i < 130; i++) {
      assertThat(forward.next().key()).isEqualTo(entryKey(i));
      assertThat(reverse.next().key()).isEqualTo(entryKey(129 - i));
    }
    assertThat(forward.hasNext()).isFalse();
    assertThat(reverse.hasNext()).isFalse();
    var scratch = IndexKeyBufferPool.acquire();
    try {
      Arrays.fill(
          scratch.array(),
          scratch.arrayOffset(),
          scratch.arrayOffset() + scratch.capacity(),
          (byte) 0xa5);
    } finally {
      IndexKeyBufferPool.release(scratch);
    }
    assertThat(retainedKey).isEqualTo(entryKey(64));
    assertThat(retained.valueNullable()).isSameAs(retainedValue).isEqualTo(key("value-64"));
    assertThat(first.key()).isEqualTo(entryKey(0));
    assertThat(last.key()).isEqualTo(entryKey(129));
    assertThat(input.position()).isEqualTo(position);
    assertThat(input.limit()).isEqualTo(limit);

    var output = read.serialize();
    assertThatThrownBy(() -> output.put(0, (byte) 7)).isInstanceOf(ReadOnlyBufferException.class);
    output.position(output.limit());
    input.clear(); // Buffer metadata belongs to the caller, not the view.
    writer.put(entryKey(64), key("changed"));
    writer.remove(entryKey(0));
    writer.serialize();
    assertThat(read.get(entryKey(64))).isEqualTo(key("value-64"));
    assertThat(copy(read.serialize())).containsExactly(expected);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 63, 64, 65, 127, 128, 129})
  void memoizesValuesAndNullsPerView(int ordinal) {
    var writer = writer();
    var codec = spy(INDEX_KEY_SERIALIZER);
    var bytes = writer.serialize();
    var read = deserializeStoreIndex(bytes, codec, false);
    var target = entryKey(ordinal);
    var value = read.get(target);
    for (var i = 0; i < 20; i++) {
      assertThat(read.get(target)).isSameAs(value);
      assertThat(read.getElement(target).valueNullable()).isSameAs(value);
      assertThat(read.contains(target)).isEqualTo(ordinal % 3 != 0);
      assertThat(read.getElement(target).hasValue()).isEqualTo(ordinal % 3 != 0);
    }
    verify(codec, times(1)).deserialize(any(ByteBuffer.class));
    verify(codec, never()).isNullSerialized(any(ByteBuffer.class));
    var other = deserializeStoreIndex(bytes, codec, false);
    var otherValue = other.get(target);
    assertThat(otherValue).isEqualTo(value);
    if (value != null) {
      assertThat(otherValue).isNotSameAs(value);
    }
    verify(codec, times(2)).deserialize(any(ByteBuffer.class));
  }

  @ParameterizedTest
  @CsvSource({
    "63,false",
    "63,true",
    "64,false",
    "64,true",
    "127,false",
    "127,true",
    "128,false",
    "128,true"
  })
  void concurrentFirstAccessPublishesOneWinningValueOrNull(int ordinal, boolean nullable)
      throws Exception {
    var writer = writer();
    var target = entryKey(ordinal);
    writer.add(indexElement(target, nullable ? null : key("concurrent-value")));
    var codec = spy(INDEX_KEY_SERIALIZER);
    var entered = new CountDownLatch(2);
    var release = new CountDownLatch(1);
    doAnswer(
            call -> {
              entered.countDown();
              assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
              return INDEX_KEY_SERIALIZER.deserialize(call.getArgument(0));
            })
        .when(codec)
        .deserialize(any(ByteBuffer.class));
    var read = deserializeStoreIndex(writer.serialize(), codec, false);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var first = CompletableFuture.supplyAsync(() -> read.get(target), executor);
      var second =
          CompletableFuture.supplyAsync(() -> read.getElement(target).valueNullable(), executor);
      try {
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
      } finally {
        release.countDown();
      }
      var firstValue = first.get(10, TimeUnit.SECONDS);
      var secondValue = second.get(10, TimeUnit.SECONDS);
      assertThat(secondValue).isSameAs(firstValue);
      assertThat(firstValue).isEqualTo(nullable ? null : key("concurrent-value"));
      for (var i = 0; i < 20; i++) {
        assertThat(read.get(target)).isSameAs(firstValue);
      }
      verify(codec, times(2)).deserialize(any(ByteBuffer.class));
    } finally {
      release.countDown();
    }
  }

  @Test
  void concurrentPointReadsAndIndependentIteratorsCoverAllMemoPages() throws Exception {
    var writer = writer();
    var bytes = writer.serialize();
    var expected = copy(bytes);
    var read = deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false);
    var start = new CyclicBarrier(4);
    try (var executor = Executors.newFixedThreadPool(4)) {
      var futures = new ArrayList<CompletableFuture<Void>>();
      for (var thread = 0; thread < 4; thread++) {
        var offset = thread;
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    start.await(10, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                  for (var iteration = 0; iteration < 16; iteration++) {
                    for (var j = 0; j < 130; j++) {
                      var i = (j * 7 + offset + iteration) % 130;
                      assertThat(read.get(entryKey(i)))
                          .isEqualTo(i % 3 == 0 ? null : key("value-" + i));
                    }
                    var forward = read.elementIterator();
                    var reverse = read.reverseElementIterator();
                    for (var i = 0; i < 130; i++) {
                      assertThat(forward.next().key()).isEqualTo(entryKey(i));
                      assertThat(reverse.next().key()).isEqualTo(entryKey(129 - i));
                    }
                    assertThat(copy(read.serialize())).containsExactly(expected);
                  }
                },
                executor));
      }
      for (var future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void rangesAndPrefixesAgreeWithOrderedModel() {
    var writer = writer();
    var model = new TreeMap<IndexKey, IndexKey>();
    for (var i = 0; i < 130; i++) {
      model.put(entryKey(i), i % 3 == 0 ? null : key("value-" + i));
    }
    var read = deserializeStoreIndex(writer.serialize(), INDEX_KEY_SERIALIZER, false);
    var bounds =
        List.of(
            key("a"),
            entryKey(0),
            key("namespace/shared-prefix/063x"),
            entryKey(64),
            entryKey(129),
            key("z"));
    for (var lower : bounds) {
      for (var higher : bounds) {
        if (lower.compareTo(higher) >= 0) {
          continue;
        }
        var expected = model.subMap(lower, true, higher, true).keySet();
        var forward = new ArrayList<IndexKey>();
        read.elementIterator(lower, higher, false)
            .forEachRemaining(element -> forward.add(element.key()));
        assertThat(forward).containsExactlyElementsOf(expected);
        var reverse = new ArrayList<IndexKey>();
        read.reverseElementIterator(lower, higher, false)
            .forEachRemaining(element -> reverse.add(element.key()));
        assertThat(reverse).containsExactlyElementsOf(new ArrayList<>(expected).reversed());
      }
    }
    var prefix = key("namespace/shared-prefix/06");
    var matching = new ArrayList<IndexKey>();
    read.elementIterator(prefix, prefix, false)
        .forEachRemaining(element -> matching.add(element.key()));
    assertThat(matching)
        .containsExactlyElementsOf(
            model.keySet().stream().filter(k -> k.startsWith(prefix)).toList());
    assertThatThrownBy(() -> read.reverseElementIterator(prefix, prefix, false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "01",
        "0200",
        "0101",
        "010001",
        "01016101",
        "010261017601ff7f",
        "0101610176010262017701"
      })
  void rejectsUnsupportedOrTruncatedMetadata(String hex) {
    assertThatThrownBy(() ->
            deserializeStoreIndex(
                ByteBuffer.wrap(HexFormat.of().parseHex(hex)), INDEX_KEY_SERIALIZER, false))
        .isInstanceOfAny(
            IllegalArgumentException.class,
            BufferUnderflowException.class,
            IndexOutOfBoundsException.class);
  }

  @Test
  void retainsNoncanonicalVarintWidthsWhenLocatingValueBoundaries() {
    var writer = new IndexLegacyImpl<>(INDEX_KEY_SERIALIZER);
    for (var i = 0; i < 3; i++) {
      writer.put(key("common-" + i), key("value-" + i));
    }
    var canonical = writer.serialize();
    var cursor = canonical.duplicate();
    assertThat(cursor.get()).isEqualTo((byte) 1);
    assertThat(readVarInt(cursor)).isEqualTo(3);
    var body = cursor.position();
    IndexKey.skip(cursor);
    INDEX_KEY_SERIALIZER.skip(cursor);
    var delta = cursor.position();
    var strip = readVarInt(cursor);
    assertThat(strip).isLessThan(128);
    assertThat(cursor.position()).isEqualTo(delta + 1);
    var encoded = ByteBuffer.allocate(canonical.remaining() + 2);
    encoded.put((byte) 1).put((byte) 0x83).put((byte) 0);
    encoded.put(canonical.duplicate().position(body).limit(delta));
    encoded.put((byte) (strip | 0x80)).put((byte) 0);
    encoded.put(canonical.duplicate().position(delta + 1)).flip();
    var read = deserializeStoreIndex(encoded, INDEX_KEY_SERIALIZER, false);
    assertThat(copy(read.serialize())).containsExactly(copy(encoded));
    assertThat(read.asKeyList()).containsExactlyElementsOf(writer.asKeyList());
    for (var i = 0; i < 3; i++) {
      var expected = key("value-" + i);
      var element = read.getElement(key("common-" + i));
      assertThat(element.valueNullable()).isEqualTo(expected);
      assertThat(element.contentSerializedSize(INDEX_KEY_SERIALIZER))
          .isEqualTo(expected.serializedSize());
      var rawValue = ByteBuffer.allocate(expected.serializedSize());
      element.serializeContent(INDEX_KEY_SERIALIZER, rawValue);
      assertThat(INDEX_KEY_SERIALIZER.deserialize(rawValue.flip())).isEqualTo(expected);
    }
  }

  @Test
  void supportsZeroLengthCodecPayloads() {
    var codec =
        new IndexValueSerializer<String>() {
          @Override
          public int serializedSize(String value) {
            return 0;
          }

          @Override
          public ByteBuffer serialize(String value, ByteBuffer target) {
            return target;
          }

          @Override
          public String deserialize(ByteBuffer buffer) {
            return "empty";
          }

          @Override
          public boolean isNullSerialized(ByteBuffer buffer) {
            return false;
          }

          @Override
          public void skip(ByteBuffer buffer) {}
        };
    var writer = newStoreIndex(codec);
    for (var i = 0; i < 70; i++) {
      writer.put(entryKey(i), "empty");
    }
    var read = deserializeStoreIndex(writer.serialize(), codec, false);
    for (var i = 0; i < 70; i++) {
      assertThat(read.get(entryKey(i))).isEqualTo("empty");
      assertThat(read.getElement(entryKey(i)).contentSerializedSize(codec)).isZero();
    }
  }
}

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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestMutableV2IndexImpl {
  static Stream<V2IndexFormat.Options> policies() {
    return Stream.of(V2IndexFormat.Layout.values())
        .flatMap(layout ->
            Stream.of(V2IndexFormat.DirectoryEncoding.values())
                .map(directory -> new V2IndexFormat.Options(layout, 16, directory, 8)));
  }

  private static MutableV2IndexImpl<ObjRef> mutable(
      ByteBuffer bytes, V2IndexFormat.Options policy) {
    return new MutableV2IndexImpl<>(new ReadOnlyV2IndexImpl<>(bytes, OBJ_REF_SERIALIZER), policy);
  }

  private static TreeMap<IndexKey, ObjRef> model(IndexSpi<ObjRef> index) {
    var result = new TreeMap<IndexKey, ObjRef>();
    index.elementIterator().forEachRemaining(e -> result.put(e.key(), e.valueNullable()));
    return result;
  }

  private static void assertModel(IndexSpi<ObjRef> index, TreeMap<IndexKey, ObjRef> expected) {
    assertThat(index.hasElements()).isEqualTo(!expected.isEmpty());
    assertThat(index.asKeyList()).containsExactlyElementsOf(expected.keySet());
    assertThat(index.first()).isEqualTo(expected.isEmpty() ? null : expected.firstKey());
    assertThat(index.last()).isEqualTo(expected.isEmpty() ? null : expected.lastKey());
    var forward = index.elementIterator();
    for (var e : expected.entrySet()) {
      var actual = forward.next();
      assertThat(actual.key()).isEqualTo(e.getKey());
      var value = actual.valueNullable();
      if (e.getValue() == null) assertThat(value).isNull();
      else assertThat(requireNonNull(value).toByteBuffer()).isEqualTo(e.getValue().toByteBuffer());
      assertThat(index.containsElement(e.getKey())).isTrue();
      assertThat(index.contains(e.getKey())).isEqualTo(e.getValue() != null);
    }
    assertThat(forward).isExhausted();
    assertThat(index.reverseElementIterator())
        .toIterable()
        .extracting(InternalIndexElement::key)
        .containsExactlyElementsOf(expected.descendingKeySet());
    if (expected.size() > 4) {
      var keys = new ArrayList<>(expected.keySet());
      var lo = keys.get(1);
      var hi = keys.get(keys.size() - 2);
      assertThat(index.elementIterator(lo, hi, false))
          .toIterable()
          .extracting(InternalIndexElement::key)
          .containsExactlyElementsOf(expected.subMap(lo, true, hi, true).keySet());
      assertThat(index.reverseElementIterator(lo, hi, false))
          .toIterable()
          .extracting(InternalIndexElement::key)
          .containsExactlyElementsOf(expected.subMap(lo, true, hi, true).descendingKeySet());
      assertThat(index.elementIterator(lo, lo, false))
          .toIterable()
          .extracting(InternalIndexElement::key)
          .containsExactlyElementsOf(
              expected.keySet().stream().filter(k -> k.startsWith(lo)).toList());
    }
  }

  @ParameterizedTest
  @MethodSource("policies")
  void writerMatchesIndependentFixedPartitionPrototype(V2IndexFormat.Options policy) {
    var source = V2TestData.create(130, "BINARY").mutable();
    var prototype =
        new SplitV2Format.Options(
            SplitV2Format.Layout.valueOf(policy.layout().name()),
            16,
            SplitV2Format.DirectoryEncoding.valueOf(policy.directory().name()),
            8,
            false);
    var bytes = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    assertThat(bytes).isEqualTo(SplitV2IndexWriter.write(source, OBJ_REF_SERIALIZER, prototype));
    assertModel(deserializeStoreIndex(bytes, OBJ_REF_SERIALIZER, false, 1), model(source));
  }

  @ParameterizedTest
  @MethodSource("policies")
  void reproducibleStatefulMutationAndSnapshots(V2IndexFormat.Options policy) {
    for (String shape : List.of("WORDS", "BINARY"))
      for (long seed : new long[] {7, 991, 31719}) {
        var source = V2TestData.create(65, shape).mutable();
        var expected = model(source);
        var input = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
        var index = mutable(input, policy);
        var frozen = input.duplicate();
        var sourceModel = new TreeMap<>(expected);
        var random = new Random(seed);
        var keys = new ArrayList<>(expected.keySet());
        for (int i = 0; i < 50; i++) keys.add(key("future/" + i));
        for (int step = 0; step < 160; step++) {
          var k = keys.get(random.nextInt(keys.size()));
          if (random.nextInt(3) == 0) {
            assertThat(index.remove(k)).isEqualTo(expected.containsKey(k));
            expected.remove(k);
          } else {
            ObjRef value =
                step % 7 == 0
                    ? null
                    : ObjRef.objRef("value-" + (step % 3), 100 + step, step % 2 == 0 ? 1 : 400);
            var result =
                !expected.containsKey(k)
                    ? IndexSpi.AddResult.NEW_KEY
                    : expected.get(k) == null
                        ? IndexSpi.AddResult.UPDATED_NO_VALUE
                        : IndexSpi.AddResult.UPDATED;
            assertThat(index.add(indexElement(k, value))).isEqualTo(result);
            expected.put(k, value);
          }
          assertThat(index.asKeyList()).hasSize(expected.size());
          if (step % 17 == 0) {
            assertModel(index, expected);
            var estimate = index.estimatedSerializedSize();
            var snapshot = index.serialize();
            assertThat(snapshot.remaining()).isLessThanOrEqualTo(estimate);
            assertModel(deserializeStoreIndex(snapshot, OBJ_REF_SERIALIZER, false, 1), expected);
            var old = new TreeMap<>(expected);
            index.put(key("temporary-snapshot"), ObjRef.objRef("value", 1, 1));
            assertModel(deserializeStoreIndex(snapshot, OBJ_REF_SERIALIZER, false, 1), old);
            index.remove(key("temporary-snapshot"));
          }
          if (step % 53 == 0) {
            var accumulated = new TreeMap<IndexKey, ObjRef>();
            for (var part : index.splitByTargetSize(1024)) {
              assertThat(part.isMutable()).isTrue();
              assertThat(part.serialize().get(0)).isEqualTo((byte) 2);
              var actual = part.serialize();
              if (part.asKeyList().size() > 1)
                assertThat(actual.remaining()).isLessThanOrEqualTo(1024);
              accumulated.putAll(model(part));
            }
            assertThat(accumulated.keySet()).containsExactlyElementsOf(expected.keySet());
            for (var e : expected.entrySet()) {
              if (e.getValue() == null) assertThat(accumulated.get(e.getKey())).isNull();
              else
                assertThat(accumulated.get(e.getKey()).toByteBuffer())
                    .isEqualTo(e.getValue().toByteBuffer());
            }
          }
        }
        assertModel(index, expected);
        assertModel(deserializeStoreIndex(frozen, OBJ_REF_SERIALIZER, false, 1), sourceModel);
        var reload = deserializeStoreIndex(index.serialize(), OBJ_REF_SERIALIZER, true, 1);
        assertThat(reload).isExactlyInstanceOf(MutableV2IndexImpl.class);
        assertThat(reload.isModified()).isFalse();
        assertModel(reload, expected);
        reload.put(key("after-reload"), ObjRef.objRef("value", 888, 1));
        assertModel(index, expected);
      }
  }

  @ParameterizedTest
  @MethodSource("policies")
  void reusesUntouchedBlocksAndValueOnlyKeySections(V2IndexFormat.Options policy) {
    var source = V2TestData.create(130, "WORDS").mutable();
    var bytes = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    var codec = spy(OBJ_REF_SERIALIZER);
    var base = new ReadOnlyV2IndexImpl<>(bytes, codec);
    // Use the production FRONT preset even when existing key sections use another layout.
    var index = new MutableV2IndexImpl<>(base);
    var k = source.asKeyList().get(65);
    index.put(k, ObjRef.objRef("replacement-with-different-size", 432, 2));
    var estimate = index.estimatedSerializedSize();
    var written = index.serialize();
    assertThat(written.remaining()).isEqualTo(estimate);
    verify(codec, never()).deserialize(any());
    verify(codec, never()).skip(any());
    var next = new ReadOnlyV2IndexImpl<>(written, codec);
    int target = base.blockForKey(k);
    assertThat(next.blockCount).isEqualTo(base.blockCount);
    for (int i = 0; i < base.blockCount; i++) {
      var old = base.block(i);
      var replacement = next.block(i);
      if (i == target)
        assertThat(
                written.slice(
                    replacement.keyTable(), replacement.valueMeta() - replacement.keyTable()))
            .isEqualTo(bytes.slice(old.keyTable(), old.valueMeta() - old.keyTable()));
      else
        assertThat(written.slice(replacement.start(), replacement.end() - replacement.start()))
            .isEqualTo(bytes.slice(old.start(), old.end() - old.start()));
    }
    assertThat(index.get(k).numParts()).isEqualTo(2);
  }

  @Test
  void countsTombstonesCancellationAuxiliaryHintsAndObjectIds() {
    var index = MutableV2IndexImpl.empty(OBJ_REF_SERIALIZER);
    var id = ObjRef.objRef("stripe", 42, 1);
    assertThat(index.setObjId(id)).isSameAs(index);
    assertThat(index.getObjId()).isSameAs(id);
    assertThat(index.isModified()).isFalse();
    assertThat(index.remove(key("absent"))).isFalse();
    assertThat(index.isModified()).isFalse();
    assertThat(index.add(indexElement(key("a"), null))).isEqualTo(IndexSpi.AddResult.NEW_KEY);
    assertThat(index.hasElements()).isTrue();
    assertThat(index.contains(key("a"))).isFalse();
    assertThat(index.containsElement(key("a"))).isTrue();
    assertThat(index.remove(key("a"))).isTrue();
    assertThat(index.hasElements()).isFalse();
    assertThat(index.isModified()).isTrue();
    assertThat(index.splitByTargetSize(1)).isEmpty();
    var one = ObjRef.objRef("value", 1, 1);
    var other = ObjRef.objRef("value", 1, 10000);
    assertThat(one).isEqualTo(other);
    index.put(key("a"), one);
    var first = index.serialize();
    var saved = deserializeStoreIndex(first, OBJ_REF_SERIALIZER, false, 1);
    index.estimatedSerializedSize();
    index.put(key("a"), other);
    var second = index.serialize();
    assertThat(
            requireNonNull(
                    deserializeStoreIndex(second, OBJ_REF_SERIALIZER, false, 1).get(key("a")))
                .numParts())
        .isEqualTo(10000);
    assertThat(requireNonNull(saved.get(key("a"))).numParts()).isEqualTo(1);
    assertThat(index.getObjId()).isSameAs(id);
    assertThat(index.stripes()).containsExactly(index);
    assertThat(index.mutableStripeForKey(key("z"))).isSameAs(index);
    assertThat(index.splitByTargetSize(1)).containsExactly(index);
    assertThatThrownBy(() -> index.splitByTargetSize(0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> index.asKeyList().get(-1))
        .isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> index.elementIterator(key("z"), key("a"), false))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> index.reverseElementIterator(key("a"), key("a"), false))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static final class Value {
    byte[] bytes;

    Value(int length) {
      bytes = new byte[length];
    }
  }

  private static class Codec implements IndexValueSerializer<Value> {
    final AtomicBoolean fail = new AtomicBoolean();

    @Override
    public int serializedSize(@Nullable Value value) {
      return 4 + (value == null ? 0 : value.bytes.length);
    }

    @Override
    public @NonNull ByteBuffer serialize(@Nullable Value value, @NonNull ByteBuffer target) {
      if (fail.getAndSet(false)) throw new IllegalStateException("deliberate codec failure");
      return value == null ? target.putInt(-1) : target.putInt(value.bytes.length).put(value.bytes);
    }

    @Override
    public Value deserialize(@NonNull ByteBuffer bytes) {
      int length = bytes.getInt();
      if (length < 0) return null;
      var value = new Value(length);
      bytes.get(value.bytes);
      return value;
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer bytes) {
      return bytes.getInt(bytes.position()) < 0;
    }

    @Override
    public void skip(@NonNull ByteBuffer bytes) {
      throw new AssertionError("No V2 skip");
    }
  }

  @Test
  void preparedPlansTrackValueGrowthShrinkAndRecoverFromCodecFailure() {
    var codec = new Codec();
    var index = MutableV2IndexImpl.empty(codec);
    var value = new Value(8);
    index.put(key("a"), value);
    index.put(key("b"), new Value(8));
    int small = index.estimatedSerializedSize();
    var snapshot = index.serialize();
    value.bytes[0] = 0x55;
    assertThat(
            requireNonNull(deserializeStoreIndex(index.serialize(), codec, false, 1).get(key("a")))
                .bytes[0])
        .isEqualTo((byte) 0x55);
    assertThat(
            requireNonNull(deserializeStoreIndex(snapshot, codec, false, 1).get(key("a"))).bytes[0])
        .isZero();
    value.bytes = new byte[70000];
    assertThat(index.estimatedSerializedSize()).isGreaterThan(small);
    var grown = index.serialize();
    assertThat(grown.remaining()).isEqualTo(index.estimatedSerializedSize());
    assertThat(requireNonNull(deserializeStoreIndex(grown, codec, false, 1).get(key("a"))).bytes)
        .hasSize(70000);
    assertThat(requireNonNull(deserializeStoreIndex(snapshot, codec, false, 1).get(key("a"))).bytes)
        .hasSize(8);
    value.bytes = new byte[1];
    var shrunk = index.serialize();
    assertThat(shrunk.remaining()).isLessThan(grown.remaining());
    codec.fail.set(true);
    assertThatThrownBy(index::serialize).isInstanceOf(IllegalStateException.class);
    assertThat(
            requireNonNull(deserializeStoreIndex(index.serialize(), codec, false, 1).get(key("a")))
                .bytes)
        .hasSize(1);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void removingWholeBlocksAndAllEntriesIsSnapshotSafe(V2IndexFormat.Options policy) {
    var source = V2TestData.create(65, "BINARY").mutable();
    var input = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    var base = new ReadOnlyV2IndexImpl<>(input, OBJ_REF_SERIALIZER);
    var index = new MutableV2IndexImpl<>(base, policy);
    var keys = source.asKeyList();
    for (int i = 0; i < base.blockEntries(0); i++) index.remove(keys.get(i));
    var saved = index.serialize();
    assertThat(new ReadOnlyV2IndexImpl<>(saved, OBJ_REF_SERIALIZER).blockCount)
        .isEqualTo(base.blockCount - 1);
    for (var k : keys) index.remove(k);
    assertThat(index.first()).isNull();
    assertThat(index.last()).isNull();
    assertThat(index.elementIterator()).isExhausted();
    assertThat(index.serialize().remaining()).isEqualTo(12);
    assertThat(deserializeStoreIndex(saved, OBJ_REF_SERIALIZER, false, 1).hasElements()).isTrue();
    index.put(key("new"), ObjRef.objRef("value", 3, 1));
    assertThat(index.first()).isEqualTo(key("new"));
    assertThat(index.asKeyList()).hasSize(1);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void structuralEditsFromEveryLayoutIntoDefaultAndSplitIndependence(V2IndexFormat.Options policy) {
    var source = V2TestData.create(130, "BINARY").mutable();
    var expected = model(source);
    var input = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    var index = new MutableV2IndexImpl<>(new ReadOnlyV2IndexImpl<>(input, OBJ_REF_SERIALIZER));
    for (int i = 0; i < 20; i++) {
      var removed = source.asKeyList().get(i * 3);
      index.remove(removed);
      expected.remove(removed);
      var added = key("insert/" + i);
      var value = ObjRef.objRef("new", 500 + i, 2);
      index.put(added, value);
      expected.put(added, value);
    }
    assertModel(index, expected);
    assertModel(deserializeStoreIndex(index.serialize(), OBJ_REF_SERIALIZER, false, 1), expected);
    var id = ObjRef.objRef("stripe", 88, 1);
    index.setObjId(id);
    assertThat(index.splitByTargetSize(Long.MAX_VALUE)).containsExactly(index);
    assertThat(index.getObjId()).isSameAs(id);
    var parts = index.splitByTargetSize(1024);
    assertThat(parts).hasSizeGreaterThan(1);
    var first = parts.getFirst();
    var before = first.serialize();
    var firstModel = model(first);
    var sibling = parts.get(1);
    var siblingModel = model(sibling);
    for (var part : parts) {
      assertThat(part.isModified()).isTrue();
      assertThat(part.getObjId()).isNull();
    }
    first.put(key("part-only"), ObjRef.objRef("value", 10000, 1));
    assertModel(deserializeStoreIndex(before, OBJ_REF_SERIALIZER, false, 1), firstModel);
    assertModel(sibling, siblingModel);
    assertModel(index, expected);
    index.put(key("source-only"), ObjRef.objRef("value", 20000, 1));
    assertThat(first.containsElement(key("source-only"))).isFalse();
  }

  @Test
  void absentBoundsAndPrefixMergeSourceInsertTombstoneAndRemoval() {
    var original = MutableV2IndexImpl.empty(OBJ_REF_SERIALIZER);
    var v = ObjRef.objRef("value", 1, 1);
    for (String k : List.of("a/01", "a/03", "b/01")) original.put(key(k), v);
    var index = deserializeStoreIndex(original.serialize(), OBJ_REF_SERIALIZER, true, 1);
    index.remove(key("a/03"));
    index.put(key("a/02"), v);
    index.add(indexElement(key("a/01"), null));
    index.put(key("b/00"), v);
    assertThat(index.elementIterator(key("a/"), key("a/"), false))
        .toIterable()
        .extracting(InternalIndexElement::key)
        .containsExactly(key("a/01"), key("a/02"));
    assertThat(index.elementIterator(key("a/015"), key("b/005"), false))
        .toIterable()
        .extracting(InternalIndexElement::key)
        .containsExactly(key("a/02"), key("b/00"));
    assertThat(index.reverseElementIterator(key("a/015"), key("b/005"), false))
        .toIterable()
        .extracting(InternalIndexElement::key)
        .containsExactly(key("b/00"), key("a/02"));
    assertThat(index.iterator(key("a/"), key("a/"), false))
        .toIterable()
        .extracting(e -> e.key())
        .containsExactly(key("a/02"));
    assertThat(index.elementIterator(key("c"), null, false)).isExhausted();
    assertThat(index.reverseElementIterator(null, key("0"), false)).isExhausted();
  }

  @ParameterizedTest
  @MethodSource("policies")
  void wideKeyTablesSurviveStructuralEditsAndRestartChanges(V2IndexFormat.Options policy) {
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var random = new Random(437);
    for (int i = 0; i < 256; i++) {
      var bytes = new byte[IndexKey.MAX_LENGTH];
      random.nextBytes(bytes);
      source.put(key(ByteBuffer.wrap(bytes)), ObjRef.objRef("value", i + 1, 1));
    }
    var oldPolicy = new V2IndexFormat.Options(policy.layout(), 256, policy.directory(), 2);
    var input = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, oldPolicy);
    var read = new ReadOnlyV2IndexImpl<>(input, OBJ_REF_SERIALIZER);
    assertThat(read.block(0).keyWidth()).isEqualTo(4);
    var nextPolicy = new V2IndexFormat.Options(policy.layout(), 256, policy.directory(), 3);
    var index = new MutableV2IndexImpl<>(read, nextPolicy);
    var expected = model(source);
    var removed = source.asKeyList().get(100);
    index.remove(removed);
    expected.remove(removed);
    var added = key("new-key");
    var value = ObjRef.objRef("value", 1000, 1);
    index.put(added, value);
    expected.put(added, value);
    var output = index.serialize();
    assertThat(new ReadOnlyV2IndexImpl<>(output, OBJ_REF_SERIALIZER).block(0).keyWidth())
        .isEqualTo(4);
    assertModel(deserializeStoreIndex(output, OBJ_REF_SERIALIZER, false, 1), expected);
  }

  @ParameterizedTest
  @MethodSource("policies")
  void balancedDeferredOverflowPreservesNeighborsAndCancellation(V2IndexFormat.Options policy) {
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var value = ObjRef.objRef("value", 1, 1);
    for (int i = 0; i < 48; i++) source.put(key(String.format("%04d", i * 100)), value);
    var bytes = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    var base = new ReadOnlyV2IndexImpl<>(bytes, OBJ_REF_SERIALIZER);
    var balanced =
        new V2IndexFormat.Options(
            policy.layout(), 16, policy.directory(), 8, V2IndexFormat.BlockSplit.BALANCED);
    for (int added : new int[] {1, 17, 64, 130}) {
      var index = new MutableV2IndexImpl<>(base, balanced);
      var expected = model(source);
      for (int i = 1; i <= added; i++) {
        var k = key(String.format("%04d", 1600 + i));
        index.put(k, value);
        expected.put(k, value);
      }
      // Avoid collisions with the original keys in larger batches.
      int grown = expected.size() - 32;
      int blocks = 1 + (grown - 1) / 16;
      var written = index.serialize();
      var next = new ReadOnlyV2IndexImpl<>(written, OBJ_REF_SERIALIZER);
      assertThat(written.remaining()).isLessThanOrEqualTo(index.estimatedSerializedSize());
      assertThat(next.blockCount).isEqualTo(blocks + 2);
      int min = Integer.MAX_VALUE, max = 0;
      for (int block = 1; block <= blocks; block++) {
        min = Math.min(min, next.blockEntries(block));
        max = Math.max(max, next.blockEntries(block));
      }
      assertThat(max).isLessThanOrEqualTo(16);
      assertThat(max - min).isLessThanOrEqualTo(1);
      for (int old : new int[] {0, 2}) {
        int replacement = old == 0 ? 0 : next.blockCount - 1;
        assertThat(
                written.slice(
                    next.blockStart(replacement),
                    next.blockEnd(replacement) - next.blockStart(replacement)))
            .isEqualTo(
                bytes.slice(base.blockStart(old), base.blockEnd(old) - base.blockStart(old)));
      }
      assertModel(next, expected);
    }
    var cancelled = new MutableV2IndexImpl<>(base, balanced);
    cancelled.put(key("1601"), value);
    cancelled.estimatedSerializedSize();
    cancelled.remove(key("1601"));
    assertThat(cancelled.serialize()).isEqualTo(bytes);
    assertThat(cancelled.isModified()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("policies")
  void splitTargetsTrackReplacementGrowthAndRepeatedHistory(V2IndexFormat.Options policy) {
    var codec = new Codec();
    var source = new IndexImpl<>(codec);
    for (int i = 0; i < 35; i++) source.put(key(String.format("key/%04d", i)), new Value(8));
    var original = V2IndexWriter.write(source, codec, policy);
    for (long target : new long[] {1, 255, 256, 1024, Long.MAX_VALUE}) {
      var index = new MutableV2IndexImpl<>(new ReadOnlyV2IndexImpl<>(original, codec), policy);
      var growth = new Value(7);
      index.put(key("key/0017"), growth);
      index.estimatedSerializedSize();
      growth.bytes = new byte[70000];
      index.put(key("key/0017a"), new Value(11));
      index.add(indexElement(key("key/0002"), null));
      index.remove(key("key/0003"));
      var before = index.serialize();
      assertThat(before.remaining()).isLessThanOrEqualTo(index.estimatedSerializedSize());
      var keys = new ArrayList<IndexKey>();
      for (var part : index.splitByTargetSize(target)) {
        var snapshot = part.serialize();
        assertThat(snapshot.get(0)).isEqualTo((byte) 2);
        assertThat(snapshot.remaining()).isLessThanOrEqualTo(part.estimatedSerializedSize());
        if (part.asKeyList().size() > 1)
          assertThat((long) snapshot.remaining()).isLessThanOrEqualTo(target);
        var read = new ReadOnlyV2IndexImpl<>(snapshot, codec);
        for (var it = read.elementIterator(); it.hasNext(); ) {
          var entry = it.next();
          keys.add(entry.key());
          var expected = index.getElement(entry.key()).valueNullable();
          var actual = entry.valueNullable();
          if (expected == null) assertThat(actual).isNull();
          else assertThat(requireNonNull(actual).bytes).containsExactly(expected.bytes);
        }
        // Repeated split/read/edit must not corrupt sibling parts or earlier output.
        var next = (MutableV2IndexImpl<Value>) deserializeStoreIndex(snapshot, codec, true, 1);
        next.put(next.first(), new Value(13));
        for (var child : next.splitByTargetSize(target)) {
          var childBytes = child.serialize();
          if (child.asKeyList().size() > 1)
            assertThat((long) childBytes.remaining()).isLessThanOrEqualTo(target);
          assertThat(childBytes.remaining()).isLessThanOrEqualTo(child.estimatedSerializedSize());
        }
        assertThat(part.serialize()).isEqualTo(snapshot);
      }
      assertThat(keys).containsExactlyElementsOf(index.asKeyList());
      assertThat(index.serialize()).isEqualTo(before);
    }
  }

  @Test
  void maximumWireBlockSplitsAfterOneInsertWithoutChangingSource() {
    var value = ObjRef.objRef("value", 1, 1);
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < 4096; i++) source.put(key(String.format("%05d", i * 2)), value);
    var policy =
        new V2IndexFormat.Options(
            V2IndexFormat.Layout.FRONT_ENDS,
            4096,
            V2IndexFormat.DirectoryEncoding.FULL,
            8,
            V2IndexFormat.BlockSplit.BALANCED);
    var bytes = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, policy);
    var base = new ReadOnlyV2IndexImpl<>(bytes, OBJ_REF_SERIALIZER);
    var index = new MutableV2IndexImpl<>(base, policy);
    index.put(key("00001"), value);
    var output = new ReadOnlyV2IndexImpl<>(index.serialize(), OBJ_REF_SERIALIZER);
    assertThat(output.blockCount).isEqualTo(2);
    assertThat(output.blockEntries(0)).isEqualTo(2049);
    assertThat(output.blockEntries(1)).isEqualTo(2048);
    assertThat(output.asKeyList()).hasSize(4097);
    assertThat(base.blockEntries(0)).isEqualTo(4096);
    assertThat(base.containsElement(key("00001"))).isFalse();
    assertThat(output.get(key("00001"))).isEqualTo(value);
    assertThat(output.last()).isEqualTo(source.last());
  }

  @ParameterizedTest
  @MethodSource("policies")
  void valueOffsetWidthsGrowAndShrinkOnReusedKeySections(V2IndexFormat.Options policy) {
    var codec = new Codec();
    var source = new IndexImpl<>(codec);
    source.put(key("a"), new Value(8));
    source.put(key("b"), new Value(0));
    var bytes = V2IndexWriter.write(source, codec, policy);
    var base = new ReadOnlyV2IndexImpl<>(bytes, codec);
    var index = new MutableV2IndexImpl<>(base, policy);
    var value = new Value(247);
    index.put(key("a"), value);
    for (int length : new int[] {247, 248, 65527, 65528, 247}) {
      value.bytes = new byte[length];
      int estimate = index.estimatedSerializedSize();
      var encoded = index.serialize();
      assertThat(encoded.remaining()).isLessThanOrEqualTo(estimate);
      var read = new ReadOnlyV2IndexImpl<>(encoded, codec);
      var old = base.block(0);
      var next = read.block(0);
      assertThat(next.valueWidth()).isEqualTo(V2IndexFormat.width(length + 8));
      assertThat(encoded.slice(next.keyTable(), next.valueMeta() - next.keyTable()))
          .isEqualTo(bytes.slice(old.keyTable(), old.valueMeta() - old.keyTable()));
      assertThat(requireNonNull(read.get(key("a"))).bytes).hasSize(length);
      assertThat(requireNonNull(read.get(key("b"))).bytes).isEmpty();
    }
  }

  @Test
  void offsetEmissionRejectsTruncationAndSizeOverflowBeforeAllocation() {
    for (int width : new int[] {1, 2, 4}) {
      int maximum = width == 4 ? Integer.MAX_VALUE : (1 << (width * 8)) - 1;
      var output = ByteBuffer.allocate(width);
      V2IndexFormat.putOffset(output, maximum, width);
      assertThat(V2IndexFormat.offset(output, 0, width)).isEqualTo(maximum);
      assertThatThrownBy(() -> V2IndexFormat.putOffset(output.clear(), -1, width))
          .isInstanceOf(IllegalArgumentException.class);
      if (width != 4)
        assertThatThrownBy(() -> V2IndexFormat.putOffset(output.clear(), maximum + 1, width))
            .isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> V2IndexFormat.putOffset(ByteBuffer.allocate(4), 1, 3))
        .isInstanceOf(IllegalArgumentException.class);
    var codec =
        new Codec() {
          @Override
          public int serializedSize(Value value) {
            return Integer.MAX_VALUE;
          }
        };
    var index = MutableV2IndexImpl.empty(codec);
    index.put(key("overflow"), new Value(0));
    assertThatThrownBy(index::estimatedSerializedSize).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(index::serialize).isInstanceOf(ArithmeticException.class);
  }

  @Test
  void independentWriterGolden() {
    var index = new IndexImpl<>(INDEX_KEY_SERIALIZER);
    index.put(key("a"), key("x"));
    index.put(key("ab"), key("yz"));
    var expected =
        HexFormat.of()
            .parseHex(
                "020200000012600301620102057801797a010000000100010000000600000000000000246101");
    assertThat(V2IndexWriter.write(index, INDEX_KEY_SERIALIZER, V2IndexFormat.DEFAULT))
        .isEqualTo(ByteBuffer.wrap(expected));
  }
}

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestV2MutableIndex {
  static Stream<Arguments> cases() {
    return Stream.of("HIERARCHICAL", "WORDS", "UUID", "BINARY")
        .flatMap(shape ->
            Stream.of(false, true)
                .flatMap(uneven ->
                    Stream.of(V2IndexWriter.DirectoryEncoding.values())
                        .map(encoding ->
                            arguments(shape, new V2IndexWriter.Options(16, uneven, encoding, 3)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void mutationsMatchV1AndBulkV2Writer(String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(129, shape);
    var expected = data.fresh("BASELINE", 64);
    var actual = new V2IndexImpl<>(OBJ_REF_SERIALIZER, options);
    var keys = new ArrayList<>(data.keys());
    Collections.shuffle(keys, new Random(71));
    for (var key : keys)
      assertThat(actual.add(indexElement(key, expected.get(key))))
          .isEqualTo(IndexSpi.AddResult.NEW_KEY);
    verify(expected, actual, options);
    var random = new Random(521);
    for (int step = 0; step < 180; step++) {
      var key = data.keys().get(random.nextInt(data.keys().size()));
      if (step % 5 == 0) assertThat(actual.remove(key)).isEqualTo(expected.remove(key));
      else {
        var value =
            step % 7 == 0
                ? null
                : ObjRef.objRef(step % 2 == 0 ? "longer-value-name" : "v", step + 9000L, 3);
        assertThat(actual.add(indexElement(key, value)))
            .isEqualTo(expected.add(indexElement(key, value)));
      }
      if (step % 9 == 0) verify(expected, actual, options);
    }
    verify(expected, actual, options);
    for (var key : data.keys()) assertThat(actual.remove(key)).isEqualTo(expected.remove(key));
    verify(expected, actual, options);
    assertThat(actual.hasElements()).isFalse();
    assertThat(actual.first()).isNull();
    assertThat(actual.last()).isNull();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void readerPromotionIsIsolatedAndSerializesV2(String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(129, shape);
    var expected = data.fresh("BASELINE", 64);
    var bytes = V2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var actual = new V2IndexImpl<>(bytes, OBJ_REF_SERIALIZER, options);
    var snapshot = actual.serialize();
    assertThat(actual.isModified()).isFalse();
    assertThat(actual.isMutable()).isTrue();
    assertThat(actual.asMutableIndex()).isSameAs(actual);
    assertThat(actual.estimatedSerializedSize()).isEqualTo(bytes.remaining());
    assertThat(actual.remove(IndexKey.key("not-present"))).isFalse();
    assertThat(actual.isModified()).isFalse();
    var ref = ObjRef.objRef("source", 333);
    actual.setObjId(ref);
    var key = data.keys().get(64);
    var replacement = ObjRef.objRef("changed", 777, 3);
    actual.put(key, replacement);
    expected.put(key, replacement);
    assertThat(actual.isModified()).isTrue();
    assertThat(actual.getObjId()).isEqualTo(ref);
    verify(expected, actual, options);
    assertThat(actual.isModified()).isTrue();
    assertThat(snapshot).isEqualTo(bytes);
    assertThat(new V2IndexReader<>(snapshot, OBJ_REF_SERIALIZER).get(key))
        .isNotEqualTo(replacement);
    assertThatThrownBy(() -> snapshot.put(0, (byte) 1)).isInstanceOf(ReadOnlyBufferException.class);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void splittingPreservesValuesTargetsAndIndependentMutation(
      String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(129, shape);
    var baseline = data.fresh("BASELINE", 64);
    var actual =
        new V2IndexImpl<>(
            V2IndexWriter.write(baseline, OBJ_REF_SERIALIZER, options),
            OBJ_REF_SERIALIZER,
            options);
    for (int target : new int[] {1, 200, 1000, 1000000}) {
      var parts = actual.splitByTargetSize(target);
      var keys = new ArrayList<IndexKey>();
      for (var part : parts) {
        var bytes = part.serialize();
        assertThat(part.estimatedSerializedSize()).isEqualTo(bytes.remaining());
        if (part.asKeyList().size() > 1) assertThat(bytes.remaining()).isLessThanOrEqualTo(target);
        keys.addAll(part.asKeyList());
        var reader = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
        for (var key : part.asKeyList()) assertThat(reader.get(key)).isEqualTo(baseline.get(key));
      }
      assertThat(keys).containsExactlyElementsOf(data.keys());
    }
    var parts = actual.splitByTargetSize(200);
    assertThat(parts).hasSizeGreaterThan(1);
    var first = parts.getFirst();
    var removed = requireNonNull(first.first());
    assertThat(first.remove(removed)).isTrue();
    assertThat(actual.containsElement(removed)).isTrue();
    assertThat(actual.serialize())
        .isEqualTo(V2IndexWriter.write(baseline, OBJ_REF_SERIALIZER, options));
    assertThatThrownBy(() -> actual.splitByTargetSize(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tombstoneResultsAndCachedSerializationRemainCorrect() {
    var index = new V2IndexImpl<>(OBJ_REF_SERIALIZER);
    var key = IndexKey.key("key");
    assertThat(index.isModified()).isFalse();
    assertThat(index.add(indexElement(key, null))).isEqualTo(IndexSpi.AddResult.NEW_KEY);
    assertThat(index.containsElement(key)).isTrue();
    assertThat(index.contains(key)).isFalse();
    var value = ObjRef.objRef("value", 1234, 1);
    assertThat(index.add(indexElement(key, value))).isEqualTo(IndexSpi.AddResult.UPDATED_NO_VALUE);
    assertThat(index.add(indexElement(key, value))).isEqualTo(IndexSpi.AddResult.UPDATED);
    var bytes = index.serialize();
    assertThat(index.serialize()).isEqualTo(bytes);
    bytes.position(1);
    assertThat(index.serialize().position()).isZero();
    assertThat(index.setModified().serialize()).isEqualTo(bytes.position(0));
    assertThat(index.invalidatePlan().estimatedSerializedSize()).isEqualTo(bytes.remaining());
    assertThat(index.serialize()).isEqualTo(bytes);
    assertThat(index.remove(key)).isTrue();
    assertThat(index.splitByTargetSize(100)).isEmpty();
  }

  @Test
  void cachedValueSizesAvoidSerializerSizingDuringSerialization() {
    var codec = new CountingSerializer();
    var index = new V2IndexImpl<>(codec);
    for (int i = 0; i < 300; i++) index.put(IndexKey.key(i), ObjRef.objRef("value", i + 1000L, 1));
    int before = codec.sizes;
    var first = index.serialize();
    index.setModified().serialize();
    index.invalidatePlan().serialize();
    assertThat(codec.sizes).isEqualTo(before);
    var immutable = index.serialize();
    index.put(IndexKey.key(150), ObjRef.objRef("significantly-longer-value", 9999, 1));
    assertThat(codec.sizes).isEqualTo(before + 1);
    assertThat(index.estimatedSerializedSize()).isGreaterThan(first.remaining());
    assertThat(index.serialize().remaining()).isEqualTo(index.estimatedSerializedSize());
    assertThat(immutable).isEqualTo(first);
  }

  @Test
  void indexKeyValuesAndMaximumEscapedKeysSurviveUpdatesAndSplits() {
    var index =
        new V2IndexImpl<>(
            INDEX_KEY_SERIALIZER,
            new V2IndexWriter.Options(2, true, V2IndexWriter.DirectoryEncoding.PREFIX, 3));
    var keys = new ArrayList<IndexKey>();
    for (int i = 0; i < 8; i++) {
      var raw = new byte[IndexKey.MAX_LENGTH];
      Arrays.fill(raw, (byte) 1);
      raw[raw.length - 1] = (byte) i;
      var key = IndexKey.key(ByteBuffer.wrap(raw));
      keys.add(key);
      index.put(key, key);
    }
    index.add(indexElement(keys.get(2), null));
    var bytes = index.serialize();
    var reader = new V2IndexReader<>(bytes, INDEX_KEY_SERIALIZER);
    for (var key : keys) assertThat(reader.get(key)).isEqualTo(index.get(key));
    index.put(keys.get(2), IndexKey.key("short"));
    assertThat(index.serialize().remaining()).isEqualTo(index.estimatedSerializedSize());
    var all = new ArrayList<IndexKey>();
    for (var part : index.splitByTargetSize(200)) {
      all.addAll(part.asKeyList());
      var decoded = new V2IndexReader<>(part.serialize(), INDEX_KEY_SERIALIZER);
      for (var key : part.asKeyList()) assertThat(decoded.get(key)).isEqualTo(index.get(key));
    }
    assertThat(all).containsExactlyElementsOf(index.asKeyList());
    assertThat(new V2IndexReader<>(bytes, INDEX_KEY_SERIALIZER).get(keys.get(2))).isNull();
  }

  private static void verify(
      IndexSpi<ObjRef> expected, V2IndexImpl<ObjRef> actual, V2IndexWriter.Options options) {
    assertThat(actual.asKeyList()).containsExactlyElementsOf(expected.asKeyList());
    var bytes = actual.serialize();
    assertThat(bytes).isEqualTo(V2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options));
    assertThat(actual.estimatedSerializedSize()).isEqualTo(bytes.remaining());
    var reader = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    for (var key : expected.asKeyList()) {
      assertThat(actual.get(key)).isEqualTo(expected.get(key));
      assertThat(reader.get(key)).isEqualTo(expected.get(key));
    }
    assertThat(actual.first()).isEqualTo(expected.first());
    assertThat(actual.last()).isEqualTo(expected.last());
    var forward = new ArrayList<IndexKey>();
    actual.elementIterator().forEachRemaining(entry -> forward.add(entry.key()));
    assertThat(forward).containsExactlyElementsOf(expected.asKeyList());
    var reverse = new ArrayList<IndexKey>();
    actual.reverseElementIterator().forEachRemaining(entry -> reverse.add(entry.key()));
    assertThat(reverse).containsExactlyElementsOf(expected.asKeyList().reversed());
    if (forward.size() > 5) {
      var lower = forward.get(1);
      var upper = forward.get(forward.size() - 2);
      var subset = new ArrayList<IndexKey>();
      actual
          .elementIterator(lower, upper, false)
          .forEachRemaining(entry -> subset.add(entry.key()));
      assertThat(subset).containsExactlyElementsOf(forward.subList(1, forward.size() - 1));
    }
  }

  private static final class CountingSerializer implements IndexValueSerializer<ObjRef> {
    int sizes;

    @Override
    public int serializedSize(@Nullable ObjRef value) {
      sizes++;
      return OBJ_REF_SERIALIZER.serializedSize(value);
    }

    @Override
    public @NonNull ByteBuffer serialize(@Nullable ObjRef value, @NonNull ByteBuffer target) {
      return OBJ_REF_SERIALIZER.serialize(value, target);
    }

    @Override
    public ObjRef deserialize(@NonNull ByteBuffer input) {
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

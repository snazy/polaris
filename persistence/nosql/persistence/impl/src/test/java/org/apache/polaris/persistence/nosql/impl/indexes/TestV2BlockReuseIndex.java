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
import java.util.List;
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

class TestV2BlockReuseIndex {
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
  void mutationsReadsAndSnapshotsMatchV1(String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(321, shape);
    var expected = data.fresh("BASELINE", 64);
    var original = V2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var actual = new V2BlockReuseIndex<>(original, OBJ_REF_SERIALIZER, options);
    assertThat(actual.isModified()).isFalse();
    assertThat(actual.serialize()).isEqualTo(original);
    var initial = actual.serialize();
    var random = new Random(910);
    for (int step = 0; step < 200; step++) {
      var key =
          step % 9 == 0
              ? IndexKey.key("new-" + step)
              : data.keys().get(random.nextInt(data.keys().size()));
      assertThat(actual.get(key)).isEqualTo(expected.get(key));
      if (step % 4 == 0) assertThat(actual.remove(key)).isEqualTo(expected.remove(key));
      else {
        var value =
            step % 7 == 0
                ? null
                : ObjRef.objRef(step % 3 == 0 ? "longer-value-label" : "v", 9000L + step, 2);
        assertThat(actual.add(indexElement(key, value)))
            .isEqualTo(expected.add(indexElement(key, value)));
      }
      if (step % 19 == 0) verify(expected, actual);
    }
    verify(expected, actual);
    assertThat(initial).isEqualTo(original);
    assertThat(actual.isModified()).isTrue();
    assertThat(actual.asMutableIndex()).isSameAs(actual);
    var ref = ObjRef.objRef("source", 99);
    assertThat(actual.setObjId(ref).getObjId()).isEqualTo(ref);
    assertThatThrownBy(() -> actual.serialize().put(0, (byte) 1))
        .isInstanceOf(ReadOnlyBufferException.class);
    actual.serialize().position(1);
    assertThat(actual.serialize().position()).isZero();
  }

  @ParameterizedTest
  @MethodSource("cases")
  void onlyTouchedBlocksMaterializeAndUnchangedBodiesAreCopied(
      String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(321, shape);
    var bytes = V2IndexWriter.write(data.fresh("BASELINE", 64), OBJ_REF_SERIALIZER, options);
    var codec = new CountingSerializer();
    var reader = new V2IndexReader<>(bytes, codec);
    var actual = new V2BlockReuseIndex<>(reader, codec, options);
    var key = data.liveKeyAt(160);
    int changed = reader.blockForKey(key);
    actual.put(key, ObjRef.objRef("replacement", 99999, 2));
    assertThat(codec.decodes).isZero();
    assertThat(actual.changedBlocks()).isEqualTo(1);
    assertThat(actual.materializedBlocks()).isEqualTo(1);
    var serialized = actual.serialize();
    assertThat(codec.decodes).isZero();
    assertThat(actual.materializedBlocks()).isEqualTo(1);
    var written = new V2IndexReader<>(serialized, codec);
    assertThat(written.blockCount).isEqualTo(reader.blockCount);
    for (int block = 0; block < reader.blockCount; block++) {
      if (block != changed)
        assertThat(written.blockBytes(block)).isEqualTo(reader.blockBytes(block));
    }
    assertThat(written.get(key)).isEqualTo(ObjRef.objRef("replacement", 99999, 2));
  }

  @Test
  void readsBeforeMutationReuseDecodedValues() {
    var data = PrototypeIndexData.create(321, "WORDS");
    var options = V2IndexWriter.Options.defaults(16);
    var codec = new CountingSerializer();
    var reader =
        new V2IndexReader<>(
            V2IndexWriter.write(data.fresh("BASELINE", 64), OBJ_REF_SERIALIZER, options), codec);
    var actual = new V2BlockReuseIndex<>(reader, codec, options);
    var key = data.liveKeyAt(100);
    var value = actual.get(key);
    assertThat(codec.decodes).isEqualTo(1);
    actual.put(data.liveKeyAt(101), ObjRef.objRef("changed", 77));
    assertThat(actual.get(key)).isSameAs(value);
    assertThat(reader.get(key)).isSameAs(value);
    actual.serialize();
    assertThat(codec.decodes).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void removedBoundariesPrependingOverflowAndSplitRemainIndependent(
      String shape, V2IndexWriter.Options options) {
    var data = PrototypeIndexData.create(129, shape);
    var expected = data.fresh("BASELINE", 64);
    var bytes = V2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var reader = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var actual = new V2BlockReuseIndex<>(reader, OBJ_REF_SERIALIZER, options);
    for (int slot = 0; slot < reader.blockEntryCount(0); slot++) {
      var key = reader.blockElement(0, slot).key();
      actual.remove(key);
      expected.remove(key);
    }
    var first = IndexKey.key(ByteBuffer.wrap(new byte[] {0}));
    actual.put(first, ObjRef.objRef("prepend", 999));
    expected.put(first, ObjRef.objRef("prepend", 999));
    for (int i = 0; i < 40; i++) {
      var key = IndexKey.key("between-boundaries-" + i);
      var value = ObjRef.objRef("insert", i + 1000L);
      actual.put(key, value);
      expected.put(key, value);
    }
    verify(expected, actual);
    for (int target : new int[] {1, 200, 1000}) {
      var parts = actual.splitByTargetSize(target);
      var keys = new ArrayList<IndexKey>();
      for (var part : parts) {
        var serialized = part.serialize();
        assertThat(serialized.remaining()).isEqualTo(part.estimatedSerializedSize());
        if (part.asKeyList().size() > 1)
          assertThat(serialized.remaining()).isLessThanOrEqualTo(target);
        keys.addAll(part.asKeyList());
      }
      assertThat(keys).containsExactlyElementsOf(expected.asKeyList());
    }
    var parts = actual.splitByTargetSize(200);
    var part = parts.getFirst();
    var removed = requireNonNull(part.first());
    part.remove(removed);
    assertThat(actual.containsElement(removed)).isTrue();
    var snapshot = actual.serialize();
    for (var key : List.copyOf(actual.asKeyList())) actual.remove(key);
    assertThat(actual.hasElements()).isFalse();
    assertThat(actual.first()).isNull();
    assertThat(actual.last()).isNull();
    assertThat(actual.splitByTargetSize(1)).isEmpty();
    assertThat(new V2IndexReader<>(actual.serialize(), OBJ_REF_SERIALIZER).hasElements()).isFalse();
    assertThat(new V2IndexReader<>(snapshot, OBJ_REF_SERIALIZER).hasElements()).isTrue();
  }

  @Test
  void emptyTombstonesAndPrefixIteration() {
    var actual = new V2BlockReuseIndex<>(OBJ_REF_SERIALIZER, V2IndexWriter.Options.defaults(2));
    assertThat(actual.remove(IndexKey.key("missing"))).isFalse();
    assertThat(actual.isModified()).isFalse();
    assertThat(actual.hasElements()).isFalse();
    var key = IndexKey.key("a/one");
    assertThat(actual.add(indexElement(key, null))).isEqualTo(IndexSpi.AddResult.NEW_KEY);
    assertThat(actual.containsElement(key)).isTrue();
    assertThat(actual.contains(key)).isFalse();
    assertThat(actual.add(indexElement(key, ObjRef.objRef("value", 1))))
        .isEqualTo(IndexSpi.AddResult.UPDATED_NO_VALUE);
    actual.put(IndexKey.key("a/two"), ObjRef.objRef("value", 2));
    actual.put(IndexKey.key("b/three"), ObjRef.objRef("value", 3));
    var prefix = IndexKey.key("a/");
    var keys = new ArrayList<IndexKey>();
    actual.elementIterator(prefix, prefix, false).forEachRemaining(e -> keys.add(e.key()));
    assertThat(keys).containsExactly(key, IndexKey.key("a/two"));
    assertThatThrownBy(() -> actual.reverseElementIterator(prefix, prefix, false))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> actual.splitByTargetSize(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void positionedDirectInputAndZeroByteValues() {
    var codec =
        new IndexValueSerializer<Integer>() {
          @Override
          public int serializedSize(@Nullable Integer value) {
            return 0;
          }

          @Override
          public @NonNull ByteBuffer serialize(
              @Nullable Integer value, @NonNull ByteBuffer target) {
            return target;
          }

          @Override
          public Integer deserialize(@NonNull ByteBuffer input) {
            return 42;
          }

          @Override
          public boolean isNullSerialized(@NonNull ByteBuffer input) {
            return false;
          }

          @Override
          public void skip(@NonNull ByteBuffer input) {}
        };
    var options = V2IndexWriter.Options.defaults(1);
    var source = new V2IndexImpl<>(codec, options);
    for (int i = 0; i < 20; i++) source.put(IndexKey.key(i), 42);
    var bytes = source.serialize();
    var direct = ByteBuffer.allocateDirect(bytes.remaining() + 11);
    direct.position(7).put(bytes).flip().position(7);
    int position = direct.position();
    var actual = new V2BlockReuseIndex<>(direct, codec, options);
    actual.remove(IndexKey.key(4));
    actual.put(IndexKey.key(20), 42);
    var written = new V2IndexReader<>(actual.serialize(), codec);
    assertThat(direct.position()).isEqualTo(position);
    assertThat(written.count).isEqualTo(20);
    assertThat(written.get(IndexKey.key(4))).isNull();
    assertThat(written.get(IndexKey.key(20))).isEqualTo(42);
  }

  @Test
  void maximumEscapedKeysAndIndexKeyValues() {
    var options = new V2IndexWriter.Options(2, true, V2IndexWriter.DirectoryEncoding.PREFIX, 3);
    var expected = new V2IndexImpl<>(INDEX_KEY_SERIALIZER, options);
    var keys = new ArrayList<IndexKey>();
    for (int i = 0; i < 8; i++) {
      var raw = new byte[IndexKey.MAX_LENGTH];
      Arrays.fill(raw, (byte) 1);
      raw[raw.length - 1] = (byte) i;
      var key = IndexKey.key(ByteBuffer.wrap(raw));
      keys.add(key);
      expected.put(key, key);
    }
    var actual = new V2BlockReuseIndex<>(expected.serialize(), INDEX_KEY_SERIALIZER, options);
    expected.put(keys.get(2), IndexKey.key("short"));
    actual.put(keys.get(2), IndexKey.key("short"));
    expected.remove(keys.get(3));
    actual.remove(keys.get(3));
    var written = new V2IndexReader<>(actual.serialize(), INDEX_KEY_SERIALIZER);
    assertThat(written.asKeyList()).containsExactlyElementsOf(expected.asKeyList());
    for (var key : keys) assertThat(written.get(key)).isEqualTo(expected.get(key));
  }

  private static void verify(IndexSpi<ObjRef> expected, V2BlockReuseIndex<ObjRef> actual) {
    assertThat(actual.asKeyList()).containsExactlyElementsOf(expected.asKeyList());
    assertThat(actual.first()).isEqualTo(expected.first());
    assertThat(actual.last()).isEqualTo(expected.last());
    var bytes = actual.serialize();
    assertThat(bytes.remaining()).isEqualTo(actual.estimatedSerializedSize());
    var reader = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    for (var key : expected.asKeyList()) {
      assertThat(actual.get(key)).isEqualTo(expected.get(key));
      assertThat(actual.contains(key)).isEqualTo(expected.contains(key));
      assertThat(reader.get(key)).isEqualTo(expected.get(key));
    }
    var reverse = new ArrayList<IndexKey>();
    actual.reverseElementIterator().forEachRemaining(e -> reverse.add(e.key()));
    assertThat(reverse).containsExactlyElementsOf(expected.asKeyList().reversed());
    if (expected.asKeyList().size() > 5) {
      var lower = expected.asKeyList().get(1);
      var higher = expected.asKeyList().get(expected.asKeyList().size() - 2);
      var keys = new ArrayList<IndexKey>();
      actual.elementIterator(lower, higher, false).forEachRemaining(e -> keys.add(e.key()));
      assertThat(keys)
          .containsExactlyElementsOf(
              expected.asKeyList().subList(1, expected.asKeyList().size() - 1));
      keys.clear();
      actual.reverseElementIterator(lower, higher, false).forEachRemaining(e -> keys.add(e.key()));
      assertThat(keys)
          .containsExactlyElementsOf(
              expected.asKeyList().subList(1, expected.asKeyList().size() - 1).reversed());
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

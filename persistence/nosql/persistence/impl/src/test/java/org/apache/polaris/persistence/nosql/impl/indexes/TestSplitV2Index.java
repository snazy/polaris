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
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestSplitV2Index {
  static Stream<Arguments> cases() {
    return Stream.of("HIERARCHICAL", "WORDS", "UUID", "BINARY")
        .flatMap(shape ->
            Stream.of(SplitV2Format.Layout.values())
                .flatMap(layout ->
                    Stream.of(V2IndexWriter.DirectoryEncoding.values())
                        .map(directory ->
                            arguments(shape, new SplitV2Format.Options(layout, 16, directory)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void bulkAndRepeatedSortedMergesMatchV1(String shape, SplitV2Format.Options options) {
    for (int size : new int[] {0, 1, 17, 129}) {
      var data = PrototypeIndexData.create(size, shape);
      var expected = data.fresh("BASELINE", 64);
      var bytes = SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
      verify(expected, new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER));
      var snapshot = bytes.duplicate();
      var random = new Random(871 + size);
      for (int pass = 0; pass < 8; pass++) {
        var changes = new TreeMap<IndexKey, InternalIndexElement<ObjRef>>();
        for (int i = 0; i < 24; i++) {
          var key =
              i % 5 == 0 || size == 0
                  ? IndexKey.key("new-" + random.nextInt(100))
                  : data.keys().get(random.nextInt(size));
          if (i % 4 == 0) {
            expected.remove(key);
            changes.put(key, null);
          } else {
            ObjRef value =
                i % 7 == 0 ? null : ObjRef.objRef(i % 3 == 0 ? "longer-value" : "v", 9000L + i, 2);
            var element = indexElement(key, value);
            expected.add(element);
            changes.put(key, element);
          }
        }
        var pending = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
        changes.forEach(
            (key, element) -> pending.add(new SplitV2IndexWriter.Change<>(key, element)));
        bytes =
            SplitV2IndexWriter.merge(
                new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER), pending, options);
        verify(expected, new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER));
      }
      assertThat(new SplitV2IndexReader<>(snapshot, OBJ_REF_SERIALIZER).count).isEqualTo(size);
    }
  }

  @ParameterizedTest
  @MethodSource("cases")
  void replacementsReuseKeyBytesAndUntouchedBlocksWithoutSkippingValues(
      String shape, SplitV2Format.Options options) {
    var expected = PrototypeIndexData.create(129, shape).fresh("BASELINE", 64);
    var bytes = SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var codec = new CountingCodec();
    var source = new SplitV2IndexReader<>(bytes, codec);
    var key = expected.asKeyList().get(65);
    var value = ObjRef.objRef("replacement-with-new-size", 7777, 2);
    var change = new SplitV2IndexWriter.Change<>(key, indexElement(key, value));
    int target = source.blockForKey(key);
    var written = SplitV2IndexWriter.merge(source, List.of(change), options);
    assertThat(codec.decodes).isZero();
    assertThat(codec.skips).isZero();
    var result = new SplitV2IndexReader<>(written, codec);
    for (int block = 0; block < source.blockCount; block++) {
      var old = source.block(block);
      var next = result.block(block);
      if (block != target)
        assertThat(written.slice(next.start(), next.end() - next.start()))
            .isEqualTo(bytes.slice(old.start(), old.end() - old.start()));
      else
        assertThat(written.slice(next.keyTable(), next.valueMeta() - next.keyTable()))
            .isEqualTo(bytes.slice(old.keyTable(), old.valueMeta() - old.keyTable()));
    }
    expected.put(key, value);
    verify(expected, result);
    assertThat(codec.skips).isZero();
    assertThat(result.get(key)).isSameAs(requireNonNull(result.getElement(key)).valueNullable());
  }

  @ParameterizedTest
  @MethodSource("cases")
  void directPositionedInputRemovalOfBlocksAndMixedPolicies(
      String shape, SplitV2Format.Options options) {
    var expected = PrototypeIndexData.create(65, shape).fresh("BASELINE", 64);
    var bytes = SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var direct = ByteBuffer.allocateDirect(bytes.remaining() + 11);
    direct.position(7).put(bytes).flip().position(7);
    var source = new SplitV2IndexReader<>(direct, OBJ_REF_SERIALIZER);
    var changes = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
    for (int i = 0; i < source.blockEntries(0); i++) {
      var key = expected.asKeyList().get(i);
      changes.add(new SplitV2IndexWriter.Change<>(key, null));
    }
    for (var change : changes) expected.remove(change.key());
    var different =
        new SplitV2Format.Options(
            options.layout() == SplitV2Format.Layout.PREFIX_ENDS
                ? SplitV2Format.Layout.FRONT_ENDS
                : SplitV2Format.Layout.PREFIX_ENDS,
            8,
            options.directory());
    var written = SplitV2IndexWriter.merge(source, changes, different);
    verify(expected, new SplitV2IndexReader<>(written, OBJ_REF_SERIALIZER));
    assertThat(direct.position()).isEqualTo(7);
    var removeAll = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
    for (var key : expected.asKeyList()) removeAll.add(new SplitV2IndexWriter.Change<>(key, null));
    var empty =
        SplitV2IndexWriter.merge(
            new SplitV2IndexReader<>(written, OBJ_REF_SERIALIZER), removeAll, different);
    assertThat(new SplitV2IndexReader<>(empty, OBJ_REF_SERIALIZER).count).isZero();
    assertThatThrownBy(() -> new V2IndexReader<>(empty, OBJ_REF_SERIALIZER))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SplitV2IndexReader<>(expected.serialize(), OBJ_REF_SERIALIZER))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rejectsUnsortedChangesAndPublishesConcurrentValues(
      String shape, SplitV2Format.Options options) throws Exception {
    var expected = PrototypeIndexData.create(129, shape).fresh("BASELINE", 64);
    var bytes = SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options);
    var source = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var lower = expected.asKeyList().get(20);
    var higher = expected.asKeyList().get(90);
    assertThatThrownBy(() ->
            SplitV2IndexWriter.merge(
                source,
                List.of(
                    new SplitV2IndexWriter.Change<>(higher, null),
                    new SplitV2IndexWriter.Change<>(lower, null)),
                options))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            SplitV2IndexWriter.merge(
                source,
                List.of(
                    new SplitV2IndexWriter.Change<>(lower, null),
                    new SplitV2IndexWriter.Change<>(lower, null)),
                options))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(SplitV2IndexWriter.merge(source, List.of(), options)).isEqualTo(bytes);
    var key = expected.asKeyList().get(100);
    try (var executor = Executors.newFixedThreadPool(4)) {
      var tasks = new ArrayList<Callable<ObjRef>>();
      for (int i = 0; i < 40; i++) tasks.add(() -> source.get(key));
      var results = executor.invokeAll(tasks);
      var first = results.getFirst().get();
      for (var result : results) assertThat(result.get()).isSameAs(first);
    }
  }

  static Stream<SplitV2Format.Options> layouts() {
    return Stream.of(SplitV2Format.Layout.values())
        .flatMap(layout ->
            Stream.of(V2IndexWriter.DirectoryEncoding.values())
                .map(directory -> new SplitV2Format.Options(layout, 256, directory)));
  }

  @ParameterizedTest
  @MethodSource("layouts")
  void wideKeyAndValueOffsetsAndOversizedValues(SplitV2Format.Options options) {
    var codec =
        new IndexValueSerializer<byte[]>() {
          @Override
          public int serializedSize(byte @Nullable [] value) {
            return value == null ? 1 : varIntLen(value.length + 1) + value.length;
          }

          @Override
          public @NonNull ByteBuffer serialize(
              byte @Nullable [] value, @NonNull ByteBuffer output) {
            if (value == null) return output.put((byte) 0);
            putVarInt(output, value.length + 1);
            return output.put(value);
          }

          @Override
          public byte[] deserialize(@NonNull ByteBuffer input) {
            int length = readVarInt(input) - 1;
            if (length < 0) return null;
            var result = new byte[length];
            input.get(result);
            return result;
          }

          @Override
          public boolean isNullSerialized(@NonNull ByteBuffer input) {
            return input.get(input.position()) == 0;
          }

          @Override
          public void skip(@NonNull ByteBuffer input) {
            throw new AssertionError("No value skip needed");
          }
        };
    var original = new IndexImpl<>(codec);
    var random = new Random(73);
    for (int i = 0; i < 256; i++) {
      var key = new byte[IndexKey.MAX_LENGTH];
      random.nextBytes(key);
      original.put(IndexKey.key(ByteBuffer.wrap(key)), new byte[i == 2 ? 70000 : i % 4]);
    }
    var bytes = SplitV2IndexWriter.write(original, codec, options);
    var reader = new SplitV2IndexReader<>(bytes, codec);
    assertThat(reader.block(0).keyWidth()).isEqualTo(4);
    assertThat(reader.block(0).valueWidth()).isEqualTo(4);
    for (var key : original.asKeyList())
      assertThat(reader.get(key)).containsExactly(original.get(key));
    var removeKey = original.asKeyList().get(20);
    var changes = List.of(new SplitV2IndexWriter.Change<byte[]>(removeKey, null));
    original.remove(removeKey);
    var parts = SplitV2IndexWriter.mergeSplit(reader, changes, options, 16384);
    var keys = new ArrayList<IndexKey>();
    for (var part : parts) {
      var read = new SplitV2IndexReader<>(part, codec);
      keys.addAll(read.asKeyList());
      if (read.count > 1) assertThat(part.remaining()).isLessThanOrEqualTo(16384);
      for (var key : read.asKeyList()) assertThat(read.get(key)).containsExactly(original.get(key));
    }
    assertThat(keys).containsExactlyElementsOf(original.asKeyList());
  }

  @ParameterizedTest
  @MethodSource("layouts")
  void zeroByteFixedStrideAndBlockAwareSplitting(SplitV2Format.Options options) {
    var codec =
        new IndexValueSerializer<Integer>() {
          @Override
          public int serializedSize(@Nullable Integer value) {
            return 0;
          }

          @Override
          public @NonNull ByteBuffer serialize(
              @Nullable Integer value, @NonNull ByteBuffer output) {
            return output;
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
          public void skip(@NonNull ByteBuffer input) {
            throw new AssertionError("No value skip needed");
          }
        };
    var original = new IndexImpl<>(codec);
    for (int i = 0; i < 300; i++) original.put(IndexKey.key(i), 42);
    var bytes = SplitV2IndexWriter.write(original, codec, options);
    var reader = new SplitV2IndexReader<>(bytes, codec);
    assertThat(reader.block(0).mode()).isEqualTo(SplitV2Format.STRIDE);
    for (var key : original.asKeyList()) assertThat(reader.get(key)).isEqualTo(42);
    var parts = SplitV2IndexWriter.mergeSplit(reader, List.of(), options, 1000);
    var keys = new ArrayList<IndexKey>();
    for (var part : parts) {
      var read = new SplitV2IndexReader<>(part, codec);
      keys.addAll(read.asKeyList());
      if (read.count > 1) assertThat(part.remaining()).isLessThanOrEqualTo(1000);
      for (var key : read.asKeyList()) assertThat(read.get(key)).isEqualTo(42);
    }
    assertThat(keys).containsExactlyElementsOf(original.asKeyList());
    assertThatThrownBy(() -> SplitV2IndexWriter.mergeSplit(reader, List.of(), options, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void mergedSplitsPreserveAllValuesAndBounds(String shape, SplitV2Format.Options options) {
    var expected = PrototypeIndexData.create(129, shape).fresh("BASELINE", 64);
    var reader =
        new SplitV2IndexReader<>(
            SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options), OBJ_REF_SERIALIZER);
    var changes = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
    for (int i = 0; i < expected.asKeyList().size(); i += 5) {
      var key = expected.asKeyList().get(i);
      changes.add(
          new SplitV2IndexWriter.Change<>(
              key, i % 2 == 0 ? null : indexElement(key, ObjRef.objRef("updated", i + 9999L))));
    }
    for (var change : changes) {
      if (change.element() == null) expected.remove(change.key());
      else expected.add(change.element());
    }
    for (int target : new int[] {1, 200, 1000, 1000000}) {
      var parts = SplitV2IndexWriter.mergeSplit(reader, changes, options, target);
      var keys = new ArrayList<IndexKey>();
      for (var part : parts) {
        var read = new SplitV2IndexReader<>(part, OBJ_REF_SERIALIZER);
        keys.addAll(read.asKeyList());
        if (read.count > 1) assertThat(part.remaining()).isLessThanOrEqualTo(target);
        for (var key : read.asKeyList()) assertThat(read.get(key)).isEqualTo(expected.get(key));
      }
      assertThat(keys).containsExactlyElementsOf(expected.asKeyList());
    }
  }

  @ParameterizedTest
  @MethodSource("layouts")
  void differentRestartPoliciesAndPrefixDirectoryRemainIndependent(SplitV2Format.Options options) {
    var original = PrototypeIndexData.create(513, "WORDS").fresh("BASELINE", 64);
    for (int restart : new int[] {16, 32}) {
      var policy =
          new SplitV2Format.Options(
              options.layout(), 16, V2IndexWriter.DirectoryEncoding.PREFIX, restart);
      var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, policy);
      var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
      verify(original, reader);
      var key = original.asKeyList().get(256);
      var changes =
          List.of(
              new SplitV2IndexWriter.Change<>(
                  key, indexElement(key, ObjRef.objRef("replacement", 9999))));
      var output = SplitV2IndexWriter.merge(reader, changes, policy);
      assertThat(new SplitV2IndexReader<>(output, OBJ_REF_SERIALIZER).get(key))
          .isEqualTo(ObjRef.objRef("replacement", 9999));
    }
  }

  static Stream<SplitV2Format.Options> frontPolicies() {
    return Stream.of(SplitV2Format.Layout.FRONT_ENDS, SplitV2Format.Layout.FRONT_LENGTHS)
        .flatMap(layout ->
            Stream.of(V2IndexWriter.DirectoryEncoding.values())
                .flatMap(directory ->
                    Stream.of(8, 16, 32)
                        .map(
                            restart -> new SplitV2Format.Options(layout, 64, directory, restart))));
  }

  @ParameterizedTest
  @MethodSource("frontPolicies")
  void structuralEditsDoNotAccumulateUnnecessaryFullKeyRestarts(SplitV2Format.Options options) {
    var original = PrototypeIndexData.create(129, "HIERARCHICAL").fresh("BASELINE", 64);
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var raw = original.asKeyList().get(65).asByteBuffer();
    var inserted =
        IndexKey.key(ByteBuffer.allocate(raw.remaining() + 1).put(raw).put((byte) 127).flip());
    for (int pass = 0; pass < 12; pass++) {
      var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
      var element = pass % 2 == 0 ? indexElement(inserted, ObjRef.objRef("new", 777)) : null;
      bytes =
          SplitV2IndexWriter.merge(
              reader, List.of(new SplitV2IndexWriter.Change<>(inserted, element)), options);
      var result = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
      for (int block = 0; block < result.blockCount; block++) {
        var b = result.block(block);
        for (int slot = 1; slot < b.count(); slot++) {
          if (slot % options.keyRestart() != 0
              && (int) SplitV2Format.varIntAt(result.source, result.keyStart(b, slot)) == 0)
            assertThat(
                    SplitV2Format.commonPrefix(
                        result.encodedKey(b.first() + slot - 1),
                        result.encodedKey(b.first() + slot)))
                .isZero();
        }
      }
    }
    assertThat(new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER).asKeyList())
        .containsExactlyElementsOf(original.asKeyList());
  }

  static Stream<SplitV2Format.Options> sharedPolicies() {
    return layouts().filter(options -> options.layout() == SplitV2Format.Layout.PREFIX_ENDS);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void commonPrefixRecoversAfterRemovingAnOutlier(SplitV2Format.Options options) {
    var original = PrototypeIndexData.create(129, "HIERARCHICAL").fresh("BASELINE", 64);
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var key = IndexKey.key("a");
    var inserted =
        SplitV2IndexWriter.merge(
            new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER),
            List.of(
                new SplitV2IndexWriter.Change<>(key, indexElement(key, ObjRef.objRef("new", 777)))),
            options);
    var withOutlier = new SplitV2IndexReader<>(inserted, OBJ_REF_SERIALIZER);
    var removed =
        SplitV2IndexWriter.merge(
            withOutlier, List.of(new SplitV2IndexWriter.Change<ObjRef>(key, null)), options);
    var recovered = new SplitV2IndexReader<>(removed, OBJ_REF_SERIALIZER);
    assertThat(recovered.block(0).prefix()).isGreaterThan(withOutlier.block(0).prefix());
    verify(original, recovered);
  }

  private static void verify(IndexSpi<ObjRef> expected, SplitV2IndexReader<ObjRef> actual) {
    assertThat(actual.asKeyList()).containsExactlyElementsOf(expected.asKeyList());
    var forward = new ArrayList<IndexKey>();
    actual.elementIterator().forEachRemaining(e -> forward.add(e.key()));
    assertThat(forward).containsExactlyElementsOf(expected.asKeyList());
    var reverse = new ArrayList<IndexKey>();
    actual.reverseElementIterator().forEachRemaining(e -> reverse.add(e.key()));
    assertThat(reverse).containsExactlyElementsOf(expected.asKeyList().reversed());
    assertThat(actual.first()).isEqualTo(expected.first());
    assertThat(actual.last()).isEqualTo(expected.last());
    for (var key : expected.asKeyList()) {
      assertThat(actual.get(key)).isEqualTo(expected.get(key));
      assertThat(actual.contains(key)).isEqualTo(expected.contains(key));
    }
    var absent = IndexKey.key("not-present-in-this-dataset");
    assertThat(actual.get(absent)).isEqualTo(expected.get(absent));
    if (expected.asKeyList().size() > 5) {
      var lower = expected.asKeyList().get(1);
      var upper = expected.asKeyList().get(expected.asKeyList().size() - 2);
      var keys = new ArrayList<IndexKey>();
      actual.elementIterator(lower, upper, false).forEachRemaining(e -> keys.add(e.key()));
      assertThat(keys)
          .containsExactlyElementsOf(
              expected.asKeyList().subList(1, expected.asKeyList().size() - 1));
    }
  }

  private static final class CountingCodec implements IndexValueSerializer<ObjRef> {
    int skips;
    int decodes;

    @Override
    public int serializedSize(@Nullable ObjRef value) {
      return OBJ_REF_SERIALIZER.serializedSize(value);
    }

    @Override
    public @NonNull ByteBuffer serialize(@Nullable ObjRef value, @NonNull ByteBuffer output) {
      return OBJ_REF_SERIALIZER.serialize(value, output);
    }

    @Override
    public ObjRef deserialize(@NonNull ByteBuffer bytes) {
      decodes++;
      return OBJ_REF_SERIALIZER.deserialize(bytes);
    }

    @Override
    public boolean isNullSerialized(@NonNull ByteBuffer bytes) {
      return OBJ_REF_SERIALIZER.isNullSerialized(bytes);
    }

    @Override
    public void skip(@NonNull ByteBuffer bytes) {
      skips++;
      OBJ_REF_SERIALIZER.skip(bytes);
    }
  }
}

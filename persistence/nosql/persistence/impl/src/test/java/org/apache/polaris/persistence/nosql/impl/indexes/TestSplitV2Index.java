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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestSplitV2Index {
  @Test
  void encodedCommonPrefixesAreDeterminedBySortedEndpoints() {
    var encoded = new TreeMap<IndexKey, byte[]>();
    // Include empty keys, prefix-of-another keys, NUL, and both escaped byte values.
    for (int length = 0; length <= 4; length++) {
      for (int bits = 0; bits < (1 << (2 * length)); bits++) {
        var raw = new byte[length];
        for (int i = 0; i < length; i++) raw[i] = (byte) ((bits >>> (2 * i)) & 3);
        var key = IndexKey.key(ByteBuffer.wrap(raw));
        encoded.put(key, key.serialize(ByteBuffer.allocate(key.serializedSize())).array());
      }
    }
    var keys = new ArrayList<>(encoded.values());
    for (int first = 0; first < keys.size(); first++) {
      int prefix = keys.get(first).length;
      for (int last = first; last < keys.size(); last++) {
        int endpoints = SplitV2Format.commonPrefix(keys.get(first), keys.get(last));
        prefix = Math.min(prefix, endpoints);
        assertThat(endpoints).isEqualTo(prefix);
      }
    }
  }

  static Stream<Arguments> cases() {
    return Stream.of("HIERARCHICAL", "WORDS", "UUID", "BINARY")
        .flatMap(shape ->
            Stream.of(SplitV2Format.Layout.values())
                .flatMap(layout ->
                    Stream.of(SplitV2Format.DirectoryEncoding.values())
                        .map(directory ->
                            arguments(shape, new SplitV2Format.Options(layout, 16, directory)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void bulkAndRepeatedSortedMergesMatchV1(String shape, SplitV2Format.Options options) {
    for (int size : new int[] {0, 1, 17, 129}) {
      var data = V2TestData.create(size, shape);
      var expected = data.mutable();
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
    var expected = V2TestData.create(129, shape).mutable();
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
    var expected = V2TestData.create(65, shape).mutable();
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
    assertThatThrownBy(() -> new SplitV2IndexReader<>(expected.serialize(), OBJ_REF_SERIALIZER))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void rejectsUnsortedChangesAndPublishesConcurrentValues(
      String shape, SplitV2Format.Options options) throws Exception {
    var expected = V2TestData.create(129, shape).mutable();
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
            Stream.of(SplitV2Format.DirectoryEncoding.values())
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
    var expected = V2TestData.create(129, shape).mutable();
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
    var original = V2TestData.create(513, "WORDS").mutable();
    for (int restart : new int[] {4, 8, 16, 32}) {
      var policy =
          new SplitV2Format.Options(
              options.layout(), 16, SplitV2Format.DirectoryEncoding.PREFIX, restart);
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
    return Stream.of(
            SplitV2Format.Layout.FRONT_ENDS,
            SplitV2Format.Layout.FRONT_LENGTHS,
            SplitV2Format.Layout.FRONT_PREFIX_ENDS)
        .flatMap(layout ->
            Stream.of(SplitV2Format.DirectoryEncoding.values())
                .flatMap(directory ->
                    Stream.of(4, 8, 16, 32)
                        .map(
                            restart -> new SplitV2Format.Options(layout, 64, directory, restart))));
  }

  @ParameterizedTest
  @MethodSource("frontPolicies")
  void structuralEditsDoNotAccumulateUnnecessaryFullKeyRestarts(SplitV2Format.Options options) {
    var original = V2TestData.create(129, "HIERARCHICAL").mutable();
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
                .isEqualTo(b.prefix());
        }
      }
    }
    assertThat(new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER).asKeyList())
        .containsExactlyElementsOf(original.asKeyList());
  }

  static Stream<SplitV2Format.Options> sharedPolicies() {
    return layouts()
        .filter(options ->
            options.layout() == SplitV2Format.Layout.PREFIX_ENDS
                || options.layout() == SplitV2Format.Layout.FRONT_PREFIX_ENDS);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void structuralPrefixChangesCrossVarIntWidths(SplitV2Format.Options options) {
    var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < 30; i++)
      original.put(
          IndexKey.key("p".repeat(180) + String.format("/key-%04d", i)), ObjRef.objRef("value", i));
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var initial = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    assertThat(initial.block(0).prefix()).isGreaterThan(127);
    var outlier = IndexKey.key("a");
    original.put(outlier, ObjRef.objRef("outlier", 999));
    bytes =
        SplitV2IndexWriter.merge(
            initial,
            List.of(
                new SplitV2IndexWriter.Change<>(
                    outlier, indexElement(outlier, original.get(outlier)))),
            options);
    var expanded = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    assertThat(expanded.block(0).prefix()).isZero();
    verify(original, expanded);
    original.remove(outlier);
    bytes =
        SplitV2IndexWriter.merge(
            expanded, List.of(new SplitV2IndexWriter.Change<ObjRef>(outlier, null)), options);
    var compacted = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    assertThat(compacted.block(0).prefix()).isGreaterThan(127);
    verify(original, compacted);
    verify(original, initial);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void commonPrefixRecoversAfterRemovingAnOutlier(SplitV2Format.Options options) {
    var original = V2TestData.create(129, "HIERARCHICAL").mutable();
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

  @ParameterizedTest
  @MethodSource("layouts")
  void compactHeadersAndImplicitKeyPayloadSize(SplitV2Format.Options options) {
    var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var empty = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    assertThat(empty.remaining()).isEqualTo(12);
    var emptyReader = new SplitV2IndexReader<>(empty, OBJ_REF_SERIALIZER);
    assertThat(emptyReader.directoryOffset).isEqualTo(SplitV2Format.fileHeaderSize(0));
    assertThat(emptyReader.count).isZero();
    var key = IndexKey.key("common/key");
    original.add(indexElement(key, null));
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var block = reader.block(0);
    int flags = Byte.toUnsignedInt(bytes.get(block.start()));
    int header =
        options.layout() == SplitV2Format.Layout.PREFIX_ENDS
                || options.layout() == SplitV2Format.Layout.FRONT_PREFIX_ENDS
            ? 3
            : 1;
    assertThat(SplitV2Format.headerSize(flags)).isEqualTo(header);
    assertThat(block.keyTable() - block.start()).isEqualTo(header);
    assertThat(block.keyBytes()).isZero();
    assertThat(block.keyPayload()).isEqualTo(block.valueMeta());
    assertThat(bytes.remaining())
        .isEqualTo(
            SplitV2Format.fileHeaderSize(1)
                + header
                + OBJ_REF_SERIALIZER.serializedSize(null)
                + SplitV2Format.DIRECTORY_HEADER
                + SplitV2Format.DESCRIPTOR
                + key.serializedSize()
                + (options.directory() == SplitV2Format.DirectoryEncoding.PREFIX
                    ? 1 + varIntLen(key.serializedSize())
                    : 0));
    verify(original, reader);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void encodedBinaryAndLongUrlPrefixesSurviveGrowthAndShrink(SplitV2Format.Options options) {
    for (boolean binary : new boolean[] {false, true}) {
      var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
      String root = "https://example.org/warehouse/" + "segment/".repeat(55);
      for (int i = 0; i < 41; i++) {
        var key =
            binary
                ? IndexKey.key(ByteBuffer.wrap(new byte[] {0, 1, 0, 1, (byte) i, 0}))
                : IndexKey.key(root + String.format("%04d", i));
        original.add(indexElement(key, i % 5 == 0 ? null : ObjRef.objRef("value", i)));
      }
      var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
      var saved = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
      var snapshotKeys = List.copyOf(original.asKeyList());
      for (int round = 0; round < 3; round++) {
        var changes = new TreeMap<IndexKey, InternalIndexElement<ObjRef>>();
        var keys = original.asKeyList();
        for (int slot : new int[] {0, 7, 8, 15, 16, 24}) {
          var key = keys.get(slot);
          changes.put(key, round == 1 ? indexElement(key, null) : null);
        }
        var inserted =
            binary
                ? IndexKey.key(ByteBuffer.wrap(new byte[] {0, 1, 0, (byte) round, 127}))
                : IndexKey.key(root.substring(0, root.length() - (round + 1) * 8) + "new" + round);
        changes.put(inserted, indexElement(inserted, ObjRef.objRef("inserted", round + 500)));
        var pending = new ArrayList<SplitV2IndexWriter.Change<ObjRef>>();
        changes.forEach((key, element) -> {
          pending.add(new SplitV2IndexWriter.Change<>(key, element));
          if (element == null) original.remove(key);
          else original.add(element);
        });
        bytes =
            SplitV2IndexWriter.merge(
                new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER), pending, options);
        verify(original, new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER));
      }
      assertThat(saved.asKeyList()).containsExactlyElementsOf(snapshotKeys);
      for (int i = 0; i < snapshotKeys.size(); i++)
        assertThat(saved.get(snapshotKeys.get(i)))
            .isEqualTo(i % 5 == 0 ? null : ObjRef.objRef("value", i));
    }
  }

  @ParameterizedTest
  @MethodSource("frontPolicies")
  void restartRecordsStoreOnlyTheSuffixAfterCommonPrefix(SplitV2Format.Options options) {
    var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
    String root = "https://example.org/warehouse/" + "namespace/".repeat(30);
    for (int i = 0; i < 41; i++)
      original.put(IndexKey.key(root + String.format("%04d", i)), ObjRef.objRef("value", i));
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var block = reader.block(0);
    assertThat(block.frontPrefix())
        .isEqualTo(options.layout() == SplitV2Format.Layout.FRONT_PREFIX_ENDS);
    for (int slot = options.keyRestart(); slot < block.count(); slot += options.keyRestart()) {
      int start = reader.keyStart(block, slot);
      long record = SplitV2Format.varIntAt(bytes, start);
      assertThat((int) record).isZero();
      var key = reader.encodedKey(slot);
      assertThat(
              bytes.slice(
                  (int) (record >>> 32), reader.keyLimit(block, slot) - (int) (record >>> 32)))
          .isEqualTo(ByteBuffer.wrap(key, block.prefix(), key.length - block.prefix()));
    }
    verify(original, new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER));
  }

  @ParameterizedTest
  @MethodSource("layouts")
  void rejectsInvalidFlagsAndTruncatedKeyTables(SplitV2Format.Options options) {
    var original = new IndexImpl<>(OBJ_REF_SERIALIZER);
    original.add(indexElement(IndexKey.key("common/a"), null));
    original.add(indexElement(IndexKey.key("common/b"), null));
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var block = reader.block(0);
    int flags = Byte.toUnsignedInt(bytes.get(block.start()));
    for (int invalid :
        new int[] {
          flags | SplitV2Format.PREFIX | SplitV2Format.FRONT_PREFIX,
          flags | SplitV2Format.KEY_BYTE | SplitV2Format.KEY_WIDE,
          flags | SplitV2Format.VALUE_BYTE | SplitV2Format.VALUE_WIDE,
          flags | (3 << SplitV2Format.MODE_SHIFT)
        }) {
      var damaged = ByteBuffer.allocate(bytes.remaining()).put(bytes.duplicate()).flip();
      damaged.put(block.start(), (byte) invalid);
      assertThatThrownBy(() -> new SplitV2IndexReader<>(damaged, OBJ_REF_SERIALIZER).block(0))
          .isInstanceOf(IllegalArgumentException.class);
    }
    assertThat(block.keyWidth()).isEqualTo(1);
    var damaged = ByteBuffer.allocate(bytes.remaining()).put(bytes.duplicate()).flip();
    damaged.put(block.keyTable(), (byte) 255);
    assertThatThrownBy(() -> new SplitV2IndexReader<>(damaged, OBJ_REF_SERIALIZER).block(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncated key area");
    damaged.put(block.keyTable(), (byte) 0);
    damaged.putInt(reader.bodyOffset, 0);
    assertThatThrownBy(() -> new SplitV2IndexReader<>(damaged, OBJ_REF_SERIALIZER))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid directory");
  }

  @ParameterizedTest
  @MethodSource("layouts")
  void malformedIntermediateKeyBoundsAreRejectedOnAccess(SplitV2Format.Options options) {
    var original = V2TestData.create(16, "HIERARCHICAL").mutable();
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var block = reader.block(0);
    int secondEnd =
        SplitV2Format.offset(bytes, block.keyTable() + block.keyWidth(), block.keyWidth());
    for (boolean outside : new boolean[] {true, false}) {
      var damaged = ByteBuffer.allocate(bytes.remaining()).put(bytes.duplicate()).flip();
      SplitV2Format.putOffset(
          damaged.duplicate().position(block.keyTable()),
          outside ? block.keyBytes() + 1 : secondEnd + 1,
          block.keyWidth());
      var invalid = new SplitV2IndexReader<>(damaged, OBJ_REF_SERIALIZER);
      assertThatThrownBy(() -> invalid.encodedKey(outside ? 1 : 2))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @ParameterizedTest
  @MethodSource("frontPolicies")
  void oversizedFrontPrefixIsRejectedOnAccess(SplitV2Format.Options options) {
    var original = V2TestData.create(16, "HIERARCHICAL").mutable();
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var damaged = ByteBuffer.allocate(bytes.remaining()).put(bytes.duplicate()).flip();
    putVarInt(damaged.duplicate().position(reader.keyStart(reader.block(0), 1)), Integer.MAX_VALUE);
    var invalid = new SplitV2IndexReader<>(damaged, OBJ_REF_SERIALIZER);
    assertThatThrownBy(() -> invalid.encodedKey(1)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void mixedBlockKeyModesKeepTheirOwnHeaders(SplitV2Format.Options options) {
    var original = V2TestData.create(41, "HIERARCHICAL").mutable();
    var plain = new SplitV2Format.Options(SplitV2Format.Layout.FRONT_ENDS, 8, options.directory());
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, plain);
    int ordinal = 9;
    for (var layout :
        new SplitV2Format.Layout[] {
          SplitV2Format.Layout.PREFIX_ENDS, SplitV2Format.Layout.FRONT_PREFIX_ENDS
        }) {
      var key = original.asKeyList().get(ordinal);
      var policy = new SplitV2Format.Options(layout, 8, options.directory());
      bytes =
          SplitV2IndexWriter.merge(
              new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER),
              List.of(new SplitV2IndexWriter.Change<ObjRef>(key, null)),
              policy);
      original.remove(key);
      ordinal += 15;
    }
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var modes = new ArrayList<String>();
    for (int i = 0; i < reader.blockCount; i++) {
      var block = reader.block(i);
      modes.add(block.sharedPrefix() ? "PREFIX" : block.frontPrefix() ? "HYBRID" : "FRONT");
    }
    assertThat(modes).contains("FRONT", "PREFIX", "HYBRID");
    verify(original, reader);
  }

  @ParameterizedTest
  @MethodSource("sharedPolicies")
  void zeroPrefixFallbackStaysFrontForValueOnlyChanges(SplitV2Format.Options options) {
    if (options.layout() != SplitV2Format.Layout.FRONT_PREFIX_ENDS) return;
    var original = V2TestData.create(41, "HIERARCHICAL").mutable();
    var outlier = IndexKey.key("a");
    original.put(outlier, ObjRef.objRef("first", 100));
    var bytes = SplitV2IndexWriter.write(original, OBJ_REF_SERIALIZER, options);
    var reader = new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
    var block = reader.block(0);
    assertThat(block.prefix()).isZero();
    assertThat(block.frontPrefix()).isFalse();
    assertThat(block.keyTable() - block.start()).isEqualTo(1);
    var value = ObjRef.objRef("longer replacement", 101);
    var updated =
        SplitV2IndexWriter.merge(
            reader,
            List.of(new SplitV2IndexWriter.Change<>(outlier, indexElement(outlier, value))),
            options);
    var next = new SplitV2IndexReader<>(updated, OBJ_REF_SERIALIZER);
    var nextBlock = next.block(0);
    assertThat(nextBlock.frontPrefix()).isFalse();
    assertThat(updated.slice(nextBlock.keyTable(), nextBlock.valueMeta() - nextBlock.keyTable()))
        .isEqualTo(bytes.slice(block.keyTable(), block.valueMeta() - block.keyTable()));
    original.put(outlier, value);
    verify(original, next);
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

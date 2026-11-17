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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Shared V1 contracts. The legacy writer must remain independent of production replacements. */
class TestIndexV1Compatibility {
  enum Implementation {
    LEGACY {
      @Override
      <V> IndexSpi<V> create(IndexValueSerializer<V> codec) {
        return new IndexLegacyImpl<>(codec);
      }

      @Override
      <V> IndexSpi<V> load(ByteBuffer bytes, IndexValueSerializer<V> codec) {
        return IndexLegacyImpl.deserializeStoreIndex(bytes.duplicate(), codec);
      }
    },
    CURRENT {
      @Override
      <V> IndexSpi<V> create(IndexValueSerializer<V> codec) {
        return IndexesInternal.newStoreIndex(codec);
      }

      @Override
      <V> IndexSpi<V> load(ByteBuffer bytes, IndexValueSerializer<V> codec) {
        return IndexesInternal.deserializeStoreIndex(bytes, codec, true);
      }
    };

    abstract <V> IndexSpi<V> create(IndexValueSerializer<V> codec);

    abstract <V> IndexSpi<V> load(ByteBuffer bytes, IndexValueSerializer<V> codec);
  }

  static Stream<Arguments> histories() {
    return Stream.of(Implementation.values())
        .flatMap(implementation ->
            Stream.of(1L, 42L, 81215L, 0x5eedL).map(seed -> Arguments.of(implementation, seed)));
  }

  @ParameterizedTest
  @MethodSource("histories")
  void editReloadAndSplitAgainstIndependentModel(Implementation implementation, long seed) {
    var random = new Random(seed);
    var keys = keys();
    var model = new TreeMap<IndexKey, IndexKey>();
    var index = implementation.create(INDEX_KEY_SERIALIZER);
    var snapshots = new ArrayList<Snapshot<IndexKey>>();
    var trace =
        new StringBuilder("generator=v1 seed=" + seed + " implementation=" + implementation);
    var dirty = false;
    for (var step = 0; step < 400; step++) {
      var selected = keys.get(random.nextInt(keys.size()));
      var operation = random.nextInt(8);
      trace
          .append('\n')
          .append(step)
          .append(':')
          .append(operation)
          .append(':')
          .append(selected.toString());
      try {
        switch (operation) {
          case 0, 1, 2 -> {
            var value = operation == 2 ? null : randomValue(random);
            trace
                .append(" value=")
                .append(HexFormat.of().formatHex(encoded(INDEX_KEY_SERIALIZER, value)));
            var expected =
                !model.containsKey(selected)
                    ? IndexSpi.AddResult.NEW_KEY
                    : model.get(selected) == null
                        ? IndexSpi.AddResult.UPDATED_NO_VALUE
                        : IndexSpi.AddResult.UPDATED;
            assertThat(index.add(indexElement(selected, value))).isEqualTo(expected);
            model.put(selected, value);
            dirty = true;
          }
          case 3 -> {
            var present = model.containsKey(selected);
            assertThat(index.remove(selected)).isEqualTo(present);
            model.remove(selected);
            dirty |= present;
          }
          case 4 -> {
            // Alternate both readers over bytes produced by either writer.
            var reader = Implementation.values()[random.nextInt(Implementation.values().length)];
            index = reader.load(index.serialize(), INDEX_KEY_SERIALIZER);
            dirty = false;
          }
          case 5 -> {
            var parts = index.splitByTargetSize(128);
            var combined = implementation.create(INDEX_KEY_SERIALIZER);
            var seen = new ArrayList<IndexKey>();
            for (var part : parts) {
              var serialized = part.serialize();
              assertThat(part.estimatedSerializedSize())
                  .isGreaterThanOrEqualTo(serialized.remaining());
              if (serialized.remaining() > 128) {
                assertThat(part.asKeyList()).hasSize(1);
              }
              var expectedPart = new TreeMap<IndexKey, IndexKey>();
              for (var key : part.asKeyList()) {
                assertThat(model).containsKey(key);
                expectedPart.put(key, model.get(key));
                seen.add(key);
              }
              for (var reader : Implementation.values()) {
                assertContents(
                    reader.load(serialized, INDEX_KEY_SERIALIZER),
                    expectedPart,
                    INDEX_KEY_SERIALIZER);
              }
              assertContents(
                  IndexesInternal.deserializeStoreIndex(serialized, INDEX_KEY_SERIALIZER, false),
                  expectedPart,
                  INDEX_KEY_SERIALIZER);
              part.elementIterator().forEachRemaining(combined::add);
            }
            assertThat(seen).containsExactlyElementsOf(model.keySet());
            index = implementation.load(combined.serialize(), INDEX_KEY_SERIALIZER);
            dirty = false;
          }
          default -> {
            var lower = keys.get(random.nextInt(keys.size()));
            var higher = keys.get(random.nextInt(keys.size()));
            if (lower.compareTo(higher) > 0) {
              var swap = lower;
              lower = higher;
              higher = swap;
            }
            if (!lower.equals(higher)) {
              var range = model.subMap(lower, true, higher, true);
              assertEntries(
                  index.elementIterator(lower, higher, false), range, INDEX_KEY_SERIALIZER);
              assertEntries(
                  index.reverseElementIterator(lower, higher, false),
                  range.descendingMap(),
                  INDEX_KEY_SERIALIZER);
            }
            var prefix = key("ns/");
            var expectedPrefix = new TreeMap<IndexKey, IndexKey>();
            model.forEach((key, value) -> {
              if (key.startsWith(prefix)) {
                expectedPrefix.put(key, value);
              }
            });
            assertEntries(
                index.elementIterator(prefix, prefix, false), expectedPrefix, INDEX_KEY_SERIALIZER);
          }
        }
        assertThat(index.isModified()).isEqualTo(dirty);
        assertContents(index, model, INDEX_KEY_SERIALIZER);
        assertThat(index.containsElement(key("absent"))).isFalse();
        assertThat(index.contains(key("absent"))).isFalse();
        assertThat(index.get(key("absent"))).isNull();
        assertThat(index.getElement(key("absent"))).isNull();
        var bytes = index.serialize();
        assertContents(
            IndexesInternal.deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false),
            model,
            INDEX_KEY_SERIALIZER);
        assertThat(index.estimatedSerializedSize()).isGreaterThanOrEqualTo(bytes.remaining());
        assertThat(index.isModified()).isEqualTo(dirty);
        if (step % 32 == 0) {
          snapshots.add(
              new Snapshot<>(
                  bytes,
                  copy(bytes),
                  implementation.load(bytes, INDEX_KEY_SERIALIZER),
                  new TreeMap<>(model)));
          snapshots.add(
              new Snapshot<>(
                  bytes,
                  copy(bytes),
                  IndexesInternal.deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false),
                  new TreeMap<>(model)));
        }
        for (var snapshot : snapshots) {
          assertThat(copy(snapshot.bytes())).containsExactly(snapshot.copy());
          assertContents(snapshot.view(), snapshot.model(), INDEX_KEY_SERIALIZER);
        }
      } catch (AssertionError | RuntimeException failure) {
        throw new AssertionError(trace.toString(), failure);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Implementation.class)
  void replacementPreservesFullObjRefState(Implementation implementation) {
    var codec = OBJ_REF_SERIALIZER;
    var index = implementation.create(codec);
    var key = key("entry");
    var original = objRef("test", 17, 1);
    index.put(key, original);
    index = implementation.load(index.serialize(), codec);
    var snapshot = index.serialize();
    var snapshotCopy = copy(snapshot);
    var model = new TreeMap<IndexKey, ObjRef>();
    // numParts is auxiliary for equality, but must survive every write and reload.
    for (var parts : new int[] {128, 16384, 0, 1}) {
      var replacement = objRef("test", 17, parts);
      assertThat(replacement).isEqualTo(original);
      assertThat(index.put(key, replacement)).isFalse();
      model.put(key, replacement);
      var bytes = index.serialize();
      assertThat(index.estimatedSerializedSize()).isGreaterThanOrEqualTo(bytes.remaining());
      for (var reader : Implementation.values()) {
        assertContents(reader.load(bytes, codec), model, codec);
      }
      assertContents(IndexesInternal.deserializeStoreIndex(bytes, codec, false), model, codec);
      index = implementation.load(bytes, codec);
      assertThat(copy(snapshot)).containsExactly(snapshotCopy);
    }
  }

  private static List<IndexKey> keys() {
    var keys = new ArrayList<IndexKey>();
    for (var i = 0; i < 48; i++) {
      keys.add(key("ns/" + (i / 8) + "/table-" + i));
    }
    keys.add(key(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, (byte) 255})));
    keys.add(key(ByteBuffer.wrap(new byte[] {0, 1, 2, 3, (byte) 254})));
    keys.add(key("x".repeat(IndexKey.MAX_LENGTH)));
    keys.add(key("x".repeat(IndexKey.MAX_LENGTH - 1) + "y"));
    keys.add(key("é/namespace/表"));
    keys.sort(IndexKey::compareTo);
    return keys;
  }

  private static IndexKey randomValue(Random random) {
    var bytes = new byte[new int[] {1, 2, 63, 127, 128, 255, 500}[random.nextInt(7)]];
    random.nextBytes(bytes);
    return key(ByteBuffer.wrap(bytes));
  }

  private static <V> void assertContents(
      IndexSpi<V> index, NavigableMap<IndexKey, V> model, IndexValueSerializer<V> codec) {
    assertThat(index.hasElements()).isEqualTo(!model.isEmpty());
    assertThat(index.asKeyList()).containsExactlyElementsOf(model.keySet());
    assertThat(index.first()).isEqualTo(model.isEmpty() ? null : model.firstKey());
    assertThat(index.last()).isEqualTo(model.isEmpty() ? null : model.lastKey());
    assertEntries(index.elementIterator(), model, codec);
    assertEntries(index.reverseElementIterator(), model.descendingMap(), codec);
    var publicKeys = new ArrayList<IndexKey>();
    index
        .iterator()
        .forEachRemaining(element -> {
          publicKeys.add(element.key());
          assertThat(encoded(codec, element.value()))
              .containsExactly(encoded(codec, model.get(element.key())));
        });
    assertThat(publicKeys)
        .containsExactlyElementsOf(
            model.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .map(entry -> entry.getKey())
                .toList());
    for (var entry : model.entrySet()) {
      assertThat(index.containsElement(entry.getKey())).isTrue();
      assertThat(index.contains(entry.getKey())).isEqualTo(entry.getValue() != null);
      var element = index.getElement(entry.getKey());
      assertThat(element).isNotNull();
      assertThat(element.hasValue()).isEqualTo(entry.getValue() != null);
      assertThat(encoded(codec, index.get(entry.getKey())))
          .containsExactly(encoded(codec, entry.getValue()));
    }
  }

  private static <V> void assertEntries(
      Iterator<InternalIndexElement<V>> actual,
      NavigableMap<IndexKey, V> expected,
      IndexValueSerializer<V> codec) {
    for (var entry : expected.entrySet()) {
      assertThat(actual.hasNext()).isTrue();
      var element = actual.next();
      assertThat(element.key()).isEqualTo(entry.getKey());
      assertThat(encoded(codec, element.valueNullable()))
          .containsExactly(encoded(codec, entry.getValue()));
    }
    assertThat(actual.hasNext()).isFalse();
  }

  private static <V> byte[] encoded(IndexValueSerializer<V> codec, V value) {
    return copy(codec.serialize(value, ByteBuffer.allocate(codec.serializedSize(value))).flip());
  }

  private static byte[] copy(ByteBuffer buffer) {
    var bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private record Snapshot<V>(
      ByteBuffer bytes, byte[] copy, IndexSpi<V> view, NavigableMap<IndexKey, V> model) {}

  @ParameterizedTest
  @EnumSource(Implementation.class)
  void readsFrozenV1GoldensAndContinuesEditing(Implementation implementation) throws IOException {
    var contents = goldenContents();
    var names = List.of("empty", "singleton", "prefix-null", "escaped-binary", "count128");
    for (var i = 0; i < names.size(); i++) {
      var entries = contents.get(i);
      var golden = golden(names.get(i));
      var legacy = Implementation.LEGACY.create(INDEX_KEY_SERIALIZER);
      entries.forEach((key, value) -> legacy.add(indexElement(key, value)));
      // Only the frozen legacy writer is required to reproduce these exact compression choices.
      assertThat(copy(legacy.serialize())).containsExactly(golden);
      var index = implementation.load(ByteBuffer.wrap(golden), INDEX_KEY_SERIALIZER);
      assertThat(index.isModified()).isFalse();
      assertContents(index, entries, INDEX_KEY_SERIALIZER);
      var readOnly =
          IndexesInternal.deserializeStoreIndex(
              ByteBuffer.wrap(golden), INDEX_KEY_SERIALIZER, false);
      assertContents(readOnly, entries, INDEX_KEY_SERIALIZER);
      assertThat(copy(readOnly.serialize())).containsExactly(golden);
      assertThat(copy(index.serialize())).containsExactly(golden);
      var updated = new TreeMap<>(entries);
      updated.put(key("golden-new"), key("updated"));
      index.put(key("golden-new"), key("updated"));
      for (var reader : Implementation.values()) {
        assertContents(
            reader.load(index.serialize(), INDEX_KEY_SERIALIZER), updated, INDEX_KEY_SERIALIZER);
      }
      assertContents(readOnly, entries, INDEX_KEY_SERIALIZER);
      assertContents(
          IndexesInternal.deserializeStoreIndex(index.serialize(), INDEX_KEY_SERIALIZER, false),
          updated,
          INDEX_KEY_SERIALIZER);
      assertThat(golden(names.get(i))).containsExactly(golden);
    }
    var reference = objRef("test", 17, 128);
    var expected = new TreeMap<IndexKey, ObjRef>();
    expected.put(key("entry"), reference);
    var legacy = Implementation.LEGACY.create(OBJ_REF_SERIALIZER);
    legacy.put(key("entry"), reference);
    assertThat(copy(legacy.serialize())).containsExactly(golden("objref-parts"));
    assertContents(
        implementation.load(ByteBuffer.wrap(golden("objref-parts")), OBJ_REF_SERIALIZER),
        expected,
        OBJ_REF_SERIALIZER);
    assertContents(
        IndexesInternal.deserializeStoreIndex(
            ByteBuffer.wrap(golden("objref-parts")), OBJ_REF_SERIALIZER, false),
        expected,
        OBJ_REF_SERIALIZER);
  }

  private static byte[] golden(String name) throws IOException {
    try (var input =
        requireNonNull(
            TestIndexV1Compatibility.class.getResourceAsStream("v1-goldens/" + name + ".txt"))) {
      var hex =
          new String(input.readAllBytes(), UTF_8)
              .lines()
              .filter(line -> !line.startsWith("#"))
              .map(String::trim)
              .collect(Collectors.joining());
      return HexFormat.of().parseHex(hex);
    }
  }

  private static List<NavigableMap<IndexKey, IndexKey>> goldenContents() {
    var empty = new TreeMap<IndexKey, IndexKey>();
    var singleton = new TreeMap<IndexKey, IndexKey>();
    singleton.put(key("a"), key("v"));
    var prefix = new TreeMap<IndexKey, IndexKey>();
    prefix.put(key("namespace/table-a"), key("first"));
    prefix.put(key("namespace/table-b"), null);
    prefix.put(key("namespace/table-c"), key("last"));
    var binary = new TreeMap<IndexKey, IndexKey>();
    binary.put(key(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 255})), key("é"));
    binary.put(key(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 255, 0})), null);
    var boundary = new TreeMap<IndexKey, IndexKey>();
    for (var i = 0; i < 128; i++) {
      boundary.put(key(String.format("ns/%03d", i)), i % 2 == 0 ? key("v".repeat(128)) : null);
    }
    return List.of(empty, singleton, prefix, binary, boundary);
  }
}

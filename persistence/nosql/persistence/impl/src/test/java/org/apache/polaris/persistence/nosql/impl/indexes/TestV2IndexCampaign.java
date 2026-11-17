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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Reproducible legal traces; extended budgets are opt-in test environment variables. */
class TestV2IndexCampaign {
  static final int GENERATOR_VERSION = 1;

  static Stream<Long> seeds() {
    int count = Integer.parseInt(System.getenv().getOrDefault("POLARIS_INDEX_TEST_SEEDS", "3"));
    if (count <= 0) throw new IllegalArgumentException("Positive campaign seed budget required");
    return LongStream.range(0, count).map(n -> 0x5eed1234L + 104729L * n).boxed();
  }

  static int steps() {
    int count = Integer.parseInt(System.getenv().getOrDefault("POLARIS_INDEX_TEST_STEPS", "180"));
    if (count <= 0) throw new IllegalArgumentException("Positive campaign step budget required");
    return count;
  }

  @TestFactory
  Stream<DynamicTest> reproducibleSplitReloadAndPreferenceHistories() {
    return seeds()
        .flatMap(seed ->
            Stream.of("V1", "PROMOTE", "V2")
                .flatMap(role ->
                    Stream.of(
                        DynamicTest.dynamicTest(
                            "seed=" + seed + "/" + role + "/ObjRef",
                            () ->
                                campaign(
                                    seed,
                                    role,
                                    ObjRef.OBJ_REF_SERIALIZER,
                                    n ->
                                        ObjRef.objRef(
                                            "value-" + "x".repeat(n % 25),
                                            100 + n % 7,
                                            n % 2 == 0 ? 1 : 400))),
                        DynamicTest.dynamicTest(
                            "seed=" + seed + "/" + role + "/IndexKey",
                            () ->
                                campaign(
                                    seed,
                                    role,
                                    IndexKey.INDEX_KEY_SERIALIZER,
                                    n -> key("value/" + "x".repeat(n % 61) + n))))));
  }

  @Test
  void unchangedPositionedV1WriteRoundTripsWithoutPrefixBytes() {
    var codec = IndexKey.INDEX_KEY_SERIALIZER;
    for (int entries : new int[] {0, 1, 4}) {
      var source = newStoreIndex(codec, 1);
      for (int i = 0; i < entries; i++) source.put(key("entry/" + i), key("value/" + i));
      var bytes = source.serialize();
      for (int mode = 0; mode < 5; mode++) {
        var positioned = buffer(bytes, mode);
        int position = positioned.position(), limit = positioned.limit();
        var view = deserializeStoreIndex(positioned, codec, true, 1);
        assertThat(positioned.position()).isEqualTo(position);
        assertThat(positioned.limit()).isEqualTo(limit);
        assertThat(view.isModified()).isFalse();
        assertThat(view.serialize()).isEqualTo(bytes);
        assertThat(view.estimatedSerializedSize()).isGreaterThanOrEqualTo(bytes.remaining());
        assertThat(deserializeStoreIndex(view.serialize(), codec, false, 1).asKeyList())
            .containsExactlyElementsOf(source.asKeyList());
        view.put(key("entry/new"), key("updated"));
        var output = view.serialize();
        assertThat(output.remaining()).isLessThanOrEqualTo(view.estimatedSerializedSize());
        assertThat(deserializeStoreIndex(output, codec, false, 1).get(key("entry/new")))
            .isEqualTo(key("updated"));
        assertThat(deserializeStoreIndex(bytes, codec, false, 1).contains(key("entry/new")))
            .isFalse();
      }
    }
  }

  private record Snapshot<V>(List<ByteBuffer> parts, TreeMap<IndexKey, V> model) {}

  private static <V> void campaign(
      long seed, String role, IndexValueSerializer<V> codec, IntFunction<V> values) {
    var random = new Random(seed);
    var keys = new ArrayList<IndexKey>();
    for (int i = 0; i < 96; i++) {
      if (i % 3 == 0) {
        var raw = new byte[8 + i % 29];
        random.nextBytes(raw);
        keys.add(key(ByteBuffer.wrap(raw)));
      } else
        keys.add(key("namespace/" + i % 7 + "/leaf/" + i + (i % 5 == 0 ? "\u0000escaped" : "")));
    }
    var model = new TreeMap<IndexKey, V>();
    var initial = newStoreIndex(codec, 1);
    for (int i = 0; i < 32; i++) {
      V value = i % 7 == 0 ? null : values.apply(i);
      initial.add(indexElement(keys.get(i), value));
      model.put(keys.get(i), value);
    }
    ByteBuffer starting = initial.serialize();
    if (role.equals("V2")) {
      var options =
          new V2IndexFormat.Options(
              V2IndexFormat.Layout.values()[(int) (seed % 3)],
              8,
              V2IndexFormat.DirectoryEncoding.values()[(int) (seed % 2)],
              3);
      starting = V2IndexWriter.write(initial, codec, options);
    }
    int preference = role.equals("PROMOTE") ? 2 : 1;
    var parts = new ArrayList<IndexSpi<V>>();
    parts.add(deserializeStoreIndex(starting, codec, true, preference));
    var snapshots = new ArrayList<Snapshot<V>>();
    var trace = new StringBuilder();
    var operations = new int[7];
    try {
      for (int step = 0; step < steps(); step++) {
        var k = keys.get(random.nextInt(keys.size()));
        int operation = random.nextInt(7);
        operations[operation]++;
        trace
            .append(step)
            .append(':')
            .append(operation)
            .append(':')
            .append(keys.indexOf(k))
            .append(';');
        IndexSpi<V> part = parts.getFirst();
        for (var candidate : parts) {
          if (candidate.first() != null && candidate.first().compareTo(k) <= 0) part = candidate;
        }
        if (operation <= 1) {
          V value = operation == 0 ? values.apply(step + 97) : null;
          var result =
              !model.containsKey(k)
                  ? IndexSpi.AddResult.NEW_KEY
                  : model.get(k) == null
                      ? IndexSpi.AddResult.UPDATED_NO_VALUE
                      : IndexSpi.AddResult.UPDATED;
          assertThat(part.add(indexElement(k, value))).isEqualTo(result);
          model.put(k, value);
          assertThat(part.isModified()).isTrue();
        } else if (operation == 2) {
          boolean dirty = part.isModified();
          boolean existed = model.containsKey(k);
          assertThat(part.remove(k)).isEqualTo(existed);
          assertThat(part.isModified()).isEqualTo(dirty || existed);
          model.remove(k);
        } else if (operation == 3) {
          assertThat(part.containsElement(k)).isEqualTo(model.containsKey(k));
          assertThat(part.contains(k)).isEqualTo(model.get(k) != null);
          sameValue(part.get(k), model.get(k), codec);
        } else if (operation == 4 || operation == 5) {
          var bytes = new ArrayList<ByteBuffer>();
          var next = new ArrayList<IndexSpi<V>>();
          for (var previous : parts) {
            var split = operation == 5 ? previous.splitByTargetSize(256) : List.of(previous);
            for (var child : split) {
              int estimate = child.estimatedSerializedSize();
              var output = child.serialize();
              assertThat(output.remaining()).isLessThanOrEqualTo(estimate);
              if (operation == 5 && child.asKeyList().size() > 1)
                assertThat(output.remaining()).isLessThanOrEqualTo(256);
              bytes.add(output);
              var reloaded =
                  deserializeStoreIndex(buffer(output, step % 5), codec, true, preference);
              assertThat(reloaded.isModified()).isFalse();
              // V2 may never demote, including when the next view requests preference 1.
              if (output.get(0) == 2 || preference == 2)
                assertThat(reloaded.serialize().get(0)).isEqualTo((byte) 2);
              next.add(reloaded);
            }
          }
          if (next.isEmpty()) next.add(newStoreIndex(codec, role.equals("V1") ? 1 : 2));
          snapshots.add(new Snapshot<>(List.copyOf(bytes), new TreeMap<>(model)));
          if (snapshots.size() > 3) snapshots.removeFirst();
          parts = next;
          if (!role.equals("V1")) preference = random.nextBoolean() ? 1 : 2;
          trace.append("pref=").append(preference).append(';');
        } else {
          var lower = keys.get(random.nextInt(keys.size()));
          var upper = keys.get(random.nextInt(keys.size()));
          if (lower.compareTo(upper) > 0) {
            var swap = lower;
            lower = upper;
            upper = swap;
          }
          if (!lower.equals(upper)) {
            var actual = new ArrayList<IndexKey>();
            for (var stripe : parts)
              stripe
                  .elementIterator(lower, upper, false)
                  .forEachRemaining(e -> actual.add(e.key()));
            assertThat(actual)
                .containsExactlyElementsOf(model.subMap(lower, true, upper, true).keySet());
          }
          var prefix = key("namespace/");
          var actual = new ArrayList<IndexKey>();
          for (var stripe : parts)
            stripe
                .elementIterator(prefix, prefix, false)
                .forEachRemaining(e -> actual.add(e.key()));
          assertThat(actual)
              .containsExactlyElementsOf(
                  model.keySet().stream().filter(x -> x.startsWith(prefix)).toList());
        }
        verify(parts, model, codec);
        for (var snapshot : snapshots) {
          var saved =
              snapshot.parts().stream()
                  .map(b -> deserializeStoreIndex(b, codec, false, 1))
                  .toList();
          verify(saved, snapshot.model(), codec);
        }
      }
    } catch (RuntimeException | AssertionError failure) {
      var input = new byte[starting.remaining()];
      starting.duplicate().get(input);
      throw new AssertionError(
          "generator="
              + GENERATOR_VERSION
              + " seed="
              + seed
              + " role="
              + role
              + " codec="
              + codec.getClass().getName()
              + " startingBase64="
              + Base64.getEncoder().encodeToString(input)
              + " trace="
              + trace,
          failure);
    }
    System.out.println(
        "INDEX_CAMPAIGN generator="
            + GENERATOR_VERSION
            + " seed="
            + seed
            + " role="
            + role
            + " codec="
            + codec.getClass().getName()
            + " steps="
            + steps()
            + " operations="
            + Arrays.toString(operations));
  }

  private static ByteBuffer buffer(ByteBuffer bytes, int mode) {
    var copy =
        mode == 1
            ? ByteBuffer.allocateDirect(bytes.remaining() + 11)
            : ByteBuffer.allocate(bytes.remaining() + 11);
    copy.position(7).put(bytes.duplicate()).flip().position(7);
    if (mode == 2) return copy.asReadOnlyBuffer();
    if (mode == 3) return copy.slice();
    if (mode == 4) return copy.order(ByteOrder.LITTLE_ENDIAN);
    return copy;
  }

  private static <V> void sameValue(V actual, V expected, IndexValueSerializer<V> codec) {
    if (expected == null) assertThat(actual).isNull();
    else {
      assertThat(actual).isNotNull();
      assertThat(codec.serialize(actual, ByteBuffer.allocate(codec.serializedSize(actual))).flip())
          .isEqualTo(
              codec
                  .serialize(expected, ByteBuffer.allocate(codec.serializedSize(expected)))
                  .flip());
    }
  }

  private static <V> void verify(
      List<IndexSpi<V>> parts, TreeMap<IndexKey, V> model, IndexValueSerializer<V> codec) {
    var actual = new ArrayList<IndexKey>();
    var reverse = new ArrayList<IndexKey>();
    for (var part : parts) {
      var local = new ArrayList<IndexKey>();
      part.elementIterator()
          .forEachRemaining(e -> {
            actual.add(e.key());
            local.add(e.key());
            assertThat(model).containsKey(e.key());
            sameValue(e.valueNullable(), model.get(e.key()), codec);
          });
      assertThat(part.asKeyList()).containsExactlyElementsOf(local);

      assertThat(part.first()).isEqualTo(local.isEmpty() ? null : local.getFirst());
      assertThat(part.last()).isEqualTo(local.isEmpty() ? null : local.getLast());
    }
    assertThat(actual).containsExactlyElementsOf(model.keySet());
    for (var part : parts.reversed())
      part.reverseElementIterator().forEachRemaining(e -> reverse.add(e.key()));
    assertThat(reverse).containsExactlyElementsOf(model.descendingKeySet());
  }
}

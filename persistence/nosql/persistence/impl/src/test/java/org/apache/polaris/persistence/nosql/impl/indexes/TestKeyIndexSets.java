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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.RepresentativeKeyDataset.BENCHMARK_SEEDS;
import static org.apache.polaris.persistence.nosql.impl.indexes.Util.randomObjId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.polaris.ids.api.SnowflakeIdGenerator;
import org.apache.polaris.ids.impl.SnowflakeIdGeneratorFactory;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.impl.indexes.RepresentativeKeyDataset.Family;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestKeyIndexSets {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("keyIndexSetConfigs")
  @Timeout(30) // if this test hits the timeout, then that's a legit bug !!
  void keyIndexSetTests(
      int namespaceLevels, int foldersPerLevel, int tablesPerNamespace, boolean deterministic) {

    var keyIndexTestSet =
        KeyIndexTestSet.<ObjRef>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(namespaceLevels)
                    .foldersPerLevel(foldersPerLevel)
                    .tablesPerNamespace(tablesPerNamespace)
                    .deterministic(deterministic)
                    .build())
            .elementSupplier(key -> indexElement(key, randomObjId()))
            .elementSerializer(OBJ_REF_SERIALIZER)
            .build()
            .generateIndexTestSet();

    soft.assertThatCode(keyIndexTestSet::serialize).doesNotThrowAnyException();
    soft.assertThatCode(keyIndexTestSet::deserialize).doesNotThrowAnyException();
    soft.assertThat(((IndexImpl<ObjRef>) keyIndexTestSet.deserialize()).setModified().serialize())
        .isEqualTo(keyIndexTestSet.serializedSafe());
    soft.assertThatCode(keyIndexTestSet::randomGetKey).doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              IndexSpi<ObjRef> deserialized = keyIndexTestSet.deserialize();
              deserialized.add(indexElement(key("zzzzzzzkey"), randomObjId()));
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              IndexSpi<ObjRef> deserialized = keyIndexTestSet.deserialize();
              for (char c = 'a'; c <= 'z'; c++) {
                deserialized.add(indexElement(key(c + "xkey"), randomObjId()));
              }
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @EnumSource(Family.class)
  void representativeDatasetsAreReproducible(Family family) {
    assertThat(BENCHMARK_SEEDS).hasSize(5).doesNotHaveDuplicates();
    for (long seed : BENCHMARK_SEEDS) {
      var first = RepresentativeKeyDataset.generate(family, 1024, seed);
      var repeat = RepresentativeKeyDataset.generate(family, 1024, seed);
      assertThat(first).isEqualTo(repeat);
      assertThat(first.config()).isEqualTo(new RepresentativeKeyDataset.Config(family, 1024, seed));
      assertThat(first.config().datasetId())
          .isEqualTo("representative-v1/" + family + "/" + seed + "/1024");
      assertThat(first.keys()).hasSize(1024).isSorted().doesNotHaveDuplicates();
      assertThat(first.keys().stream().map(TestKeyIndexSets::serializeKey).toList())
          .isEqualTo(repeat.keys().stream().map(TestKeyIndexSets::serializeKey).toList());
      assertThat(RepresentativeKeyDataset.generate(family, 37, seed).creationOrder())
          .containsExactlyElementsOf(first.creationOrder().subList(0, 37));
      assertThat(first.creationOrder()).containsExactlyInAnyOrderElementsOf(first.sortedEntries());
      for (int i = 0; i < first.creationOrder().size(); i++) {
        assertThat(first.creationOrder().get(i).creationOrdinal()).isEqualTo(i);
      }
    }
    assertThat(RepresentativeKeyDataset.generate(family, 1024, BENCHMARK_SEEDS.get(0)).keys())
        .isNotEqualTo(
            RepresentativeKeyDataset.generate(family, 1024, BENCHMARK_SEEDS.get(1)).keys());
  }

  @Test
  void representativeWordNamesReuseDiverseNamespaces() {
    var dataset = RepresentativeKeyDataset.generate(Family.WORDS, 8192, BENCHMARK_SEEDS.getFirst());
    assertThat(
            RepresentativeKeyDataset.generate(Family.WORDS, 10_000, BENCHMARK_SEEDS.getFirst())
                .creationOrder()
                .subList(0, 8192))
        .containsExactlyElementsOf(dataset.creationOrder());
    var corpus = new HashSet<>(KeyIndexTestSet.Words.words());
    var populations = new HashMap<String, Integer>();
    var parents = new HashSet<String>();
    for (var entry : dataset.creationOrder()) {
      var components = entry.name().toString().split("\u0000");
      assertThat(components).hasSize(3);
      assertThat(entry.key()).isEqualTo(entry.name());
      for (var component : components) {
        assertThat(component.length()).isBetween(8, 20);
        assertThat(corpus).contains(component);
      }
      parents.add(components[0]);
      populations.merge(components[0] + "\u0000" + components[1], 1, Integer::sum);
    }
    assertThat(parents).hasSize(19);
    assertThat(populations.size()).isGreaterThan(parents.size());
    assertThat(populations.values()).anyMatch(count -> count > 1);
    assertThat(new HashSet<>(populations.values())).hasSizeGreaterThan(3);
    // Assert the actual corpus limit, rather than manufacturing eight-character vocabulary.
    assertThat(corpus.stream().mapToInt(String::length).min()).hasValue(11);
  }

  @Test
  void representativeLocationsUseTheSameNamesAndNormalizedProductionKeys() {
    long seed = BENCHMARK_SEEDS.getFirst();
    var words = RepresentativeKeyDataset.generate(Family.WORDS, 2048, seed);
    var locations = RepresentativeKeyDataset.generate(Family.LOCATION, 2048, seed);
    assertThat(RepresentativeKeyDataset.BUCKET).hasSize(20);
    assertThat(RepresentativeKeyDataset.PREFIX).hasSize(30);
    assertThat(locations.creationOrder().stream().map(RepresentativeKeyDataset.Entry::uuid))
        .doesNotHaveDuplicates();
    for (int i = 0; i < words.creationOrder().size(); i++) {
      var name = words.creationOrder().get(i);
      var location = locations.creationOrder().get(i);
      assertThat(location.name()).isEqualTo(name.name());
      assertThat(location.stableId()).isEqualTo(name.stableId());
      assertThat(location.uuid()).isEqualTo(name.uuid());
      var expected =
          "s3://"
              + RepresentativeKeyDataset.BUCKET
              + "/"
              + RepresentativeKeyDataset.PREFIX
              + "/"
              + name.name().toString().replace('\u0000', '/')
              + "-"
              + name.uuid();
      assertThat(location.location()).isEqualTo(expected);
      assertThat(location.key())
          .isEqualTo(key(expected.substring("s3://".length()).replace('/', '\u0000')));
      assertThat(location.key().toString().split("\u0000")).hasSize(5);
    }
  }

  @Test
  void representativeSnowflakesMatchProductionLayoutAndFiveInterleavedNodes() {
    var decoder = new SnowflakeIdGeneratorFactory().buildSystemIdGenerator(Map.of());
    var dataset =
        RepresentativeKeyDataset.generate(Family.SNOWFLAKE, 2048, BENCHMARK_SEEDS.getFirst());
    var byNode =
        dataset.creationOrder().stream()
            .collect(Collectors.groupingBy(e -> decoder.nodeFromId(e.stableId())));
    assertThat(byNode).hasSize(5);
    assertThat(dataset.creationOrder().stream().limit(5).map(e -> decoder.nodeFromId(e.stableId())))
        .doesNotHaveDuplicates();
    assertThat(
            dataset.creationOrder().stream().map(RepresentativeKeyDataset.Entry::stableId).toList())
        .isNotEqualTo(
            dataset.sortedEntries().stream()
                .map(RepresentativeKeyDataset.Entry::stableId)
                .toList());
    for (var nodeEntries : byNode.values()) {
      long previous = 0;
      long previousTime = 0;
      boolean gap = false;
      boolean sameTime = false;
      for (var entry : nodeEntries) {
        long id = entry.stableId();
        long time = decoder.timestampFromId(id);
        assertThat(entry.key()).isEqualTo(key(id));
        assertThat(entry.key().asByteBuffer().remaining()).isEqualTo(Long.BYTES);
        assertThat(entry.key().asLong()).isEqualTo(id);
        assertThat(id).isGreaterThan(previous);
        assertThat(time).isBetween(40_000_000_000L, 41_000_000_000L);
        assertThat(decoder.timestampUtcFromId(id))
            .isEqualTo(SnowflakeIdGenerator.ID_EPOCH_MILLIS + time);
        assertThat(decoder.sequenceFromId(id)).isBetween(1L, 16L);
        assertThat(decoder.constructId(time, decoder.sequenceFromId(id), decoder.nodeFromId(id)))
            .isEqualTo(id);
        gap |= previousTime != 0 && time > previousTime + 1;
        sameTime |= previousTime == time;
        previous = id;
        previousTime = time;
      }
      assertThat(gap).isTrue();
      assertThat(sameTime).isTrue();
    }
  }

  @ParameterizedTest
  @EnumSource(Family.class)
  void representativeDatasetSizesAreValidated(Family family) {
    assertThat(RepresentativeKeyDataset.generate(family, 0, 1).keys()).isEmpty();
    assertThatThrownBy(() -> RepresentativeKeyDataset.generate(family, -1, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size");
    assertThatThrownBy(() -> RepresentativeKeyDataset.generate(family, Integer.MAX_VALUE, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("population");
  }

  @Test
  void representativeDatasetRequiresFamily() {
    assertThatThrownBy(() -> new RepresentativeKeyDataset.Config(null, 1, 1))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("family");
  }

  private static ByteBuffer serializeKey(IndexKey key) {
    var bytes = ByteBuffer.allocate(key.serializedSize());
    key.serialize(bytes);
    return bytes.flip();
  }

  static Stream<Arguments> keyIndexSetConfigs() {
    return Stream.of(
        arguments(2, 2, 5, true),
        arguments(2, 2, 5, false),
        arguments(2, 2, 20, true),
        arguments(2, 2, 20, false),
        arguments(5, 5, 50, true),
        arguments(5, 5, 50, false));
  }
}

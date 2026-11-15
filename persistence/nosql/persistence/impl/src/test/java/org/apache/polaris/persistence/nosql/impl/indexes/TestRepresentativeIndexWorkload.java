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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestRepresentativeIndexWorkload {
  static Stream<Arguments> cases() {
    return Stream.of(RepresentativeKeyDataset.Family.values())
        .flatMap(family ->
            Stream.of(RepresentativeIndexWorkload.History.values())
                .flatMap(history ->
                    Stream.of(RepresentativeIndexWorkload.Partitioning.values())
                        .map(partitioning -> arguments(family, history, partitioning))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void readersAndWritersMatchLogicalModel(
      RepresentativeKeyDataset.Family family,
      RepresentativeIndexWorkload.History history,
      RepresentativeIndexWorkload.Partitioning partitioning) {
    Map<IndexKey, ?> expected = null;
    for (var variant : RepresentativeIndexWorkload.Variant.values()) {
      var data = create(family, variant, 129, 0, history, partitioning);
      assertThat(data.input)
          .isEqualTo(create(family, variant, 129, 0, history, partitioning).input);
      if (expected == null) expected = data.model;
      assertThat(data.model).isEqualTo(expected);
      verify(data);
    }
  }

  private static <V> void verify(RepresentativeIndexWorkload<V> data) {
    var fresh = data.fresh();
    assertThat(fresh.asKeyList()).containsExactlyElementsOf(data.keys);
    data.model.forEach((key, value) -> assertThat(fresh.get(key)).isEqualTo(value));
    assertThat(fresh.get(data.missing)).isNull();
    for (var request : data.requests) {
      assertThat(new HashSet<>(Arrays.asList(request))).hasSize(16);
      for (var key : request) assertThat(fresh.get(key)).isEqualTo(data.model.get(key));
    }
    var matching = data.keys.stream().filter(key -> key.startsWith(data.prefix)).toList();
    assertThat(matching).isNotEmpty();
    var actual = new ArrayList<IndexKey>();
    fresh
        .iterator(data.prefix, data.prefixEnd, false)
        .forEachRemaining(e -> {
          actual.add(e.key());
          assertThat(e.value()).isEqualTo(data.model.get(e.key()));
        });
    assertThat(actual).containsExactlyElementsOf(matching);
    actual.clear();
    fresh.reverseIterator().forEachRemaining(e -> actual.add(e.key()));
    assertThat(actual).containsExactlyElementsOf(data.keys.reversed());
    if (data.variant == RepresentativeIndexWorkload.Variant.D3
        || data.variant == RepresentativeIndexWorkload.Variant.D4) {
      assertThatThrownBy(() ->
              data.prepareMutation(
                      16,
                      RepresentativeIndexWorkload.Locality.SCATTERED,
                      RepresentativeIndexWorkload.Operation.MIXED)
                  .run(false, 2000))
          .isInstanceOf(IllegalArgumentException.class);
      return;
    }
    for (var operation : RepresentativeIndexWorkload.Operation.values()) {
      for (var locality : RepresentativeIndexWorkload.Locality.values()) {
        var changes = data.changes(16, locality, operation);
        var expected = new TreeMap<>(data.model);
        assertThat(changes.stream().map(SplitV2IndexWriter.Change::key).toList())
            .isSorted()
            .doesNotHaveDuplicates();
        int removals = 0, insertions = 0;
        for (var change : changes) {
          if (change.element() == null) {
            assertThat(expected.remove(change.key())).isNotNull();
            removals++;
          } else {
            assertThat(change.element().valueNullable()).isNotNull();
            if (!expected.containsKey(change.key())) insertions++;
            expected.put(change.key(), change.element().valueNullable());
          }
        }
        if (operation == RepresentativeIndexWorkload.Operation.MIXED) {
          assertThat(removals).isEqualTo(6);
          assertThat(insertions).isEqualTo(5);
        } else assertThat(expected).isNotEqualTo(data.model);
        for (boolean split : new boolean[] {false, true}) {
          var outcome = data.spill(changes, split, 2000);
          var actualKeys = new ArrayList<IndexKey>();
          for (var part : outcome.parts()) {
            var read = data.read(part);
            actualKeys.addAll(read.asKeyList());
            for (var key : read.asKeyList()) assertThat(read.get(key)).isEqualTo(expected.get(key));
          }
          assertThat(actualKeys).containsExactlyElementsOf(expected.keySet());
          assertThat(data.fresh().asKeyList()).containsExactlyElementsOf(data.keys);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(RepresentativeKeyDataset.Family.class)
  void budgetSelectionIsFeasibleAndUsesNestedCreationPopulation(
      RepresentativeKeyDataset.Family family) {
    for (var variant : RepresentativeIndexWorkload.Variant.values()) {
      var full =
          create(
              family,
              variant,
              257,
              0,
              RepresentativeIndexWorkload.History.FRESH,
              RepresentativeIndexWorkload.Partitioning.NATIVE);
      long budget = full.input.remaining() * 3L / 4;
      var selected =
          create(
              family,
              variant,
              257,
              budget,
              RepresentativeIndexWorkload.History.FRESH,
              RepresentativeIndexWorkload.Partitioning.NATIVE);
      assertThat(selected.input.remaining()).isLessThanOrEqualTo((int) budget);
      assertThat(selected.keys.size()).isBetween(32, 257);
      assertThat(new HashSet<>(full.keys)).containsAll(selected.keys);
      assertThat(selected.model.keySet())
          .containsExactlyInAnyOrderElementsOf(
              selected.dataset.creationOrder().subList(0, selected.keys.size()).stream()
                  .map(RepresentativeKeyDataset.Entry::key)
                  .toList());
      assertThatThrownBy(() ->
              create(
                  family,
                  variant,
                  257,
                  1,
                  RepresentativeIndexWorkload.History.FRESH,
                  RepresentativeIndexWorkload.Partitioning.NATIVE))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void fixedPartitioningKeepsIdentical64EntryBoundaries() {
    for (var variant :
        new RepresentativeIndexWorkload.Variant[] {
          RepresentativeIndexWorkload.Variant.FRONT_ENDS,
          RepresentativeIndexWorkload.Variant.PREFIX_ENDS
        }) {
      var data =
          create(
              RepresentativeKeyDataset.Family.WORDS,
              variant,
              257,
              0,
              RepresentativeIndexWorkload.History.FRESH,
              RepresentativeIndexWorkload.Partitioning.FIXED64);
      var reader = (SplitV2IndexReader<?>) data.fresh();
      assertThat(reader.blockCount).isEqualTo(5);
      for (int i = 0; i < 5; i++) assertThat(reader.firstOrdinal(i)).isEqualTo(i * 64);
    }
    assertThat(
            new SplitV2Format.Options(
                    SplitV2Format.Layout.PREFIX_ENDS, 64, V2IndexWriter.DirectoryEncoding.FULL)
                .partitionAtPrefix())
        .isTrue();
  }

  @Test
  void minimumEditedHistoryStillSupportsDistinctRequestsAndMetadata() {
    for (var variant : RepresentativeIndexWorkload.Variant.values()) {
      var data =
          create(
              RepresentativeKeyDataset.Family.LOCATION,
              variant,
              32,
              0,
              RepresentativeIndexWorkload.History.EDITED4,
              RepresentativeIndexWorkload.Partitioning.NATIVE);
      assertThat(data.keys).hasSize(28);
      for (var request : data.requests)
        assertThat(new HashSet<>(Arrays.asList(request))).hasSize(16);
      var metadata =
          data.metadata(
              Map.of("family", "LOCATION", "seed", "1369956643"),
              new RepresentativeIndexWorkload.Outcome(List.of(data.input), 0),
              null);
      assertThat(metadata.get("inputEntries")).isEqualTo(28);
      assertThat(metadata.get("outputEntries")).isEqualTo(28);
      assertThat(metadata.get("inputBytes")).isEqualTo(metadata.get("outputBytes"));
    }
  }

  @Test
  void numericPrefixBoundsMatchStartsWithAcrossAllSeeds() {
    for (long seed : RepresentativeKeyDataset.BENCHMARK_SEEDS) {
      var data =
          RepresentativeIndexWorkload.create(
              RepresentativeKeyDataset.Family.SNOWFLAKE,
              seed,
              RepresentativeIndexWorkload.Variant.BASELINE,
              257,
              0,
              RepresentativeIndexWorkload.History.FRESH,
              RepresentativeIndexWorkload.Partitioning.NATIVE);
      var matching = data.keys.stream().filter(key -> key.startsWith(data.prefix)).toList();
      var actual = new ArrayList<IndexKey>();
      data.fresh()
          .iterator(data.prefix, data.prefixEnd, false)
          .forEachRemaining(e -> actual.add(e.key()));
      assertThat(actual).containsExactlyElementsOf(matching);
    }
  }

  @Test
  void fixtureUsesProductionValueCodecs() {
    var snowflake =
        create(
            RepresentativeKeyDataset.Family.SNOWFLAKE,
            RepresentativeIndexWorkload.Variant.BASELINE,
            129,
            0,
            RepresentativeIndexWorkload.History.FRESH,
            RepresentativeIndexWorkload.Partitioning.NATIVE);
    assertThat(snowflake.codec).isSameAs(IndexKey.INDEX_KEY_SERIALIZER);
    var words =
        create(
            RepresentativeKeyDataset.Family.WORDS,
            RepresentativeIndexWorkload.Variant.BASELINE,
            129,
            0,
            RepresentativeIndexWorkload.History.FRESH,
            RepresentativeIndexWorkload.Partitioning.NATIVE);
    assertThat(words.codec).isSameAs(ObjRef.OBJ_REF_SERIALIZER);
    var location =
        create(
            RepresentativeKeyDataset.Family.LOCATION,
            RepresentativeIndexWorkload.Variant.BASELINE,
            129,
            0,
            RepresentativeIndexWorkload.History.FRESH,
            RepresentativeIndexWorkload.Partitioning.NATIVE);
    assertThat(location.codec.getClass().getSimpleName()).isEqualTo("EntityIdSetSerializer");
  }

  private static RepresentativeIndexWorkload<?> create(
      RepresentativeKeyDataset.Family family,
      RepresentativeIndexWorkload.Variant variant,
      int entries,
      long budget,
      RepresentativeIndexWorkload.History history,
      RepresentativeIndexWorkload.Partitioning partitioning) {
    return RepresentativeIndexWorkload.create(
        family,
        RepresentativeKeyDataset.BENCHMARK_SEEDS.getFirst(),
        variant,
        entries,
        budget,
        history,
        partitioning);
  }
}

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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.PersistenceParams.BuildablePersistenceParams;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestIndexMutationWorkload {
  static Stream<Arguments> workloads() {
    return Stream.of("HIERARCHICAL", "WORDS")
        .flatMap(shape ->
            Stream.of("CLUSTERED", "SCATTERED")
                .flatMap(locality ->
                    Stream.of("UPDATE", "INSERT", "REMOVE", "GROW", "MIXED")
                        .flatMap(operation ->
                            Stream.of(V2IndexWriter.DirectoryEncoding.values())
                                .map(
                                    encoding -> arguments(shape, locality, operation, encoding)))));
  }

  @ParameterizedTest
  @MethodSource("workloads")
  void embeddedCleanupMatchesProduction(
      String shape, String locality, String operation, V2IndexWriter.DirectoryEncoding encoding) {
    var workload = workload(shape, locality, operation, encoding);
    var expected =
        new UpdatableIndexImpl<>(
            null,
            workload.initialEmbedded(IndexMutationWorkload.Variant.BASELINE),
            workload.initialReference(IndexMutationWorkload.Variant.BASELINE),
            BuildablePersistenceParams.builder().build(),
            () -> 1L,
            OBJ_REF_SERIALIZER);
    long observed = 0;
    int added = 0;
    for (var change : workload.changes) {
      var value = expected.get(change.key());
      if (value != null) observed += value.id();
      if (change.value() == null) expected.remove(change.key());
      else if (expected.put(change.key(), change.value())) added++;
    }
    var expectedContainer = expected.toIndexed("test-", (key, obj) -> fail("Unexpected spill"));
    var expectedEmbedded =
        IndexImpl.deserializeStoreIndex(expectedContainer.embedded(), OBJ_REF_SERIALIZER);
    for (var variant : IndexMutationWorkload.Variant.values()) {
      for (boolean rebuild : new boolean[] {true, false}) {
        var view = workload.updateEmbedded(variant);
        assertThat(view.observed()).isEqualTo(observed);
        assertThat(view.added()).isEqualTo(added);
        var bytes = workload.serializeEmbedded(view, variant, rebuild);
        var written = V2IndexReader.read(bytes, OBJ_REF_SERIALIZER);
        verify(expectedEmbedded, written);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("workloads")
  void sortedSpillAndSplittingMatchIndividualProductionMutations(
      String shape, String locality, String operation, V2IndexWriter.DirectoryEncoding encoding) {
    var workload = workload(shape, locality, operation, encoding);
    var expected = workload.initialReference(IndexMutationWorkload.Variant.BASELINE);
    for (var change : workload.changes) {
      if (change.value() == null) expected.remove(change.key());
      else expected.put(change.key(), change.value());
    }
    var persisted = new ArrayList<IndexStripeObj>();
    var production =
        new UpdatableIndexImpl<>(
            null,
            workload.initialPending(),
            workload.initialReference(IndexMutationWorkload.Variant.BASELINE),
            BuildablePersistenceParams.builder()
                .maxEmbeddedIndexSize(MemorySize.valueOf("1"))
                .build(),
            () -> 1L,
            OBJ_REF_SERIALIZER);
    production.toIndexed("test-", (name, obj) -> persisted.add((IndexStripeObj) obj));
    assertThat(persisted).hasSize(1);
    verify(
        expected,
        IndexImpl.deserializeStoreIndex(persisted.getFirst().index(), OBJ_REF_SERIALIZER));
    Long observed = null;
    for (var variant : IndexMutationWorkload.Variant.values()) {
      var view = workload.spill(variant, 4);
      if (observed == null) observed = view.observed();
      assertThat(view.observed()).isEqualTo(observed);
      verify(expected, view.stripe());
      var size = view.stripe().estimatedSerializedSize();
      var serialized = view.stripe().serialize();
      assertThat(size).isGreaterThanOrEqualTo(serialized.remaining());
      verify(expected, V2IndexReader.read(serialized, OBJ_REF_SERIALIZER));
      var keys = new ArrayList<IndexKey>();
      for (var part : view.stripe().splitByTargetSize(2000)) {
        keys.addAll(part.asKeyList());
        var bytes = part.serialize();
        var read = V2IndexReader.read(bytes, OBJ_REF_SERIALIZER);
        verify(part, read);
        if (part.asKeyList().size() > 1) assertThat(bytes.remaining()).isLessThanOrEqualTo(2000);
      }
      assertThat(keys).containsExactlyElementsOf(expected.asKeyList());
    }
  }

  @Test
  void embeddedHitDoesNotLoadReferencedStripe() {
    var workload =
        new IndexMutationWorkload(
            5000,
            "HIERARCHICAL",
            128,
            1,
            "CLUSTERED",
            "UPDATE",
            V2IndexWriter.Options.defaults(64));
    for (var variant : IndexMutationWorkload.Variant.values()) {
      var view = workload.updateEmbedded(variant);
      assertThat(view.reference().isLoaded()).isFalse();
    }
  }

  @ParameterizedTest
  @MethodSource("workloads")
  void defaultBenchmarkInputsExerciseSparseAndDenseBlocks(
      String shape, String locality, String operation, V2IndexWriter.DirectoryEncoding encoding) {
    var workload =
        new IndexMutationWorkload(
            4096,
            shape,
            128,
            256,
            locality,
            operation,
            new V2IndexWriter.Options(64, false, encoding, 8));
    var reference = workload.initialReference(IndexMutationWorkload.Variant.V2);
    assertThat(reference.estimatedSerializedSize()).isLessThanOrEqualTo(131072);
    var view = workload.spill(IndexMutationWorkload.Variant.V2_REUSE, 4);
    var stripe = (V2BlockReuseIndex<ObjRef>) view.stripe();
    assertThat(workload.pendingBytes).isLessThan(32768);
    if (locality.equals("CLUSTERED")) assertThat(stripe.changedBlocks()).isEqualTo(4);
    else assertThat(stripe.changedBlocks()).isEqualTo(64);
  }

  private static IndexMutationWorkload workload(
      String shape, String locality, String operation, V2IndexWriter.DirectoryEncoding encoding) {
    return new IndexMutationWorkload(
        513, shape, 64, 32, locality, operation, new V2IndexWriter.Options(16, false, encoding, 3));
  }

  private static void verify(IndexSpi<ObjRef> expected, IndexSpi<ObjRef> actual) {
    assertThat(actual.asKeyList()).containsExactlyElementsOf(expected.asKeyList());
    for (var key : expected.asKeyList()) assertThat(actual.get(key)).isEqualTo(expected.get(key));
  }
}

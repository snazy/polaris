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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexStripeObj.indexStripeObj;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams.BuildablePersistenceParams;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexStripe;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexesProvider {
  @ParameterizedTest
  @CsvSource({
    "false,0,false",
    "true,0,false",
    "false,1,false",
    "false,1,true",
    "true,1,false",
    "true,1,true",
    "false,3,false",
    "false,3,true",
    "true,3,false",
    "true,3,true"
  })
  void constructionCapabilitySurvivesIndividualAndBulkLoads(
      boolean mutable, int stripeCount, boolean prefetch) {
    var fixture = fixture(stripeCount);
    if (mutable) {
      var lazy =
          (LazyUpdatableIndex<ObjRef>)
              IndexesProvider.buildWriteIndex(
                  fixture.container(), fixture.persistence(), fixture.codec());
      verifyNoInteractions(fixture.persistence(), fixture.codec());
      var index = lazy.getOrCreate();
      assertThat(index.embedded).isInstanceOf(IndexImpl.class);
      assertThat(index.embedded.isMutable()).isTrue();
      assertThat(index.reference.isMutable()).isTrue();
      assertThat(index.get(key("embedded"))).isEqualTo(fixture.value());
      assertNoLoads(fixture);
      if (stripeCount > 0) {
        exerciseReference(index.reference, fixture, true, prefetch);
      } else {
        assertThat(index.reference.hasElements()).isFalse();
      }
      assertThat(index.isModified()).isFalse();
      assertThat(
              lazy.toIndexed(
                  "idx-",
                  (name, obj) -> {
                    throw new AssertionError("Untouched index must not persist stripes");
                  }))
          .isSameAs(fixture.container());
    } else {
      var read =
          IndexesProvider.buildReadIndex(
              fixture.container(), fixture.persistence(), fixture.codec());
      assertThat(read.get(key("embedded"))).isEqualTo(fixture.value());
      assertNoLoads(fixture);
      assertThat((Object) read)
          .extracting("delegate")
          .satisfies(delegate -> {
            assertThat(delegate).isInstanceOf(IndexSpi.class);
            var root = (IndexSpi<?>) delegate;
            assertThat(root.isMutable()).isFalse();
            if (stripeCount == 0) {
              assertThat(root).isInstanceOf(ReadOnlyIndexImpl.class);
              assertThatThrownBy(() -> root.remove(key("embedded")))
                  .isInstanceOf(IllegalStateException.class);
            } else {
              assertThat(delegate).isInstanceOf(AbstractLayeredIndexImpl.class);
              var layered = (AbstractLayeredIndexImpl<?>) delegate;
              assertThat(layered.embedded).isInstanceOf(ReadOnlyIndexImpl.class);
              assertThat(layered.embedded.isMutable()).isFalse();
              exerciseReference(layered.reference, fixture, false, prefetch);
            }
          });
    }
    verify(fixture.codec(), times(stripeCount == 0 ? 1 : 4)).skip(any(ByteBuffer.class));
  }

  private static void exerciseReference(
      IndexSpi<?> reference, Fixture fixture, boolean mutable, boolean prefetch) {
    assertThat(reference.isMutable()).isEqualTo(mutable);
    assertThat(reference.isLoaded()).isFalse();
    var key = key("stripe-0/m");
    if (prefetch) {
      reference.prefetchIfNecessary(List.of(key));
    }
    assertThat(reference.get(key)).isEqualTo(fixture.value());
    var leaf = reference.stripes().getFirst().stripes().getFirst();
    assertThat((Object) leaf).isInstanceOf(mutable ? IndexImpl.class : ReadOnlyIndexImpl.class);
    assertThat(leaf.isMutable()).isEqualTo(mutable);
    assertThat(leaf.isModified()).isFalse();
    assertThat(leaf.getObjId()).isEqualTo(fixture.container().stripes().getFirst().segment());
    if (mutable) {
      assertThat(reference.mutableStripeForKey(key)).isSameAs(leaf);
      assertThat(reference.mutableStripeForKey(key)).isSameAs(leaf);
    } else {
      assertThatThrownBy(() -> reference.mutableStripeForKey(key))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> leaf.remove(key)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> leaf.splitByTargetSize(64))
          .isInstanceOf(IllegalStateException.class);
    }
    assertThat(leaf.isModified()).isFalse();
    assertThat(reference.get(key)).isEqualTo(fixture.value());
    boolean bulk = prefetch && fixture.container().stripes().size() > 1;
    assertThat(fixture.individualLoads()).hasValue(bulk ? 0 : 1);
    assertThat(fixture.bulkLoads()).hasValue(bulk ? 1 : 0);
  }

  @ParameterizedTest
  @CsvSource({"1,false", "1,true", "3,false", "3,true"})
  void readThenSpillReusesLoadedLeafWithoutRefetchOrReparse(int stripeCount, boolean prefetch) {
    var fixture = fixture(stripeCount);
    var index =
        IndexesProvider.buildEagerWriteIndex(
            fixture.container(), fixture.persistence(), fixture.codec());
    var middle = key("stripe-0/m");
    if (prefetch) {
      index.prefetchIfNecessary(List.of(middle));
    }
    assertThat(index.get(middle)).isEqualTo(fixture.value());
    var leaf = index.reference.mutableStripeForKey(middle);
    assertThat(leaf.isModified()).isFalse();
    verify(fixture.codec(), times(4)).skip(any(ByteBuffer.class));

    var replacement = objRef("test", 23, 128);
    assertThat(index.put(middle, replacement)).isFalse();
    var trigger = key("zz-" + "x".repeat(80));
    assertThat(index.put(trigger, fixture.value())).isTrue();
    var persisted = new ArrayList<IndexStripeObj>();
    var indexed =
        index.toIndexed(
            "idx-",
            (name, obj) -> {
              var stripe = IndexStripeObj.class.cast(obj);
              persisted.add(stripe);
              fixture.objects().put(objRef(stripe), stripe);
            });

    assertThat(index.reference.mutableStripeForKey(middle)).isSameAs(leaf);
    verify(fixture.codec(), times(1 + 3 * stripeCount)).skip(any(ByteBuffer.class));
    boolean bulk = prefetch && stripeCount > 1;
    assertThat(fixture.bulkLoads()).hasValue(bulk ? 1 : 0);
    assertThat(fixture.individualLoads()).hasValue(stripeCount - (bulk ? 1 : 0));
    assertThat(persisted).hasSize(stripeCount == 1 ? 1 : 2);
    if (stripeCount == 3) {
      assertThat(indexed.stripes().get(1).segment())
          .isEqualTo(fixture.container().stripes().get(1).segment());
    }
    var expected = new TreeMap<>(fixture.contents());
    expected.put(middle, replacement);
    expected.put(trigger, fixture.value());
    var read = IndexesProvider.buildReadIndex(indexed, fixture.persistence(), OBJ_REF_SERIALIZER);
    assertThat(read)
        .containsExactlyElementsOf(
            expected.entrySet().stream()
                .map(entry -> Index.Element.of(entry.getKey(), entry.getValue()))
                .toList());
    assertThat(read.get(middle).numParts()).isEqualTo(128);
    assertThat(
            IndexesProvider.buildReadIndex(
                    fixture.container(), fixture.persistence(), OBJ_REF_SERIALIZER)
                .get(middle)
                .numParts())
        .isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void firstSpillUsesDirectlyConstructedEmptyMutableReference(boolean existingContainer) {
    var fixture = fixture(0);
    var write =
        IndexesProvider.buildWriteIndex(
            existingContainer ? fixture.container() : null, fixture.persistence(), fixture.codec());
    var trigger = key("zz-" + "x".repeat(80));
    write.put(trigger, fixture.value());
    var persisted = new ArrayList<IndexStripeObj>();
    var indexed =
        write.toIndexed(
            "idx-",
            (name, obj) -> {
              var stripe = IndexStripeObj.class.cast(obj);
              persisted.add(stripe);
              fixture.objects().put(objRef(stripe), stripe);
            });
    assertNoLoads(fixture);
    assertThat(persisted).hasSize(1);
    assertThat(indexed.stripes()).hasSize(1);
    var read = IndexesProvider.buildReadIndex(indexed, fixture.persistence(), OBJ_REF_SERIALIZER);
    assertThat(read.get(trigger)).isEqualTo(fixture.value());
    assertThat(read).hasSize(existingContainer ? 2 : 1);
  }

  private static void assertNoLoads(Fixture fixture) {
    assertThat(fixture.individualLoads()).hasValue(0);
    assertThat(fixture.bulkLoads()).hasValue(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3})
  void concurrentD3LayersPreserveTombstonesHintsAndOldViewsAcrossSpill(int stripeCount)
      throws Exception {
    var fixture = fixture(stripeCount);
    var hidden = key("stripe-0/m");
    var overridden = key("stripe-0/z");
    var embedded = new IndexLegacyImpl<>(OBJ_REF_SERIALIZER);
    embedded.put(key("embedded"), fixture.value());
    embedded.add(IndexesInternal.indexElement(hidden, null));
    embedded.put(overridden, objRef("test", 17, 128));
    var container =
        ImmutableIndexContainer.<ObjRef>builder()
            .from(fixture.container())
            .embedded(embedded.serialize())
            .build();
    var expected = new TreeMap<>(fixture.contents());
    expected.remove(hidden);
    expected.put(overridden, objRef("test", 17, 128));
    var read = IndexesProvider.buildReadIndex(container, fixture.persistence(), fixture.codec());
    assertThat(read.get(key("embedded"))).isEqualTo(fixture.value());
    assertNoLoads(fixture);
    var start = new CyclicBarrier(4);
    try (var executor = Executors.newFixedThreadPool(4)) {
      var futures = new ArrayList<CompletableFuture<Void>>();
      for (var thread = 0; thread < 4; thread++) {
        var prefetch = thread % 2 == 0;
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    start.await(10, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                  for (var iteration = 0; iteration < 16; iteration++) {
                    if (prefetch) {
                      read.prefetchIfNecessary(new ArrayList<>(expected.keySet()));
                    }
                    assertThat(read.get(hidden)).isNull();
                    assertThat(read.contains(hidden)).isFalse();
                    for (var entry : expected.entrySet()) {
                      assertThat(read.get(entry.getKey()).numParts())
                          .isEqualTo(entry.getValue().numParts());
                    }
                    assertThat(read)
                        .containsExactlyElementsOf(
                            expected.entrySet().stream()
                                .map(entry -> Index.Element.of(entry.getKey(), entry.getValue()))
                                .toList());
                    var reverse = new ArrayList<IndexKey>();
                    read.reverseIterator().forEachRemaining(entry -> reverse.add(entry.key()));
                    assertThat(reverse)
                        .containsExactlyElementsOf(new ArrayList<>(expected.keySet()).reversed());
                  }
                },
                executor));
      }
      for (var future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    }

    var write = IndexesProvider.buildWriteIndex(container, fixture.persistence(), fixture.codec());
    var trigger = key("zz-" + "x".repeat(80));
    write.put(trigger, fixture.value());
    write.put(hidden, fixture.value());
    write.remove(overridden);
    var updated =
        write.toIndexed(
            "idx-",
            (name, obj) -> {
              var stripe = IndexStripeObj.class.cast(obj);
              fixture.objects().put(objRef(stripe), stripe);
            });
    assertThat(read.get(hidden)).isNull();
    assertThat(read.get(overridden).numParts()).isEqualTo(128);
    var after = new TreeMap<>(expected);
    after.put(trigger, fixture.value());
    after.put(hidden, fixture.value());
    after.remove(overridden);
    // Emulate an old node decoding the newly persisted V1 container and its stripes.
    var legacy = new TreeMap<IndexKey, ObjRef>();
    for (var stripe : updated.stripes()) {
      var leaf =
          IndexLegacyImpl.deserializeStoreIndex(
              fixture.objects().get(stripe.segment()).index().duplicate(), OBJ_REF_SERIALIZER);
      leaf.elementIterator()
          .forEachRemaining(entry -> legacy.put(entry.key(), entry.valueNullable()));
    }
    IndexLegacyImpl.deserializeStoreIndex(updated.embedded().duplicate(), OBJ_REF_SERIALIZER)
        .elementIterator()
        .forEachRemaining(entry -> {
          if (entry.valueNullable() == null) {
            legacy.remove(entry.key());
          } else {
            legacy.put(entry.key(), entry.valueNullable());
          }
        });
    assertThat(legacy.keySet()).containsExactlyElementsOf(after.keySet());
    after.forEach((key, value) -> {
      assertThat(legacy.get(key)).isEqualTo(value);
      assertThat(legacy.get(key).numParts()).isEqualTo(value.numParts());
    });
  }

  private static Fixture fixture(int stripeCount) {
    var value = objRef("test", 17, 1);
    var embedded = newStoreIndex(OBJ_REF_SERIALIZER);
    embedded.put(key("embedded"), value);
    var builder = ImmutableIndexContainer.<ObjRef>builder().embedded(embedded.serialize());
    var contents = new TreeMap<IndexKey, ObjRef>();
    contents.put(key("embedded"), value);
    var objects = new HashMap<ObjRef, IndexStripeObj>();
    for (var i = 0; i < stripeCount; i++) {
      var stripe = newStoreIndex(OBJ_REF_SERIALIZER);
      for (var suffix : List.of("a", "m", "z")) {
        var key = key("stripe-" + i + "/" + suffix);
        stripe.put(key, value);
        contents.put(key, value);
      }
      var obj = indexStripeObj(100 + i, stripe.serialize());
      var id = objRef(obj);
      objects.put(id, obj);
      builder.addStripe(IndexStripe.indexStripe(stripe.first(), stripe.last(), id));
    }
    var persistence = mock(Persistence.class);
    var individual = new AtomicInteger();
    var bulk = new AtomicInteger();
    when(persistence.fetch(any(ObjRef.class), eq(IndexStripeObj.class)))
        .thenAnswer(call -> {
          individual.incrementAndGet();
          return objects.get(call.<ObjRef>getArgument(0));
        });
    when(persistence.fetchMany(eq(IndexStripeObj.class), any(ObjRef[].class)))
        .thenAnswer(call -> {
          bulk.incrementAndGet();
          var ids = (ObjRef[]) call.getRawArguments()[1];
          var result = new IndexStripeObj[ids.length];
          for (var i = 0; i < ids.length; i++) {
            result[i] = objects.get(ids[i]);
          }
          return result;
        });
    when(persistence.params())
        .thenReturn(
            BuildablePersistenceParams.builder()
                .maxEmbeddedIndexSize(MemorySize.valueOf("128"))
                .maxIndexStripeSize(MemorySize.valueOf("16k"))
                .build());
    var ids = new AtomicLong(1000);
    when(persistence.generateId()).thenAnswer(call -> ids.incrementAndGet());
    return new Fixture(
        persistence,
        builder.build(),
        spy(OBJ_REF_SERIALIZER),
        objects,
        contents,
        value,
        individual,
        bulk);
  }

  private record Fixture(
      Persistence persistence,
      IndexContainer<ObjRef> container,
      IndexValueSerializer<ObjRef> codec,
      Map<ObjRef, IndexStripeObj> objects,
      Map<IndexKey, ObjRef> contents,
      ObjRef value,
      AtomicInteger individualLoads,
      AtomicInteger bulkLoads) {}
}

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
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.index.IndexStripe.indexStripe;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.serializedIndexVersion;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexVersionPolicy {
  private static final class Store {
    final Map<ObjRef, IndexStripeObj> stripes = new HashMap<>();
    final AtomicLong ids = new AtomicLong(1000);
  }

  private static final class Node {
    final Store store;
    final Persistence persistence = mock(Persistence.class);
    final List<ObjRef> fetched = new ArrayList<>();
    final List<IndexStripeObj> written = new ArrayList<>();

    Node(Store store, int preference, int embeddedLimit, int stripeLimit) {
      this.store = store;
      var params =
          PersistenceParams.BuildablePersistenceParams.builder()
              .preferredIndexWriteVersion(preference)
              .maxEmbeddedIndexSize(MemorySize.valueOf(Integer.toString(embeddedLimit)))
              .maxIndexStripeSize(MemorySize.valueOf(Integer.toString(stripeLimit)))
              .build();
      when(persistence.params()).thenReturn(params);
      when(persistence.generateId()).thenAnswer(x -> store.ids.incrementAndGet());
      when(persistence.fetch(any(ObjRef.class), eq(IndexStripeObj.class)))
          .thenAnswer(x -> {
            ObjRef id = x.getArgument(0);
            fetched.add(id);
            return requireNonNull(store.stripes.get(id));
          });
      when(persistence.fetchMany(eq(IndexStripeObj.class), any(ObjRef[].class)))
          .thenAnswer(x -> {
            ObjRef[] ids = (ObjRef[]) x.getRawArguments()[1];
            var result = new IndexStripeObj[ids.length];
            for (int i = 0; i < ids.length; i++)
              if (ids[i] != null) {
                fetched.add(ids[i]);
                result[i] = requireNonNull(store.stripes.get(ids[i]));
              }
            return result;
          });
    }

    UpdatableIndex<IndexKey> write(IndexContainer<IndexKey> container) {
      return IndexesProvider.buildWriteIndex(container, persistence, INDEX_KEY_SERIALIZER);
    }

    Index<IndexKey> read(IndexContainer<IndexKey> container) {
      return IndexesProvider.buildReadIndex(container, persistence, INDEX_KEY_SERIALIZER);
    }

    void persist(String prefix, Obj value) {
      var stripe = (IndexStripeObj) value;
      store.stripes.put(objRef(stripe), stripe);
      written.add(stripe);
    }

    IndexContainer<IndexKey> finish(UpdatableIndex<IndexKey> view) {
      return view.toIndexed("idx-", this::persist);
    }

    ObjRef stripe(int version, Map<IndexKey, IndexKey> values) {
      var obj = IndexStripeObj.indexStripeObj(store.ids.incrementAndGet(), bytes(version, values));
      store.stripes.put(objRef(obj), obj);
      return objRef(obj);
    }
  }

  private static ByteBuffer bytes(int version, Map<IndexKey, IndexKey> values) {
    var index = newStoreIndex(INDEX_KEY_SERIALIZER, version);
    new TreeMap<>(values).forEach(index::put);
    return index.serialize();
  }

  private static IndexContainer<IndexKey> embedded(int version) {
    return ImmutableIndexContainer.<IndexKey>builder()
        .embedded(bytes(version, Map.of(key("a"), key("old"))))
        .build();
  }

  static Stream<Arguments> versions() {
    return Stream.of(1, 2)
        .flatMap(source -> Stream.of(1, 2).map(preference -> Arguments.of(source, preference)));
  }

  static Stream<Arguments> stripedVersions() {
    return Stream.of(1, 2)
        .flatMap(embedded ->
            Stream.of(1, 2)
                .flatMap(reference ->
                    Stream.of(1, 2)
                        .map(preference -> Arguments.of(embedded, reference, preference))));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void newIndexesUsePreferenceAndEmptyIndexesRetainTheirFormat(int preference) {
    var node = new Node(new Store(), preference, 65536, 65536);
    var index = node.write(null);
    index.put(key("a"), key("value"));
    var container = node.finish(index);
    assertThat(serializedIndexVersion(container.embedded())).isEqualTo(preference);
    assertThat(node.read(container).get(key("a"))).isEqualTo(key("value"));
    var remove = node.write(container);
    assertThat(remove.remove(key("a"))).isTrue();
    assertThat(remove.toOptionalIndexed("idx-", node::persist))
        .hasValueSatisfying(empty -> {
          assertThat(node.read(empty)).isEmpty();
          assertThat(serializedIndexVersion(empty.embedded())).isEqualTo(preference);
        });
    assertThat(node.write(null).toOptionalIndexed("idx-", node::persist))
        .hasValueSatisfying(empty -> {
          assertThat(node.read(empty)).isEmpty();
          assertThat(serializedIndexVersion(empty.embedded())).isEqualTo(preference);
        });
    assertThat(node.written).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("versions")
  void embeddedRewritesPromoteButNeverDemote(int source, int preference) {
    var node = new Node(new Store(), preference, 65536, 65536);
    var old = embedded(source);
    var index = node.write(old);
    assertThat(index.get(key("a"))).isEqualTo(key("old"));
    index.put(key("b"), key("new"));
    var next = node.finish(index);
    assertThat(serializedIndexVersion(next.embedded())).isEqualTo(Math.max(source, preference));
    assertThat(node.read(next)).extracting(e -> e.key()).containsExactly(key("a"), key("b"));
    assertThat(node.read(old).get(key("b"))).isNull();
    assertThat(node.written).isEmpty();
    assertThat(node.fetched).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("versions")
  void untouchedAndReadOnlyWritesReuseContainerWithoutMigration(int source, int preference) {
    var node = new Node(new Store(), preference, 65536, 65536);
    var container = embedded(source);
    assertThat(node.finish(node.write(container))).isSameAs(container);
    var read = node.write(container);
    assertThat(read.get(key("a"))).isEqualTo(key("old"));
    assertThat(read.remove(key("missing"))).isFalse();
    assertThat(node.finish(read)).isSameAs(container);
    assertThat(node.written).isEmpty();
    assertThat(node.fetched).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("stripedVersions")
  void spillCarriesEmbeddedVersionAndPreservesUntouchedStripeIds(
      int embeddedVersion, int referenceVersion, int preference) {
    var store = new Store();
    var node = new Node(store, preference, 64, 8192);
    var first =
        node.stripe(referenceVersion, Map.of(key("a1"), key("old"), key("a2"), key("stable")));
    var untouched = node.stripe(1, Map.of(key("m1"), key("other"), key("m2"), key("other2")));
    var old =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(bytes(embeddedVersion, Map.of()))
            .addStripes(
                indexStripe(key("a1"), key("a2"), first),
                indexStripe(key("m1"), key("m2"), untouched))
            .build();
    var index = node.write(old);
    assertThat(index.get(key("a1"))).isEqualTo(key("old"));
    assertThat(node.fetched).containsExactly(first);
    index.put(key("a1"), key("replacement-long-enough-to-force-spill"));
    var next = node.finish(index);
    assertThat(next.stripes()).hasSize(2);
    assertThat(next.stripes().get(1).segment()).isEqualTo(untouched);
    var changed = store.stripes.get(next.stripes().getFirst().segment());
    assertThat(serializedIndexVersion(changed.index()))
        .isEqualTo(Math.max(preference, Math.max(embeddedVersion, referenceVersion)));
    assertThat(serializedIndexVersion(next.embedded()))
        .isEqualTo(Math.max(preference, embeddedVersion));
    assertThat(node.written).hasSize(1);
    assertThat(node.fetched.stream().filter(first::equals).count()).isEqualTo(1);
    assertThat(node.read(next).get(key("a1")))
        .isEqualTo(key("replacement-long-enough-to-force-spill"));
    assertThat(node.read(old).get(key("a1"))).isEqualTo(key("old"));
  }

  @ParameterizedTest
  @MethodSource("versions")
  void tombstonesMaskReferencesUntilSpillAndCleanupKeepsSelectedFormat(int source, int preference) {
    var node = new Node(new Store(), preference, 65536, 8192);
    var id = node.stripe(source, Map.of(key("a"), key("value")));
    var container =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(bytes(source, Map.of()))
            .addStripe(indexStripe(key("a"), key("a"), id))
            .build();
    var view = node.write(container);
    assertThat(view.remove(key("a"))).isTrue();
    assertThat(view.get(key("a"))).isNull();
    var next = node.finish(view);
    assertThat(serializedIndexVersion(next.embedded())).isEqualTo(Math.max(source, preference));
    assertThat(
            deserializeStoreIndex(next.embedded(), INDEX_KEY_SERIALIZER, false, 1)
                .containsElement(key("a")))
        .isTrue();
    assertThat(node.read(next).get(key("a"))).isNull();
    assertThat(next.stripes().getFirst().segment()).isEqualTo(id);
    var spillNode = new Node(node.store, preference, 1, 8192);
    assertThat(spillNode.write(next).toOptionalIndexed("idx-", spillNode::persist))
        .hasValueSatisfying(empty -> {
          assertThat(spillNode.read(empty)).isEmpty();
          assertThat(empty.stripes()).isEmpty();
          assertThat(serializedIndexVersion(empty.embedded()))
              .isEqualTo(Math.max(source, preference));
        });
    assertThat(spillNode.written).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void embeddedHitDoesNotLoadReferencedStripesOrMigrateUnchangedData(int preference) {
    var node = new Node(new Store(), preference, 65536, 8192);
    var ref = node.stripe(1, Map.of(key("a"), key("from-reference")));
    var old =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(bytes(1, Map.of(key("a"), key("from-embedded"))))
            .addStripe(indexStripe(key("a"), key("a"), ref))
            .build();
    var view = node.write(old);
    assertThat(view.get(key("a"))).isEqualTo(key("from-embedded"));
    assertThat(node.finish(view)).isSameAs(old);
    assertThat(node.fetched).isEmpty();
    assertThat(node.written).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void promotionPreservesSplitVersionsAndOriginalSnapshots(int preference) {
    var node = new Node(new Store(), preference, 64, 128);
    var values = new TreeMap<IndexKey, IndexKey>();
    for (int i = 0; i < 50; i++)
      values.put(key("key-" + String.format("%03d", i)), key("original-value-" + i));
    var old = ImmutableIndexContainer.<IndexKey>builder().embedded(bytes(1, values)).build();
    var view = node.write(old);
    view.put(key("new"), key("new-value"));
    var next = node.finish(view);
    assertThat(next.stripes()).hasSizeGreaterThan(1);
    for (var stripe : node.written)
      assertThat(serializedIndexVersion(stripe.index())).isEqualTo(preference);
    assertThat(node.read(next)).hasSize(51);
    assertThat(node.read(old)).hasSize(50);
  }

  @Test
  void loweredPreferenceReadsV2AndKeepsItAcrossFurtherWrites() {
    var store = new Store();
    var enabled = new Node(store, 2, 65536, 8192);
    var v2 = enabled.write(null);
    v2.put(key("a"), key("first"));
    var container = enabled.finish(v2);
    var rollback = new Node(store, 1, 65536, 8192);
    assertThat(rollback.read(container).get(key("a"))).isEqualTo(key("first"));
    var changed = rollback.write(container);
    changed.put(key("a"), key("second"));
    var next = rollback.finish(changed);
    assertThat(serializedIndexVersion(next.embedded())).isEqualTo(2);
    var fresh = rollback.write(null);
    fresh.put(key("new"), key("fresh"));
    assertThat(serializedIndexVersion(rollback.finish(fresh).embedded())).isEqualTo(1);
  }

  @ParameterizedTest
  @MethodSource("versions")
  void bulkLoadedMixedVersionsKeepPolicyWhenLaterMutated(int firstVersion, int preference) {
    var node = new Node(new Store(), preference, 64, 8192);
    var first = node.stripe(firstVersion, Map.of(key("a"), key("first")));
    var second = node.stripe(3 - firstVersion, Map.of(key("m"), key("second")));
    var container =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(bytes(1, Map.of()))
            .addStripes(
                indexStripe(key("a"), key("a"), first), indexStripe(key("m"), key("m"), second))
            .build();
    var view = node.write(container);
    view.prefetchIfNecessary(List.of(key("a"), key("m")));
    verify(node.persistence).fetchMany(eq(IndexStripeObj.class), any(ObjRef[].class));
    assertThat(node.fetched).containsExactly(first, second);
    assertThat(view.get(key("a"))).isEqualTo(key("first"));
    view.put(key("a"), key("updated-first"));
    view.put(key("m"), key("updated-second"));
    var next = node.finish(view);
    assertThat(node.written).hasSize(2);
    assertThat(
            serializedIndexVersion(node.store.stripes.get(next.stripes().get(0).segment()).index()))
        .isEqualTo(Math.max(firstVersion, preference));
    assertThat(
            serializedIndexVersion(node.store.stripes.get(next.stripes().get(1).segment()).index()))
        .isEqualTo(Math.max(3 - firstVersion, preference));
    assertThat(node.fetched).containsExactly(first, second);
  }

  @Test
  void opportunisticV1ResplittingPersistsPromotedPartsAndOversizedSingleton() {
    var node = new Node(new Store(), 2, 64, 128);
    var first =
        node.stripe(
            1,
            Map.of(
                key("a1"), key("first"), key("a2"), key("x".repeat(400)), key("a3"), key("last")));
    var second = node.stripe(1, Map.of(key("z"), key("other")));
    var container =
        ImmutableIndexContainer.<IndexKey>builder()
            .embedded(bytes(1, Map.of()))
            .addStripes(
                indexStripe(key("a1"), key("a3"), first), indexStripe(key("z"), key("z"), second))
            .build();
    var view = node.write(container);
    view.put(key("z"), key("changed"));
    var next = node.finish(view);
    assertThat(next.stripes()).hasSize(4);
    assertThat(next.stripes()).extracting(stripe -> stripe.segment()).doesNotContain(first, second);
    assertThat(node.written).hasSize(4);
    for (var stripe : node.written) assertThat(serializedIndexVersion(stripe.index())).isEqualTo(2);
    assertThat(node.read(next).get(key("a2"))).isEqualTo(key("x".repeat(400)));
    assertThat(node.read(container).get(key("z"))).isEqualTo(key("other"));
  }

  @ParameterizedTest
  @ValueSource(longs = {7, 991, 31719})
  void deferredV1PromotionMatchesStatefulModelAndPreservesSnapshots(long seed) {
    var model = new TreeMap<IndexKey, IndexKey>();
    var source = newStoreIndex(INDEX_KEY_SERIALIZER, 1);
    for (int i = 0; i < 100; i++) {
      var k = key("entry/" + i);
      var v = key("value/" + i);
      model.put(k, v);
      source.put(k, v);
    }
    var old = source.serialize();
    var index = deserializeStoreIndex(old, INDEX_KEY_SERIALIZER, true, 2);
    var random = new Random(seed);
    for (int step = 0; step < 300; step++) {
      var k = key("entry/" + random.nextInt(150));
      if (random.nextBoolean()) {
        assertThat(index.remove(k))
            .as("seed %s step %s", seed, step)
            .isEqualTo(model.containsKey(k));
        model.remove(k);
      } else {
        var v = step % 5 == 0 ? null : key("changed/" + step);
        var expected =
            !model.containsKey(k)
                ? IndexSpi.AddResult.NEW_KEY
                : model.get(k) == null
                    ? IndexSpi.AddResult.UPDATED_NO_VALUE
                    : IndexSpi.AddResult.UPDATED;
        assertThat(index.add(indexElement(k, v)))
            .as("seed %s step %s", seed, step)
            .isEqualTo(expected);
        model.put(k, v);
      }
      if (step % 19 == 0) {
        var estimate = index.estimatedSerializedSize();
        var snapshot = index.serialize();
        assertThat(snapshot.remaining()).isEqualTo(estimate);
        var read = deserializeStoreIndex(snapshot, INDEX_KEY_SERIALIZER, false, 1);
        assertThat(read.asKeyList()).containsExactlyElementsOf(model.keySet());
        model.forEach((key, value) -> assertThat(read.get(key)).isEqualTo(value));
        assertThat(index.reverseElementIterator())
            .toIterable()
            .extracting(InternalIndexElement::key)
            .containsExactlyElementsOf(model.descendingKeySet());
        var parts = index.splitByTargetSize(512);
        var combined = new ArrayList<IndexKey>();
        for (var part : parts) {
          assertThat(serializedIndexVersion(part.serialize())).isEqualTo(2);
          combined.addAll(part.asKeyList());
        }
        assertThat(combined).containsExactlyElementsOf(model.keySet());
        index.add(indexElement(key("snapshot-only"), key("transient")));
        assertThat(read.containsElement(key("snapshot-only"))).isFalse();
        index.remove(key("snapshot-only"));
      }
    }
    assertThat(deserializeStoreIndex(old, INDEX_KEY_SERIALIZER, false, 1).asKeyList()).hasSize(100);
  }

  static Stream<Long> campaignSeeds() {
    return TestV2IndexCampaign.seeds();
  }

  @ParameterizedTest
  @MethodSource("campaignSeeds")
  void sharedStoreNodesSurviveRepeatedSpillReloadAndRollback(long seed) {
    var store = new Store();
    var expected = new TreeMap<IndexKey, IndexKey>();
    IndexContainer<IndexKey> current = null;
    var saved = new ArrayList<IndexContainer<IndexKey>>();
    var savedModels = new ArrayList<TreeMap<IndexKey, IndexKey>>();
    var random = new Random(seed);
    var trace = new StringBuilder();
    int requests = Math.max(8, TestV2IndexCampaign.steps() / 6);
    try {
      for (int request = 0; request < requests; request++) {
        int preference = request < 3 ? 1 : request % 3 == 0 ? 2 : 1;
        var node = new Node(store, preference, request % 2 == 0 ? 96 : 512, 512);
        var view = node.write(current);
        // Alternate cold mutation with reads before later mutation in the same request.
        if (request % 2 == 0)
          expected.forEach((k, value) -> assertThat(view.get(k)).isEqualTo(value));
        for (int edit = 0; edit < 6; edit++) {
          var k = key("namespace/" + random.nextInt(5) + "/leaf/" + random.nextInt(24));
          boolean remove = random.nextInt(3) == 0;
          trace
              .append(request)
              .append('/')
              .append(preference)
              .append(':')
              .append(k)
              .append(remove ? "-;" : "+;");
          if (remove) {
            assertThat(view.remove(k)).isEqualTo(expected.containsKey(k));
            expected.remove(k);
          } else {
            var value = key("value/" + request + "/" + "x".repeat(random.nextInt(160)));
            assertThat(view.put(k, value)).isEqualTo(!expected.containsKey(k));
            expected.put(k, value);
          }
          assertThat(view.get(k)).isEqualTo(expected.get(k));
        }
        // Periodically drop every logical key, then continue from the formatted empty container.
        if (request % 11 == 10) {
          trace.append("clear;");
          for (var k : List.copyOf(expected.keySet())) view.remove(k);
          expected.clear();
        }
        var next = node.finish(view);
        if (current != null && serializedIndexVersion(current.embedded()) == 2)
          assertThat(serializedIndexVersion(next.embedded())).isEqualTo(2);
        var read = new Node(store, 1, 96, 512).read(next);
        assertThat(read.iterator())
            .toIterable()
            .extracting(e -> e.key())
            .containsExactlyElementsOf(expected.keySet());
        assertThat(read.reverseIterator())
            .toIterable()
            .extracting(e -> e.key())
            .containsExactlyElementsOf(expected.descendingKeySet());
        expected.forEach((k, value) -> assertThat(read.get(k)).isEqualTo(value));
        saved.add(next);
        savedModels.add(new TreeMap<>(expected));
        if (saved.size() > 3) {
          saved.removeFirst();
          savedModels.removeFirst();
        }
        for (int i = 0; i < saved.size(); i++) {
          var old = node.read(saved.get(i));
          assertThat(old.iterator())
              .toIterable()
              .extracting(e -> e.key())
              .containsExactlyElementsOf(savedModels.get(i).keySet());
          savedModels.get(i).forEach((k, value) -> assertThat(old.get(k)).isEqualTo(value));
        }
        current = next;
      }
    } catch (RuntimeException | AssertionError failure) {
      throw new AssertionError("generator=1 node-seed=" + seed + " trace=" + trace, failure);
    }
    System.out.println("INDEX_NODE_CAMPAIGN generator=1 seed=" + seed + " requests=" + requests);
  }

  @Test
  void deferredPromotionIncludesChangesWithoutAnIntermediateV2Snapshot() {
    var source = newStoreIndex(INDEX_KEY_SERIALIZER, 1);
    source.put(key("a"), key("first"));
    var old = source.serialize();
    var view = deserializeStoreIndex(old, INDEX_KEY_SERIALIZER, true, 2);
    assertThat(view).isExactlyInstanceOf(MutableV2IndexImpl.class);
    assertThat(view.isModified()).isFalse();
    view.put(key("a"), key("second"));
    view.put(key("b"), key("added"));
    var size = view.estimatedSerializedSize();
    var bytes = view.serialize();
    assertThat(bytes.remaining()).isEqualTo(size);
    assertThat(serializedIndexVersion(bytes)).isEqualTo(2);
    assertThat(deserializeStoreIndex(bytes, INDEX_KEY_SERIALIZER, false, 1).get(key("b")))
        .isEqualTo(key("added"));
    assertThat(deserializeStoreIndex(old, INDEX_KEY_SERIALIZER, false, 1).get(key("a")))
        .isEqualTo(key("first"));
    var unmodified = deserializeStoreIndex(old, INDEX_KEY_SERIALIZER, true, 2);
    unmodified.splitByTargetSize(Long.MAX_VALUE);
    assertThat(unmodified.isModified()).isTrue();
  }
}

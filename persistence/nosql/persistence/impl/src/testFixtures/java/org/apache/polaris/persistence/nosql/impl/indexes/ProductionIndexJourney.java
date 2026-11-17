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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.polaris.persistence.nosql.api.index.IndexStripe.indexStripe;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import com.google.common.hash.Hashing;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.index.ImmutableIndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexContainer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/**
 * Real index factories and finalization over immutable in-memory stripe payloads; no backend I/O.
 */
final class ProductionIndexJourney<V> {
  record Outcome<V>(
      IndexContainer<V> container, List<IndexStripeObj> written, long observed, int fetched) {}

  private final RepresentativeIndexWorkload<V> workload;
  private final List<SplitV2IndexWriter.Change<V>> changes;
  private final IndexContainer<V> input;
  private final Map<ObjRef, IndexStripeObj> stored = new HashMap<>();
  private final PersistenceParams params;
  private final Persistence persistence;
  private List<IndexStripeObj> written;
  private int fetched;
  private long nextId;

  ProductionIndexJourney(
      RepresentativeIndexWorkload<V> workload, String variant, String scenario, int mutations) {
    this.workload = workload;
    changes =
        workload.changes(
            mutations,
            RepresentativeIndexWorkload.Locality.SCATTERED,
            RepresentativeIndexWorkload.Operation.MIXED);
    boolean v2 = variant.equals("PRODUCTION_V2");
    checkState(v2 || variant.equals("BASELINE") || variant.equals("PROMOTE_V2"));
    int version = v2 ? 2 : 1;
    boolean defaults = scenario.startsWith("DEFAULT_");
    var parameters =
        PersistenceParams.BuildablePersistenceParams.builder()
            .preferredIndexWriteVersion(variant.equals("BASELINE") ? 1 : 2);
    if (!defaults)
      parameters
          .maxEmbeddedIndexSize(MemorySize.valueOf(scenario.equals("EMBEDDED") ? "1000000" : "64"))
          .maxIndexStripeSize(MemorySize.valueOf("32768"));
    params = parameters.build();
    var builder = ImmutableIndexContainer.<V>builder();
    if (scenario.equals("EMBEDDED")) builder.embedded(encode(workload.keys, version));
    else if (defaults) {
      checkState(scenario.equals("DEFAULT_UPDATE") || scenario.equals("DEFAULT_SPILL"));
      var full =
          IndexesInternal.deserializeStoreIndex(
              encode(workload.keys, version), workload.codec, true, 1);
      int id = 100;
      for (var part : full.splitByTargetSize(params.maxIndexStripeSize().asLong())) {
        var stripe = IndexStripeObj.indexStripeObj(id++, part.serialize());
        var ref = objRef(stripe);
        stored.put(ref, stripe);
        builder.addStripes(indexStripe(part.first(), part.last(), ref));
      }
      ByteBuffer cache = encode(List.of(), version);
      if (scenario.equals("DEFAULT_SPILL")) {
        // Most recent creation keys, sorted by the encoder: names/locations scatter, IDs cluster.
        var candidates =
            workload.dataset.creationOrder().stream()
                .map(RepresentativeKeyDataset.Entry::key)
                .filter(workload.model::containsKey)
                .toList()
                .reversed();
        int low = 0, high = candidates.size();
        while (low <= high) {
          int count = (low + high) >>> 1;
          var trial = encode(candidates.subList(0, count), version);
          if (trial.remaining() <= params.maxEmbeddedIndexSize().asLong() - 32) {
            cache = trial;
            low = count + 1;
          } else high = count - 1;
        }
      }
      builder.embedded(cache);
    } else {
      checkState(scenario.equals("STRIPED_SPILL"));
      builder.embedded(encode(List.of(), version));
      for (int from = 0; from < workload.keys.size(); from += 512) {
        var keys = workload.keys.subList(from, Math.min(from + 512, workload.keys.size()));
        var stripe = IndexStripeObj.indexStripeObj(100 + from, encode(keys, version));
        var id = objRef(stripe);
        stored.put(id, stripe);
        builder.addStripes(indexStripe(keys.getFirst(), keys.getLast(), id));
      }
    }
    input = builder.build();
    persistence =
        (Persistence)
            Proxy.newProxyInstance(
                Persistence.class.getClassLoader(),
                new Class<?>[] {Persistence.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "params" -> params;
                      case "generateId" -> ++nextId;
                      case "fetch" -> {
                        fetched++;
                        yield stored.get((ObjRef) args[0]);
                      }
                      case "fetchMany" -> {
                        var ids = (ObjRef[]) args[1];
                        var values = new IndexStripeObj[ids.length];
                        for (int i = 0; i < ids.length; i++)
                          if (ids[i] != null) {
                            fetched++;
                            values[i] = stored.get(ids[i]);
                          }
                        yield values;
                      }
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
  }

  private ByteBuffer encode(List<IndexKey> keys, int version) {
    var index = newStoreIndex(workload.codec, 1);
    for (var key : keys) index.put(key, workload.model.get(key));
    return version == 2
        ? V2IndexWriter.write(index, workload.codec, V2IndexFormat.DEFAULT)
        : index.serialize();
  }

  Outcome<V> run(boolean write, int request) {
    fetched = 0;
    nextId = 1_000_000;
    long observed = 0;
    if (!write) {
      var view = IndexesProvider.buildReadIndex(input, persistence, workload.codec);
      for (var key : workload.requests[request & 255]) {
        var value = view.get(key);
        if (value != null) observed += value.hashCode();
      }
      return new Outcome<>(input, List.of(), observed, fetched);
    }
    written = new ArrayList<>();
    var view = IndexesProvider.buildWriteIndex(input, persistence, workload.codec);
    for (int i = 0; i < 4; i++) {
      var value = view.get(changes.get(i % changes.size()).key());
      if (value != null) observed += value.hashCode();
    }
    for (var change : changes) {
      if (change.element() == null) view.remove(change.key());
      else view.put(change.key(), change.element().valueNullable());
    }
    var container = view.toIndexed("bench-", (prefix, obj) -> written.add((IndexStripeObj) obj));
    return new Outcome<>(container, written, observed, fetched);
  }

  Map<String, Object> metadata(Map<String, String> parameters, boolean write, String method) {
    var outcome = run(write, 0);
    var expected = new TreeMap<>(workload.model);
    if (write) RepresentativeIndexWorkload.applyModel(expected, changes);
    var all = new HashMap<>(stored);
    for (var stripe : outcome.written()) all.put(objRef(stripe), stripe);
    // Validate outputs with independent leaves/model, without using the request persistence
    // adapter.
    var actual = new TreeMap<IndexKey, V>();
    for (var stripe : outcome.container().stripes()) {
      var read =
          IndexesInternal.deserializeStoreIndex(
              all.get(stripe.segment()).index(), workload.codec, false, 1);
      read.elementIterator().forEachRemaining(e -> actual.put(e.key(), e.valueNullable()));
    }
    var embedded =
        IndexesInternal.deserializeStoreIndex(
            outcome.container().embedded(), workload.codec, false, 1);
    embedded
        .elementIterator()
        .forEachRemaining(e -> {
          if (e.hasValue()) actual.put(e.key(), e.valueNullable());
          else actual.remove(e.key());
        });
    checkState(actual.keySet().equals(expected.keySet()), "Journey output keys differ");
    for (var e : expected.entrySet()) {
      var value = actual.get(e.getKey());
      var got =
          workload
              .codec
              .serialize(value, ByteBuffer.allocate(workload.codec.serializedSize(value)))
              .flip();
      var wanted =
          workload
              .codec
              .serialize(
                  e.getValue(), ByteBuffer.allocate(workload.codec.serializedSize(e.getValue())))
              .flip();
      checkState(got.equals(wanted), "Journey output value differs");
    }
    var metadata =
        workload.metadata(
            parameters, new RepresentativeIndexWorkload.Outcome(List.of(workload.input), 0), null);
    metadata.put("operation", method);
    metadata.put("implementation", IndexesProvider.class.getName());
    metadata.put("indexRole", write ? "WRITE" : "READ");
    metadata.put("inputVersion", Byte.toUnsignedInt(input.embedded().get(0)));
    metadata.put("outputVersion", Byte.toUnsignedInt(outcome.container().embedded().get(0)));
    metadata.put("promotionIncluded", write && parameters.get("variant").equals("PROMOTE_V2"));
    metadata.put("inputBytes", size(input, stored));
    metadata.put("outputBytes", size(outcome.container(), all));
    metadata.put("outputEntries", expected.size());
    var hasher = Hashing.sha256().newHasher();
    hasher.putBytes(input.embedded().duplicate());
    for (var stripe : input.stripes())
      hasher.putBytes(stored.get(stripe.segment()).index().duplicate());
    metadata.put("inputSha256", hasher.hash().toString());
    metadata.put("configuredEmbeddedLimit", params.maxEmbeddedIndexSize().asLong());
    metadata.put("configuredStripeLimit", params.maxIndexStripeSize().asLong());
    metadata.put(
        "initialEmbeddedEntries",
        IndexesInternal.deserializeStoreIndex(input.embedded(), workload.codec, false, 1)
            .asKeyList()
            .size());
    metadata.put("initialEmbeddedBytes", input.embedded().remaining());
    metadata.put("inputStripes", input.stripes().size());
    metadata.put("outputStripes", outcome.container().stripes().size());
    metadata.put("fetchedStripes", outcome.fetched());
    metadata.put("writtenStripes", outcome.written().size());
    metadata.put("storageAdapter", "in-memory payload lookup; no backend I/O or REST scope");
    return new LinkedHashMap<>(metadata);
  }

  private int size(IndexContainer<V> container, Map<ObjRef, IndexStripeObj> stripes) {
    int bytes = container.embedded().remaining();
    for (var stripe : container.stripes())
      bytes += stripes.get(stripe.segment()).index().remaining();
    return bytes;
  }
}

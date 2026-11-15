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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;

/** Fixed logical populations and traces, shared by the representative read and mutation benches. */
final class RepresentativeIndexWorkload<V> {
  enum Variant {
    BASELINE,
    D3,
    D4,
    PREFIX_ENDS,
    FRONT_ENDS
  }

  enum History {
    FRESH,
    EDITED4
  }

  enum Partitioning {
    NATIVE,
    FIXED64
  }

  enum Operation {
    UPDATE,
    MIXED
  }

  enum Locality {
    CLUSTERED,
    SCATTERED
  }

  record Outcome(List<ByteBuffer> parts, long observed) {}

  final RepresentativeKeyDataset dataset;
  final Variant variant;
  final IndexValueSerializer<V> codec;
  final List<IndexKey> keys;
  final IndexKey[][] requests = new IndexKey[256][16];
  final IndexKey missing;
  final IndexKey prefix;
  final IndexKey prefixEnd;
  final ByteBuffer input;
  final Map<IndexKey, V> model;
  private final BiFunction<RepresentativeKeyDataset.Entry, Integer, V> values;
  private final SplitV2Format.Options options;
  private final int futureOrdinal;

  static RepresentativeIndexWorkload<?> create(
      RepresentativeKeyDataset.Family family,
      long seed,
      Variant variant,
      int entries,
      long byteBudget,
      History history,
      Partitioning partitioning) {
    checkArgument(entries >= 32, "Need at least 32 entries");
    checkArgument(byteBudget >= 0, "Negative budget");
    // 'entries' caps the search. Binary search retains a feasible nested prefix; compression can
    // make size nonmonotonic, so this deliberately does not claim the globally largest prefix.
    if (byteBudget == 0) return build(family, seed, variant, entries, history, partitioning);
    RepresentativeIndexWorkload<?> selected = null;
    int low = 32, high = entries;
    while (low <= high) {
      int count = low + (high - low) / 2;
      var candidate = build(family, seed, variant, count, history, partitioning);
      if (candidate.input.remaining() <= byteBudget) {
        selected = candidate;
        low = count + 1;
      } else high = count - 1;
    }
    checkArgument(selected != null, "Budget cannot fit the minimum 32-entry population");
    return selected;
  }

  private static RepresentativeIndexWorkload<?> build(
      RepresentativeKeyDataset.Family family,
      long seed,
      Variant variant,
      int entries,
      History history,
      Partitioning partitioning) {
    var population = RepresentativeKeyDataset.generate(family, entries + 1024, seed);
    return switch (family) {
      case SNOWFLAKE ->
          new RepresentativeIndexWorkload<>(
              population,
              variant,
              entries,
              history,
              partitioning,
              IndexKey.INDEX_KEY_SERIALIZER,
              (entry, revision) -> revision == 0 ? entry.name() : renamed(entry.name(), revision));
      case WORDS ->
          new RepresentativeIndexWorkload<>(
              population,
              variant,
              entries,
              history,
              partitioning,
              ObjRef.OBJ_REF_SERIALIZER,
              (entry, revision) ->
                  ObjRef.objRef(
                      IcebergTableObj.TYPE,
                      // A new object version gets a new ID, not just auxiliary part-count hints.
                      entry.stableId()
                          + revision
                              * (1_000_000L << (DEFAULT_NODE_ID_BITS + DEFAULT_SEQUENCE_BITS)),
                      1 + (entry.creationOrdinal() + revision) % 3));
      case LOCATION ->
          new RepresentativeIndexWorkload<>(
              population,
              variant,
              entries,
              history,
              partitioning,
              EntityIdSet.ENTITY_ID_SET_SERIALIZER,
              (entry, revision) ->
                  EntityIdSet.entityIdSet(
                      entry.creationOrdinal() % 16 == 0
                          ? new LinkedHashSet<>(
                              List.of(
                                  entry.stableId(),
                                  population
                                      .creationOrder()
                                      .get((entry.creationOrdinal() + 1 + revision) % entries)
                                      .stableId(),
                                  population
                                      .creationOrder()
                                      .get((entry.creationOrdinal() + 20 + revision) % entries)
                                      .stableId()))
                          : Set.of(
                              revision == 0
                                  ? entry.stableId()
                                  : population
                                      .creationOrder()
                                      .get((entry.creationOrdinal() + revision) % entries)
                                      .stableId())));
    };
  }

  private static IndexKey renamed(IndexKey name, int revision) {
    if (revision % 2 == 0) return name;
    var raw = name.asByteBuffer();
    int first = 0, second = 0;
    for (int i = 0; i < raw.remaining(); i++)
      if (raw.get(i) == 0) {
        if (first == 0) first = i;
        else {
          second = i;
          break;
        }
      }
    var output = ByteBuffer.allocate(raw.remaining());
    output
        .put(raw.slice(first + 1, second - first - 1))
        .put((byte) 0)
        .put(raw.slice(0, first))
        .put((byte) 0)
        .put(raw.slice(second + 1, raw.remaining() - second - 1));
    return IndexKey.key(output.flip());
  }

  private RepresentativeIndexWorkload(
      RepresentativeKeyDataset population,
      Variant variant,
      int entries,
      History history,
      Partitioning partitioning,
      IndexValueSerializer<V> codec,
      BiFunction<RepresentativeKeyDataset.Entry, Integer, V> values) {
    this.variant = variant;
    this.codec = codec;
    this.values = values;
    options =
        new SplitV2Format.Options(
            variant == Variant.PREFIX_ENDS
                ? SplitV2Format.Layout.PREFIX_ENDS
                : SplitV2Format.Layout.FRONT_ENDS,
            64,
            V2IndexWriter.DirectoryEncoding.FULL,
            8,
            partitioning == Partitioning.NATIVE);
    // Reserve genuine future creations for each history and measured structural insertion.
    dataset = population;
    var family = dataset.config().family();
    long seed = dataset.config().seed();
    var logical = new TreeMap<IndexKey, V>();
    for (int i = 0; i < entries; i++) {
      var entry = dataset.creationOrder().get(i);
      logical.put(entry.key(), values.apply(entry, 0));
    }
    var mutable = index(logical);
    ByteBuffer encoded = encode(mutable);
    int next = entries;
    if (history == History.EDITED4) {
      // D3/D4 are read-only views of V1 history, so prepare their history with the production
      // writer.
      for (int round = 0; round < 4; round++) {
        var changes = changes(logical, 16, Locality.SCATTERED, Operation.MIXED, next, round + 1);
        next += 16;
        encoded = apply(encoded, changes, false, Long.MAX_VALUE).parts().getFirst();
        applyModel(logical, changes);
      }
    }
    futureOrdinal = next;
    input = encoded.asReadOnlyBuffer();
    model = Map.copyOf(logical);
    keys = List.copyOf(logical.keySet());
    var random = new Random(seed ^ 0x678adcL);
    for (var request : requests) {
      var selected = new HashSet<Integer>();
      for (int i = 0; i < request.length; i++) {
        int ordinal;
        do {
          ordinal = random.nextInt(keys.size());
        } while (!selected.add(ordinal));
        request[i] = keys.get(ordinal);
      }
    }
    missing = dataset.creationOrder().get(entries + 1023).key();
    var selected = keys.get(keys.size() / 2).asByteBuffer();
    // Namespace prefix ends after the parent separator; numeric keys use a real binary prefix.
    int end = family == RepresentativeKeyDataset.Family.SNOWFLAKE ? 5 : 0;
    if (end == 0) {
      int separators = family == RepresentativeKeyDataset.Family.WORDS ? 2 : 4;
      for (int i = 0; i < selected.remaining(); i++) {
        if (selected.get(i) == 0 && --separators == 0) {
          end = i + 1;
          break;
        }
      }
    }
    var prefixBytes = new byte[end];
    selected.get(prefixBytes);
    prefix = IndexKey.key(ByteBuffer.wrap(prefixBytes));
    // A lexicographic successor bounds all descendants, including numeric prefixes ending in ff.
    int increment = end - 1;
    while (increment >= 0 && prefixBytes[increment] == (byte) 255) prefixBytes[increment--] = 0;
    checkArgument(increment >= 0, "Unbounded prefix");
    prefixBytes[increment]++;
    prefixEnd = IndexKey.key(ByteBuffer.wrap(Arrays.copyOf(prefixBytes, increment + 1)));
  }

  private IndexImpl<V> index(Map<IndexKey, V> logical) {
    var index = new IndexImpl<>(codec);
    logical.forEach(index::put);
    return index;
  }

  private ByteBuffer encode(IndexSpi<V> index) {
    return variant == Variant.PREFIX_ENDS || variant == Variant.FRONT_ENDS
        ? SplitV2IndexWriter.write(index, codec, options)
        : index.serialize();
  }

  IndexSpi<V> fresh() {
    return read(input);
  }

  IndexSpi<V> read(ByteBuffer bytes) {
    return switch (variant) {
      case BASELINE -> IndexImpl.deserializeStoreIndex(bytes.duplicate(), codec);
      case D3 -> new Compact3IndexImpl<>(bytes.duplicate(), codec);
      case D4 -> new CompactIndexImpl<>(bytes.duplicate(), codec);
      case PREFIX_ENDS, FRONT_ENDS -> new SplitV2IndexReader<>(bytes.duplicate(), codec);
    };
  }

  List<SplitV2IndexWriter.Change<V>> changes(int count, Locality locality, Operation operation) {
    return changes(new TreeMap<>(model), count, locality, operation, futureOrdinal, 7);
  }

  private List<SplitV2IndexWriter.Change<V>> changes(
      TreeMap<IndexKey, V> logical,
      int count,
      Locality locality,
      Operation operation,
      int future,
      int revision) {
    checkArgument(count > 0 && count <= logical.size(), "Changes exceed input population");
    checkArgument(
        (long) future + count <= dataset.creationOrder().size() - 1,
        "Changes exceed reserved future population");
    var sorted = List.copyOf(logical.keySet());
    var entriesByKey = new HashMap<IndexKey, RepresentativeKeyDataset.Entry>();
    for (var entry : dataset.creationOrder()) entriesByKey.put(entry.key(), entry);
    var changes = new ArrayList<SplitV2IndexWriter.Change<V>>();
    for (int i = 0; i < count; i++) {
      int ordinal =
          locality == Locality.CLUSTERED
              ? (sorted.size() - count) / 2 + i
              : (int) ((long) i * sorted.size() / count);
      var key = sorted.get(ordinal);
      if (operation == Operation.MIXED && i % 3 == 0) {
        changes.add(
            new SplitV2IndexWriter.Change<>(key, null)); // Physical removal, never tombstone.
      } else {
        boolean insertion = operation == Operation.MIXED && i % 3 == 2;
        var entry = insertion ? dataset.creationOrder().get(future + i) : entriesByKey.get(key);
        var value = values.apply(entry, insertion ? 0 : revision);
        if (!insertion && Objects.equals(value, logical.get(key)))
          value = values.apply(entry, revision + 1);
        changes.add(new SplitV2IndexWriter.Change<>(entry.key(), indexElement(entry.key(), value)));
      }
    }
    changes.sort(Comparator.comparing(SplitV2IndexWriter.Change::key));
    return List.copyOf(changes);
  }

  static <V> void applyModel(Map<IndexKey, V> logical, List<SplitV2IndexWriter.Change<V>> changes) {
    for (var change : changes) {
      if (change.element() == null) logical.remove(change.key());
      else logical.put(change.key(), change.element().valueNullable());
    }
  }

  record PreparedMutation<V>(
      RepresentativeIndexWorkload<V> workload, List<SplitV2IndexWriter.Change<V>> changes) {
    Outcome run(boolean split, long target) {
      return workload.spill(changes, split, target);
    }
  }

  PreparedMutation<V> prepareMutation(int count, Locality locality, Operation operation) {
    return new PreparedMutation<>(this, changes(count, locality, operation));
  }

  Outcome spill(List<SplitV2IndexWriter.Change<V>> changes, boolean split, long target) {
    checkArgument(variant != Variant.D3 && variant != Variant.D4, "Read-only variant");
    return apply(input, changes, split, target);
  }

  private Outcome apply(
      ByteBuffer source, List<SplitV2IndexWriter.Change<V>> changes, boolean split, long target) {
    IndexSpi<V> view =
        variant == Variant.D3 || variant == Variant.D4
            ? IndexImpl.deserializeStoreIndex(source.duplicate(), codec)
            : read(source);
    long observed = 0;
    for (int i = 0; i < 4; i++) {
      var value = view.get(changes.get(i % changes.size()).key());
      if (value != null) observed += value.hashCode();
    }
    if (view instanceof SplitV2IndexReader<V> base) {
      return new Outcome(
          split
              ? SplitV2IndexWriter.mergeSplit(base, changes, options, target)
              : List.of(SplitV2IndexWriter.merge(base, changes, options)),
          observed);
    }
    for (var change : changes) {
      if (change.element() == null) view.remove(change.key());
      else view.add(change.element());
    }
    var parts = new ArrayList<ByteBuffer>();
    if (split) for (var part : view.splitByTargetSize(target)) parts.add(part.serialize());
    else {
      view.estimatedSerializedSize();
      parts.add(view.serialize());
    }
    return new Outcome(List.copyOf(parts), observed);
  }

  Map<String, Object> metadata(Map<String, String> params, Outcome output, String operation) {
    var result = new LinkedHashMap<String, Object>();
    result.put("params", params);
    result.put(
        "datasetId",
        RepresentativeKeyDataset.GENERATOR_VERSION
            + "/"
            + params.get("family")
            + "/"
            + params.get("seed")
            + "/"
            + keys.size());
    result.put("valueSerializer", codec.getClass().getName());
    result.put("inputEntries", keys.size());
    result.put("inputBytes", input.remaining());
    result.put("inputSha256", Hashing.sha256().hashBytes(input.duplicate()).toString());
    int outputEntries = 0, outputBytes = 0;
    for (var part : output.parts()) {
      outputBytes += part.remaining();
      outputEntries += read(part).asKeyList().size();
    }
    result.put("outputEntries", outputEntries);
    result.put("outputBytes", outputBytes);
    if (operation != null) result.put("operation", operation);
    var lengths = keys.stream().mapToInt(k -> k.asByteBuffer().remaining()).summaryStatistics();
    // Compute namespace occupancy from raw names, independently of selected key family.
    var occupancy = new TreeMap<IndexKey, Integer>();
    for (var entry : dataset.creationOrder())
      if (model.containsKey(entry.key())) {
        var raw = entry.name().asByteBuffer();
        int zeros = 0, end = 0;
        for (int i = 0; i < raw.remaining(); i++)
          if (raw.get(i) == 0 && ++zeros == 2) {
            end = i;
            break;
          }
        var bytes = new byte[end];
        raw.get(bytes);
        occupancy.merge(IndexKey.key(ByteBuffer.wrap(bytes)), 1, Integer::sum);
      }
    int globalPrefix = keys.getFirst().asByteBuffer().remaining();
    long adjacentPrefixes = 0, blockPrefixes = 0;
    int blockCount = 0;
    var previous = keys.getFirst().asByteBuffer();
    int blockPrefix = previous.remaining();
    for (int i = 1; i < keys.size(); i++) {
      var current = keys.get(i).asByteBuffer();
      int shared = 0;
      while (shared < Math.min(previous.remaining(), current.remaining())
          && previous.get(shared) == current.get(shared)) shared++;
      globalPrefix = Math.min(globalPrefix, shared);
      adjacentPrefixes += shared;
      if (i % 64 == 0) {
        blockPrefixes += blockPrefix;
        blockCount++;
        blockPrefix = current.remaining();
      } else blockPrefix = Math.min(blockPrefix, shared);
      previous = current;
    }
    blockPrefixes += blockPrefix;
    blockCount++;
    var distribution = new LinkedHashMap<String, Object>();
    distribution.put("keyLengthMin", lengths.getMin());
    distribution.put("keyLengthMax", lengths.getMax());
    distribution.put("keyLengthMean", lengths.getAverage());
    distribution.put("namespaceFamilies", occupancy.size());
    distribution.put(
        "familyPopulationMin",
        occupancy.values().stream().mapToInt(Integer::intValue).min().orElse(0));
    distribution.put(
        "familyPopulationMax",
        occupancy.values().stream().mapToInt(Integer::intValue).max().orElse(0));
    distribution.put("rawGlobalPrefixBytes", globalPrefix);
    distribution.put("rawAdjacentPrefixMeanBytes", (double) adjacentPrefixes / (keys.size() - 1));
    distribution.put("rawFixed64BlockPrefixMeanBytes", (double) blockPrefixes / blockCount);
    result.put("distribution", distribution);
    return result;
  }
}

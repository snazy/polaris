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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_NODE_ID_BITS;
import static org.apache.polaris.ids.api.SnowflakeIdGenerator.DEFAULT_SEQUENCE_BITS;
import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;

/**
 * Seeded populations shared by numeric, name, and location index experiments. Population prefixes
 * remain unchanged when the requested size increases; sorted positions need not remain unchanged.
 * The v1 distribution policy uses 19 top namespaces, 1–13 children per top namespace, and
 * per-namespace creation bursts of 1–128 leaves. These are deliberate test distributions, not
 * measured production frequencies. Creation visits namespace families grouped by their top-level
 * namespace, so a small prefix can share one top-level namespace; the full population has 19 roots.
 * Numeric IDs interleave five nodes with logical time and sequence gaps; location UUIDs are
 * deterministic name-based UUIDs. This fixture does not change the legacy {@link
 * KeyIndexTestSet.RealisticKeySet} populations.
 */
public record RepresentativeKeyDataset(
    Config config, List<Entry> creationOrder, List<Entry> sortedEntries)
    implements KeyIndexTestSet.KeySet {

  public static final String GENERATOR_VERSION = "representative-v1";

  public static final List<Long> BENCHMARK_SEEDS =
      List.of(0x51a7e123L, 0x6b029f45L, 0x17c8d9abL, 0x342fed01L, 0x79abc653L);

  public static final String BUCKET = "polaris-index-bucket";
  public static final String PREFIX = "polaris-index-benchmark-prefix";

  public enum Family {
    SNOWFLAKE,
    WORDS,
    LOCATION
  }

  public record Config(Family family, int size, long seed) {
    public Config {
      Objects.requireNonNull(family, "family");
      checkArgument(size >= 0, "size must be non-negative");
    }

    public String datasetId() {
      return GENERATOR_VERSION + "/" + family + "/" + seed + "/" + size;
    }
  }

  /**
   * Association metadata follows creation order, independently of numeric or lexical sorting. Names
   * contain two namespace components and a leaf, separated by zero bytes like
   * ContentIdentifier.toIndexKey(). Location retains the human-readable URL for value generation.
   */
  public record Entry(
      int creationOrdinal,
      long stableId,
      IndexKey name,
      UUID uuid,
      String location,
      IndexKey key) {}

  public RepresentativeKeyDataset {
    creationOrder = List.copyOf(creationOrder);
    sortedEntries = List.copyOf(sortedEntries);
  }

  /** List positions are sorted ordinals; entries retain their original creation ordinals. */
  @Override
  public List<IndexKey> keys() {
    return sortedEntries.stream().map(Entry::key).toList();
  }

  public static RepresentativeKeyDataset generate(Family family, int size, long seed) {
    var config = new Config(family, size, seed);
    // The bundled corpus actually starts at 11 characters; do not synthesize shorter words.
    var words =
        new ArrayList<>(
            KeyIndexTestSet.Words.words().stream()
                .filter(w -> w.length() >= 8 && w.length() <= 20)
                .toList());
    var namesRandom = new Random(seed);
    Collections.shuffle(words, namesRandom);

    // Reuse namespace families with different occupancies instead of choosing fresh ancestors
    // for each leaf. The schedule and per-family leaf offsets are independent of requested size.
    var namespaces = new ArrayList<String>();
    for (int top = 0, word = 19; top < 19; top++) {
      int children = 1 + namesRandom.nextInt(13);
      for (int child = 0; child < children; child++) {
        namespaces.add(words.get(top) + "\u0000" + words.get(word++));
      }
    }
    checkArgument(
        (long) size <= (long) namespaces.size() * words.size(),
        "size exceeds the unique word-name population");
    var leafOffsets = new int[namespaces.size()];
    var leafCounts = new int[namespaces.size()];
    for (int i = 0; i < leafOffsets.length; i++) {
      leafOffsets[i] = namesRandom.nextInt(words.size());
    }

    var idRandom = new Random(seed ^ 0x724de38bL);
    var nodes = new ArrayList<Integer>();
    for (int i = 0; i < (1 << DEFAULT_NODE_ID_BITS); i++) {
      nodes.add(i);
    }
    Collections.shuffle(nodes, idRandom);
    var timestamps = new long[5];
    var sequences = new int[5];
    // Milliseconds since SnowflakeIdGenerator.ID_EPOCH, not an absolute or wall-clock time.
    long logicalTime = 40_000_000_000L + idRandom.nextInt(1_000_000);
    var entries = new ArrayList<Entry>(size);
    while (entries.size() < size) {
      for (int ns = 0; ns < namespaces.size() && entries.size() < size; ns++) {
        int occupancy = 1 << (ns % 8);
        for (int leaf = 0;
            leaf < occupancy && entries.size() < size && leafCounts[ns] < words.size();
            leaf++) {
          int ordinal = entries.size();
          var name =
              namespaces.get(ns)
                  + "\u0000"
                  + words.get((leafOffsets[ns] + leafCounts[ns]++) % words.size());
          var uuid = UUID.nameUUIDFromBytes((seed + "\u0000" + name).getBytes(UTF_8));
          var location =
              "s3://" + BUCKET + "/" + PREFIX + "/" + name.replace('\u0000', '/') + "-" + uuid;

          if (ordinal % 20 == 0) {
            logicalTime += 1 + idRandom.nextInt(11);
          }
          int node = ordinal % 5;
          if (timestamps[node] != logicalTime) {
            timestamps[node] = logicalTime;
            sequences[node] = 0;
          }
          sequences[node] += 1 + idRandom.nextInt(4);
          long id =
              (logicalTime << (DEFAULT_NODE_ID_BITS + DEFAULT_SEQUENCE_BITS))
                  | ((long) nodes.get(node) << DEFAULT_SEQUENCE_BITS)
                  | sequences[node];
          IndexKey selected =
              switch (family) {
                case SNOWFLAKE -> key(id);
                case WORDS -> key(name);
                // MutationAttempt.updateLocationsIndex strips the scheme, then ContentIdentifier
                // splits path components and joins them with zero bytes for the actual index key.
                case LOCATION -> key(BUCKET + "\u0000" + PREFIX + "\u0000" + name + "-" + uuid);
              };
          entries.add(new Entry(ordinal, id, key(name), uuid, location, selected));
        }
      }
    }
    var sorted = entries.stream().sorted(Comparator.comparing(Entry::key)).toList();
    return new RepresentativeKeyDataset(config, entries, sorted);
  }
}

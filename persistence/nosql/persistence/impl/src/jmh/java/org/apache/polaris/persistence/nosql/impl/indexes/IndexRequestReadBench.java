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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/** Fresh-view request batches; all lookup scheduling is outside the timed operation. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class IndexRequestReadBench {
  @State(Scope.Thread)
  public static class Data {
    @Param({
      "BASELINE",
      "CHECKPOINT",
      "COMPACT",
      "COMPACT3",
      "OFFSET_PERIODIC",
      "OFFSET_BINARY",
      "ADAPTIVE_LOCKED",
      "ADAPTIVE_PUBLISHED"
    })
    public String variant;

    @Param({"5000"})
    public int entries;

    @Param({"HIERARCHICAL"})
    public String shape;

    @Param({"64"})
    public int checkpointInterval;

    PrototypeIndexData data;
    IndexKey[] uniformQueries;
    IndexKey[][] randomRequests;
    int nextPoint;
    int nextRequest;

    @Setup
    public void setupData() {
      data = newData();
      var liveKeys = new ArrayList<IndexKey>();
      for (int i = 0; i < entries; i++) {
        if (i % 17 != 0) liveKeys.add(data.keys().get(i));
      }
      if (liveKeys.size() < 17) throw new IllegalArgumentException("Need at least 17 live keys");
      // Every live ordinal appears exactly once per cycle, shuffled deterministically.
      Collections.shuffle(liveKeys, new Random(84));
      uniformQueries = liveKeys.toArray(IndexKey[]::new);
      randomRequests = new IndexKey[256][17];
      var random = new Random(182);
      for (var request : randomRequests) {
        var selected = new HashSet<Integer>();
        for (int i = 0; i < request.length; i++) {
          int ordinal;
          do {
            ordinal = random.nextInt(uniformQueries.length);
          } while (!selected.add(ordinal));
          request[i] = uniformQueries[ordinal];
        }
      }
    }

    PrototypeIndexData newData() {
      return PrototypeIndexData.create(entries, shape).encodedFor(variant, checkpointInterval);
    }

    IndexSpi<?> fresh() {
      return data.fresh(variant, checkpointInterval);
    }

    IndexKey point() {
      var key = uniformQueries[nextPoint++];
      if (nextPoint == uniformQueries.length) nextPoint = 0;
      return key;
    }

    int request() {
      return nextRequest++ & (randomRequests.length - 1);
    }
  }

  @State(Scope.Thread)
  public static class PercentileData extends Data {
    @Param({"1", "10", "25", "50", "75", "90", "99"})
    public int percentile;

    IndexKey target;

    @Override
    @Setup
    public void setupData() {
      super.setupData();
      int ordinal = (int) ((long) (entries - 1) * percentile / 100);
      if (ordinal % 17 == 0) ordinal++;
      target = data.keys().get(ordinal);
    }
  }

  @State(Scope.Thread)
  public static class BatchData extends Data {
    @Param({"2", "4", "8", "16"})
    public int lookups;

    @Param({"ASCENDING", "DESCENDING", "RANDOM"})
    public String order;

    IndexKey[][] requests;

    @Override
    @Setup
    public void setupData() {
      super.setupData();
      requests = new IndexKey[randomRequests.length][];
      for (int r = 0; r < requests.length; r++) {
        var keys = Arrays.copyOf(randomRequests[r], lookups);
        if (!order.equals("RANDOM")) {
          Arrays.sort(keys);
          if (order.equals("DESCENDING")) {
            Collections.reverse(Arrays.asList(keys));
          }
        }
        requests[r] = keys;
      }
    }
  }

  @State(Scope.Thread)
  public static class ParentData extends Data {
    @Param({"2", "4", "8", "16"})
    public int children;
  }

  @Benchmark
  public Object freshUniformPoint(Data state) {
    var key = state.point();
    return state.fresh().get(key);
  }

  @Benchmark
  public Object freshPercentilePoint(PercentileData state) {
    return state.fresh().get(state.target);
  }

  @Benchmark
  public void freshDistinctBatch(BatchData state, Blackhole sink) {
    var keys = state.requests[state.request()];
    var view = state.fresh();
    for (var key : keys) sink.consume(view.get(key));
  }

  /** Synthetic same-view reuse pattern, not an end-to-end parent/child metastore operation. */
  @Benchmark
  public void freshRepeatedParent(ParentData state, Blackhole sink) {
    var keys = state.randomRequests[state.request()];
    var view = state.fresh();
    for (int i = 1; i <= state.children; i++) {
      sink.consume(view.get(keys[0]));
      sink.consume(view.get(keys[i]));
    }
  }
}

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

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
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

/**
 * Matched v1 workloads; prepared cases exclude metadata construction, but cold cases include it.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class IndexReadComparisonBench {
  @State(Scope.Thread)
  public static class Data {
    @Param({
      "BASELINE",
      "CHECKPOINT",
      "COMPACT",
      "CHECKPOINT_PAGED",
      "COMPACT_PAGED",
      "CHECKPOINT_UNMEMOIZED",
      "COMPACT_UNMEMOIZED",
      "OFFSET_PERIODIC",
      "OFFSET_PERIODIC_PAGED",
      "OFFSET_PERIODIC_UNMEMOIZED",
      "OFFSET_BINARY",
      "OFFSET_BINARY_PAGED",
      "OFFSET_BINARY_UNMEMOIZED"
    })
    public String variant;

    @Param({"1000", "5000"})
    public int entries;

    @Param({"HIERARCHICAL", "UUID"})
    public String shape;

    @Param({"64"})
    public int checkpointInterval;

    PrototypeIndexData data;
    Object prepared;
    IndexSpi<ObjRef> warm;
    IndexKey early;
    IndexKey middle;
    IndexKey late;
    IndexKey missing;
    IndexKey[] queries;
    int next;

    @Setup
    public void setup() {
      data = PrototypeIndexData.create(entries, shape).encodedFor(variant, checkpointInterval);
      prepared = data.prepare(variant, checkpointInterval);
      warm = preparedView();
      queries = new IndexKey[256];
      var random = new Random(84);
      for (int i = 0; i < queries.length; i++) {
        int ordinal = random.nextInt(entries - 1);
        if (ordinal % 17 == 0) ordinal++;
        queries[i] = data.keys().get(ordinal);
      }
      early = hitAt(entries / 20);
      middle = hitAt(entries / 2);
      late = hitAt(entries * 19 / 20);
      missing = IndexKey.key("~missing-after-the-index");
      // Comparable repeated reads on one view, including the baseline's own key/value memoization.
      for (var key : queries) warm.get(key);
    }

    private IndexKey hitAt(int ordinal) {
      return data.liveKeyAt(ordinal);
    }

    IndexSpi<ObjRef> fresh() {
      return data.fresh(variant, checkpointInterval);
    }

    IndexSpi<ObjRef> preparedView() {
      return data.fromPrepared(variant, prepared, checkpointInterval);
    }

    IndexKey query() {
      return queries[next++ & (queries.length - 1)];
    }
  }

  @Benchmark
  public Object constructOnly(Data state) {
    return state.fresh();
  }

  @Benchmark
  public Object buildPreparedState(Data state) {
    return state.variant.equals("BASELINE")
        ? state.fresh()
        : state.data.prepare(state.variant, state.checkpointInterval);
  }

  @Benchmark
  public void coldFirstPage10(Data state, Blackhole sink) {
    consume(state.fresh().iterator(), 10, sink);
  }

  @Benchmark
  public void coldFirstPage250(Data state, Blackhole sink) {
    consume(state.fresh().iterator(), 250, sink);
  }

  @Benchmark
  public ObjRef coldPointEarly(Data state) {
    return state.fresh().get(state.early);
  }

  @Benchmark
  public ObjRef coldPointMiddle(Data state) {
    return state.fresh().get(state.middle);
  }

  @Benchmark
  public ObjRef coldPointLate(Data state) {
    return state.fresh().get(state.late);
  }

  @Benchmark
  public ObjRef coldMissAfter(Data state) {
    return state.fresh().get(state.missing);
  }

  @Benchmark
  public void coldRangeLate10(Data state, Blackhole sink) {
    consume(state.fresh().iterator(state.late, null, false), 10, sink);
  }

  @Benchmark
  public void coldFullScan(Data state, Blackhole sink) {
    consume(state.fresh().iterator(), Integer.MAX_VALUE, sink);
  }

  @Benchmark
  public void coldReverse10(Data state, Blackhole sink) {
    consume(state.fresh().reverseIterator(), 10, sink);
  }

  @Benchmark
  public ObjRef preparedFreshPoint(Data state) {
    return state.preparedView().get(state.query());
  }

  @Benchmark
  public ObjRef warmSameViewPoint(Data state) {
    return state.warm.get(state.query());
  }

  private static void consume(Iterator<Index.Element<ObjRef>> iterator, int limit, Blackhole sink) {
    for (int i = 0; i < limit && iterator.hasNext(); i++) {
      var entry = iterator.next();
      sink.consume(entry.key());
      sink.consume(entry.value());
    }
  }
}

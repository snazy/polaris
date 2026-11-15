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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.index.Index;
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
import tools.jackson.databind.json.JsonMapper;

/** Equal-entry or capped equal-byte-budget comparisons on fixed production codec populations. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class RepresentativeIndexReadBench {
  @State(Scope.Thread)
  public static class Common {
    @Param({"WORDS"})
    public String family;

    @Param({"1369956643"})
    public long seed;

    @Param({"4096"})
    public int entries;

    @Param({"0"})
    public long byteBudget;

    @Param({"FRESH"})
    public String history;

    @Param({"NATIVE"})
    public String partitioning;

    RepresentativeIndexWorkload<?> workload;

    Map<String, String> params(String variant) {
      var params = new LinkedHashMap<String, String>();
      params.put("family", family);
      params.put("seed", Long.toString(seed));
      params.put("variant", variant);
      params.put("entries", Integer.toString(entries));
      params.put("byteBudget", Long.toString(byteBudget));
      params.put("history", history);
      params.put("partitioning", partitioning);
      return params;
    }

    void initialize(String variant) {
      workload =
          RepresentativeIndexWorkload.create(
              RepresentativeKeyDataset.Family.valueOf(family),
              seed,
              RepresentativeIndexWorkload.Variant.valueOf(variant),
              entries,
              byteBudget,
              RepresentativeIndexWorkload.History.valueOf(history),
              RepresentativeIndexWorkload.Partitioning.valueOf(partitioning));
    }

    void printMetadata(
        Map<String, String> params, RepresentativeIndexWorkload.Outcome output, String operation) {
      System.out.println(
          "INDEX_METADATA "
              + JsonMapper.builder()
                  .build()
                  .writeValueAsString(workload.metadata(params, output, operation)));
    }
  }

  @State(Scope.Thread)
  public static class Data extends Common {
    @Param({"BASELINE", "D3", "D4", "PREFIX_ENDS", "FRONT_ENDS", "FRONT_PREFIX_ENDS"})
    public String variant;

    IndexSpi<?> warm;
    int next;

    @Setup
    public void setup() {
      initialize(variant);
      warm = workload.fresh();
      for (var request : workload.requests) for (var key : request) warm.get(key);
      printMetadata(
          params(variant),
          new RepresentativeIndexWorkload.Outcome(List.of(workload.input), 0),
          null);
    }

    int request() {
      return next++ & 255;
    }
  }

  @Benchmark
  public Object constructOnly(Data data) {
    return data.workload.fresh();
  }

  @Benchmark
  public Object freshPoint(Data data) {
    return data.workload.fresh().get(data.workload.requests[data.request()][0]);
  }

  @Benchmark
  public Object freshMiss(Data data) {
    return data.workload.fresh().get(data.workload.missing);
  }

  @Benchmark
  public void freshScattered16(Data data, Blackhole sink) {
    var view = data.workload.fresh();
    for (var key : data.workload.requests[data.request()]) sink.consume(view.get(key));
  }

  @Benchmark
  public void warmScattered16(Data data, Blackhole sink) {
    for (var key : data.workload.requests[data.request()]) sink.consume(data.warm.get(key));
  }

  @Benchmark
  public void freshPrefix10(Data data, Blackhole sink) {
    consume(
        data.workload.fresh().iterator(data.workload.prefix, data.workload.prefixEnd, false),
        10,
        sink);
  }

  @Benchmark
  public void freshReverse10(Data data, Blackhole sink) {
    consume(data.workload.fresh().reverseIterator(), 10, sink);
  }

  @Benchmark
  public void freshFullScan(Data data, Blackhole sink) {
    consume(data.workload.fresh().iterator(), Integer.MAX_VALUE, sink);
  }

  private static void consume(
      Iterator<? extends Index.Element<?>> iterator, int limit, Blackhole sink) {
    for (int i = 0; i < limit && iterator.hasNext(); i++) {
      var element = iterator.next();
      sink.consume(element.key());
      sink.consume(element.value());
    }
  }
}

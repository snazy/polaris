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

import java.util.concurrent.TimeUnit;
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
 * Greenfield formats plus their sorted-merge writer, compared with unchanged physical-index
 * controls.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class IndexV2LayoutBench {
  @State(Scope.Thread)
  public static class Data {
    @Param({"BASELINE", "V2", "V2_REUSE", "FRONT_ENDS", "FRONT_LENGTHS", "PREFIX_ENDS"})
    public String variant;

    @Param({"4096"})
    public int entries;

    @Param({"HIERARCHICAL", "WORDS"})
    public String shape;

    @Param({"SINGLE_PART", "MIXED_PARTS"})
    public String valueShape;

    @Param({"64"})
    public int blockEntries;

    @Param({"FULL"})
    public String directoryEncoding;

    @Param({"8"})
    public int keyRestart;

    SplitV2Workload workload;
    SplitV2Workload.Variant implementation;
    int next;

    void prepare(int changes, String locality, String operation) {
      implementation = SplitV2Workload.Variant.valueOf(variant);
      workload =
          new SplitV2Workload(
              entries,
              shape,
              valueShape,
              changes,
              locality,
              operation,
              blockEntries,
              V2IndexWriter.DirectoryEncoding.valueOf(directoryEncoding),
              keyRestart);
    }
  }

  @State(Scope.Thread)
  public static class ReadData extends Data {
    @Setup
    public void setup() {
      prepare(16, "SCATTERED", "UPDATE");
    }
  }

  @State(Scope.Thread)
  public static class SpillData extends Data {
    @Param({"16", "64", "128", "256"})
    public int changes;

    @Param({"CLUSTERED", "SCATTERED"})
    public String locality;

    @Param({"UPDATE", "MIXED"})
    public String operation;

    @Param({"4"})
    public int preReads;

    @Param({"65536"})
    public long splitTarget;

    @Setup
    public void setup() {
      prepare(changes, locality, operation);
    }
  }

  @Benchmark
  public Object constructOnly(ReadData state) {
    return state.workload.read(state.implementation);
  }

  @Benchmark
  public Object freshPoint(ReadData state) {
    return state
        .workload
        .read(state.implementation)
        .get(state.workload.queries[state.next++ & 1023]);
  }

  @Benchmark
  public void freshScattered16(ReadData state, Blackhole sink) {
    var index = state.workload.read(state.implementation);
    for (int i = 0; i < 16; i++)
      sink.consume(index.get(state.workload.queries[state.next++ & 1023]));
  }

  @Benchmark
  public void freshFullScan(ReadData state, Blackhole sink) {
    var iterator = state.workload.read(state.implementation).iterator(null, null, false);
    while (iterator.hasNext()) {
      var element = iterator.next();
      sink.consume(element.key());
      sink.consume(element.value());
    }
  }

  @Benchmark
  public void freshReverse10(ReadData state, Blackhole sink) {
    var iterator = state.workload.read(state.implementation).reverseIterator(null, null, false);
    for (int i = 0; i < 10 && iterator.hasNext(); i++) {
      var element = iterator.next();
      sink.consume(element.key());
      sink.consume(element.value());
    }
  }

  @Benchmark
  public Object sortedSpillSerialize(SpillData state) {
    return state.workload.spill(state.implementation, state.preReads, false, state.splitTarget);
  }

  @Benchmark
  public Object sortedSpillSplitSerialize(SpillData state) {
    return state.workload.spill(state.implementation, state.preReads, true, state.splitTarget);
  }
}

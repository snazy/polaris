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

/** Includes fresh views, reads, mutations and finalization; excludes persistence I/O. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class IndexV2WorkloadBench {
  @State(Scope.Thread)
  public static class Data {
    @Param({"BASELINE", "V2", "V2_REUSE"})
    public String variant;

    @Param({"4096"})
    public int entries;

    @Param({"HIERARCHICAL", "WORDS"})
    public String shape;

    @Param({"CLUSTERED", "SCATTERED"})
    public String locality;

    @Param({"64"})
    public int blockEntries;

    @Param({"FULL"})
    public String directoryEncoding;

    IndexMutationWorkload.Variant implementation;
    IndexMutationWorkload workload;

    void prepare(int embeddedEntries, int changes, String operation) {
      implementation = IndexMutationWorkload.Variant.valueOf(variant);
      workload =
          new IndexMutationWorkload(
              entries,
              shape,
              embeddedEntries,
              changes,
              locality,
              operation,
              new V2IndexWriter.Options(
                  blockEntries,
                  false,
                  V2IndexWriter.DirectoryEncoding.valueOf(directoryEncoding),
                  8));
    }
  }

  @State(Scope.Thread)
  public static class EmbeddedData extends Data {
    @Param({"128", "512"})
    public int embeddedEntries;

    @Param({"1", "16"})
    public int changes;

    @Param({"UPDATE", "MIXED"})
    public String operation;

    @Setup
    public void setup() {
      prepare(embeddedEntries, changes, operation);
    }
  }

  @State(Scope.Thread)
  public static class EmbeddedSerializeData extends EmbeddedData {
    @Param({"REBUILD", "IN_PLACE"})
    public String cleanup;
  }

  @State(Scope.Thread)
  public static class SpillData extends Data {
    @Param({"16", "256"})
    public int changes;

    @Param({"UPDATE", "MIXED"})
    public String operation;

    @Param({"0", "4"})
    public int preReads;

    @Param({"131072"})
    public long splitTarget;

    @Setup
    public void setup() {
      prepare(128, changes, operation);
    }
  }

  @Benchmark
  public Object embeddedUpdate(EmbeddedData state) {
    return state.workload.updateEmbedded(state.implementation);
  }

  @Benchmark
  public void embeddedUpdateSerialize(EmbeddedSerializeData state, Blackhole sink) {
    var view = state.workload.updateEmbedded(state.implementation);
    sink.consume(view.observed());
    sink.consume(view.added());
    sink.consume(
        state.workload.serializeEmbedded(
            view, state.implementation, state.cleanup.equals("REBUILD")));
  }

  @Benchmark
  public Object sortedSpill(SpillData state) {
    return state.workload.spill(state.implementation, state.preReads);
  }

  @Benchmark
  public void sortedSpillSerialize(SpillData state, Blackhole sink) {
    var view = state.workload.spill(state.implementation, state.preReads);
    sink.consume(view.observed());
    sink.consume(view.stripe().estimatedSerializedSize());
    sink.consume(view.stripe().serialize());
  }

  @Benchmark
  public void sortedSpillSplitSerialize(SpillData state, Blackhole sink) {
    var view = state.workload.spill(state.implementation, state.preReads);
    sink.consume(view.observed());
    for (var part : view.stripe().splitByTargetSize(state.splitTarget))
      sink.consume(part.serialize());
  }
}

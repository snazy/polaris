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
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DEFAULT;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import tools.jackson.databind.json.JsonMapper;

/**
 * Fresh mutable view, four reads, ordinary mutations and serialization; promotion is charged
 * separately.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class V2MutableLifecycleBench {
  private static final SplitV2Format.Options PROTOTYPE_OPTIONS =
      new SplitV2Format.Options(
          SplitV2Format.Layout.FRONT_ENDS, 64, SplitV2Format.DirectoryEncoding.FULL, 8, false);

  private interface Runner {
    RepresentativeIndexWorkload.Outcome run(boolean promote, boolean split);

    Map<String, Object> metadata(
        Map<String, String> params, boolean promote, boolean split, String method);
  }

  @State(Scope.Thread)
  public static class Data extends RepresentativeIndexReadBench.Common {
    @Param({
      "LEGACY",
      "BASELINE",
      "PROTOTYPE",
      "MUTABLE_V2",
      "POLICY_V2",
      "MAX_REMAINDER",
      "BALANCED"
    })
    public String variant;

    @Param({"16"})
    public int mutations;

    @Param({"32768"})
    public long splitTarget;

    @Param({"MIXED"})
    public String operation;

    Runner runner;

    @Setup
    public void setup() {
      initialize("BASELINE");
      runner = prepare(workload, variant, mutations, splitTarget, operation);
      var params = params(variant);
      params.put("mutations", Integer.toString(mutations));
      params.put("splitTarget", Long.toString(splitTarget));
      params.put("operation", operation);
      for (String method : List.of("mutateAndSerialize", "promoteAndMutate", "mutateAndSplit")) {
        var metadata =
            runner.metadata(
                params, method.equals("promoteAndMutate"), method.equals("mutateAndSplit"), method);
        System.out.println(
            "INDEX_METADATA " + JsonMapper.builder().build().writeValueAsString(metadata));
      }
    }
  }

  private static <V> Runner prepare(
      RepresentativeIndexWorkload<V> workload,
      String variant,
      int count,
      long target,
      String operation) {
    var changes =
        workload.changes(
            count,
            RepresentativeIndexWorkload.Locality.SCATTERED,
            RepresentativeIndexWorkload.Operation.valueOf(operation));
    var sourceV1 = workload.input;
    var sourceV2 = V2IndexWriter.write(workload.fresh(), workload.codec, DEFAULT);
    boolean legacy = variant.equals("LEGACY"),
        baseline = variant.equals("BASELINE"),
        prototype = variant.equals("PROTOTYPE"),
        policy = variant.equals("POLICY_V2");
    return new Runner() {
      @Override
      public RepresentativeIndexWorkload.Outcome run(boolean promote, boolean split) {
        var bytes = legacy || baseline || (promote && policy) ? sourceV1 : sourceV2;
        if (promote && !legacy && !baseline && !policy) {
          var read = IndexesInternal.deserializeStoreIndex(sourceV1, workload.codec, false, 1);
          bytes =
              prototype
                  ? SplitV2IndexWriter.write(read, workload.codec, PROTOTYPE_OPTIONS)
                  : V2IndexWriter.write(read, workload.codec, DEFAULT);
        }
        IndexSpi<V> view =
            legacy
                ? IndexLegacyImpl.deserializeStoreIndex(bytes.duplicate(), workload.codec)
                : (variant.equals("BALANCED") || variant.equals("MAX_REMAINDER"))
                    ? new MutableV2IndexImpl<>(
                        new ReadOnlyV2IndexImpl<>(bytes, workload.codec),
                        new V2IndexFormat.Options(
                            DEFAULT.layout(),
                            DEFAULT.blockEntries(),
                            DEFAULT.directory(),
                            DEFAULT.keyRestart(),
                            variant.equals("BALANCED")
                                ? V2IndexFormat.BlockSplit.BALANCED
                                : V2IndexFormat.BlockSplit.MAXIMUM_REMAINDER))
                    : prototype
                        ? new SplitV2IndexReader<>(bytes, workload.codec)
                        : IndexesInternal.deserializeStoreIndex(
                            bytes, workload.codec, true, policy ? 2 : 1);
        long observed = 0;
        for (int i = 0; i < 4; i++) {
          var value = view.get(changes.get(i % changes.size()).key());
          if (value != null) observed += value.hashCode();
        }
        if (prototype) {
          var read = (SplitV2IndexReader<V>) view;
          var parts =
              split
                  ? SplitV2IndexWriter.mergeSplit(read, changes, PROTOTYPE_OPTIONS, target)
                  : List.of(SplitV2IndexWriter.merge(read, changes, PROTOTYPE_OPTIONS));
          return new RepresentativeIndexWorkload.Outcome(parts, observed);
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
        return new RepresentativeIndexWorkload.Outcome(List.copyOf(parts), observed);
      }

      @Override
      public Map<String, Object> metadata(
          Map<String, String> params, boolean promote, boolean split, String method) {
        var output = run(promote, split);
        validate(workload, changes, output);
        var result = workload.metadata(params, output, method);
        var bytes = promote || legacy || baseline ? sourceV1 : sourceV2;
        result.put("inputBytes", bytes.remaining());
        result.put("inputSha256", Hashing.sha256().hashBytes(bytes.duplicate()).toString());
        result.put(
            "implementation",
            (legacy
                    ? IndexLegacyImpl.class
                    : baseline
                        ? IndexImpl.class
                        : prototype ? SplitV2IndexWriter.class : MutableV2IndexImpl.class)
                .getName());
        result.put("inputVersion", promote || legacy || baseline ? 1 : 2);
        var occupancy = new ArrayList<Integer>();
        for (var part : output.parts()) {
          if (part.get(part.position()) == V2IndexFormat.VERSION) {
            var read = new ReadOnlyV2IndexImpl<>(part, workload.codec);
            for (int block = 0; block < read.blockCount; block++)
              occupancy.add(read.blockEntries(block));
          }
        }
        result.put("outputBlockOccupancy", occupancy);
        result.put("promotionIncluded", promote && !legacy && !baseline);
        result.put(
            "promotionStrategy",
            promote && !legacy && !baseline ? policy ? "DEFERRED" : "CONVERT_THEN_MUTATE" : "NONE");
        return result;
      }
    };
  }

  private static <V> void validate(
      RepresentativeIndexWorkload<V> workload,
      List<SplitV2IndexWriter.Change<V>> changes,
      RepresentativeIndexWorkload.Outcome output) {
    var expected = new TreeMap<>(workload.model);
    RepresentativeIndexWorkload.applyModel(expected, changes);
    var actualKeys = new ArrayList<IndexKey>();
    for (var part : output.parts()) {
      var read = IndexesInternal.deserializeStoreIndex(part, workload.codec, false, 1);
      for (var it = read.elementIterator(); it.hasNext(); ) {
        var entry = it.next();
        var key = entry.key();
        checkState(expected.containsKey(key), "Unexpected output key");
        actualKeys.add(key);
        var value = entry.valueNullable();
        var wanted = expected.get(key);
        var actual =
            workload
                .codec
                .serialize(value, ByteBuffer.allocate(workload.codec.serializedSize(value)))
                .flip();
        var target =
            workload
                .codec
                .serialize(wanted, ByteBuffer.allocate(workload.codec.serializedSize(wanted)))
                .flip();
        checkState(actual.equals(target), "Output value differs from logical model");
      }
    }
    checkState(
        actualKeys.equals(List.copyOf(expected.keySet())), "Output keys differ from logical model");
  }

  @Benchmark
  public Object mutateAndSerialize(Data data) {
    return data.runner.run(false, false);
  }

  @Benchmark
  public Object promoteAndMutate(Data data) {
    return data.runner.run(true, false);
  }

  @Benchmark
  public Object mutateAndSplit(Data data) {
    return data.runner.run(false, true);
  }
}

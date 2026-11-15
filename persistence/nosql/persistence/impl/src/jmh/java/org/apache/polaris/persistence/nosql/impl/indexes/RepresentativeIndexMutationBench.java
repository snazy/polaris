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

/** Four source reads, sorted physical changes, then serialization or target-size splitting. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class RepresentativeIndexMutationBench {
  @State(Scope.Thread)
  public static class Data extends RepresentativeIndexReadBench.Common {
    @Param({"BASELINE", "PREFIX_ENDS", "FRONT_ENDS"})
    public String variant;

    @Param({"16", "256"})
    public int mutations;

    @Param({"SCATTERED"})
    public String locality;

    @Param({"MIXED"})
    public String operation;

    @Param({"8192"})
    public long splitTarget;

    RepresentativeIndexWorkload.PreparedMutation<?> prepared;

    @Setup
    public void setup() {
      initialize(variant);
      prepared =
          workload.prepareMutation(
              mutations,
              RepresentativeIndexWorkload.Locality.valueOf(locality),
              RepresentativeIndexWorkload.Operation.valueOf(operation));
      var params = params(variant);
      params.put("mutations", Integer.toString(mutations));
      params.put("locality", locality);
      params.put("operation", operation);
      params.put("splitTarget", Long.toString(splitTarget));
      printMetadata(params, prepared.run(false, splitTarget), "spill");
      printMetadata(params, prepared.run(true, splitTarget), "spillSplit");
    }
  }

  @Benchmark
  public Object spill(Data data) {
    return data.prepared.run(false, data.splitTarget);
  }

  @Benchmark
  public Object spillSplit(Data data) {
    return data.prepared.run(true, data.splitTarget);
  }
}

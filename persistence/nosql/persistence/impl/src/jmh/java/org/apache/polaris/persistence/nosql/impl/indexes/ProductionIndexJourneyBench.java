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
import tools.jackson.databind.json.JsonMapper;

/**
 * Actual factory/request views and finalization; immutable in-memory stripe lookup excludes I/O.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ProductionIndexJourneyBench {
  @State(Scope.Thread)
  public static class Data extends RepresentativeIndexReadBench.Common {
    @Param({"BASELINE", "PRODUCTION_V2", "PROMOTE_V2"})
    public String variant;

    @Param({"EMBEDDED", "STRIPED_SPILL", "DEFAULT_UPDATE", "DEFAULT_SPILL"})
    public String scenario;

    @Param({"16"})
    public int mutations;

    ProductionIndexJourney<?> journey;
    int request;

    @Setup
    public void setup() {
      initialize("BASELINE");
      journey = create(workload, variant, scenario, mutations);
      var params = params(variant);
      params.put("scenario", scenario);
      params.put("mutations", Integer.toString(mutations));
      for (var method : new String[] {"readRequest", "writeRequest"})
        System.out.println(
            "INDEX_METADATA "
                + JsonMapper.builder()
                    .build()
                    .writeValueAsString(
                        journey.metadata(params, method.equals("writeRequest"), method)));
    }
  }

  private static <V> ProductionIndexJourney<V> create(
      RepresentativeIndexWorkload<V> workload, String variant, String scenario, int mutations) {
    return new ProductionIndexJourney<>(workload, variant, scenario, mutations);
  }

  @Benchmark
  public Object readRequest(Data data) {
    return data.journey.run(false, data.request++);
  }

  @Benchmark
  public Object writeRequest(Data data) {
    return data.journey.run(true, 0);
  }
}

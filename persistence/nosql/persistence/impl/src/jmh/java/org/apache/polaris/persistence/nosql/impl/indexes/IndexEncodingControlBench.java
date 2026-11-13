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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;

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
 * Controls separate the v1 encoding policy from the reader; rewrite measures a two-pass prototype.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class IndexEncodingControlBench {
  @State(Scope.Thread)
  public static class Data extends IndexRequestReadBench.Data {
    @Param({"NONE", "PERIODIC", "BINARY"})
    public String policy;

    PrototypeIndexData original;

    @Override
    @Setup
    public void setupData() {
      super.setupData();
      original = PrototypeIndexData.create(entries, shape);
      data =
          new PrototypeIndexData(
              FullKeyCheckpoints.rewrite(
                  original.serialized(),
                  OBJ_REF_SERIALIZER,
                  FullKeyCheckpoints.Policy.valueOf(policy),
                  checkpointInterval),
              original.keys());
    }
  }

  @Benchmark
  public Object freshUniformPoint(Data state) {
    return state.fresh().get(state.point());
  }

  @Benchmark
  public void coldFullScan(Data state, Blackhole sink) {
    for (var entry : state.fresh()) {
      sink.consume(entry.key());
      sink.consume(entry.value());
    }
  }

  @Benchmark
  public Object rewrite(Data state) {
    return FullKeyCheckpoints.rewrite(
        state.original.serialized(),
        OBJ_REF_SERIALIZER,
        FullKeyCheckpoints.Policy.valueOf(state.policy),
        state.checkpointInterval);
  }
}

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

/** Same bytes/schedules compare adaptive reconstruction separately from covered-read locking. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class IndexAdaptiveReadBench {
  @State(Scope.Thread)
  public static class Data extends IndexRequestReadBench.Data {
    @Param({"NONE", "PERIODIC", "BINARY"})
    public String encoding;

    IndexSpi<?> warm;
    IndexKey late;
    int nextWarm;

    @Override
    PrototypeIndexData newData() {
      var original = PrototypeIndexData.create(entries, shape);
      return new PrototypeIndexData(
          FullKeyCheckpoints.rewrite(
              original.serialized(),
              OBJ_REF_SERIALIZER,
              FullKeyCheckpoints.Policy.valueOf(encoding),
              checkpointInterval),
          original.keys());
    }

    @Override
    @Setup
    public void setupData() {
      super.setupData();
      late = data.liveKeyAt(entries * 99 / 100);
      warm = fresh();
      for (int i = 0; i < 256; i++) warm.get(uniformQueries[i]);
    }
  }

  @Benchmark
  public Object freshUniformPoint(Data state) {
    return state.fresh().get(state.point());
  }

  @Benchmark
  public Object freshLatePoint(Data state) {
    return state.fresh().get(state.late);
  }

  @Benchmark
  public void freshBatch16(Data state, Blackhole sink) {
    var keys = state.randomRequests[state.request()];
    var view = state.fresh();
    for (int i = 0; i < 16; i++) sink.consume(view.get(keys[i]));
  }

  @Benchmark
  public Object coveredSameViewPoint(Data state) {
    return state.warm.get(state.uniformQueries[state.nextWarm++ & 255]);
  }

  @Benchmark
  public void freshFullScan(Data state, Blackhole sink) {
    for (var entry : state.fresh()) {
      sink.consume(entry.key());
      sink.consume(entry.value());
    }
  }

  @Benchmark
  public void freshReverse10(Data state, Blackhole sink) {
    var iterator = state.fresh().reverseIterator();
    for (int i = 0; i < 10 && iterator.hasNext(); i++) {
      var entry = iterator.next();
      sink.consume(entry.key());
      sink.consume(entry.value());
    }
  }
}

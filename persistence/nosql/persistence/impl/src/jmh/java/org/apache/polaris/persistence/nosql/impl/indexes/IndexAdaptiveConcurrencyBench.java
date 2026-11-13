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
import org.openjdk.jmh.infra.ThreadParams;

/** Secondary workload: rare concurrent read-only access to one already-covered view. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
public class IndexAdaptiveConcurrencyBench {
  @State(Scope.Benchmark)
  public static class Shared {
    @Param({"ADAPTIVE_LOCKED_UNMEMOIZED", "ADAPTIVE_PUBLISHED_UNMEMOIZED"})
    public String variant;

    @Param({"HIERARCHICAL", "WORDS"})
    public String shape;

    @Param({"NONE", "PERIODIC"})
    public String encoding;

    IndexSpi<?> index;
    IndexKey[] queries;

    @Setup
    public void setup() {
      var original = PrototypeIndexData.create(5000, shape);
      var data =
          new PrototypeIndexData(
              FullKeyCheckpoints.rewrite(
                  original.serialized(),
                  OBJ_REF_SERIALIZER,
                  FullKeyCheckpoints.Policy.valueOf(encoding),
                  64),
              original.keys());
      var prepared = data.prepare(variant, 64);
      index = data.fromPrepared(variant, prepared, 64);
      queries = new IndexKey[256];
      for (int i = 0; i < queries.length; i++) {
        int ordinal = (i * 19) % 5000;
        if (ordinal % 17 == 0) ordinal++;
        queries[i] = data.keys().get(ordinal);
        index.get(queries[i]);
      }
    }
  }

  @State(Scope.Thread)
  public static class Position {
    int next;

    @Setup
    public void setup(ThreadParams params) {
      next = params.getThreadIndex() * 31;
    }
  }

  @Benchmark
  public Object coveredSharedPoint(Shared state, Position position) {
    return state.index.get(state.queries[position.next++ & 255]);
  }
}

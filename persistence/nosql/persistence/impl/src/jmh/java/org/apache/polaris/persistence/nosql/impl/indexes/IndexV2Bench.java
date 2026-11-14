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
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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

/** Includes all fresh-reader setup; v2 inputs are written once outside read timings. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class IndexV2Bench {
  @State(Scope.Thread)
  public static class Data {
    @Param({"BASELINE", "COMPACT", "V2", "V2_PREFIX"})
    public String variant;

    @Param({"5000"})
    public int entries;

    @Param({"HIERARCHICAL", "WORDS"})
    public String shape;

    @Param({"64"})
    public int blockEntries;

    ByteBuffer input;
    PrototypeIndexData inputData;
    V2IndexWriter.Options options;
    IndexKey[] queries;
    IndexKey[][] requests;
    IndexKey early;
    IndexKey late;
    IndexKey absent;
    IndexSpi<ObjRef> warm;
    int next;
    int nextRequest;
    int nextWarm;

    @Setup
    public void setup() {
      var logical = PrototypeIndexData.create(entries, shape);
      options =
          new V2IndexWriter.Options(
              blockEntries,
              variant.contains("UNEVEN"),
              variant.contains("PREFIX")
                  ? V2IndexWriter.DirectoryEncoding.PREFIX
                  : V2IndexWriter.DirectoryEncoding.FULL,
              8);
      input =
          variant.startsWith("V2")
              ? logical.encodedFor(variant, blockEntries).serialized()
              : FullKeyCheckpoints.rewrite(
                  logical.serialized(),
                  OBJ_REF_SERIALIZER,
                  FullKeyCheckpoints.Policy.NONE,
                  blockEntries);
      inputData = new PrototypeIndexData(input, logical.keys());
      var live = new ArrayList<IndexKey>();
      for (int i = 0; i < entries; i++) if (i % 17 != 0) live.add(logical.keys().get(i));
      Collections.shuffle(live, new Random(84));
      queries = live.toArray(IndexKey[]::new);
      requests = new IndexKey[256][16];
      var random = new Random(182);
      for (var request : requests) {
        var used = new HashSet<Integer>();
        for (int i = 0; i < request.length; i++) {
          int position;
          do {
            position = random.nextInt(queries.length);
          } while (!used.add(position));
          request[i] = queries[position];
        }
      }
      early = logical.liveKeyAt(entries / 32);
      late = logical.liveKeyAt(entries - 2);
      byte[] raw = new byte[IndexKey.MAX_LENGTH];
      Arrays.fill(raw, (byte) -1);
      absent = IndexKey.key(ByteBuffer.wrap(raw));
      warm = fresh();
      for (int i = 0; i < 256; i++) warm.get(queries[i % queries.length]);
    }

    IndexSpi<ObjRef> fresh() {
      return inputData.fresh(variant, blockEntries);
    }

    IndexKey point() {
      var key = queries[next++];
      if (next == queries.length) next = 0;
      return key;
    }
  }

  @State(Scope.Thread)
  public static class WriteData extends Data {
    IndexImpl<ObjRef> writerSource;

    @Setup
    public void setupWriter() {
      writerSource = new IndexImpl<>(OBJ_REF_SERIALIZER);
      for (int i = 0; i < entries; i++)
        writerSource.add(
            indexElement(
                inputData.keys().get(i),
                i % 17 == 0 ? null : ObjRef.objRef("value", i + 100L, 1 + i % 3)));
    }
  }

  @Benchmark
  public Object constructOnly(Data state) {
    return state.fresh();
  }

  @Benchmark
  public Object freshUniformPoint(Data state) {
    return state.fresh().get(state.point());
  }

  @Benchmark
  public Object freshEarlyPoint(Data state) {
    return state.fresh().get(state.early);
  }

  @Benchmark
  public Object freshLatePoint(Data state) {
    return state.fresh().get(state.late);
  }

  @Benchmark
  public Object freshAfterRangeMiss(Data state) {
    return state.fresh().get(state.absent);
  }

  @Benchmark
  public void freshScattered16(Data state, Blackhole sink) {
    var view = state.fresh();
    for (var key : state.requests[state.nextRequest++ & 255]) sink.consume(view.get(key));
  }

  @Benchmark
  public Object coveredSameViewPoint(Data state) {
    return state.warm.get(state.queries[state.nextWarm++ & 255]);
  }

  @Benchmark
  public void freshSeekPage10(Data state, Blackhole sink) {
    var iterator = state.fresh().iterator(state.point(), null, false);
    for (int i = 0; i < 10 && iterator.hasNext(); i++) {
      var entry = iterator.next();
      sink.consume(entry.key());
      sink.consume(entry.value());
    }
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

  @Benchmark
  public Object write(WriteData state) {
    return state.variant.startsWith("V2")
        ? V2IndexWriter.write(state.writerSource, OBJ_REF_SERIALIZER, state.options)
        : state.writerSource.setModified().serialize();
  }

  /** Prints serialized sizes without treating them as timed benchmark measurements. */
  public static void main(String[] args) {
    System.out.println("shape,entries,variant,blockEntries,bytes,blocks");
    for (String shape : new String[] {"HIERARCHICAL", "WORDS", "UUID", "BINARY"}) {
      for (int count : new int[] {0, 1, 1000, 5000}) {
        var logical = PrototypeIndexData.create(count, shape);
        var source = logical.fresh("BASELINE", 64);
        System.out.println(
            shape + "," + count + ",V1,0," + logical.serialized().remaining() + ",0");
        for (int target : new int[] {16, 64, 128}) {
          for (boolean uneven : new boolean[] {false, true}) {
            for (var encoding : V2IndexWriter.DirectoryEncoding.values()) {
              var options = new V2IndexWriter.Options(target, uneven, encoding, 8);
              var bytes = V2IndexWriter.write(source, OBJ_REF_SERIALIZER, options);
              var reader = new V2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
              String variant =
                  "V2"
                      + (encoding == V2IndexWriter.DirectoryEncoding.PREFIX ? "_PREFIX" : "")
                      + (uneven ? "_UNEVEN" : "");
              System.out.println(
                  shape
                      + ","
                      + count
                      + ","
                      + variant
                      + ","
                      + target
                      + ","
                      + bytes.remaining()
                      + ","
                      + reader.blockCount);
            }
          }
        }
      }
    }
  }
}

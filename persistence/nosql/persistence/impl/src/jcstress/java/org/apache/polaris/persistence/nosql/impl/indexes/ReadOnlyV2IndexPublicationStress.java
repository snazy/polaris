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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

/**
 * Racing first access publishes directory keys, block headers, key/value pages and both null and
 * non-null value slots. CAS/volatile reads must publish the non-final decoded fields and return the
 * same winning value. Duplicate initial decodes are allowed, subsequent decoding is not.
 */
@JCStressTest
@Outcome(
    id = "1, 1, 1",
    expect = Expect.ACCEPTABLE,
    desc = "Fully initialized, shared memo winners.")
@Outcome(
    expect = Expect.FORBIDDEN,
    desc = "Partial initialization, wrong state or lost memoization.")
@State
public class ReadOnlyV2IndexPublicationStress {
  private static final ByteBuffer BYTES =
      ByteBuffer.wrap(
              HexFormat.of()
                  .parseHex(
                      "02020000001160030162010204000012340000000100010000000600000000000000236101"))
          .asReadOnlyBuffer();
  private final AtomicInteger decodes = new AtomicInteger();
  private final IndexSpi<Value> index = deserializeStoreIndex(BYTES, new Codec(decodes), false, 1);
  private Value first;
  private Value second;

  private static final class Value {
    int number;
    int complement;

    Value(int number) {
      this.number = number;
      this.complement = ~number;
    }
  }

  private static boolean valid(Value value) {
    return value != null && value.number == 0x1234 && value.complement == ~0x1234;
  }

  @Actor
  public void first(III_Result r) {
    boolean nil = index.get(key("a")) == null;
    first = index.get(key("ab"));
    r.r1 = nil && valid(first) ? 1 : 0;
  }

  @Actor
  public void second(III_Result r) {
    boolean nil = index.getElement(key("a")).valueNullable() == null;
    second = index.getElement(key("ab")).valueNullable();
    r.r2 = nil && valid(second) ? 1 : 0;
  }

  @Arbiter
  public void check(III_Result r) {
    int before = decodes.get();
    r.r3 =
        first == second
                && first == index.get(key("ab"))
                && valid(first)
                && index.get(key("a")) == null
                && !index.contains(key("a"))
                && before >= 2
                && before <= 4
                && decodes.get() == before
            ? 1
            : 0;
  }

  private record Codec(AtomicInteger decodes) implements IndexValueSerializer<Value> {
    @Override
    public int serializedSize(Value value) {
      return 2;
    }

    @Override
    public ByteBuffer serialize(Value value, ByteBuffer target) {
      return target.putShort((short) (value == null ? 0 : value.number));
    }

    @Override
    public Value deserialize(ByteBuffer input) {
      decodes.incrementAndGet();
      int n = Short.toUnsignedInt(input.getShort());
      return n == 0 ? null : new Value(n);
    }

    @Override
    public boolean isNullSerialized(ByteBuffer input) {
      return input.getShort(input.position()) == 0;
    }

    @Override
    public void skip(ByteBuffer input) {
      throw new AssertionError("V2 reads must not skip values");
    }
  }
}

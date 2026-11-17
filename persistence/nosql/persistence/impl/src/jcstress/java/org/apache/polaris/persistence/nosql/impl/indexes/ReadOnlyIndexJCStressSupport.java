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
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.Nullable;

final class ReadOnlyIndexJCStressSupport {
  private ReadOnlyIndexJCStressSupport() {}

  static final IndexKey[] KEYS =
      IntStream.range(0, 66)
          .mapToObj(i -> key("shared-prefix/" + String.format("%03d", i)))
          .toArray(IndexKey[]::new);
  private static final ByteBuffer BYTES = serialized();

  // Deliberately non-final fields: readers must see initialization published through the memo CAS.
  static final class Value {
    int number;
    int complement;

    Value(int number) {
      this.number = number;
      this.complement = ~number;
    }
  }

  static boolean valid(Value value, int expected) {
    return value != null && value.number == expected && value.complement == ~expected;
  }

  private static ByteBuffer serialized() {
    var writer = newStoreIndex(new Codec(new AtomicInteger()));
    for (var i = 0; i < KEYS.length; i++) {
      writer.add(indexElement(KEYS[i], i == 63 ? null : new Value(i)));
    }
    return writer.serialize().asReadOnlyBuffer();
  }

  static IndexSpi<Value> read(AtomicInteger decodes) {
    return deserializeStoreIndex(BYTES, new Codec(decodes), false);
  }

  private record Codec(AtomicInteger decodes) implements IndexValueSerializer<Value> {
    @Override
    public int serializedSize(@Nullable Value value) {
      return value == null ? 1 : 5;
    }

    @Override
    public ByteBuffer serialize(@Nullable Value value, ByteBuffer target) {
      if (value == null) {
        return target.put((byte) 0);
      }
      return target.put((byte) 1).putInt(value.number);
    }

    @Override
    public @Nullable Value deserialize(ByteBuffer buffer) {
      decodes.incrementAndGet();
      return buffer.get() == 0 ? null : new Value(buffer.getInt());
    }

    @Override
    public boolean isNullSerialized(ByteBuffer buffer) {
      return buffer.get(buffer.position()) == 0;
    }

    @Override
    public void skip(ByteBuffer buffer) {
      if (buffer.get() != 0) {
        buffer.position(buffer.position() + 4);
      }
    }
  }
}

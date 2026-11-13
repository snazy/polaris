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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;

/** Experimental v1 re-encoder; values and logical keys are preserved byte-for-byte. */
final class FullKeyCheckpoints {
  enum Policy {
    NONE,
    PERIODIC,
    BINARY
  }

  private FullKeyCheckpoints() {}

  static BitSet anchors(int count, Policy policy, int interval) {
    checkArgument(interval > 0, "Interval must be positive");
    var anchors = new BitSet();
    if (count == 0) return anchors;
    anchors.set(0);
    if (policy == Policy.PERIODIC) {
      for (int i = interval; i < count; i += interval) anchors.set(i);
    } else if (policy == Policy.BINARY) {
      mark(anchors, 0, count - 1, interval);
    }
    return anchors;
  }

  private static void mark(BitSet anchors, int low, int high, int interval) {
    if (high - low + 1 <= interval) return;
    int mid = (low + high) >>> 1;
    anchors.set(mid);
    mark(anchors, low, mid - 1, interval);
    mark(anchors, mid + 1, high, interval);
  }

  static <V> ByteBuffer rewrite(
      ByteBuffer source, IndexValueSerializer<V> codec, Policy policy, int interval) {
    var input = source.slice();
    checkArgument(input.get() == 1, "Expected v1");
    int count = readVarInt(input);
    return rewrite(source, codec, anchors(count, policy, interval));
  }

  static <V> ByteBuffer rewrite(ByteBuffer source, IndexValueSerializer<V> codec, BitSet anchors) {
    var input = source.slice();
    checkArgument(input.get() == 1, "Expected v1");
    int count = readVarInt(input);
    int body = input.position();
    int size = input.limit();
    int previousLength = 0;
    for (int i = 0; i < count; i++) {
      int strip = i == 0 ? 0 : readVarInt(input);
      int start = input.position();
      IndexKey.skip(input);
      int prefix = previousLength - strip;
      checkArgument(prefix >= 0, "Invalid prefix");
      if (i != 0 && anchors.get(i)) {
        size = Math.addExact(size, prefix + varIntLen(previousLength) - varIntLen(strip));
      }
      previousLength = prefix + input.position() - start;
      codec.skip(input);
    }
    checkArgument(!input.hasRemaining(), "Entry count mismatch");
    var target = ByteBuffer.allocate(size);
    target.put((byte) 1);
    putVarInt(target, count);
    input.position(body);
    byte[] key = new byte[0];
    previousLength = 0;
    for (int i = 0; i < count; i++) {
      int strip = i == 0 ? 0 : readVarInt(input);
      int start = input.position();
      IndexKey.skip(input);
      int value = input.position();
      int prefix = previousLength - strip;
      int length = prefix + value - start;
      if (key.length < length)
        key = Arrays.copyOf(key, Math.max(length, Math.max(32, key.length * 2)));
      input.get(start, key, prefix, value - start);
      if (i != 0) putVarInt(target, anchors.get(i) ? previousLength : strip);
      if (anchors.get(i)) target.put(key, 0, length);
      else target.put(input.duplicate().position(start).limit(value));
      codec.skip(input);
      target.put(input.duplicate().position(value).limit(input.position()));
      previousLength = length;
    }
    checkArgument(!target.hasRemaining(), "Wrong output size");
    return target.flip().asReadOnlyBuffer();
  }
}

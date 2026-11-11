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
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;

import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** Option D: eagerly scan v1 boundaries into five primitive ints per physical entry. */
final class CompactIndexImpl<V> extends IndexReadPrototype<V> {
  private static final int STRIDE = 5;
  private static final int KEY = 0;
  private static final int PREFIX = 1;
  private static final int VALUE = 2;
  private static final int END = 3;
  private static final int PREDECESSOR = 4;
  final Layout layout;
  // Per-view memoization of binary-search keys, deliberately not part of the shared layout.
  private final Map<Integer, IndexKey> searchKeys = new ConcurrentHashMap<>();

  CompactIndexImpl(ByteBuffer source, IndexValueSerializer<V> serializer) {
    super(source, serializer);
    int[] entries = new int[Math.multiplyExact(count, STRIDE)];
    var scan = this.source.duplicate().position(bodyOffset);
    int previousLength = 0;
    int predecessor = -1;
    for (int i = 0; i < count; i++) {
      int prefix = i == 0 ? 0 : previousLength - readVarInt(scan);
      checkArgument(prefix >= 0, "Invalid prefix length");
      int key = scan.position();
      IndexKey.skip(scan);
      int value = scan.position();
      serializer.skip(scan);
      if (predecessor >= 0) {
        if (entries[predecessor * STRIDE + PREFIX] < prefix) predecessor = i - 1;
        else {
          for (int p = predecessor;
              p >= 0 && entries[p * STRIDE + PREFIX] >= prefix;
              p = entries[p * STRIDE + PREDECESSOR]) predecessor = p;
        }
      }
      int offset = i * STRIDE;
      entries[offset + KEY] = key;
      entries[offset + PREFIX] = prefix;
      entries[offset + VALUE] = value;
      entries[offset + END] = scan.position();
      entries[offset + PREDECESSOR] = prefix == 0 ? -1 : predecessor;
      if (prefix == 0 || entries[predecessor * STRIDE + PREFIX] > prefix) predecessor = i;
      previousLength = prefix + value - key;
      checkArgument(previousLength <= IndexImpl.MAX_KEY_BYTES, "Key too large");
    }
    checkArgument(!scan.hasRemaining(), "Entry count does not match payload");
    layout = new Layout(this.source, entries);
  }

  CompactIndexImpl(Layout layout, IndexValueSerializer<V> serializer) {
    super(layout.source, serializer);
    this.layout = layout;
  }

  @Override
  long metadataBytes() {
    return 32L + 16 + (long) layout.entries.length * Integer.BYTES;
  }

  private IndexKey keyAt(int ordinal, boolean memoize) {
    IndexKey known = searchKeys.get(ordinal);
    if (known != null) return known;
    var decoded = requireNonNull(IndexKey.deserializeKey(serializedKeyAt(ordinal)));
    if (memoize) searchKeys.put(ordinal, decoded);
    return decoded;
  }

  private ByteBuffer serializedKeyAt(int ordinal) {
    int offset = ordinal * STRIDE;
    int key = layout.entries[offset + KEY];
    int prefix = layout.entries[offset + PREFIX];
    int value = layout.entries[offset + VALUE];
    ByteBuffer bytes;
    if (prefix == 0) bytes = source.duplicate().position(key).limit(value);
    else {
      byte[] full = new byte[prefix + value - key];
      int remaining = prefix;
      for (int p = layout.entries[offset + PREDECESSOR];
          p >= 0 && remaining > 0;
          p = layout.entries[p * STRIDE + PREDECESSOR]) {
        int pOffset = p * STRIDE;
        int pPrefix = layout.entries[pOffset + PREFIX];
        int take = remaining - pPrefix;
        if (take > 0) {
          source.get(layout.entries[pOffset + KEY], full, pPrefix, take);
          remaining -= take;
        }
      }
      checkArgument(remaining == 0, "Incomplete predecessor chain");
      source.get(key, full, prefix, value - key);
      bytes = ByteBuffer.wrap(full);
    }
    return bytes;
  }

  private int search(IndexKey key) {
    int low = 0;
    int high = count - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int compare = keyAt(mid, true).compareTo(key);
      if (compare < 0) low = mid + 1;
      else if (compare > 0) high = mid - 1;
      else return mid;
    }
    return -low - 1;
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    int found = search(key);
    return found < 0 ? null : entryAt(found);
  }

  @Override
  InternalIndexElement<V> entryAt(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < count, "Invalid ordinal");
    int offset = ordinal * STRIDE;
    return element(
        keyAt(ordinal, false), layout.entries[offset + VALUE], layout.entries[offset + END]);
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    int found = lower == null ? 0 : search(lower);
    int start = found < 0 ? -found - 1 : found;
    return new AbstractIterator<>() {
      int next = start;
      byte[] keyBytes = new byte[0];
      boolean first = true;

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (next == count) return endOfData();
        int offset = next * STRIDE;
        int value = layout.entries[offset + VALUE];
        int key = layout.entries[offset + KEY];
        int length;
        if (first) {
          first = false;
          var encoded = serializedKeyAt(next);
          length = encoded.remaining();
          keyBytes = new byte[Math.max(32, length)];
          encoded.get(keyBytes, 0, length);
        } else {
          int prefix = layout.entries[offset + PREFIX];
          length = prefix + value - key;
          if (keyBytes.length < length) {
            keyBytes = Arrays.copyOf(keyBytes, Math.max(length, keyBytes.length * 2));
          }
          source.get(key, keyBytes, prefix, value - key);
        }
        next++;
        return element(
            requireNonNull(IndexKey.deserializeKey(ByteBuffer.wrap(keyBytes, 0, length))),
            value,
            layout.entries[offset + END]);
      }
    };
  }

  @Override
  Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
    int found = higher == null ? count - 1 : search(higher);
    int start = found < 0 ? -found - 2 : found;
    return new AbstractIterator<>() {
      int next = start;

      @Override
      protected InternalIndexElement<V> computeNext() {
        return next < 0 ? endOfData() : entryAt(next--);
      }
    };
  }

  record Layout(ByteBuffer source, int[] entries) {}
}

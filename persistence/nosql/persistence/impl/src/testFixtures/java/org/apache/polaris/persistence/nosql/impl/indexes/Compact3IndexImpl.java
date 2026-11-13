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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** Option D: three primitive ints per v1 entry, with lazy paged search-key memoization. */
final class Compact3IndexImpl<V> extends IndexReadPrototype<V> {
  private static final int STRIDE = 3;
  private static final int LENGTH_BITS = 13;
  private static final int LENGTH_MASK = (1 << LENGTH_BITS) - 1;
  private static final int DELTA_BITS = 6;
  private static final int DELTA_MASK = (1 << DELTA_BITS) - 1;
  private static final int PREFIX_SHIFT = LENGTH_BITS + DELTA_BITS;
  private static final int LENGTHS = 0;
  private static final int VALUE = 1;
  private static final int PREDECESSOR = 2;
  private static final int PAGE_SHIFT = 6;
  private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
  private static final VarHandle KEY_PAGE = MethodHandles.arrayElementVarHandle(IndexKey[][].class);
  private static final VarHandle KEY_SLOT = MethodHandles.arrayElementVarHandle(IndexKey[].class);
  final Layout layout;
  private final int[] entries;
  // Per-view memoization of binary-search keys, deliberately not part of the shared layout.
  private final IndexKey[][] searchKeyPages;

  Compact3IndexImpl(ByteBuffer source, IndexValueSerializer<V> serializer) {
    super(source, serializer);
    entries = new int[Math.multiplyExact(count, STRIDE)];
    searchKeyPages = new IndexKey[(int) (((long) count + PAGE_SIZE - 1) >>> PAGE_SHIFT)][];
    var scan = this.source.duplicate().position(bodyOffset);
    int previousLength = 0;
    int predecessor = -1;
    for (int i = 0; i < count; i++) {
      int deltaStart = scan.position();
      int prefix = i == 0 ? 0 : previousLength - readVarInt(scan);
      checkArgument(prefix >= 0, "Invalid prefix length");
      int key = scan.position();
      int deltaBytes = key - deltaStart;
      IndexKey.skip(scan);
      int value = scan.position();
      serializer.skip(scan);
      if (prefix != 0 && predecessor >= 0) {
        if (prefixAt(predecessor * STRIDE) < prefix) predecessor = i - 1;
        else {
          for (int p = predecessor;
              p >= 0 && prefixAt(p * STRIDE) >= prefix;
              p = predecessorAt(p * STRIDE)) predecessor = p;
        }
      }
      previousLength = prefix + value - key;
      checkArgument(previousLength <= IndexImpl.MAX_KEY_BYTES, "Key too large");
      int offset = i * STRIDE;
      // Prefix in the high 13 bits needs only a shift on the reconstruction path.
      // Six middle bits retain the actual delta width, including noncanonical varints.
      entries[offset + LENGTHS] =
          (prefix << PREFIX_SHIFT) | (deltaBytes << LENGTH_BITS) | (value - key);
      entries[offset + VALUE] = value;
      entries[offset + PREDECESSOR] = prefix == 0 ? -1 : predecessor;
      if (prefix == 0 || prefixAt(predecessor * STRIDE) > prefix) predecessor = i;
    }
    checkArgument(!scan.hasRemaining(), "Entry count does not match payload");
    layout = new Layout(this.source, entries);
  }

  Compact3IndexImpl(Layout layout, IndexValueSerializer<V> serializer) {
    super(layout.source, serializer);
    this.layout = layout;
    entries = layout.entries;
    searchKeyPages = new IndexKey[(int) (((long) count + PAGE_SIZE - 1) >>> PAGE_SHIFT)][];
  }

  @Override
  long metadataBytes() {
    return 32L + 16 + (long) entries.length * Integer.BYTES;
  }

  private IndexKey cachedKeyAt(int ordinal) {
    var page = (IndexKey[]) KEY_PAGE.getVolatile(searchKeyPages, ordinal >>> PAGE_SHIFT);
    return page == null ? null : (IndexKey) KEY_SLOT.getVolatile(page, ordinal & (PAGE_SIZE - 1));
  }

  private IndexKey memoizeKey(int ordinal, IndexKey decoded, IndexKey[] page) {
    int pageIndex = ordinal >>> PAGE_SHIFT;
    if (page == null) {
      var created = new IndexKey[Math.min(PAGE_SIZE, count - (pageIndex << PAGE_SHIFT))];
      var existing =
          (IndexKey[]) KEY_PAGE.compareAndExchange(searchKeyPages, pageIndex, null, created);
      page = existing == null ? created : existing;
    }
    var existing =
        (IndexKey) KEY_SLOT.compareAndExchange(page, ordinal & (PAGE_SIZE - 1), null, decoded);
    return existing == null ? decoded : existing;
  }

  private IndexKey keyAt(int ordinal) {
    var known = cachedKeyAt(ordinal);
    return known != null
        ? known
        : requireNonNull(IndexKey.deserializeKey(serializedKeyAt(ordinal, null)));
  }

  int memoizedKeyCount() {
    int found = 0;
    for (int p = 0; p < searchKeyPages.length; p++) {
      var page = (IndexKey[]) KEY_PAGE.getVolatile(searchKeyPages, p);
      if (page != null)
        for (int i = 0; i < page.length; i++) if (KEY_SLOT.getVolatile(page, i) != null) found++;
    }
    return found;
  }

  private int prefixAt(int offset) {
    return entries[offset + LENGTHS] >>> PREFIX_SHIFT;
  }

  private int valueAt(int offset) {
    return entries[offset + VALUE];
  }

  private int keyOffsetAt(int offset) {
    return valueAt(offset) - (entries[offset + LENGTHS] & LENGTH_MASK);
  }

  private int endAt(int offset) {
    int next = offset + STRIDE;
    return next == entries.length
        ? source.limit()
        : keyOffsetAt(next) - ((entries[next + LENGTHS] >>> LENGTH_BITS) & DELTA_MASK);
  }

  private int predecessorAt(int offset) {
    return entries[offset + PREDECESSOR];
  }

  private int serializedKeyLength(int ordinal) {
    int lengths = entries[ordinal * STRIDE + LENGTHS];
    return (lengths >>> PREFIX_SHIFT) + (lengths & LENGTH_MASK);
  }

  private void reconstructKey(int ordinal, byte[] full) {
    int offset = ordinal * STRIDE;
    int key = keyOffsetAt(offset);
    int prefix = prefixAt(offset);
    int remaining = prefix;
    for (int p = predecessorAt(offset); p >= 0 && remaining > 0; p = predecessorAt(p * STRIDE)) {
      int pOffset = p * STRIDE;
      int pPrefix = prefixAt(pOffset);
      int take = remaining - pPrefix;
      if (take > 0) {
        source.get(keyOffsetAt(pOffset), full, pPrefix, take);
        remaining -= take;
      }
    }
    checkArgument(remaining == 0, "Incomplete predecessor chain");
    source.get(key, full, prefix, valueAt(offset) - key);
  }

  private ByteBuffer serializedKeyAt(int ordinal, ByteBuffer scratch) {
    int offset = ordinal * STRIDE;
    if (prefixAt(offset) == 0)
      return source.duplicate().position(keyOffsetAt(offset)).limit(valueAt(offset));
    int length = serializedKeyLength(ordinal);
    if (scratch == null) scratch = ByteBuffer.allocate(length);
    reconstructKey(ordinal, scratch.array());
    return scratch.clear().limit(length);
  }

  private int search(IndexKey key) {
    int low = 0;
    int high = count - 1;
    ByteBuffer scratch = null;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      var page = (IndexKey[]) KEY_PAGE.getVolatile(searchKeyPages, mid >>> PAGE_SHIFT);
      var probe =
          page == null ? null : (IndexKey) KEY_SLOT.getVolatile(page, mid & (PAGE_SIZE - 1));
      if (probe == null) {
        if (prefixAt(mid * STRIDE) != 0) {
          int length = serializedKeyLength(mid);
          if (scratch == null || scratch.capacity() < length)
            scratch =
                ByteBuffer.allocate(
                    Math.max(length, scratch == null ? 32 : scratch.capacity() * 2));
        }
        probe =
            memoizeKey(
                mid, requireNonNull(IndexKey.deserializeKey(serializedKeyAt(mid, scratch))), page);
      }
      int compare = probe.compareTo(key);
      if (compare < 0) low = mid + 1;
      else if (compare > 0) high = mid - 1;
      else return mid;
    }
    return -low - 1;
  }

  @Override
  public V get(@NonNull IndexKey key) {
    int found = search(key);
    if (found < 0) return null;
    int offset = found * STRIDE;
    return readValue(found, valueAt(offset), endAt(offset));
  }

  @Override
  public boolean contains(IndexKey key) {
    int found = search(key);
    if (found < 0) return false;
    int offset = found * STRIDE;
    return hasValue(found, valueAt(offset), endAt(offset));
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
    return element(keyAt(ordinal), ordinal, valueAt(offset), endAt(offset));
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    int found = lower == null ? 0 : search(lower);
    int start = found < 0 ? -found - 1 : found;
    return new AbstractIterator<>() {
      int next = start;
      byte[] keyBytes = new byte[0];
      ByteBuffer keyBuffer;
      boolean first = true;

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (next == count) return endOfData();
        int offset = next * STRIDE;
        int value = valueAt(offset);
        int key = keyOffsetAt(offset);
        int prefix = prefixAt(offset);
        int length = prefix + value - key;
        if (keyBytes.length < length) {
          keyBytes = Arrays.copyOf(keyBytes, Math.max(length, Math.max(32, keyBytes.length * 2)));
          keyBuffer = ByteBuffer.wrap(keyBytes);
        }
        if (first) {
          first = false;
          reconstructKey(next, keyBytes);
        } else {
          source.get(key, keyBytes, prefix, value - key);
        }
        next++;
        return element(
            requireNonNull(IndexKey.deserializeKey(keyBuffer.clear().limit(length))),
            next - 1,
            value,
            endAt(offset));
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

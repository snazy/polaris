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
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;

import com.google.common.collect.AbstractIterator;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;

/**
 * Read-only V1 index with three packed ints per entry (D3) and per-view paged key/value memos.
 * Input bytes are borrowed with independent positions; decoded keys never retain scratch storage.
 */
final class ReadOnlyIndexImpl<V> implements IndexSpi<V> {
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
  private static final VarHandle ARRAY_ELEMENT =
      MethodHandles.arrayElementVarHandle(Object[].class);
  private final ByteBuffer source;
  private final IndexValueSerializer<V> serializer;
  private final int count;
  private final int[] entries;
  // Pages and slots are installed with CAS and read with volatile access, including null sentinels.
  private final IndexKey[][] searchKeyPages;
  private final Object[][] values;
  private volatile ObjRef objRef;

  ReadOnlyIndexImpl(ByteBuffer bytes, IndexValueSerializer<V> serializer) {
    source = bytes.slice().asReadOnlyBuffer();
    this.serializer = serializer;
    var scan = source.duplicate();
    checkArgument(scan.get() == 1, "Unsupported serialized representation of KeyIndexSegment");
    count = readVarInt(scan);
    checkArgument(count >= 0 && count <= scan.remaining(), "Invalid entry count");
    var pages = (int) (((long) count + PAGE_SIZE - 1) >>> PAGE_SHIFT);
    values = new Object[pages][];
    searchKeyPages = new IndexKey[pages][];
    entries = new int[Math.multiplyExact(count, STRIDE)];
    int previousLength = 0;
    int predecessor = -1;
    for (int i = 0; i < count; i++) {
      int deltaStart = scan.position();
      var strip = i == 0 ? 0 : readVarInt(scan);
      checkArgument(strip >= 0 && strip <= previousLength, "Invalid prefix length");
      var prefix = previousLength - strip;
      int key = scan.position();
      int deltaBytes = key - deltaStart;
      checkArgument(deltaBytes <= DELTA_MASK, "Invalid key delta width");
      IndexKey.skip(scan);
      int value = scan.position();
      serializer.skip(scan);
      checkArgument(scan.position() >= value, "Value codec moved backwards");
      if (prefix != 0 && predecessor >= 0) {
        if (prefixAt(predecessor * STRIDE) < prefix) predecessor = i - 1;
        else {
          for (int p = predecessor;
              p >= 0 && prefixAt(p * STRIDE) >= prefix;
              p = predecessorAt(p * STRIDE)) predecessor = p;
        }
      }
      checkArgument(value - key <= IndexImpl.MAX_KEY_BYTES - prefix, "Key too large");
      previousLength = prefix + value - key;
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
    if (known != null) return known;
    var scratch = prefixAt(ordinal * STRIDE) == 0 ? null : IndexKeyBufferPool.acquire();
    try {
      return requireNonNull(IndexKey.deserializeKey(serializedKeyAt(ordinal, scratch)));
    } finally {
      if (scratch != null) IndexKeyBufferPool.release(scratch);
    }
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
    reconstructKey(ordinal, requireNonNull(scratch).array());
    return scratch.clear().limit(length);
  }

  private int search(IndexKey key) {
    int low = 0;
    int high = count - 1;
    ByteBuffer scratch = null;
    try {
      while (low <= high) {
        int mid = (low + high) >>> 1;
        var page = (IndexKey[]) KEY_PAGE.getVolatile(searchKeyPages, mid >>> PAGE_SHIFT);
        var probe =
            page == null ? null : (IndexKey) KEY_SLOT.getVolatile(page, mid & (PAGE_SIZE - 1));
        if (probe == null) {
          if (prefixAt(mid * STRIDE) != 0 && scratch == null)
            scratch = IndexKeyBufferPool.acquire();
          probe =
              memoizeKey(
                  mid,
                  requireNonNull(IndexKey.deserializeKey(serializedKeyAt(mid, scratch))),
                  page);
        }
        int compare = probe.compareTo(key);
        if (compare < 0) low = mid + 1;
        else if (compare > 0) high = mid - 1;
        else return mid;
      }
      return -low - 1;
    } finally {
      if (scratch != null) IndexKeyBufferPool.release(scratch);
    }
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

  private InternalIndexElement<V> entryAt(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < count, "Invalid ordinal");
    int offset = ordinal * STRIDE;
    return element(keyAt(ordinal), ordinal, valueAt(offset), endAt(offset));
  }

  private Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
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

  private Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
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

  @Override
  public boolean hasElements() {
    return count != 0;
  }

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

  @Override
  public ObjRef getObjId() {
    return objRef;
  }

  @Override
  public IndexSpi<V> setObjId(ObjRef id) {
    objRef = id;
    return this;
  }

  @Override
  public int estimatedSerializedSize() {
    return source.remaining();
  }

  @Override
  public @NonNull ByteBuffer serialize() {
    return source.duplicate();
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return List.of(this);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    throw new IllegalStateException("Index is read-only");
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long size) {
    throw new IllegalStateException("Index is read-only");
  }

  @Override
  public AddResult add(@NonNull InternalIndexElement<V> entry) {
    throw new IllegalStateException("Index is read-only");
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    throw new IllegalStateException("Index is read-only");
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    return getElement(key) != null;
  }

  @Override
  public IndexKey first() {
    return count == 0 ? null : entryAt(0).key();
  }

  @Override
  public IndexKey last() {
    return count == 0 ? null : entryAt(count - 1).key();
  }

  @Override
  public List<IndexKey> asKeyList() {
    return new AbstractList<>() {
      @Override
      public int size() {
        return count;
      }

      @Override
      public IndexKey get(int index) {
        return keyAt(checkIndex(index, count));
      }
    };
  }

  @Override
  public Iterator<InternalIndexElement<V>> elementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    return bounded(forward(lower), lower, higher, false);
  }

  @Override
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    checkArgument(lower == null || !lower.equals(higher), "Reverse prefix queries are unsupported");
    return bounded(backward(higher), lower, higher, true);
  }

  private Iterator<InternalIndexElement<V>> bounded(
      Iterator<InternalIndexElement<V>> raw, IndexKey lower, IndexKey higher, boolean reverse) {
    checkArgument(
        lower == null || higher == null || lower.compareTo(higher) <= 0, "Invalid bounds");
    boolean prefix = lower != null && lower.equals(higher);
    return new AbstractIterator<>() {
      @Override
      protected InternalIndexElement<V> computeNext() {
        while (raw.hasNext()) {
          var entry = raw.next();
          var key = entry.key();
          if (reverse) {
            if (higher != null && key.compareTo(higher) > 0) continue;
            if (lower != null && key.compareTo(lower) < 0) return endOfData();
          } else {
            if (lower != null && key.compareTo(lower) < 0) continue;
            if (prefix ? !key.startsWith(lower) : higher != null && key.compareTo(higher) > 0)
              return endOfData();
          }
          return entry;
        }
        return endOfData();
      }
    };
  }

  private Object[] valuePage(int ordinal, boolean create) {
    int pageIndex = ordinal >>> PAGE_SHIFT;
    var page = (Object[]) ARRAY_ELEMENT.getVolatile(values, pageIndex);
    if (page == null && create) {
      page = new Object[PAGE_SIZE];
      var existing = (Object[]) ARRAY_ELEMENT.compareAndExchange(values, pageIndex, null, page);
      if (existing != null) page = existing;
    }
    return page;
  }

  private Object readValueState(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, true);
    int slot = ordinal & (PAGE_SIZE - 1);
    var value = ARRAY_ELEMENT.getVolatile(page, slot);
    if (value == null) {
      V decoded = serializer.deserialize(valueBuffer(valueOffset, endOffset));
      value = decoded == null ? NULL : decoded;
      // Concurrent misses may decode twice; all readers use the published winner.
      var existing = ARRAY_ELEMENT.compareAndExchange(page, slot, null, value);
      if (existing != null) value = existing;
    }
    return value;
  }

  final V readValue(int ordinal, int valueOffset, int endOffset) {
    var value = readValueState(ordinal, valueOffset, endOffset);
    return value == NULL ? null : cast(value);
  }

  final boolean hasValue(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, false);
    var value = page == null ? null : ARRAY_ELEMENT.getVolatile(page, ordinal & (PAGE_SIZE - 1));
    return value == null
        ? !serializer.isNullSerialized(valueBuffer(valueOffset, endOffset))
        : value != NULL;
  }

  private ByteBuffer valueBuffer(int valueOffset, int endOffset) {
    return source.duplicate().position(valueOffset).limit(endOffset);
  }

  final InternalIndexElement<V> element(IndexKey key, int ordinal, int valueOffset, int endOffset) {
    return new AbstractIndexElement<>() {
      private volatile Object state = UNREAD;

      @Override
      public @NonNull IndexKey key() {
        return key;
      }

      @Override
      public boolean hasValue() {
        var value = state;
        return value == UNREAD
            ? ReadOnlyIndexImpl.this.hasValue(ordinal, valueOffset, endOffset)
            : value != NULL;
      }

      @Override
      public V valueNullable() {
        var value = state;
        if (value == UNREAD) {
          value = readValueState(ordinal, valueOffset, endOffset);
          state = value;
        }
        return value == NULL ? null : cast(value);
      }

      private ByteBuffer valueBuffer() {
        return ReadOnlyIndexImpl.this.valueBuffer(valueOffset, endOffset);
      }

      @Override
      public int contentSerializedSize(IndexValueSerializer<V> codec) {
        return endOffset - valueOffset;
      }

      @Override
      public void serializeContent(IndexValueSerializer<V> codec, ByteBuffer target) {
        target.put(valueBuffer());
      }
    };
  }

  private static final Object UNREAD = new Object();
  private static final Object NULL = new Object();

  @SuppressWarnings("unchecked")
  private V cast(Object value) {
    return (V) value;
  }
}

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
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DESCRIPTOR;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DIRECTORY_HEADER;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DIRECTORY_POINTER;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.ENDS;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.FRONT_PREFIX;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.HEADER;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.KEY_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.KEY_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.MODE_SHIFT;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.PREFIX;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.STRIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.VALUE_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.VALUE_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.copy;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.headerSize;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.offset;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.varIntAt;
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
 * Read-only V2 index with persisted routing and per-view key/value memoization. Input storage is
 * borrowed; independent buffer positions and owned decoded keys permit concurrent reads. Pages,
 * slots, directory keys and block headers publish immutable winners using CAS.
 */
final class ReadOnlyV2IndexImpl<V> implements IndexSpi<V> {
  final ByteBuffer source;
  final IndexValueSerializer<V> serializer;
  final int count;
  final int bodyOffset;
  private static final int PAGE_SHIFT = 6;
  private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
  // Decoded values belong to this view, never to its shared layout.
  private final Object[][] values;
  private volatile ObjRef objRef;

  private static final VarHandle SLOT = MethodHandles.arrayElementVarHandle(Object[].class);
  final int directoryOffset;
  final int blockCount;
  final int encoding;
  final int restart;
  private final int descriptors;
  private final Object[] headers;
  private final Object[] directoryKeys;
  private final Object[] directoryDecoded;
  private final Object[][] keys;

  record Block(
      int first,
      int count,
      int start,
      int end,
      int keyWidth,
      int valueWidth,
      int keyTable,
      int keyPayload,
      int keyBytes,
      int prefix,
      int mode,
      int valueMeta,
      int values,
      int stride,
      boolean sharedPrefix,
      boolean frontPrefix) {}

  static final class Scratch {
    ByteBuffer buffer;

    Scratch() {
      this(ByteBuffer.allocate(32));
    }

    Scratch(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    void ensure(int length) {
      checkArgument(length >= 0 && length <= IndexImpl.MAX_KEY_BYTES, "Oversized key");
      if (buffer.capacity() >= length) return;
      var next = ByteBuffer.allocate(Math.max(length, buffer.capacity() * 2));
      next.put(buffer.array(), buffer.arrayOffset(), buffer.position());
      buffer = next;
    }
  }

  ReadOnlyV2IndexImpl(ByteBuffer bytes, IndexValueSerializer<V> codec) {
    source = bytes.slice().asReadOnlyBuffer();
    this.serializer = requireNonNull(codec);
    var header = source.duplicate();
    checkArgument(
        header.hasRemaining() && header.get() == V2IndexFormat.VERSION, "Unexpected index version");
    count = readVarInt(header);
    checkArgument(count >= 0 && count <= source.limit(), "Invalid entry count");
    bodyOffset = header.position();
    values = new Object[(int) (((long) count + PAGE_SIZE - 1) >>> PAGE_SHIFT)][];
    checkArgument(source.remaining() >= bodyOffset + DIRECTORY_POINTER, "Truncated V2 header");
    directoryOffset = source.getInt(bodyOffset);
    int body = bodyOffset + DIRECTORY_POINTER;
    checkArgument(
        directoryOffset >= body && directoryOffset <= source.limit() - DIRECTORY_HEADER,
        "Invalid directory");
    blockCount = source.getInt(directoryOffset);
    encoding = Byte.toUnsignedInt(source.get(directoryOffset + 4));
    restart = Byte.toUnsignedInt(source.get(directoryOffset + 5));
    checkArgument(
        blockCount >= 0 && blockCount <= count && (blockCount == 0) == (count == 0),
        "Invalid block count");
    checkArgument(
        encoding <= 1 && restart > 0 && (encoding != 0 || restart == 1),
        "Invalid directory encoding");
    descriptors = directoryOffset + DIRECTORY_HEADER;
    long keyStart = (long) descriptors + (long) blockCount * DESCRIPTOR;
    checkArgument(keyStart <= source.limit(), "Truncated directory");
    int previousOffset = body;
    int previousOrdinal = -1;
    int previousKey = (int) keyStart - 1;
    for (int i = 0; i < blockCount; i++) {
      int at = field(i, 0), ordinal = field(i, 1), key = field(i, 2);
      checkArgument(
          (i == 0 ? at == previousOffset : at > previousOffset)
              && at < directoryOffset
              && ordinal > previousOrdinal
              && ordinal < count
              && key > previousKey
              && key >= keyStart
              && key < source.limit(),
          "Invalid descriptor");
      checkArgument(i != 0 || (ordinal == 0 && key == keyStart), "Invalid first block");
      previousOffset = at;
      previousOrdinal = ordinal;
      previousKey = key;
    }
    if (count == 0)
      checkArgument(directoryOffset == body && keyStart == source.limit(), "Invalid empty index");
    headers = new Object[blockCount];
    directoryKeys = new Object[blockCount];
    directoryDecoded = new Object[blockCount];
    keys = new Object[(int) (((long) count + 63) >>> 6)][];
  }

  private int field(int block, int field) {
    return source.getInt(descriptors + block * DESCRIPTOR + field * Integer.BYTES);
  }

  int firstOrdinal(int block) {
    return field(block, 1);
  }

  int blockEntries(int block) {
    return (block + 1 == blockCount ? count : firstOrdinal(block + 1)) - firstOrdinal(block);
  }

  int blockStart(int block) {
    return field(block, 0);
  }

  int blockEnd(int block) {
    return block + 1 == blockCount ? directoryOffset : blockStart(block + 1);
  }

  ByteBuffer directoryKey(int block) {
    var known = (ByteBuffer) SLOT.getVolatile(directoryKeys, block);
    if (known != null) return known;
    ByteBuffer result;
    if (encoding == 0) {
      int start = field(block, 2);
      int end = block + 1 == blockCount ? source.limit() : field(block + 1, 2);
      checkArgument(
          end > start && end - start <= IndexImpl.MAX_KEY_BYTES, "Invalid directory key size");
      result = source.slice(start, end - start);
    } else {
      byte[] bytes = new byte[0];
      int length = 0;
      for (int i = block - block % restart; i <= block; i++) {
        int start = field(i, 2), end = i + 1 == blockCount ? source.limit() : field(i + 1, 2);
        var record = source.slice(start, end - start);
        int prefix = readVarInt(record), suffix = readVarInt(record);
        checkArgument(
            prefix >= 0
                && prefix <= length
                && (i % restart != 0 || prefix == 0)
                && suffix > 0
                && suffix == record.remaining()
                && prefix + suffix <= IndexImpl.MAX_KEY_BYTES,
            "Invalid directory key");
        length = prefix + suffix;
        if (bytes.length < length)
          bytes = Arrays.copyOf(bytes, Math.max(length, Math.max(32, bytes.length * 2)));
        record.get(bytes, prefix, suffix);
      }
      result = ByteBuffer.wrap(bytes, 0, length).slice().asReadOnlyBuffer();
    }
    var previous = (ByteBuffer) SLOT.compareAndExchange(directoryKeys, block, null, result);
    return previous == null ? result : previous;
  }

  Block block(int index) {
    var known = (Block) SLOT.getVolatile(headers, index);
    if (known != null) return known;
    int start = blockStart(index), end = blockEnd(index), n = blockEntries(index);
    checkArgument(n > 0 && n <= 4096 && end - start >= HEADER, "Invalid block");
    int flags = Byte.toUnsignedInt(source.get(start));
    int header = headerSize(flags);
    checkArgument(end - start >= header, "Truncated block header");
    int prefix = header == HEADER ? 0 : Short.toUnsignedInt(source.getShort(start + 1));
    int mode = (flags >>> MODE_SHIFT) & 3;
    checkArgument(
        (flags & (PREFIX | FRONT_PREFIX)) != (PREFIX | FRONT_PREFIX)
            && (flags & (KEY_BYTE | KEY_WIDE)) != (KEY_BYTE | KEY_WIDE)
            && (flags & (VALUE_BYTE | VALUE_WIDE)) != (VALUE_BYTE | VALUE_WIDE)
            && (mode == ENDS || mode == STRIDE)
            && prefix <= IndexImpl.MAX_KEY_BYTES
            && ((flags & FRONT_PREFIX) == 0 || prefix > 0),
        "Invalid block flags");
    int kw = (flags & KEY_BYTE) != 0 ? 1 : (flags & KEY_WIDE) == 0 ? 2 : 4;
    int vw = (flags & VALUE_BYTE) != 0 ? 1 : (flags & VALUE_WIDE) == 0 ? 2 : 4;
    int keyTable = start + header;
    long payload = (long) keyTable + (long) (n - 1) * kw;
    checkArgument(payload <= end, "Truncated key table");
    int keyBytes = n == 1 ? 0 : offset(source, (int) payload - kw, kw);
    checkArgument(keyBytes >= 0, "Invalid key size");
    long meta = payload + keyBytes;
    checkArgument(meta <= end, "Truncated key area");
    int keyPayload = (int) payload, valueMeta = (int) meta;
    int values = mode == ENDS ? Math.addExact(valueMeta, n * vw) : valueMeta;
    checkArgument(
        values <= end && (mode != STRIDE || (end - values) % n == 0), "Invalid values area");
    int stride = mode == STRIDE ? (end - values) / n : 0;
    var built =
        new Block(
            firstOrdinal(index),
            n,
            start,
            end,
            kw,
            vw,
            keyTable,
            keyPayload,
            keyBytes,
            prefix,
            mode,
            valueMeta,
            values,
            stride,
            (flags & PREFIX) != 0,
            (flags & FRONT_PREFIX) != 0);
    checkArgument((int) valueRange(built, n - 1) == end, "Value size mismatch");
    var previous = (Block) SLOT.compareAndExchange(headers, index, null, built);
    return previous == null ? built : previous;
  }

  private int keyEnd(Block block, int slot) {
    return slot == 0
        ? 0
        : offset(source, block.keyTable + (slot - 1) * block.keyWidth, block.keyWidth);
  }

  int keyStart(Block block, int slot) {
    // Restart probes only need the start; keyRange validates records before reconstruction/copying.
    return block.keyPayload + keyEnd(block, slot - 1);
  }

  long keyRange(Block block, int slot) {
    int start = keyEnd(block, slot - 1), end = keyEnd(block, slot);
    checkArgument(start >= 0 && end >= start && end <= block.keyBytes, "Invalid key bounds");
    return ((long) (block.keyPayload + start) << 32) | (block.keyPayload + end);
  }

  long valueRange(Block block, int slot) {
    int start, end;
    if (block.mode == STRIDE) {
      start = slot * block.stride;
      end = start + block.stride;
    } else {
      start =
          slot == 0
              ? 0
              : offset(source, block.valueMeta + (slot - 1) * block.valueWidth, block.valueWidth);
      end = offset(source, block.valueMeta + slot * block.valueWidth, block.valueWidth);
    }
    checkArgument(
        start >= 0 && end >= start && end <= block.end - block.values, "Invalid value bounds");
    return ((long) (block.values + start) << 32) | (block.values + end);
  }

  int blockForOrdinal(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < count);
    int low = 0, high = blockCount - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (firstOrdinal(mid) <= ordinal) low = mid + 1;
      else high = mid - 1;
    }
    return high;
  }

  private IndexKey directoryIndexKey(int index) {
    var known = (IndexKey) SLOT.getVolatile(directoryDecoded, index);
    if (known != null) return known;
    var bytes = directoryKey(index).duplicate();
    var decoded = requireNonNull(IndexKey.deserializeKey(bytes));
    checkArgument(!bytes.hasRemaining(), "Trailing directory bytes");
    var previous = (IndexKey) SLOT.compareAndExchange(directoryDecoded, index, null, decoded);
    return previous == null ? decoded : previous;
  }

  int blockForKey(IndexKey key) {
    int low = 0, high = blockCount - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (directoryIndexKey(mid).compareTo(key) <= 0) low = mid + 1;
      else high = mid - 1;
    }
    return high;
  }

  private IndexKey cached(int ordinal) {
    var page = (Object[]) SLOT.getVolatile(keys, ordinal >>> 6);
    return page == null ? null : (IndexKey) SLOT.getVolatile(page, ordinal & 63);
  }

  private IndexKey keyAt(int ordinal, int index, Block b, Scratch scratch) {
    if (ordinal == b.first) return directoryIndexKey(index);
    var known = cached(ordinal);
    if (known != null) return known;
    if (scratch == null) {
      var buffer = IndexKeyBufferPool.acquire();
      try {
        return keyAt(ordinal, index, b, new Scratch(buffer));
      } finally {
        IndexKeyBufferPool.release(buffer);
      }
    }
    var bytes = encodedKey(ordinal, index, b, scratch);
    var decoded = requireNonNull(IndexKey.deserializeKey(bytes));
    checkArgument(!bytes.hasRemaining(), "Trailing key bytes");
    var page = (Object[]) SLOT.getVolatile(keys, ordinal >>> 6);
    if (page == null) {
      var created = new Object[Math.min(64, count - (ordinal & ~63))];
      var existing = (Object[]) SLOT.compareAndExchange(keys, ordinal >>> 6, null, created);
      page = existing == null ? created : existing;
    }
    var existing = (IndexKey) SLOT.compareAndExchange(page, ordinal & 63, null, decoded);
    return existing == null ? decoded : existing;
  }

  ByteBuffer encodedKey(int ordinal, Scratch scratch) {
    int index = blockForOrdinal(ordinal);
    return encodedKey(ordinal, index, block(index), scratch);
  }

  int keyLimit(Block block, int slot) {
    return (int) keyRange(block, slot);
  }

  byte[] copyEncodedKey(int ordinal, Scratch scratch) {
    var key = cached(ordinal);
    ByteBuffer bytes;
    if (key == null) bytes = encodedKey(ordinal, scratch);
    else {
      scratch.buffer.clear();
      scratch.ensure(key.serializedSize());
      bytes = key.serialize(scratch.buffer).flip();
    }
    var result = new byte[bytes.remaining()];
    bytes.get(result);
    return result;
  }

  private ByteBuffer encodedKey(int ordinal, int index, Block b, Scratch scratch) {
    int slot = ordinal - b.first;
    var output = scratch.buffer.clear();
    var first = directoryKey(index);
    if (slot == 0 || b.sharedPrefix) {
      int prefix = slot == 0 ? first.remaining() : b.prefix;
      checkArgument(prefix <= first.remaining(), "Invalid shared prefix");
      long range = slot == 0 ? 0 : keyRange(b, slot);
      int suffix = (int) range - (int) (range >>> 32);
      scratch.ensure(prefix + suffix);
      output = scratch.buffer;
      copy(output, first, 0, prefix);
      if (slot != 0) copy(output, source, (int) (range >>> 32), suffix);
    } else {
      int restartSlot = slot;
      while (restartSlot > 0 && (int) varIntAt(source, keyStart(b, restartSlot)) != 0)
        restartSlot--;
      int common = b.frontPrefix ? b.prefix : 0;
      checkArgument(common <= first.remaining(), "Invalid common prefix");
      int initial = restartSlot == 0 ? first.remaining() : common;
      if (initial > 0) {
        scratch.ensure(initial);
        output = scratch.buffer;
        copy(output, first, 0, initial);
      }
      for (int i = Math.max(1, restartSlot); i <= slot; i++) {
        long range = keyRange(b, i);
        int start = (int) (range >>> 32), limit = (int) range;
        long record = varIntAt(source, start);
        // Both terms are nonnegative; overflow is negative and rejected below.
        int prefix = common + (int) record;
        int suffixAt = (int) (record >>> 32), suffix = limit - suffixAt;
        checkArgument(
            prefix >= 0 && prefix <= output.position() && suffix > 0, "Invalid front-coded key");
        scratch.ensure(prefix + suffix);
        output = scratch.buffer;
        output.position(prefix);
        copy(output, source, suffixAt, suffix);
      }
    }
    return output.flip();
  }

  // A miss is -(global insertion ordinal)-1, including insertion before the first key.
  int find(IndexKey key) {
    return find(key, null);
  }

  int find(IndexKey key, Scratch scratch) {
    if (count == 0) return -1;
    int block = Math.max(0, blockForKey(key));
    var b = block(block);
    int low = b.first, high = b.first + b.count - 1;
    ByteBuffer borrowed = null;
    try {
      while (low <= high) {
        int mid = (low + high) >>> 1;
        if (mid != b.first && cached(mid) == null && scratch == null) {
          borrowed = IndexKeyBufferPool.acquire();
          scratch = new Scratch(borrowed);
        }
        int compare = keyAt(mid, block, b, scratch).compareTo(key);
        if (compare < 0) low = mid + 1;
        else if (compare > 0) high = mid - 1;
        else return mid;
      }
      return -low - 1;
    } finally {
      if (borrowed != null) IndexKeyBufferPool.release(borrowed);
    }
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    int ordinal = find(key);
    return ordinal < 0 ? null : entryAt(ordinal);
  }

  @Override
  public V get(@NonNull IndexKey key) {
    int ordinal = find(key);
    if (ordinal < 0) return null;
    var b = block(blockForOrdinal(ordinal));
    long range = valueRange(b, ordinal - b.first);
    return readValue(ordinal, (int) (range >>> 32), (int) range);
  }

  private InternalIndexElement<V> entryAt(int ordinal) {
    int index = blockForOrdinal(ordinal);
    var b = block(index);
    long range = valueRange(b, ordinal - b.first);
    return element(keyAt(ordinal, index, b, null), ordinal, (int) (range >>> 32), (int) range);
  }

  private Iterator<InternalIndexElement<V>> iterate(int start, boolean reverse) {
    return new AbstractIterator<>() {
      int ordinal = start;
      int index = start < 0 || start >= count ? -1 : blockForOrdinal(start);
      Block currentBlock;
      int previous = -2;
      final Scratch scratch = new Scratch();

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (ordinal < 0 || ordinal >= count) return endOfData();
        int current = ordinal;
        ordinal += reverse ? -1 : 1;
        boolean nextBlock =
            currentBlock == null
                || current < currentBlock.first
                || current >= currentBlock.first + currentBlock.count;
        if (nextBlock) {
          if (currentBlock != null) index += reverse ? -1 : 1;
          currentBlock = block(index);
        }
        var b = currentBlock;
        int slot = current - b.first;
        boolean sequential = !reverse && previous == current - 1 && !nextBlock;
        long range = valueRange(b, slot);
        IndexKey key = current == b.first ? directoryIndexKey(index) : cached(current);
        if (!reverse || key == null) {
          ByteBuffer encoded;
          if (sequential) {
            long keyRange = keyRange(b, slot);
            int keyStart = (int) (keyRange >>> 32);
            long record = b.sharedPrefix ? 0 : varIntAt(source, keyStart);
            int prefix = b.sharedPrefix ? b.prefix : (b.frontPrefix ? b.prefix : 0) + (int) record;
            var output = scratch.buffer;
            int previousLength = output.limit();
            checkArgument(prefix >= 0 && prefix <= previousLength, "Invalid key cursor");
            output.limit(output.capacity()).position(previousLength);
            int suffixAt = b.sharedPrefix ? keyStart : (int) (record >>> 32);
            int suffix = (int) keyRange - suffixAt;
            checkArgument(suffix >= 0 && (b.sharedPrefix || suffix > 0), "Invalid key suffix");
            scratch.ensure(prefix + suffix);
            output = scratch.buffer;
            output.position(prefix);
            copy(output, source, suffixAt, suffix);
            encoded = output.flip();
          } else encoded = encodedKey(current, index, b, scratch);
          if (key == null) {
            key = requireNonNull(IndexKey.deserializeKey(encoded));
            checkArgument(!encoded.hasRemaining(), "Trailing key bytes");
          }
          encoded.position(encoded.limit());
        }
        previous = current;
        return element(key, current, (int) (range >>> 32), (int) range);
      }
    };
  }

  private Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    int found = lower == null ? 0 : find(lower);
    return iterate(found < 0 ? -found - 1 : found, false);
  }

  private Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
    int found = higher == null ? count - 1 : find(higher);
    return iterate(found < 0 ? -found - 2 : found, true);
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
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AddResult add(@NonNull InternalIndexElement<V> entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    return getElement(key) != null;
  }

  @Override
  public boolean contains(IndexKey key) {
    var entry = getElement(key);
    return entry != null && entry.hasValue();
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
        return entryAt(index).key();
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
    var page = (Object[]) SLOT.getVolatile(values, pageIndex);
    if (page == null && create) {
      page = new Object[PAGE_SIZE];
      var existing = (Object[]) SLOT.compareAndExchange(values, pageIndex, null, page);
      if (existing != null) page = existing;
    }
    return page;
  }

  private Object readValueState(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, true);
    int slot = ordinal & (PAGE_SIZE - 1);
    var value = SLOT.getVolatile(page, slot);
    if (value == null) {
      V decoded = serializer.deserialize(valueBuffer(valueOffset, endOffset));
      value = decoded == null ? NULL : decoded;
      // Concurrent misses may decode twice; all readers use the published winner.
      var existing = SLOT.compareAndExchange(page, slot, null, value);
      if (existing != null) value = existing;
    }
    return value;
  }

  private V readValue(int ordinal, int valueOffset, int endOffset) {
    var value = readValueState(ordinal, valueOffset, endOffset);
    return value == NULL ? null : cast(value);
  }

  private boolean hasValue(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, false);
    var value = page == null ? null : SLOT.getVolatile(page, ordinal & (PAGE_SIZE - 1));
    return value == null
        ? !serializer.isNullSerialized(valueBuffer(valueOffset, endOffset))
        : value != NULL;
  }

  private ByteBuffer valueBuffer(int valueOffset, int endOffset) {
    return source.duplicate().position(valueOffset).limit(endOffset);
  }

  private InternalIndexElement<V> element(
      IndexKey key, int ordinal, int valueOffset, int endOffset) {
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
            ? ReadOnlyV2IndexImpl.this.hasValue(ordinal, valueOffset, endOffset)
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
        return ReadOnlyV2IndexImpl.this.valueBuffer(valueOffset, endOffset);
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

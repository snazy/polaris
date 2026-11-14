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
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.ENDS;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.GROUP;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.HEADER;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.KEY_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.KEY_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.LENGTHS;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.MAGIC;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.MODE_SHIFT;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.PREFIX;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.STRIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.VALUE_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.VALUE_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.copy;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.offset;
import static org.apache.polaris.persistence.nosql.impl.indexes.SplitV2Format.varIntAt;
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

/**
 * Separated-key/value V2 reader; all point metadata is persisted, not reconstructed by skipping
 * values.
 */
final class SplitV2IndexReader<V> extends IndexReadPrototype<V> {
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
      int lengths,
      int values,
      int stride,
      boolean sharedPrefix) {}

  static final class Scratch {
    ByteBuffer buffer;

    Scratch() {
      this(ByteBuffer.allocate(32));
    }

    Scratch(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    void ensure(int length) {
      checkArgument(length <= IndexImpl.MAX_KEY_BYTES, "Oversized key");
      if (buffer.capacity() >= length) return;
      var next = ByteBuffer.allocate(Math.max(length, buffer.capacity() * 2));
      next.put(buffer.array(), buffer.arrayOffset(), buffer.position());
      buffer = next;
    }
  }

  SplitV2IndexReader(ByteBuffer bytes, IndexValueSerializer<V> codec) {
    super(bytes, codec, 2);
    checkArgument(
        source.remaining() >= bodyOffset + 8 && source.getInt(bodyOffset + 4) == MAGIC,
        "Not a separated V2 layout");
    directoryOffset = source.getInt(bodyOffset);
    int body = bodyOffset + 8;
    checkArgument(
        directoryOffset >= body && directoryOffset <= source.limit() - 6, "Invalid directory");
    blockCount = source.getInt(directoryOffset);
    encoding = Byte.toUnsignedInt(source.get(directoryOffset + 4));
    restart = Byte.toUnsignedInt(source.get(directoryOffset + 5));
    checkArgument(
        blockCount >= 0 && blockCount <= count && (blockCount == 0) == (count == 0),
        "Invalid block count");
    checkArgument(
        encoding <= 1 && restart > 0 && (encoding != 0 || restart == 1),
        "Invalid directory encoding");
    descriptors = directoryOffset + 6;
    long keyStart = (long) descriptors + (long) blockCount * 12;
    checkArgument(keyStart <= source.limit(), "Truncated directory");
    int previousOffset = body;
    int previousOrdinal = -1;
    int previousKey = (int) keyStart - 1;
    for (int i = 0; i < blockCount; i++) {
      int at = field(i, 0), ordinal = field(i, 1), key = field(i, 2);
      checkArgument(
          at >= previousOffset
              && at < directoryOffset
              && ordinal > previousOrdinal
              && ordinal < count
              && key > previousKey
              && key >= keyStart
              && key < source.limit(),
          "Invalid descriptor");
      checkArgument(
          i != 0 || (at == body && ordinal == 0 && key == keyStart), "Invalid first block");
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
    return source.getInt(descriptors + block * 12 + field * 4);
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
    int keyBytes = source.getInt(start + 1),
        prefix = Short.toUnsignedInt(source.getShort(start + 5));
    int mode = (flags >>> MODE_SHIFT) & 3;
    checkArgument(
        (flags & ~127) == 0
            && (flags & (KEY_BYTE | KEY_WIDE)) != (KEY_BYTE | KEY_WIDE)
            && (flags & (VALUE_BYTE | VALUE_WIDE)) != (VALUE_BYTE | VALUE_WIDE)
            && mode <= STRIDE
            && keyBytes >= 0
            && prefix <= IndexImpl.MAX_KEY_BYTES
            && ((flags & PREFIX) != 0 || prefix == 0),
        "Invalid block flags");
    int kw = (flags & KEY_BYTE) != 0 ? 1 : (flags & KEY_WIDE) == 0 ? 2 : 4;
    int vw = (flags & VALUE_BYTE) != 0 ? 1 : (flags & VALUE_WIDE) == 0 ? 2 : 4;
    int keyTable = start + HEADER;
    long payload = (long) keyTable + (long) (n - 1) * kw;
    long meta = payload + keyBytes;
    checkArgument(meta <= end, "Truncated key area");
    int keyPayload = (int) payload, valueMeta = (int) meta, lengths = 0, values;
    if (mode == ENDS) values = Math.addExact(valueMeta, n * vw);
    else if (mode == STRIDE) values = valueMeta;
    else {
      checkArgument(valueMeta <= end - 2, "Missing value lengths");
      int lengthBytes = Short.toUnsignedInt(source.getShort(valueMeta));
      lengths = Math.addExact(valueMeta + 2, ((n + GROUP - 1) / GROUP) * (vw + 2));
      values = Math.addExact(lengths, lengthBytes);
    }
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
            lengths,
            values,
            stride,
            (flags & PREFIX) != 0);
    checkArgument(n == 1 ? keyBytes == 0 : keyEnd(built, n - 1) == keyBytes, "Key size mismatch");
    checkArgument((int) valueRange(built, n - 1) == end, "Value size mismatch");
    var previous = (Block) SLOT.compareAndExchange(headers, index, null, built);
    return previous == null ? built : previous;
  }

  int keyEnd(Block block, int slot) {
    return slot == 0
        ? 0
        : offset(source, block.keyTable + (slot - 1) * block.keyWidth, block.keyWidth);
  }

  int keyStart(Block block, int slot) {
    return block.keyPayload + keyEnd(block, slot - 1);
  }

  int keyLimit(Block block, int slot) {
    return block.keyPayload + keyEnd(block, slot);
  }

  long valueRange(Block block, int slot) {
    int start, end;
    if (block.mode == STRIDE) {
      start = slot * block.stride;
      end = start + block.stride;
    } else if (block.mode == ENDS) {
      start =
          slot == 0
              ? 0
              : offset(source, block.valueMeta + (slot - 1) * block.valueWidth, block.valueWidth);
      end = offset(source, block.valueMeta + slot * block.valueWidth, block.valueWidth);
    } else {
      int group = slot / GROUP;
      int descriptor = block.valueMeta + 2 + group * (block.valueWidth + 2);
      start = offset(source, descriptor, block.valueWidth);
      int position =
          block.lengths + Short.toUnsignedInt(source.getShort(descriptor + block.valueWidth));
      end = start;
      for (int i = group * GROUP; i <= slot; i++) {
        long length = varIntAt(source, position);
        position = (int) (length >>> 32);
        start = end;
        end = Math.addExact(start, (int) length);
      }
      checkArgument(position <= block.values, "Invalid length metadata");
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

  private ByteBuffer encodedKey(int ordinal, int index, Block b, Scratch scratch) {
    int slot = ordinal - b.first;
    var output = scratch.buffer.clear();
    var first = directoryKey(index);
    if (slot == 0 || b.sharedPrefix) {
      int prefix = slot == 0 ? first.remaining() : b.prefix;
      checkArgument(prefix <= first.remaining(), "Invalid shared prefix");
      int suffix = slot == 0 ? 0 : keyLimit(b, slot) - keyStart(b, slot);
      scratch.ensure(prefix + suffix);
      output = scratch.buffer;
      copy(output, first, 0, prefix);
      if (slot != 0) copy(output, source, keyStart(b, slot), suffix);
    } else {
      int restartSlot = slot;
      while (restartSlot > 0 && (int) varIntAt(source, keyStart(b, restartSlot)) != 0)
        restartSlot--;
      if (restartSlot == 0) {
        scratch.ensure(first.remaining());
        output = scratch.buffer;
        copy(output, first, 0, first.remaining());
      }
      for (int i = Math.max(1, restartSlot); i <= slot; i++) {
        int start = keyStart(b, i), limit = keyLimit(b, i);
        long record = varIntAt(source, start);
        int prefix = (int) record;
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

  byte[] encodedKey(int ordinal) {
    var buffer = IndexKeyBufferPool.acquire();
    try {
      return copyEncodedKey(ordinal, new Scratch(buffer));
    } finally {
      IndexKeyBufferPool.release(buffer);
    }
  }

  byte[] copyEncodedKey(int ordinal, Scratch scratch) {
    var bytes = encodedKey(ordinal, scratch);
    var result = new byte[bytes.remaining()];
    bytes.get(result);
    return result;
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

  @Override
  InternalIndexElement<V> entryAt(int ordinal) {
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
      int lengthPosition;
      int lastValueEnd;
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
        long range;
        if (!reverse && b.mode == LENGTHS) {
          if (!sequential) {
            int group = slot / GROUP, descriptor = b.valueMeta + 2 + group * (b.valueWidth + 2);
            lengthPosition =
                b.lengths + Short.toUnsignedInt(source.getShort(descriptor + b.valueWidth));
            lastValueEnd = b.values + offset(source, descriptor, b.valueWidth);
            for (int i = group * GROUP; i < slot; i++) {
              long length = varIntAt(source, lengthPosition);
              lengthPosition = (int) (length >>> 32);
              lastValueEnd += (int) length;
            }
          }
          long length = varIntAt(source, lengthPosition);
          lengthPosition = (int) (length >>> 32);
          int valueStart = lastValueEnd;
          lastValueEnd += (int) length;
          checkArgument(
              lengthPosition <= b.values && lastValueEnd <= b.end, "Invalid value cursor");
          range = ((long) valueStart << 32) | lastValueEnd;
        } else range = valueRange(b, slot);
        IndexKey key = current == b.first ? directoryIndexKey(index) : cached(current);
        if (!reverse || key == null) {
          ByteBuffer encoded;
          if (sequential) {
            long record = b.sharedPrefix ? 0 : varIntAt(source, keyStart(b, slot));
            int prefix = b.sharedPrefix ? b.prefix : (int) record;
            var output = scratch.buffer;
            int previousLength = output.limit();
            checkArgument(prefix <= previousLength, "Invalid key cursor");
            output.limit(output.capacity()).position(previousLength);
            int suffixAt = b.sharedPrefix ? keyStart(b, slot) : (int) (record >>> 32);
            int suffix = keyLimit(b, slot) - suffixAt;
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

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    int found = lower == null ? 0 : find(lower);
    return iterate(found < 0 ? -found - 1 : found, false);
  }

  @Override
  Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
    int found = higher == null ? count - 1 : find(higher);
    return iterate(found < 0 ? -found - 2 : found, true);
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    throw new UnsupportedOperationException("Greenfield prototype uses the sorted merge writer");
  }

  @Override
  long metadataBytes() {
    return 64L + blockCount * 16L;
  }
}

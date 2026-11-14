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
import java.util.Collections;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** Fixture-only v2 reader: persisted sparse directory and lazily published decoded blocks. */
final class V2IndexReader<V> extends IndexReadPrototype<V> {
  private static final VarHandle DIRECTORY_KEY =
      MethodHandles.arrayElementVarHandle(IndexKey[].class);
  private static final VarHandle BLOCK = MethodHandles.arrayElementVarHandle(Block[].class);
  final int directoryOffset;
  final int blockCount;
  private final int descriptors;
  private final int encoding;
  private final int restart;
  private final IndexKey[] directoryKeys;
  private final Block[] blocks;

  V2IndexReader(ByteBuffer bytes, IndexValueSerializer<V> serializer) {
    super(bytes, serializer, 2);
    checkArgument(source.limit() - bodyOffset >= 4, "Missing directory pointer");
    directoryOffset = source.getInt(bodyOffset);
    int body = bodyOffset + 4;
    checkArgument(
        directoryOffset >= body && directoryOffset <= source.limit() - 6,
        "Invalid directory offset");
    blockCount = source.getInt(directoryOffset);
    encoding = Byte.toUnsignedInt(source.get(directoryOffset + 4));
    restart = Byte.toUnsignedInt(source.get(directoryOffset + 5));
    checkArgument(
        encoding <= 1 && restart > 0 && (encoding != 0 || restart == 1),
        "Invalid directory encoding");
    checkArgument(
        blockCount >= 0 && blockCount <= count && (blockCount == 0) == (count == 0),
        "Invalid block count");
    descriptors = directoryOffset + 6;
    long keyStart = (long) descriptors + (long) blockCount * 12;
    checkArgument(keyStart <= source.limit(), "Truncated descriptors");
    if (count == 0)
      checkArgument(directoryOffset == body && keyStart == source.limit(), "Invalid empty index");
    int previousOffset = body;
    int previousOrdinal = -1;
    int previousKey = (int) keyStart - 1;
    for (int i = 0; i < blockCount; i++) {
      int offset = field(i, 0);
      int ordinal = field(i, 1);
      int key = field(i, 2);
      checkArgument(offset >= previousOffset && offset <= directoryOffset, "Invalid block offset");
      checkArgument(ordinal > previousOrdinal && ordinal < count, "Invalid block ordinal");
      checkArgument(
          key > previousKey && key >= keyStart && key < source.limit(),
          "Invalid directory key offset");
      checkArgument(
          i != 0 || (offset == body && ordinal == 0 && key == keyStart), "Invalid first block");
      previousOffset = offset;
      previousOrdinal = ordinal;
      previousKey = key;
    }
    directoryKeys = new IndexKey[blockCount];
    blocks = new Block[blockCount];
  }

  static <V> IndexSpi<V> read(ByteBuffer bytes, IndexValueSerializer<V> codec) {
    return bytes.get(bytes.position()) == 1
        ? IndexImpl.deserializeStoreIndex(bytes.slice(), codec)
        : new V2IndexReader<>(bytes, codec);
  }

  private int field(int block, int field) {
    return source.getInt(descriptors + block * 12 + field * 4);
  }

  private int endOrdinal(int block) {
    return block + 1 == blockCount ? count : field(block + 1, 1);
  }

  int blockEntryCount(int block) {
    return endOrdinal(block) - field(block, 1);
  }

  ByteBuffer blockBytes(int block) {
    int start = field(block, 0);
    int end = block + 1 == blockCount ? directoryOffset : field(block + 1, 0);
    return source.slice(start, end - start);
  }

  Iterator<InternalIndexElement<V>> blockElements(int block, boolean reverse) {
    if (reverse) {
      return new AbstractIterator<>() {
        int slot = blockEntryCount(block) - 1;

        @Override
        protected InternalIndexElement<V> computeNext() {
          return slot < 0 ? endOfData() : blockElement(block, slot--);
        }
      };
    }
    return new AbstractIterator<>() {
      final Cursor cursor = new Cursor(block);

      @Override
      protected InternalIndexElement<V> computeNext() {
        return cursor.advance()
            ? element(cursor.key(), cursor.ordinal, cursor.valueOffset, cursor.endOffset)
            : endOfData();
      }
    };
  }

  private ByteBuffer directoryRecord(int block) {
    return source
        .duplicate()
        .position(field(block, 2))
        .limit(block + 1 == blockCount ? source.limit() : field(block + 1, 2));
  }

  ByteBuffer serializedDirectoryKey(int block) {
    if (encoding == 0) {
      return directoryRecord(block);
    }
    byte[] key = new byte[0];
    int length = 0;
    for (int i = block - block % restart; i <= block; i++) {
      var bytes = directoryRecord(i);
      int prefix = readVarInt(bytes);
      int suffix = readVarInt(bytes);
      checkArgument(
          prefix >= 0 && prefix <= length && (i % restart != 0 || prefix == 0),
          "Invalid directory prefix");
      checkArgument(
          suffix > 0
              && suffix == bytes.remaining()
              && (long) prefix + suffix <= IndexImpl.MAX_KEY_BYTES,
          "Invalid directory suffix");
      length = prefix + suffix;
      if (key.length < length)
        key = Arrays.copyOf(key, Math.max(length, Math.max(32, key.length * 2)));
      bytes.get(key, prefix, suffix);
    }
    return ByteBuffer.wrap(key, 0, length);
  }

  IndexKey directoryKey(int block) {
    var key = (IndexKey) DIRECTORY_KEY.getVolatile(directoryKeys, block);
    if (key != null) return key;
    var bytes = serializedDirectoryKey(block);
    key = requireNonNull(IndexKey.deserializeKey(bytes));
    checkArgument(!bytes.hasRemaining(), "Trailing directory key bytes");
    var existing = (IndexKey) DIRECTORY_KEY.compareAndExchange(directoryKeys, block, null, key);
    return existing == null ? key : existing;
  }

  int blockForKey(IndexKey key) {
    int low = 0;
    int high = blockCount - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (directoryKey(mid).compareTo(key) <= 0) low = mid + 1;
      else high = mid - 1;
    }
    return high;
  }

  private int blockForOrdinal(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < count, "Invalid ordinal");
    int low = 0;
    int high = blockCount - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (field(mid, 1) <= ordinal) low = mid + 1;
      else high = mid - 1;
    }
    return high;
  }

  private Block blockAt(int block) {
    var known = (Block) BLOCK.getVolatile(blocks, block);
    if (known != null) return known;
    int size = endOrdinal(block) - field(block, 1);
    var keys = new IndexKey[size];
    var offsets = new int[Math.multiplyExact(size, 2)];
    var cursor = new Cursor(block);
    for (int i = 0; i < size; i++) {
      checkArgument(cursor.advance(), "Missing block entry");
      keys[i] = cursor.key();
      checkArgument(i == 0 || keys[i - 1].compareTo(keys[i]) < 0, "Unsorted block");
      offsets[i * 2] = cursor.valueOffset;
      offsets[i * 2 + 1] = cursor.endOffset;
    }
    checkArgument(
        block + 1 == blockCount || keys[size - 1].compareTo(directoryKey(block + 1)) < 0,
        "Overlapping blocks");
    var built = new Block(keys, offsets);
    var existing = (Block) BLOCK.compareAndExchange(blocks, block, null, built);
    return existing == null ? built : existing;
  }

  private long find(IndexKey key) {
    int block = blockForKey(key);
    if (block < 0) return -1;
    int slot = Arrays.binarySearch(blockAt(block).keys, key);
    return slot < 0 ? -1 : ((long) block << 32) | slot;
  }

  @Override
  public V get(@NonNull IndexKey key) {
    long found = find(key);
    if (found < 0) return null;
    int block = (int) (found >>> 32);
    int slot = (int) found;
    var state = blockAt(block);
    return readValue(field(block, 1) + slot, state.offsets[slot * 2], state.offsets[slot * 2 + 1]);
  }

  @Override
  public boolean contains(IndexKey key) {
    long found = find(key);
    if (found < 0) return false;
    int block = (int) (found >>> 32);
    int slot = (int) found;
    var state = blockAt(block);
    return hasValue(field(block, 1) + slot, state.offsets[slot * 2], state.offsets[slot * 2 + 1]);
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    long found = find(key);
    return found < 0 ? null : blockElement((int) (found >>> 32), (int) found);
  }

  InternalIndexElement<V> blockElement(int block, int slot) {
    var state = blockAt(block);
    return element(
        state.keys[slot],
        field(block, 1) + slot,
        state.offsets[slot * 2],
        state.offsets[slot * 2 + 1]);
  }

  @Override
  InternalIndexElement<V> entryAt(int ordinal) {
    int block = blockForOrdinal(ordinal);
    return blockElement(block, ordinal - field(block, 1));
  }

  @Override
  public IndexKey first() {
    return count == 0 ? null : directoryKey(0);
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    return new AbstractIterator<>() {
      int block = lower == null ? 0 : Math.max(0, blockForKey(lower));
      Cursor cursor;

      @Override
      protected InternalIndexElement<V> computeNext() {
        while (block < blockCount) {
          if (cursor == null) cursor = new Cursor(block);
          if (cursor.advance()) {
            var key = cursor.key();
            if (lower == null || key.compareTo(lower) >= 0)
              return element(key, cursor.ordinal, cursor.valueOffset, cursor.endOffset);
          } else {
            block++;
            cursor = null;
          }
        }
        return endOfData();
      }
    };
  }

  @Override
  Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
    if (count == 0) return Collections.emptyIterator();
    int start = higher == null ? blockCount - 1 : blockForKey(higher);
    if (start < 0) return Collections.emptyIterator();
    return new AbstractIterator<>() {
      int block = start;
      int slot = higher == null ? blockAt(start).keys.length - 1 : lastSlot();

      private int lastSlot() {
        int found = Arrays.binarySearch(blockAt(start).keys, higher);
        return found >= 0 ? found : -found - 2;
      }

      @Override
      protected InternalIndexElement<V> computeNext() {
        while (block >= 0) {
          if (slot >= 0) return blockElement(block, slot--);
          if (--block >= 0) slot = blockAt(block).keys.length - 1;
        }
        return endOfData();
      }
    };
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return new V2IndexImpl<>(this, serializer, V2IndexWriter.Options.defaults(64));
  }

  @Override
  long metadataBytes() {
    return 32L + (long) blockCount * 12;
  }

  int materializedBlocks() {
    int count = 0;
    for (int i = 0; i < blockCount; i++) if (BLOCK.getVolatile(blocks, i) != null) count++;
    return count;
  }

  private record Block(IndexKey[] keys, int[] offsets) {}

  private final class Cursor {
    final ByteBuffer bytes;
    byte[] keyBytes;
    ByteBuffer keyBuffer;
    int keyLength;
    int ordinal;
    final int limitOrdinal;
    final IndexKey firstKey;
    int valueOffset;
    int endOffset;
    boolean first = true;
    IndexKey decoded;

    Cursor(int block) {
      bytes =
          source
              .duplicate()
              .position(field(block, 0))
              .limit(block + 1 == blockCount ? directoryOffset : field(block + 1, 0));
      var initial = serializedDirectoryKey(block);
      keyLength = initial.remaining();
      if (initial.hasArray() && initial.arrayOffset() == 0) {
        // Compressed-directory scratch is owned by this cursor; no trimming copy is needed.
        keyBytes = initial.array();
        keyBuffer = initial;
      } else {
        keyBytes = new byte[Math.max(32, keyLength)];
        initial.get(keyBytes, 0, keyLength);
        keyBuffer = ByteBuffer.wrap(keyBytes);
      }
      firstKey = (IndexKey) DIRECTORY_KEY.getVolatile(directoryKeys, block);
      ordinal = field(block, 1) - 1;
      limitOrdinal = endOrdinal(block);
    }

    boolean advance() {
      if (++ordinal >= limitOrdinal) return false;
      decoded = null;
      if (first) {
        first = false;
        decoded = firstKey;
      } else {
        int prefix = keyLength - readVarInt(bytes);
        checkArgument(prefix >= 0, "Invalid key prefix");
        int start = bytes.position();
        IndexKey.skip(bytes);
        int suffix = bytes.position() - start;
        keyLength = prefix + suffix;
        checkArgument(keyLength <= IndexImpl.MAX_KEY_BYTES, "Oversized key");
        if (keyBytes.length < keyLength) {
          keyBytes = Arrays.copyOf(keyBytes, Math.max(keyLength, keyBytes.length * 2));
          keyBuffer = ByteBuffer.wrap(keyBytes);
        }
        source.get(start, keyBytes, prefix, suffix);
      }
      valueOffset = bytes.position();
      serializer.skip(bytes);
      endOffset = bytes.position();
      checkArgument(ordinal != limitOrdinal - 1 || !bytes.hasRemaining(), "Block length mismatch");
      return true;
    }

    IndexKey key() {
      if (decoded == null) {
        keyBuffer.clear().limit(keyLength);
        decoded = requireNonNull(IndexKey.deserializeKey(keyBuffer));
        checkArgument(!keyBuffer.hasRemaining(), "Trailing key bytes");
      }
      return decoded;
    }
  }
}

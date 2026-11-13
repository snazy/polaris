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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** C with only ordinals/byte offsets for serialized full keys; accepts every valid v1 layout. */
final class OffsetCheckpointIndexImpl<V> extends IndexReadPrototype<V> {
  final Directory directory;

  OffsetCheckpointIndexImpl(
      ByteBuffer bytes, IndexValueSerializer<V> codec, boolean pagedValues, boolean memoizeValues) {
    super(bytes, codec, pagedValues, memoizeValues);
    directory = new Directory(source, bodyOffset, count == 0);
  }

  OffsetCheckpointIndexImpl(
      Directory prepared,
      IndexValueSerializer<V> codec,
      boolean pagedValues,
      boolean memoizeValues) {
    super(prepared.source, codec, pagedValues, memoizeValues);
    checkArgument(prepared.complete, "Shared directory must be complete");
    directory = prepared;
  }

  Directory prepare() {
    ensure(null, count);
    return directory;
  }

  @Override
  long metadataBytes() {
    synchronized (directory) {
      return 64L + 32L + (long) directory.ordinals.length * Integer.BYTES * 2;
    }
  }

  private IndexKey anchorKey(int anchor) {
    return requireNonNull(
        IndexKey.deserializeKey(source.duplicate().position(directory.offsets[anchor])));
  }

  // Structural progress needs lengths/offsets only: no previous-key reconstruction or copies.
  private void ensure(IndexKey key, int ordinal) {
    synchronized (directory) {
      if (directory.complete) return;
      if (directory.size > 0
          && (key != null
              ? anchorKey(directory.size - 1).compareTo(key) >= 0
              : directory.ordinals[directory.size - 1] >= ordinal)) return;
      var bytes = source.duplicate().position(directory.nextOffset);
      while (directory.nextOrdinal < count) {
        int current = directory.nextOrdinal++;
        int prefix = current == 0 ? 0 : directory.previousLength - readVarInt(bytes);
        checkArgument(prefix >= 0, "Invalid prefix");
        int offset = bytes.position();
        IndexKey.skip(bytes);
        directory.previousLength = prefix + bytes.position() - offset;
        checkArgument(directory.previousLength <= IndexImpl.MAX_KEY_BYTES, "Key too large");
        serializer.skip(bytes);
        directory.nextOffset = bytes.position();
        if (prefix == 0) directory.add(current, offset);
        if (directory.nextOrdinal == count) {
          checkArgument(!bytes.hasRemaining(), "Entry count mismatch");
          directory.complete = true;
        }
        if (prefix == 0
            && (key != null
                ? anchorKey(directory.size - 1).compareTo(key) >= 0
                : current >= ordinal)) return;
      }
    }
  }

  private void record(Cursor scan) {
    if (scan.prefixLength != 0 && scan.ordinal != count - 1) return;
    synchronized (directory) {
      if (scan.ordinal < directory.nextOrdinal) return;
      if (scan.prefixLength == 0) directory.add(scan.ordinal, scan.keyOffset);
      directory.nextOrdinal = scan.ordinal + 1;
      directory.nextOffset = scan.endOffset;
      directory.previousLength = scan.keyLength;
      if (directory.nextOrdinal == count) directory.complete = true;
    }
  }

  private int floor(IndexKey key) {
    ensure(key, 0);
    synchronized (directory) {
      int low = 0;
      int high = directory.size - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        if (anchorKey(mid).compareTo(key) <= 0) low = mid + 1;
        else high = mid - 1;
      }
      return high;
    }
  }

  private int ordinalFloor(int ordinal) {
    ensure(null, ordinal);
    synchronized (directory) {
      int found = Arrays.binarySearch(directory.ordinals, 0, directory.size, ordinal);
      return found >= 0 ? found : -found - 2;
    }
  }

  private Cursor scanAt(int anchor) {
    if (anchor < 0) return new Cursor();
    synchronized (directory) {
      int start = directory.offsets[anchor];
      var bytes = source.duplicate().position(start);
      IndexKey.skip(bytes);
      int value = bytes.position();
      byte[] key = new byte[value - start];
      source.get(start, key);
      serializer.skip(bytes);
      return new Cursor(directory.ordinals[anchor], key, value, bytes.position());
    }
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    var scan = scanAt(floor(key));
    while (scan.advance()) {
      record(scan);
      int compare = scan.key().compareTo(key);
      if (compare == 0) return scan.element();
      if (compare > 0) return null;
    }
    return null;
  }

  @Override
  InternalIndexElement<V> entryAt(int ordinal) {
    checkArgument(ordinal >= 0 && ordinal < count, "Invalid ordinal");
    var scan = scanAt(ordinalFloor(ordinal));
    while (scan.advance()) {
      record(scan);
      if (scan.ordinal == ordinal) return scan.element();
    }
    throw new IllegalStateException("Missing entry");
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    var scan = scanAt(lower == null ? -1 : floor(lower));
    return new AbstractIterator<>() {
      @Override
      protected InternalIndexElement<V> computeNext() {
        if (!scan.advance()) return endOfData();
        record(scan);
        return scan.element();
      }
    };
  }

  @Override
  Iterator<InternalIndexElement<V>> backward(IndexKey higher) {
    prepare();
    int last = count - 1;
    if (higher != null && count != 0) {
      var scan = scanAt(floor(higher));
      last = scan.ordinal;
      while (scan.advance() && scan.key().compareTo(higher) <= 0) last = scan.ordinal;
    }
    final int start = last;
    return new AbstractIterator<>() {
      int next = start;
      final ArrayDeque<InternalIndexElement<V>> block = new ArrayDeque<>();

      @Override
      protected InternalIndexElement<V> computeNext() {
        if (block.isEmpty()) {
          if (next < 0) return endOfData();
          int anchor = ordinalFloor(next);
          var scan = scanAt(anchor);
          while (scan.advance() && scan.ordinal <= next) block.addLast(scan.element());
          synchronized (directory) {
            next = directory.ordinals[anchor] - 1;
          }
        }
        return block.removeLast();
      }
    };
  }

  /** A private forward cursor; key bytes are overwritten only on its next advance. */
  private final class Cursor {
    final ByteBuffer bytes;
    byte[] keyBytes = new byte[0];
    int keyLength;
    int ordinal = -1;
    int valueOffset;
    int endOffset;
    int keyOffset;
    int prefixLength;
    private boolean anchorPending;
    private IndexKey key;

    Cursor() {
      bytes = source.duplicate().position(bodyOffset);
    }

    Cursor(int ordinal, byte[] keyBytes, int valueOffset, int endOffset) {
      bytes = source.duplicate().position(endOffset);
      this.ordinal = ordinal - 1;
      this.keyBytes = keyBytes;
      keyLength = keyBytes.length;
      this.valueOffset = valueOffset;
      this.endOffset = endOffset;
      keyOffset = valueOffset - keyBytes.length;
      anchorPending = true;
    }

    boolean advance() {
      key = null;
      if (++ordinal >= count) return false;
      if (anchorPending) {
        anchorPending = false;
        return true;
      }
      int prefix = ordinal == 0 ? 0 : keyLength - readVarInt(bytes);
      checkArgument(prefix >= 0, "Invalid prefix length");
      prefixLength = prefix;
      keyOffset = bytes.position();
      IndexKey.skip(bytes);
      valueOffset = bytes.position();
      int suffixLength = valueOffset - keyOffset;
      keyLength = prefix + suffixLength;
      checkArgument(keyLength <= IndexImpl.MAX_KEY_BYTES, "Key too large");
      if (keyBytes.length < keyLength)
        keyBytes = Arrays.copyOf(keyBytes, Math.max(keyLength, Math.max(32, keyBytes.length * 2)));
      source.get(keyOffset, keyBytes, prefix, suffixLength);
      serializer.skip(bytes);
      endOffset = bytes.position();
      checkArgument(
          ordinal != count - 1 || endOffset == source.limit(),
          "Entry count does not match payload");
      return true;
    }

    IndexKey key() {
      if (key == null)
        key = requireNonNull(IndexKey.deserializeKey(ByteBuffer.wrap(keyBytes, 0, keyLength)));
      return key;
    }

    InternalIndexElement<V> element() {
      return OffsetCheckpointIndexImpl.this.element(key(), ordinal, valueOffset, endOffset);
    }
  }

  /**
   * No encoded-key copies or decoded keys/values. Progress is guarded by this directory's monitor.
   */
  static final class Directory {
    final ByteBuffer source;
    int[] ordinals = new int[0];
    int[] offsets = new int[0];
    int size;
    int nextOrdinal;
    int nextOffset;
    int previousLength;
    boolean complete;

    Directory(ByteBuffer source, int nextOffset, boolean complete) {
      this.source = source;
      this.nextOffset = nextOffset;
      this.complete = complete;
    }

    void add(int ordinal, int offset) {
      if (size == ordinals.length) {
        int capacity = Math.max(4, size * 2);
        ordinals = Arrays.copyOf(ordinals, capacity);
        offsets = Arrays.copyOf(offsets, capacity);
      }
      ordinals[size] = ordinal;
      offsets[size++] = offset;
    }
  }
}

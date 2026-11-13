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
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.google.common.collect.AbstractIterator;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** Experimental adaptive checkpoints, with matched locked and published-prefix read paths. */
final class AdaptiveCheckpointIndexImpl<V> extends IndexReadPrototype<V> {
  private final int interval;
  private final boolean publishedReads;
  private final Builder builder;
  private final KeyMemo keyMemo;
  private final ConcurrentHashMap<Integer, KeyBlock> keyBlockMap;
  private final KeyBlock[][] keyBlocks;
  private static final int BLOCK_PAGE_SHIFT = 6;
  private static final int BLOCK_PAGE_SIZE = 1 << BLOCK_PAGE_SHIFT;
  private static final VarHandle BLOCK_PAGE =
      MethodHandles.arrayElementVarHandle(KeyBlock[][].class);
  private static final VarHandle BLOCK_SLOT = MethodHandles.arrayElementVarHandle(KeyBlock[].class);
  private volatile Snapshot published;

  enum KeyMemo {
    NONE,
    PACKED,
    DECODED,
    PACKED_PAGED,
    DECODED_PAGED;

    boolean paged() {
      return this == PACKED_PAGED || this == DECODED_PAGED;
    }

    boolean decoded() {
      return this == DECODED || this == DECODED_PAGED;
    }
  }

  AdaptiveCheckpointIndexImpl(
      ByteBuffer bytes, IndexValueSerializer<V> codec, int interval, boolean publishedReads) {
    this(bytes, codec, interval, publishedReads, KeyMemo.NONE);
  }

  AdaptiveCheckpointIndexImpl(
      ByteBuffer bytes,
      IndexValueSerializer<V> codec,
      int interval,
      boolean publishedReads,
      KeyMemo keyMemo) {
    super(bytes, codec);
    checkArgument(interval > 0, "Interval must be positive");
    this.interval = interval;
    this.publishedReads = publishedReads;
    this.keyMemo = keyMemo;
    keyBlockMap = keyMemo == KeyMemo.NONE || keyMemo.paged() ? null : new ConcurrentHashMap<>();
    keyBlocks =
        keyMemo.paged()
            ? new KeyBlock[(int) (((long) count + BLOCK_PAGE_SIZE - 1) >>> BLOCK_PAGE_SHIFT)][]
            : null;
    builder = new Builder();
    builder.nextOffset = bodyOffset;
    builder.complete = count == 0;
    published = snapshot();
  }

  AdaptiveCheckpointIndexImpl(
      Snapshot prepared, IndexValueSerializer<V> codec, boolean publishedReads, KeyMemo keyMemo) {
    super(prepared.source, codec);
    checkArgument(prepared.complete, "Shared directory must be complete");
    interval = prepared.interval;
    this.publishedReads = publishedReads;
    this.keyMemo = keyMemo;
    keyBlockMap = keyMemo == KeyMemo.NONE || keyMemo.paged() ? null : new ConcurrentHashMap<>();
    keyBlocks =
        keyMemo.paged()
            ? new KeyBlock[(int) (((long) count + BLOCK_PAGE_SIZE - 1) >>> BLOCK_PAGE_SHIFT)][]
            : null;
    builder = new Builder();
    builder.ordinals = prepared.ordinals;
    builder.references = prepared.references;
    builder.keys = prepared.keys;
    builder.size = prepared.size;
    builder.used = prepared.used;
    builder.nextOrdinal = count;
    builder.nextOffset = source.limit();
    builder.complete = true;
    published = prepared;
  }

  Snapshot prepare() {
    return ensure(null, count);
  }

  Snapshot directory() {
    return published;
  }

  int memoizedBlockCount() {
    if (keyBlocks == null) return keyBlockMap == null ? 0 : keyBlockMap.size();
    int blocks = 0;
    for (int i = 0; i < keyBlocks.length; i++) {
      var page = (KeyBlock[]) BLOCK_PAGE.getVolatile(keyBlocks, i);
      if (page != null) {
        for (int slot = 0; slot < page.length; slot++)
          if (BLOCK_SLOT.getVolatile(page, slot) != null) blocks++;
      }
    }
    return blocks;
  }

  int memoizedPageCount() {
    if (keyBlocks == null) return 0;
    int pages = 0;
    for (int i = 0; i < keyBlocks.length; i++)
      if (BLOCK_PAGE.getVolatile(keyBlocks, i) != null) pages++;
    return pages;
  }

  @Override
  long metadataBytes() {
    var state = published;
    return 96L + 3L * 16 + (long) state.ordinals.length * Integer.BYTES * 2 + state.keys.length;
  }

  private Snapshot snapshot() {
    return new Snapshot(
        source,
        interval,
        builder.ordinals,
        builder.references,
        builder.keys,
        builder.size,
        builder.used,
        builder.nextOrdinal,
        builder.complete);
  }

  private Snapshot publish() {
    return published = snapshot();
  }

  private IndexKey anchorKey(int reference, byte[] keys) {
    ByteBuffer bytes;
    if (reference >= 0) bytes = source.duplicate().position(reference);
    else {
      bytes = ByteBuffer.wrap(keys).position(~reference);
      int length = readVarInt(bytes);
      bytes.limit(bytes.position() + length);
    }
    return requireNonNull(IndexKey.deserializeKey(bytes));
  }

  private boolean covered(Snapshot state, IndexKey key, int ordinal) {
    return state.complete
        || (state.size > 0
            && (key != null
                ? anchorKey(state.references[state.size - 1], state.keys).compareTo(key) >= 0
                : state.ordinals[state.size - 1] >= ordinal));
  }

  private Snapshot ensure(IndexKey key, int ordinal) {
    var state = published;
    if (publishedReads && covered(state, key, ordinal)) return state;
    synchronized (builder) {
      return ensureLocked(key, ordinal);
    }
  }

  private Snapshot ensureLocked(IndexKey key, int ordinal) {
    if (covered(published, key, ordinal)) return published;
    var bytes = source.duplicate().position(builder.nextOffset);
    while (builder.nextOrdinal < count) {
      int ordinalNow;
      boolean anchor = false;
      if (builder.rolling != null) {
        var scan = builder.rolling;
        checkArgument(scan.advance(), "Missing entry");
        ordinalNow = scan.ordinal;
        updateProgress(scan);
        if (scan.prefixLength == 0) {
          builder.addNative(ordinalNow, scan.keyOffset);
          builder.rolling = null;
          bytes.position(builder.nextOffset);
          anchor = true;
        } else if (ordinalNow - builder.lastOrdinal() >= interval) {
          builder.addSynthetic(scan);
          anchor = true;
        }
      } else {
        ordinalNow = builder.nextOrdinal++;
        int prefix = ordinalNow == 0 ? 0 : builder.previousLength - readVarInt(bytes);
        checkArgument(prefix >= 0, "Invalid prefix");
        int keyOffset = bytes.position();
        IndexKey.skip(bytes);
        builder.previousLength = prefix + bytes.position() - keyOffset;
        checkArgument(builder.previousLength <= IndexImpl.MAX_KEY_BYTES, "Key too large");
        serializer.skip(bytes);
        builder.nextOffset = bytes.position();
        if (prefix == 0) {
          builder.addNative(ordinalNow, keyOffset);
          anchor = true;
        } else if (ordinalNow - builder.lastOrdinal() >= interval) {
          // Reconstruct once on entering a long gap, then keep the rolling bytes until a full key.
          var scan = scanAt(snapshot(), builder.size - 1);
          while (scan.ordinal < ordinalNow) checkArgument(scan.advance(), "Missing entry");
          builder.rolling = scan;
          builder.addSynthetic(scan);
          anchor = true;
        }
      }
      if (builder.nextOrdinal == count) {
        checkArgument(builder.nextOffset == source.limit(), "Entry count mismatch");
        builder.complete = true;
        builder.rolling = null;
      }
      if (anchor
          && (key != null
              ? anchorKey(builder.references[builder.size - 1], builder.keys).compareTo(key) >= 0
              : ordinalNow >= ordinal)) break;
    }
    return publish();
  }

  private void updateProgress(Cursor scan) {
    builder.nextOrdinal = scan.ordinal + 1;
    builder.nextOffset = scan.endOffset;
    builder.previousLength = scan.keyLength;
  }

  private void record(Cursor scan) {
    if (!scan.checkpoint && scan.ordinal != count - 1) return;
    if (scan.ordinal < published.nextOrdinal) return;
    synchronized (builder) {
      if (scan.ordinal < builder.nextOrdinal) return;
      if (scan.prefixLength == 0) builder.addNative(scan.ordinal, scan.keyOffset);
      else if (scan.ordinal - builder.lastOrdinal() >= interval) builder.addSynthetic(scan);
      updateProgress(scan);
      builder.rolling = null;
      builder.complete = builder.nextOrdinal == count;
      publish();
    }
  }

  private int floor(Snapshot state, IndexKey key) {
    int low = 0;
    int high = state.size - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (anchorKey(state.references[mid], state.keys).compareTo(key) <= 0) low = mid + 1;
      else high = mid - 1;
    }
    return high;
  }

  private Cursor scanAt(Snapshot state, int anchor) {
    if (anchor < 0) return new Cursor();
    int reference = state.references[anchor];
    byte[] key;
    int value;
    if (reference >= 0) {
      var bytes = source.duplicate().position(reference);
      IndexKey.skip(bytes);
      value = bytes.position();
      key = new byte[value - reference];
      source.get(reference, key);
    } else {
      var bytes = ByteBuffer.wrap(state.keys).position(~reference);
      int length = readVarInt(bytes);
      key = new byte[length];
      bytes.get(key);
      value = readVarInt(bytes);
    }
    var bytes = source.duplicate().position(value);
    serializer.skip(bytes);
    return new Cursor(state.ordinals[anchor], key, value, bytes.position());
  }

  private Cursor lookup(IndexKey key) {
    if (publishedReads) {
      var state = ensure(key, 0);
      return scanAt(state, floor(state, key));
    }
    synchronized (builder) {
      var state = ensureLocked(key, 0);
      return scanAt(state, floor(state, key));
    }
  }

  private Cursor ordinalCursor(int ordinal) {
    if (publishedReads) return ordinalCursor(ensure(null, ordinal), ordinal);
    synchronized (builder) {
      return ordinalCursor(ensureLocked(null, ordinal), ordinal);
    }
  }

  private Cursor ordinalCursor(Snapshot state, int ordinal) {
    int found = Arrays.binarySearch(state.ordinals, 0, state.size, ordinal);
    return scanAt(state, found >= 0 ? found : -found - 2);
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    if (keyMemo != KeyMemo.NONE) {
      var state = ensure(key, 0);
      int anchor = floor(state, key);
      if (anchor < 0) return null;
      var block = keyBlock(state, anchor);
      int slot = block.lowerBound(key);
      return slot < block.size() && block.key(slot).compareTo(key) == 0
          ? blockElement(block, slot)
          : null;
    }
    var scan = lookup(key);
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
    if (keyMemo != KeyMemo.NONE) {
      var state = ensure(null, ordinal);
      int found = Arrays.binarySearch(state.ordinals, 0, state.size, ordinal);
      var block = keyBlock(state, found >= 0 ? found : -found - 2);
      return blockElement(block, ordinal - block.firstOrdinal);
    }
    var scan = ordinalCursor(ordinal);
    while (scan.advance()) {
      record(scan);
      if (scan.ordinal == ordinal) return scan.element();
    }
    throw new IllegalStateException("Missing entry");
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    var scan = lower == null ? new Cursor() : lookup(lower);
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
      var scan = lookup(higher);
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
          if (keyMemo != KeyMemo.NONE) {
            var state = published;
            int found = Arrays.binarySearch(state.ordinals, 0, state.size, next);
            var keys = keyBlock(state, found >= 0 ? found : -found - 2);
            for (int i = 0; i <= next - keys.firstOrdinal; i++)
              block.addLast(blockElement(keys, i));
            next = keys.firstOrdinal - 1;
            return block.removeLast();
          }
          var scan = ordinalCursor(next);
          int anchorOrdinal = scan.ordinal + 1;
          while (scan.advance() && scan.ordinal <= next) block.addLast(scan.element());
          next = anchorOrdinal - 1;
        }
        return block.removeLast();
      }
    };
  }

  /** Only point/ordinal/reverse access populates this per-view memo; forward scans do not. */
  private KeyBlock keyBlock(Snapshot state, int anchor) {
    if (keyBlocks == null) {
      int first = state.ordinals[anchor];
      return keyBlockMap.computeIfAbsent(first, ignored -> buildKeyBlock(state, anchor));
    }
    // Directory slots are stable across snapshot growth; native anchors can occur at every entry.
    int pageIndex = anchor >>> BLOCK_PAGE_SHIFT;
    var page = (KeyBlock[]) BLOCK_PAGE.getVolatile(keyBlocks, pageIndex);
    if (page == null) {
      var created =
          new KeyBlock[Math.min(BLOCK_PAGE_SIZE, count - (pageIndex << BLOCK_PAGE_SHIFT))];
      var existing =
          (KeyBlock[]) BLOCK_PAGE.compareAndExchange(keyBlocks, pageIndex, null, created);
      page = existing == null ? created : existing;
    }
    int slot = anchor & (BLOCK_PAGE_SIZE - 1);
    var block = (KeyBlock) BLOCK_SLOT.getVolatile(page, slot);
    if (block != null) return block;
    var created = buildKeyBlock(state, anchor);
    // Duplicate construction on a concurrent miss is intentional; only one immutable block wins.
    var existing = (KeyBlock) BLOCK_SLOT.compareAndExchange(page, slot, null, created);
    return existing == null ? created : existing;
  }

  private InternalIndexElement<V> blockElement(KeyBlock block, int slot) {
    return element(
        block.key(slot),
        block.firstOrdinal + slot,
        block.offsets[slot * 3 + 1],
        block.offsets[slot * 3 + 2]);
  }

  private KeyBlock buildKeyBlock(Snapshot state, int anchor) {
    int first = state.ordinals[anchor];
    int limit = anchor + 1 < state.size ? state.ordinals[anchor + 1] : count;
    int capacity = Math.min(interval, limit - first);
    var offsets = new int[capacity * 3 + 1];
    var decoded = keyMemo.decoded() ? new IndexKey[capacity] : null;
    byte[] packed = new byte[0];
    int used = 0;
    int size = 0;
    var scan = scanAt(state, anchor);
    while (size < capacity && scan.advance()) {
      // An incomplete directory may not yet contain the next native anchor.
      if (size != 0 && scan.prefixLength == 0) break;
      offsets[size * 3] = used;
      offsets[size * 3 + 1] = scan.valueOffset;
      offsets[size * 3 + 2] = scan.endOffset;
      if (decoded != null) decoded[size] = scan.key();
      else {
        if (packed.length - used < scan.keyLength)
          packed =
              Arrays.copyOf(
                  packed, Math.max(used + scan.keyLength, Math.max(64, packed.length * 2)));
        System.arraycopy(scan.keyBytes, 0, packed, used, scan.keyLength);
        used += scan.keyLength;
      }
      size++;
    }
    offsets[size * 3] = used;
    return new KeyBlock(
        first,
        Arrays.copyOf(offsets, size * 3 + 1),
        Arrays.copyOf(packed, used),
        decoded == null ? null : Arrays.copyOf(decoded, size));
  }

  /** Fully initialized arrays become immutable before map or CAS publication. */
  private record KeyBlock(int firstOrdinal, int[] offsets, byte[] packed, IndexKey[] decoded) {
    int size() {
      return (offsets.length - 1) / 3;
    }

    IndexKey key(int slot) {
      return decoded != null
          ? decoded[slot]
          : requireNonNull(
              IndexKey.deserializeKey(
                  ByteBuffer.wrap(
                      packed, offsets[slot * 3], offsets[(slot + 1) * 3] - offsets[slot * 3])));
    }

    int lowerBound(IndexKey key) {
      int low = 0;
      int high = size();
      while (low < high) {
        int mid = (low + high) >>> 1;
        if (key(mid).compareTo(key) < 0) low = mid + 1;
        else high = mid;
      }
      return low;
    }
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
    private int nextCheckpoint;
    boolean checkpoint;

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
      checkpoint = false;
      if (++ordinal >= count) return false;
      if (anchorPending) {
        anchorPending = false;
        nextCheckpoint = ordinal + interval;
        return true;
      }
      int prefix = ordinal == 0 ? 0 : keyLength - readVarInt(bytes);
      checkArgument(prefix >= 0, "Invalid prefix length");
      prefixLength = prefix;
      checkpoint = prefix == 0 || ordinal >= nextCheckpoint;
      if (checkpoint) nextCheckpoint = ordinal + interval;
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
      return AdaptiveCheckpointIndexImpl.this.element(key(), ordinal, valueOffset, endOffset);
    }
  }

  /**
   * All slots below size/used are immutable after volatile publication; later appends are disjoint.
   */
  record Snapshot(
      ByteBuffer source,
      int interval,
      int[] ordinals,
      int[] references,
      byte[] keys,
      int size,
      int used,
      int nextOrdinal,
      boolean complete) {}

  private final class Builder {
    int[] ordinals = new int[0];
    int[] references = new int[0];
    byte[] keys = new byte[0];
    int size;
    int used;
    int nextOrdinal;
    int nextOffset;
    int previousLength;
    boolean complete;
    Cursor rolling;

    int lastOrdinal() {
      return ordinals[size - 1];
    }

    void addNative(int ordinal, int offset) {
      add(ordinal, offset);
    }

    void addSynthetic(Cursor scan) {
      int length = varIntLen(scan.keyLength) + scan.keyLength + varIntLen(scan.valueOffset);
      if (keys.length - used < length) {
        keys = Arrays.copyOf(keys, Math.max(used + length, Math.max(64, keys.length * 2)));
      }
      int start = used;
      var bytes = ByteBuffer.wrap(keys).position(start);
      putVarInt(bytes, scan.keyLength);
      bytes.put(scan.keyBytes, 0, scan.keyLength);
      putVarInt(bytes, scan.valueOffset);
      used = bytes.position();
      add(scan.ordinal, ~start);
    }

    private void add(int ordinal, int reference) {
      if (size == ordinals.length) {
        int capacity = Math.max(4, size * 2);
        ordinals = Arrays.copyOf(ordinals, capacity);
        references = Arrays.copyOf(references, capacity);
      }
      ordinals[size] = ordinal;
      references[size++] = reference;
    }
  }
}

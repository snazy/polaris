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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DESCRIPTOR;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DIRECTORY_HEADER;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.ENDS;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.FRONT_PREFIX;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.GROUP;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.KEY_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.KEY_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.Layout;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.MODE_SHIFT;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.Options;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.PREFIX;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.STRIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.VALUE_BYTE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.VALUE_WIDE;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.commonPrefix;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.copy;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.fileHeaderSize;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.headerSize;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.putOffset;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.varIntAt;
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.width;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.impl.indexes.ReadOnlyV2IndexImpl.Scratch;

/**
 * Prepared V2 block writer. Plans own reconstructed keys; untouched blocks and values stay encoded.
 */
final class V2IndexWriter {
  record Change<V>(IndexKey key, InternalIndexElement<V> element) {
    // Null element means physical removal; an element with a null value remains a physical entry.
  }

  private record Located<V>(Change<V> change, int found, int position) {}

  private record Row<V>(int original, InternalIndexElement<V> replacement) {}

  private record Piece<V>(
      int originalBlock, BlockPlan<V> plan, ByteBuffer key, int count, int size) {}

  private V2IndexWriter() {}

  static <V> ByteBuffer write(IndexSpi<V> index, IndexValueSerializer<V> codec, Options options) {
    return prepareWrite(index, codec, options).serialize();
  }

  static <V> Prepared<V> prepareWrite(
      IndexSpi<V> index, IndexValueSerializer<V> codec, Options options) {
    var buffer = IndexKeyBufferPool.acquire();
    try {
      return prepareWrite(index, codec, options, new Scratch(buffer));
    } finally {
      IndexKeyBufferPool.release(buffer);
    }
  }

  private static <V> Prepared<V> prepareWrite(
      IndexSpi<V> index, IndexValueSerializer<V> codec, Options options, Scratch scratch) {
    var pieces = new ArrayList<Piece<V>>();
    var rows = new ArrayList<Row<V>>(options.blockEntries());
    for (var iterator = index.elementIterator(); iterator.hasNext(); ) {
      var element = iterator.next();
      rows.add(new Row<>(-1, element));
      if (rows.size() == options.blockEntries()) {
        addPlan(
            pieces, new BlockPlan<>(null, -1, List.copyOf(rows), null, codec, options, scratch));
        rows.clear();
      }
    }
    if (!rows.isEmpty())
      addPlan(pieces, new BlockPlan<>(null, -1, rows, null, codec, options, scratch));
    return new Prepared<>(null, pieces, options);
  }

  static <V> List<ByteBuffer> mergeSplit(
      ReadOnlyV2IndexImpl<V> base, List<Change<V>> changes, Options options, long target) {
    var buffer = IndexKeyBufferPool.acquire();
    try {
      return mergeSplit(base, changes, options, target, new Scratch(buffer));
    } finally {
      IndexKeyBufferPool.release(buffer);
    }
  }

  private static <V> List<ByteBuffer> mergeSplit(
      ReadOnlyV2IndexImpl<V> base,
      List<Change<V>> changes,
      Options options,
      long target,
      Scratch scratch) {
    checkArgument(target > 0, "Positive split target required");
    var pieces = planMerge(base, changes, options, scratch);
    var bounded = new ArrayList<Piece<V>>();
    for (var piece : pieces) {
      if (piece.count > 1
          && fileHeaderSize(piece.count) + DIRECTORY_HEADER + pieceUpper(piece, options) > target) {
        // Rare oversized-block fallback: make independent entries, then regroup to the target.
        for (int slot = 0; slot < piece.count; slot++) {
          Row<V> row;
          if (piece.plan == null)
            row = new Row<>(base.firstOrdinal(piece.originalBlock) + slot, null);
          else row = new Row<>(piece.plan.original(slot), piece.plan.replacement(slot));
          int block =
              row.original >= 0
                  ? base.blockForOrdinal(row.original)
                  : piece.plan == null
                      ? piece.originalBlock
                      : piece.plan.old == null ? -1 : base.blockForOrdinal(piece.plan.old.first());
          var source = block < 0 ? null : base;
          addPlan(
              bounded,
              new BlockPlan<>(
                  source, block, List.of(row), null, base.serializer, options, scratch));
        }
      } else bounded.add(piece);
    }
    var result = new ArrayList<ByteBuffer>();
    var part = new ArrayList<Piece<V>>();
    int count = 0;
    long bytes = 0;
    for (var piece : bounded) {
      long added = pieceUpper(piece, options);
      if (!part.isEmpty()
          && fileHeaderSize(Math.addExact(count, piece.count)) + DIRECTORY_HEADER + bytes + added
              > target) {
        result.add(assemble(base, part, options));
        part.clear();
        count = 0;
        bytes = 0;
      }
      part.add(piece);
      count += piece.count;
      bytes += added;
    }
    if (!part.isEmpty()) result.add(assemble(base, part, options));
    return result;
  }

  private static long pieceUpper(Piece<?> piece, Options options) {
    long size = piece.size + (long) DESCRIPTOR + piece.key.remaining();
    if (options.directory() == V2IndexFormat.DirectoryEncoding.PREFIX)
      size += 2L * varIntLen(piece.key.remaining());
    return size;
  }

  private static <V> List<Piece<V>> planMerge(
      ReadOnlyV2IndexImpl<V> base, List<Change<V>> changes, Options options, Scratch scratch) {
    IndexKey previous = null;
    var pieces = new ArrayList<Piece<V>>();
    var located = new ArrayList<Located<V>>();
    int currentBlock = 0;
    for (var change : changes) {
      checkArgument(
          previous == null || previous.compareTo(change.key) < 0,
          "Changes must be sorted and unique");
      checkArgument(
          change.element == null || change.key.equals(change.element.key()),
          "Mismatched change key");
      previous = change.key;
      int block = Math.max(0, base.blockForKey(change.key));
      while (currentBlock < block) {
        addChanged(pieces, base, currentBlock, located, options, scratch);
        located.clear();
        currentBlock++;
      }
      int found = base.find(change.key, scratch);
      if (found >= 0 || change.element != null)
        located.add(new Located<>(change, found, found >= 0 ? found : -found - 1));
    }
    while (currentBlock < Math.max(1, base.blockCount)) {
      addChanged(pieces, base, currentBlock, located, options, scratch);
      located.clear();
      currentBlock++;
    }
    return pieces;
  }

  private static <V> void addPlan(List<Piece<V>> pieces, BlockPlan<V> plan) {
    if (plan.count != 0)
      pieces.add(new Piece<>(-1, plan, ByteBuffer.wrap(plan.firstKey), plan.count, plan.size));
  }

  private static <V> void addChanged(
      List<Piece<V>> pieces,
      ReadOnlyV2IndexImpl<V> base,
      int block,
      List<Located<V>> changes,
      Options options,
      Scratch scratch) {
    if (changes.isEmpty()) {
      if (base.blockCount != 0)
        pieces.add(
            new Piece<>(
                block,
                null,
                base.directoryKey(block),
                base.blockEntries(block),
                base.blockEnd(block) - base.blockStart(block)));
      return;
    }
    boolean valuesOnly = base.blockCount != 0;
    for (var change : changes) valuesOnly &= change.found >= 0 && change.change.element != null;
    if (valuesOnly) {
      var replacements =
          new ArrayList<InternalIndexElement<V>>(
              Collections.nCopies(base.blockEntries(block), null));
      for (var change : changes)
        replacements.set(change.found - base.firstOrdinal(block), change.change.element);
      addPlan(
          pieces,
          new BlockPlan<>(base, block, null, replacements, base.serializer, options, scratch));
      return;
    }
    int start = base.blockCount == 0 ? 0 : base.firstOrdinal(block);
    int end = start + (base.blockCount == 0 ? 0 : base.blockEntries(block));
    var rows = new ArrayList<Row<V>>();
    int source = start;
    for (var change : changes) {
      while (source < change.position) rows.add(new Row<>(source++, null));
      if (change.change.element != null)
        rows.add(new Row<>(change.found >= 0 ? change.found : -1, change.change.element));
      if (change.found >= 0) source++;
    }
    while (source < end) rows.add(new Row<>(source++, null));
    // Defer partitioning until the final merged rows are known: cancelled inserts do not split.
    int blocks = rows.isEmpty() ? 0 : 1 + (rows.size() - 1) / options.blockEntries();
    for (int from = 0, remaining = blocks; remaining > 0; remaining--) {
      int count =
          options.split() == V2IndexFormat.BlockSplit.BALANCED
              ? 1 + (rows.size() - from - 1) / remaining
              : Math.min(options.blockEntries(), rows.size() - from);
      var part = rows.subList(from, from + count);
      addPlan(
          pieces,
          new BlockPlan<>(
              base.blockCount == 0 ? null : base,
              base.blockCount == 0 ? -1 : block,
              part,
              null,
              base.serializer,
              options,
              scratch));
      from += count;
    }
  }

  private static byte[] encodeKey(IndexKey key) {
    checkArgument(key.serializedSize() <= IndexImpl.MAX_KEY_BYTES, "Key too large");
    var bytes = ByteBuffer.allocate(key.serializedSize());
    key.serialize(bytes);
    return bytes.array();
  }

  private static final class BlockPlan<V> {
    final ReadOnlyV2IndexImpl<V> base;
    final ReadOnlyV2IndexImpl.Block old;
    final List<Row<V>> rows;
    final List<InternalIndexElement<V>> replacements;
    final IndexValueSerializer<V> codec;
    final int count;
    final boolean reuseKeys;
    final boolean shared;
    final boolean frontPrefix;
    final byte[] firstKey;
    final byte[] oldFirstKey;
    final byte[][] freshKeys;
    final int[] prefixes;
    final int[] keyEnds;
    final int[] valueEnds;
    final int common;
    final int keyBytes;
    final int kw;
    final int vw;
    final int mode;
    final int flags;
    final int size;

    BlockPlan(
        ReadOnlyV2IndexImpl<V> base,
        int block,
        List<Row<V>> rows,
        List<InternalIndexElement<V>> replacements,
        IndexValueSerializer<V> codec,
        Options options,
        Scratch scratch) {
      this.base = base;
      this.rows = rows;
      this.replacements = replacements;
      this.codec = codec;
      old = base == null ? null : base.block(block);
      count = rows == null ? requireNonNull(old).count() : rows.size();
      checkArgument(count > 0 && count <= 4096);
      reuseKeys = rows == null;
      shared = reuseKeys ? old.sharedPrefix() : options.layout() == Layout.PREFIX_ENDS;
      oldFirstKey = old == null ? null : source().copyEncodedKey(old.first(), scratch);
      firstKey = reuseKeys ? oldFirstKey : rowKey(0, scratch);
      int prefix = 0, bytes = 0;
      freshKeys = reuseKeys ? null : new byte[count][];
      prefixes = reuseKeys ? null : new int[count];
      keyEnds = reuseKeys ? null : new int[count];
      if (reuseKeys) {
        checkArgument(shared == old.sharedPrefix(), "Cannot change key encoding in value patch");
        prefix = originalBlock().prefix();
        bytes = old.keyBytes();
      } else if (shared) {
        if (old != null && old.sharedPrefix()) {
          prefix = firstKey.length;
          boolean checkedOldPrefix = original(0) >= 0;
          for (int i = 1; i < count; i++) {
            if (original(i) < 0) {
              freshKeys[i] = rowKey(i, scratch);
              prefix = Math.min(prefix, commonPrefix(firstKey, freshKeys[i]));
            } else if (!checkedOldPrefix || prefix > originalBlock().prefix()) {
              var encoded = source().encodedKey(original(i), scratch);
              int length = Math.min(prefix, encoded.remaining()), sharedBytes = 0;
              while (sharedBytes < length && firstKey[sharedBytes] == encoded.get(sharedBytes))
                sharedBytes++;
              prefix = sharedBytes;
              checkedOldPrefix = true;
            }
          }
        } else {
          prefix = firstKey.length;
          freshKeys[0] = firstKey;
          for (int i = 1; i < count; i++) {
            freshKeys[i] = rowKey(i, scratch);
            prefix = Math.min(prefix, commonPrefix(firstKey, freshKeys[i]));
          }
        }
      } else if (options.layout() == Layout.FRONT_PREFIX_ENDS) {
        prefix = firstKey.length;
        if (count > 1) {
          freshKeys[count - 1] = rowKey(count - 1, scratch);
          // Sorted IndexKeys determine the block's common encoded prefix at its endpoints.
          prefix = commonPrefix(firstKey, freshKeys[count - 1]);
        }
      }
      frontPrefix = reuseKeys ? old.frontPrefix() : !shared && prefix > 0;
      if (!reuseKeys && shared) {
        for (int i = 1; i < count; i++) {
          int length =
              freshKeys[i] != null
                  ? freshKeys[i].length
                  : original(i) == originalBlock().first()
                      ? requireNonNull(oldFirstKey).length
                      : originalBlock().prefix()
                          + source()
                              .keyLimit(originalBlock(), original(i) - originalBlock().first())
                          - source()
                              .keyStart(originalBlock(), original(i) - originalBlock().first());
          bytes = Math.addExact(bytes, length - prefix);
          keyEnds[i] = bytes;
        }
      } else if (!reuseKeys) {
        freshKeys[0] = firstKey;
        for (int i = 1; i < count; i++) {
          int original = original(i), previous = original(i - 1);
          if (old != null
              && !old.sharedPrefix()
              && old.frontPrefix() == frontPrefix
              && old.prefix() == prefix
              && original > old.first()
              && previous == original - 1
              && i % options.keyRestart() != 0
              && (int) varIntAt(base.source, base.keyStart(old, original - old.first())) != 0) {
            int slot = original - old.first();
            freshKeys[i] = null;
            long range = base.keyRange(old, slot);
            bytes += (int) range - (int) (range >>> 32);
          } else {
            if (freshKeys[i] == null) freshKeys[i] = rowKey(i, scratch);
            // Recompress repaired successors and redundant old restarts instead of accumulating
            // full-key records across structural edits.
            int p = 0;
            if (i % options.keyRestart() != 0) {
              var previousKey =
                  freshKeys[i - 1] != null ? freshKeys[i - 1] : rowKey(i - 1, scratch);
              p = commonPrefix(previousKey, freshKeys[i]) - prefix;
            }
            prefixes[i] = p;
            bytes += varIntLen(p) + freshKeys[i].length - prefix - p;
          }
          keyEnds[i] = bytes;
        }
      }
      common = prefix;
      keyBytes = bytes;
      kw = reuseKeys ? old.keyWidth() : width(keyBytes);
      valueEnds = new int[count];
      int values = 0, previousLength = -1;
      boolean fixed = true;
      for (int i = 0; i < count; i++) {
        var replacement = replacement(i);
        int length;
        if (replacement != null) length = replacement.contentSerializedSize(codec);
        else {
          int ordinal = original(i);
          long range = source().valueRange(originalBlock(), ordinal - originalBlock().first());
          length = (int) range - (int) (range >>> 32);
        }
        checkArgument(length >= 0, "Negative value size");
        fixed &= previousLength < 0 || previousLength == length;
        previousLength = length;
        values = Math.addExact(values, length);
        valueEnds[i] = values;
      }
      vw = width(values);
      mode = fixed ? STRIDE : ENDS;
      flags =
          (shared ? PREFIX : frontPrefix ? FRONT_PREFIX : 0)
              | (kw == 1 ? KEY_BYTE : kw == 4 ? KEY_WIDE : 0)
              | (vw == 1 ? VALUE_BYTE : vw == 4 ? VALUE_WIDE : 0)
              | (mode << MODE_SHIFT);
      int locator = mode == STRIDE ? 0 : count * vw;
      size = Math.addExact(headerSize(flags) + (count - 1) * kw + keyBytes + locator, values);
    }

    private ReadOnlyV2IndexImpl<V> source() {
      return requireNonNull(base);
    }

    private ReadOnlyV2IndexImpl.Block originalBlock() {
      return requireNonNull(old);
    }

    private int original(int slot) {
      return rows == null ? old.first() + slot : rows.get(slot).original;
    }

    private InternalIndexElement<V> replacement(int slot) {
      return rows == null ? replacements.get(slot) : rows.get(slot).replacement;
    }

    private byte[] rowKey(int slot, Scratch scratch) {
      var row = rows.get(slot);
      return row.original >= 0
          ? base.copyEncodedKey(row.original, scratch)
          : encodeKey(row.replacement.key());
    }

    void write(ByteBuffer output) {
      int start = output.position();
      output.put((byte) flags);
      if (shared || frontPrefix) output.putShort((short) common);
      if (reuseKeys) copy(output, base.source, old.keyTable(), (count - 1) * kw + keyBytes);
      else {
        for (int i = 1; i < count; i++) putOffset(output, keyEnds[i], kw);
        for (int i = 1; i < count; i++) {
          int original = original(i);
          if (shared) {
            if (freshKeys[i] != null)
              output.put(freshKeys[i], common, freshKeys[i].length - common);
            else if (original == old.first())
              output.put(oldFirstKey, common, oldFirstKey.length - common);
            else {
              int prepend = Math.max(0, originalBlock().prefix() - common);
              if (prepend > 0) output.put(oldFirstKey, common, prepend);
              int skip = Math.max(0, common - originalBlock().prefix());
              int slot = original - old.first();
              copy(
                  output,
                  base.source,
                  base.keyStart(old, slot) + skip,
                  base.keyLimit(old, slot) - base.keyStart(old, slot) - skip);
            }
          } else if (freshKeys[i] != null) {
            putVarInt(output, prefixes[i]);
            output.put(
                freshKeys[i], common + prefixes[i], freshKeys[i].length - common - prefixes[i]);
          } else {
            int slot = original - old.first();
            long range = base.keyRange(old, slot);
            copy(output, base.source, (int) (range >>> 32), (int) range - (int) (range >>> 32));
          }
        }
      }
      if (mode == ENDS) for (int end : valueEnds) putOffset(output, end, vw);
      for (int i = 0; i < count; i++) {
        var replacement = replacement(i);
        if (replacement != null) replacement.serializeContent(codec, output);
        else {
          int ordinal = original(i);
          long range = source().valueRange(originalBlock(), ordinal - originalBlock().first());
          copy(output, base.source, (int) (range >>> 32), (int) range - (int) (range >>> 32));
        }
      }
      checkState(output.position() == start + size, "Block size mismatch");
    }
  }

  static <V> Prepared<V> prepare(
      ReadOnlyV2IndexImpl<V> base, List<Change<V>> changes, Options options) {
    var buffer = IndexKeyBufferPool.acquire();
    try {
      return new Prepared<>(base, planMerge(base, changes, options, new Scratch(buffer)), options);
    } finally {
      IndexKeyBufferPool.release(buffer);
    }
  }

  private static <V> ByteBuffer assemble(
      ReadOnlyV2IndexImpl<V> base, List<Piece<V>> pieces, Options options) {
    return new Prepared<>(base, pieces, options).serialize();
  }

  static final class Prepared<V> {
    private final ReadOnlyV2IndexImpl<V> base;
    private final List<Piece<V>> pieces;
    private final Options options;
    private final int entryCount, bodyStart, directory, size;
    private final int[] prefixes, keyOffsets;

    private Prepared(ReadOnlyV2IndexImpl<V> base, List<Piece<V>> pieces, Options options) {
      this.base = base;
      this.pieces = List.copyOf(pieces);
      this.options = options;
      int count = 0, bodies = 0;
      for (var piece : pieces) {
        count = Math.addExact(count, piece.count);
        bodies = Math.addExact(bodies, piece.size);
      }
      bodyStart = fileHeaderSize(count);
      directory = Math.addExact(bodyStart, bodies);
      int position =
          Math.addExact(
              directory,
              Math.addExact(DIRECTORY_HEADER, Math.multiplyExact(pieces.size(), DESCRIPTOR)));
      prefixes = new int[pieces.size()];
      keyOffsets = new int[pieces.size()];
      ByteBuffer previous = null;
      for (int i = 0; i < pieces.size(); i++) {
        var key = pieces.get(i).key;
        int prefix = 0;
        if (options.directory() == V2IndexFormat.DirectoryEncoding.PREFIX && i % GROUP != 0) {
          int length = Math.min(previous.remaining(), key.remaining());
          while (prefix < length && previous.get(prefix) == key.get(prefix)) prefix++;
          if (prefix < 2) prefix = 0;
        }
        prefixes[i] = prefix;
        keyOffsets[i] = position;
        if (options.directory() == V2IndexFormat.DirectoryEncoding.PREFIX)
          position =
              Math.addExact(position, varIntLen(prefix) + varIntLen(key.remaining() - prefix));
        position = Math.addExact(position, key.remaining() - prefix);
        previous = key;
      }
      entryCount = count;
      size = position;
    }

    int serializedSize() {
      return size;
    }

    ByteBuffer serialize() {
      var output = ByteBuffer.allocate(size);
      output.put((byte) V2IndexFormat.VERSION);
      putVarInt(output, entryCount);
      output.putInt(directory);
      for (var piece : pieces) {
        if (piece.plan != null) piece.plan.write(output);
        else copy(output, base.source, base.blockStart(piece.originalBlock), piece.size);
      }
      checkState(output.position() == directory);
      output
          .putInt(pieces.size())
          .put((byte) options.directory().ordinal())
          .put((byte) (options.directory() == V2IndexFormat.DirectoryEncoding.FULL ? 1 : GROUP));
      int ordinal = 0, at = bodyStart;
      for (int i = 0; i < pieces.size(); i++) {
        var piece = pieces.get(i);
        output.putInt(at).putInt(ordinal).putInt(keyOffsets[i]);
        at += piece.size;
        ordinal += piece.count;
      }
      for (int i = 0; i < pieces.size(); i++) {
        var key = pieces.get(i).key;
        int prefix = prefixes[i];
        if (options.directory() == V2IndexFormat.DirectoryEncoding.PREFIX) {
          putVarInt(output, prefix);
          putVarInt(output, key.remaining() - prefix);
        }
        copy(output, key, prefix, key.remaining() - prefix);
      }
      checkState(!output.hasRemaining(), "Directory size mismatch");
      return output.flip().asReadOnlyBuffer();
    }
  }
}

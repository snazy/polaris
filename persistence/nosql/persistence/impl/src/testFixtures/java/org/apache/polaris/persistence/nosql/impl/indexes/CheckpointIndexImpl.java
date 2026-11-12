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

import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.jspecify.annotations.NonNull;

/** Option C: incremental v1 reads with sparse, encoded, self-contained checkpoints. */
final class CheckpointIndexImpl<V> extends IndexReadPrototype<V> {
  final Directory directory;

  CheckpointIndexImpl(ByteBuffer source, IndexValueSerializer<V> serializer, int interval) {
    this(source, serializer, interval, false);
  }

  CheckpointIndexImpl(
      ByteBuffer source, IndexValueSerializer<V> serializer, int interval, boolean pagedValues) {
    super(source, serializer, pagedValues);
    checkArgument(interval > 0, "Interval must be positive");
    directory = new Directory(this.source, interval, count == 0);
  }

  CheckpointIndexImpl(Directory prepared, IndexValueSerializer<V> serializer) {
    this(prepared, serializer, false);
  }

  CheckpointIndexImpl(Directory prepared, IndexValueSerializer<V> serializer, boolean pagedValues) {
    super(prepared.source, serializer, pagedValues);
    checkArgument(prepared.complete, "Shared directory must be complete");
    directory = prepared;
  }

  Directory prepare() {
    ensure(null, count);
    return directory;
  }

  @Override
  long metadataBytes() {
    return directory.metadataBytes();
  }

  private Scan scanAt(int anchor) {
    if (anchor < 0) return new Scan();
    synchronized (directory) {
      int start = directory.keyStarts[anchor];
      int length = directory.keyLengths[anchor];
      return new Scan(
          anchor * directory.interval,
          Arrays.copyOfRange(directory.keys, start, start + length),
          directory.values[anchor],
          directory.ends[anchor]);
    }
  }

  /** Build only as far as the first anchor >= key, or the block containing ordinal. */
  private void ensure(IndexKey key, int ordinal) {
    synchronized (directory) {
      if (directory.complete) return;
      Scan scan = scanAt(directory.size - 1);
      while (scan.advance()) {
        record(scan);
        if (scan.ordinal % directory.interval == 0
            && (key != null ? scan.key().compareTo(key) >= 0 : scan.ordinal >= ordinal)) return;
      }
      directory.complete = true;
    }
  }

  private void record(Scan scan) {
    if (scan.ordinal % directory.interval != 0 && scan.ordinal != count - 1) return;
    synchronized (directory) {
      if (directory.complete) return;
      if (scan.ordinal % directory.interval == 0
          && scan.ordinal / directory.interval == directory.size) {
        directory.add(scan.keyBytes, scan.keyLength, scan.valueOffset, scan.endOffset);
      }
      if (scan.ordinal == count - 1) directory.complete = true;
    }
  }

  private int floor(IndexKey key) {
    ensure(key, 0);
    synchronized (directory) {
      int low = 0;
      int high = directory.size - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        IndexKey candidate =
            requireNonNull(
                IndexKey.deserializeKey(
                    ByteBuffer.wrap(
                        directory.keys, directory.keyStarts[mid], directory.keyLengths[mid])));
        if (candidate.compareTo(key) <= 0) low = mid + 1;
        else high = mid - 1;
      }
      return high;
    }
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    Scan scan = scanAt(floor(key));
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
    int block = ordinal / directory.interval;
    ensure(null, block * directory.interval);
    Scan scan = scanAt(block);
    while (scan.advance()) {
      record(scan);
      if (scan.ordinal == ordinal) return scan.element();
    }
    throw new IllegalStateException("Missing entry");
  }

  @Override
  Iterator<InternalIndexElement<V>> forward(IndexKey lower) {
    Scan scan = scanAt(lower == null ? -1 : floor(lower));
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
      Scan scan = scanAt(floor(higher));
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
          int anchor = next / directory.interval;
          Scan scan = scanAt(anchor);
          while (scan.advance() && scan.ordinal <= next) block.addLast(scan.element());
          next = anchor * directory.interval - 1;
        }
        return block.removeLast();
      }
    };
  }

  /** Primitive arrays plus packed encoded keys; no decoded keys, values or codec are retained. */
  static final class Directory {
    final ByteBuffer source;
    final int interval;
    int[] values = new int[0];
    int[] ends = new int[0];
    int[] keyStarts = new int[0];
    int[] keyLengths = new int[0];
    byte[] keys = new byte[0];
    int used;
    int size;
    boolean complete;

    Directory(ByteBuffer source, int interval, boolean complete) {
      this.source = source;
      this.interval = interval;
      this.complete = complete;
    }

    void add(byte[] key, int length, int value, int end) {
      if (size == values.length) {
        int capacity = Math.max(4, size * 2);
        values = Arrays.copyOf(values, capacity);
        ends = Arrays.copyOf(ends, capacity);
        keyStarts = Arrays.copyOf(keyStarts, capacity);
        keyLengths = Arrays.copyOf(keyLengths, capacity);
      }
      if (used + length > keys.length)
        keys = Arrays.copyOf(keys, Math.max(used + length, Math.max(64, keys.length * 2)));
      values[size] = value;
      ends[size] = end;
      keyStarts[size] = used;
      keyLengths[size] = length;
      System.arraycopy(key, 0, keys, used, length);
      used += length;
      size++;
    }

    synchronized long metadataBytes() {
      return 80L + 5L * 16 + (long) values.length * Integer.BYTES * 4 + keys.length;
    }
  }
}

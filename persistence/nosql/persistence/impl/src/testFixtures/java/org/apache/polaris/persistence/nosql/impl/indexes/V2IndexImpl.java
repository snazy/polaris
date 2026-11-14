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
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;

/** Mutable fixture-only v2 index. Mutable instances require exclusive access. */
final class V2IndexImpl<V> implements IndexSpi<V> {
  private final IndexValueSerializer<V> serializer;
  private final V2IndexWriter.Options options;
  private V2IndexReader<V> reader;
  private List<Entry<V>> entries;
  private ByteBuffer serialized;
  private Plan plan;
  private ByteBuffer keyScratch;
  private ByteBuffer previousScratch;
  private ObjRef objRef;
  private boolean modified;
  private boolean dirty = true;

  // Only integer size/prefix metadata is added; no second encoded key array is retained per entry.
  private record Entry<V>(
      InternalIndexElement<V> element, int keySize, int valueSize, int prefix, int strip) {
    int deltaSize() {
      return varIntLen(strip) + keySize - prefix;
    }
  }

  private static final class Block {
    final int ordinal;
    final int bodyOffset;
    final int prefix;
    final int keyBytes;
    int keyOffset;

    Block(int ordinal, int bodyOffset, int prefix, int keyBytes) {
      this.ordinal = ordinal;
      this.bodyOffset = bodyOffset;
      this.prefix = prefix;
      this.keyBytes = keyBytes;
    }
  }

  private record Plan(int directory, int size, List<Block> blocks) {}

  V2IndexImpl(IndexValueSerializer<V> serializer) {
    this(serializer, V2IndexWriter.Options.defaults(64));
  }

  V2IndexImpl(IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this.serializer = serializer;
    this.options = options;
    entries = new ArrayList<>();
  }

  V2IndexImpl(ByteBuffer bytes, IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this(new V2IndexReader<>(bytes, serializer), serializer, options);
  }

  V2IndexImpl(
      V2IndexReader<V> reader, IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this.serializer = serializer;
    this.options = options;
    this.reader = reader;
    serialized = reader.serialize();
    objRef = reader.getObjId();
    dirty = false;
  }

  private V2IndexImpl(
      List<Entry<V>> source,
      IndexValueSerializer<V> serializer,
      V2IndexWriter.Options options,
      Plan plan) {
    this.serializer = serializer;
    this.options = options;
    entries = new ArrayList<>(source);
    var first = entries.getFirst();
    entries.set(0, new Entry<>(first.element, first.keySize, first.valueSize, 0, 0));
    this.plan = plan;
    modified = true;
  }

  private void ensureMutable() {
    if (entries != null) return;
    entries = new ArrayList<>(reader.count);
    var iterator = reader.elementIterator();
    Entry<V> previous = null;
    while (iterator.hasNext()) {
      var element = iterator.next();
      var entry = entry(element, previous);
      entries.add(entry);
      previous = entry;
    }
    reader = null;
  }

  private ByteBuffer keyScratch() {
    if (keyScratch == null) keyScratch = ByteBuffer.allocate(IndexImpl.MAX_KEY_BYTES);
    return keyScratch;
  }

  private int commonPrefix(IndexKey previous, IndexKey current) {
    if (previousScratch == null) previousScratch = ByteBuffer.allocate(IndexImpl.MAX_KEY_BYTES);
    var a = previousScratch.clear();
    var b = keyScratch().clear();
    previous.serialize(a);
    current.serialize(b);
    int length = Math.min(a.position(), b.position());
    int prefix = 0;
    while (prefix < length && a.get(prefix) == b.get(prefix)) prefix++;
    return prefix;
  }

  private Entry<V> entry(InternalIndexElement<V> element, Entry<V> previous) {
    int prefix = previous == null ? 0 : commonPrefix(previous.element.key(), element.key());
    return new Entry<>(
        element,
        element.key().serializedSize(),
        element.contentSerializedSize(serializer),
        prefix,
        previous == null ? 0 : previous.keySize - prefix);
  }

  private void updateSuccessor(int ordinal) {
    if (ordinal >= entries.size()) return;
    var old = entries.get(ordinal);
    var previous = ordinal == 0 ? null : entries.get(ordinal - 1);
    int prefix = previous == null ? 0 : commonPrefix(previous.element.key(), old.element.key());
    entries.set(
        ordinal,
        new Entry<>(
            old.element,
            old.keySize,
            old.valueSize,
            prefix,
            previous == null ? 0 : previous.keySize - prefix));
  }

  private int search(IndexKey key) {
    int low = 0, high = entries.size() - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int compare = entries.get(mid).element.key().compareTo(key);
      if (compare < 0) low = mid + 1;
      else if (compare > 0) high = mid - 1;
      else return mid;
    }
    return -low - 1;
  }

  @Override
  public AddResult add(@NonNull InternalIndexElement<V> element) {
    ensureMutable();
    int found = search(element.key());
    modified = true;
    dirty = true;
    if (found >= 0) {
      var previous = entries.get(found);
      int size = element.contentSerializedSize(serializer);
      entries.set(
          found, new Entry<>(element, previous.keySize, size, previous.prefix, previous.strip));
      if (size != previous.valueSize) plan = null;
      return previous.element.hasValue() ? AddResult.UPDATED : AddResult.UPDATED_NO_VALUE;
    }
    int ordinal = -found - 1;
    entries.add(ordinal, entry(element, ordinal == 0 ? null : entries.get(ordinal - 1)));
    updateSuccessor(ordinal + 1);
    plan = null;
    return AddResult.NEW_KEY;
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    if (entries == null && !reader.containsElement(key)) return false;
    ensureMutable();
    int ordinal = search(key);
    if (ordinal < 0) return false;
    entries.remove(ordinal);
    updateSuccessor(ordinal);
    dirty = true;
    modified = true;
    plan = null;
    return true;
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    if (entries == null) return reader.getElement(key);
    int ordinal = search(key);
    return ordinal < 0 ? null : entries.get(ordinal).element;
  }

  @Override
  public V get(@NonNull IndexKey key) {
    if (entries == null) return reader.get(key);
    var element = getElement(key);
    return element == null ? null : element.valueNullable();
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    return entries == null ? reader.containsElement(key) : search(key) >= 0;
  }

  @Override
  public boolean contains(IndexKey key) {
    if (entries == null) return reader.contains(key);
    var element = getElement(key);
    return element != null && element.hasValue();
  }

  @Override
  public boolean hasElements() {
    return entries == null ? reader.hasElements() : !entries.isEmpty();
  }

  @Override
  public IndexKey first() {
    return entries == null
        ? reader.first()
        : entries.isEmpty() ? null : entries.getFirst().element.key();
  }

  @Override
  public IndexKey last() {
    return entries == null
        ? reader.last()
        : entries.isEmpty() ? null : entries.getLast().element.key();
  }

  @Override
  public List<IndexKey> asKeyList() {
    if (entries == null) return reader.asKeyList();
    return new AbstractList<>() {
      @Override
      public IndexKey get(int index) {
        return entries.get(index).element.key();
      }

      @Override
      public int size() {
        return entries.size();
      }
    };
  }

  @Override
  public Iterator<InternalIndexElement<V>> elementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    if (entries == null) return reader.elementIterator(lower, higher, prefetch);
    return iterate(lower, higher, false);
  }

  @Override
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    if (entries == null) return reader.reverseElementIterator(lower, higher, prefetch);
    return iterate(lower, higher, true);
  }

  private Iterator<InternalIndexElement<V>> iterate(
      IndexKey lower, IndexKey higher, boolean reverse) {
    checkArgument(
        lower == null || higher == null || lower.compareTo(higher) <= 0, "Invalid bounds");
    boolean prefix = lower != null && lower.equals(higher);
    checkArgument(!reverse || !prefix, "Reverse prefix queries unsupported");
    int from = lower == null ? 0 : bound(lower, false);
    int to = higher == null || prefix ? entries.size() : bound(higher, true);
    var range = entries.subList(from, to);
    var iterator = (reverse ? range.reversed() : range).iterator();
    return new AbstractIterator<>() {
      @Override
      protected InternalIndexElement<V> computeNext() {
        if (!iterator.hasNext()) return endOfData();
        var entry = iterator.next().element;
        return prefix && !entry.key().startsWith(lower) ? endOfData() : entry;
      }
    };
  }

  private int bound(IndexKey key, boolean inclusive) {
    int found = search(key);
    return found < 0 ? -found - 1 : found + (inclusive ? 1 : 0);
  }

  private final class Builder {
    final List<Block> blocks = new ArrayList<>();
    int count;
    int body;
    int directoryBytes;
    int inBlock;
    int previousKeySize;
    IndexKey previousDirectoryKey;

    boolean startsBlock(Entry<V> entry) {
      return count == 0
          || inBlock >= options.blockEntries()
          || (options.uneven()
              && inBlock >= Math.max(1, options.blockEntries() / 2)
              && entry.prefix * 2 < Math.min(previousKeySize, entry.keySize));
    }

    int directoryPrefix(Entry<V> entry) {
      if (options.encoding() == V2IndexWriter.DirectoryEncoding.FULL
          || blocks.size() % options.directoryRestart() == 0) return 0;
      int prefix = commonPrefix(previousDirectoryKey, entry.element.key());
      return prefix < 2 ? 0 : prefix;
    }

    int directoryKeySize(Entry<V> entry, int prefix) {
      int suffix = entry.keySize - prefix;
      return options.encoding() == V2IndexWriter.DirectoryEncoding.FULL
          ? suffix
          : Math.addExact(suffix, varIntLen(prefix) + varIntLen(suffix));
    }

    int sizeWith(Entry<V> entry) {
      boolean start = startsBlock(entry);
      int nextBody = Math.addExact(body, entry.valueSize + (start ? 0 : entry.deltaSize()));
      int nextDirectory =
          Math.addExact(
              directoryBytes,
              start ? Math.addExact(12, directoryKeySize(entry, directoryPrefix(entry))) : 0);
      return total(count + 1, nextBody, nextDirectory);
    }

    void add(Entry<V> entry) {
      boolean start = startsBlock(entry);
      if (start) {
        int prefix = directoryPrefix(entry);
        int bytes = directoryKeySize(entry, prefix);
        blocks.add(new Block(count, body, prefix, bytes));
        directoryBytes = Math.addExact(directoryBytes, Math.addExact(12, bytes));
        previousDirectoryKey = entry.element.key();
        inBlock = 0;
      }
      body = Math.addExact(body, entry.valueSize + (start ? 0 : entry.deltaSize()));
      count++;
      inBlock++;
      previousKeySize = entry.keySize;
    }

    Plan finish() {
      int directory = Math.addExact(1 + varIntLen(count) + 4, body);
      int keyOffset =
          Math.addExact(directory, Math.addExact(6, Math.multiplyExact(blocks.size(), 12)));
      for (var block : blocks) {
        block.keyOffset = keyOffset;
        keyOffset = Math.addExact(keyOffset, block.keyBytes);
      }
      return new Plan(directory, total(count, body, directoryBytes), blocks);
    }
  }

  private static int total(int count, int body, int directoryBytes) {
    return Math.addExact(1 + varIntLen(count) + 4 + 6, Math.addExact(body, directoryBytes));
  }

  private Plan plan() {
    if (plan == null) {
      var builder = new Builder();
      for (var entry : entries) builder.add(entry);
      plan = builder.finish();
    }
    return plan;
  }

  @Override
  public int estimatedSerializedSize() {
    if (!dirty) return serialized.remaining();
    ensureMutable();
    return plan().size;
  }

  @Override
  public @NonNull ByteBuffer serialize() {
    if (!dirty) return serialized.duplicate();
    ensureMutable();
    var layout = plan();
    var output = ByteBuffer.allocate(layout.size);
    output.put((byte) 2);
    putVarInt(output, entries.size());
    output.putInt(layout.directory);
    int bodyStart = output.position();
    int nextBlock = 0;
    for (int i = 0; i < entries.size(); i++) {
      var entry = entries.get(i);
      if (nextBlock < layout.blocks.size() && layout.blocks.get(nextBlock).ordinal == i) {
        checkArgument(
            output.position() == bodyStart + layout.blocks.get(nextBlock).bodyOffset,
            "Block size mismatch");
        nextBlock++;
      } else {
        var key = keyScratch().clear();
        entry.element.key().serialize(key);
        putVarInt(output, entry.strip);
        output.put(key.array(), key.arrayOffset() + entry.prefix, entry.keySize - entry.prefix);
      }
      entry.element.serializeContent(serializer, output);
    }
    checkArgument(output.position() == layout.directory, "Body size mismatch");
    output.putInt(layout.blocks.size());
    output.put((byte) options.encoding().ordinal());
    output.put(
        (byte)
            (options.encoding() == V2IndexWriter.DirectoryEncoding.FULL
                ? 1
                : options.directoryRestart()));
    for (var block : layout.blocks)
      output.putInt(bodyStart + block.bodyOffset).putInt(block.ordinal).putInt(block.keyOffset);
    for (var block : layout.blocks) {
      var entry = entries.get(block.ordinal);
      var key = keyScratch().clear();
      entry.element.key().serialize(key);
      if (options.encoding() == V2IndexWriter.DirectoryEncoding.PREFIX) {
        putVarInt(output, block.prefix);
        putVarInt(output, entry.keySize - block.prefix);
      }
      output.put(key.array(), key.arrayOffset() + block.prefix, entry.keySize - block.prefix);
    }
    checkArgument(!output.hasRemaining(), "Directory size mismatch");
    serialized = output.flip().asReadOnlyBuffer();
    dirty = false;
    return serialized.duplicate();
  }

  /** Forces actual serialization for benchmarks, retaining valid size/block metadata. */
  V2IndexImpl<V> setModified() {
    modified = true;
    dirty = true;
    return this;
  }

  /** Separates plan construction from the steady-state serialization benchmark. */
  V2IndexImpl<V> invalidatePlan() {
    plan = null;
    return setModified();
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long target) {
    checkArgument(target > 0, "Positive split target required");
    ensureMutable();
    if (entries.isEmpty()) return List.of();
    if (entries.size() == 1) return List.of(this);
    var parts = new ArrayList<IndexSpi<V>>();
    var builder = new Builder();
    int start = 0;
    for (int i = 0; i < entries.size(); i++) {
      var entry = entries.get(i);
      if (builder.count != 0 && builder.sizeWith(entry) > target) {
        parts.add(
            new V2IndexImpl<>(entries.subList(start, i), serializer, options, builder.finish()));
        builder = new Builder();
        start = i;
      }
      builder.add(entry);
    }
    if (parts.isEmpty()) return List.of(this);
    parts.add(
        new V2IndexImpl<>(
            entries.subList(start, entries.size()), serializer, options, builder.finish()));
    return parts;
  }

  @Override
  public boolean isModified() {
    return modified;
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return this;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

  @Override
  public ObjRef getObjId() {
    return objRef;
  }

  @Override
  public IndexSpi<V> setObjId(ObjRef value) {
    objRef = value;
    return this;
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return List.of(this);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    return this;
  }
}

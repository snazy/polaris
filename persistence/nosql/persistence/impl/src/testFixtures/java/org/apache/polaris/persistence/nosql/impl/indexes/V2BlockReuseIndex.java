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
import static java.util.Collections.emptyIterator;
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;

/** Fixture-only block copy-on-write experiment. Mutable views require exclusive access. */
final class V2BlockReuseIndex<V> implements IndexSpi<V> {
  private final V2IndexReader<V> base;
  private final IndexValueSerializer<V> serializer;
  private final V2IndexWriter.Options options;
  // Routing keeps the original directory boundaries, even when a block becomes empty.
  private final Map<Integer, V2IndexImpl<V>> changed = new HashMap<>();
  private int count;
  private boolean modified;
  private ObjRef objRef;
  private ByteBuffer serialized;
  private Plan plan;

  private record Chunk(ByteBuffer body, ByteBuffer key, int count, int prefix) {}

  private record Plan(List<Chunk> chunks, int directory, int size) {}

  V2BlockReuseIndex(IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this(
        new V2IndexReader<>(new V2IndexImpl<>(serializer, options).serialize(), serializer),
        serializer,
        options);
  }

  V2BlockReuseIndex(
      ByteBuffer bytes, IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this(new V2IndexReader<>(bytes, serializer), serializer, options);
  }

  V2BlockReuseIndex(
      V2IndexReader<V> base, IndexValueSerializer<V> serializer, V2IndexWriter.Options options) {
    this.base = base;
    this.serializer = serializer;
    this.options = options;
    count = base.count;
    objRef = base.getObjId();
    serialized = base.serialize();
  }

  private int blockForKey(IndexKey key) {
    return Math.max(0, base.blockForKey(key));
  }

  private V2IndexImpl<V> mutableBlock(int block) {
    return changed.computeIfAbsent(
        block,
        ignored -> {
          var result = new V2IndexImpl<>(serializer, options);
          if (base.blockCount != 0) {
            for (int slot = 0; slot < base.blockEntryCount(block); slot++)
              result.add(base.blockElement(block, slot));
          }
          return result;
        });
  }

  private void invalidate() {
    modified = true;
    serialized = null;
    plan = null;
  }

  @Override
  public AddResult add(@NonNull InternalIndexElement<V> element) {
    var result = mutableBlock(blockForKey(element.key())).add(element);
    if (result == AddResult.NEW_KEY) count++;
    invalidate();
    return result;
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    if (!containsElement(key)) return false;
    checkState(mutableBlock(blockForKey(key)).remove(key));
    count--;
    invalidate();
    return true;
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    if (changed.isEmpty()) return base.getElement(key);
    var block = changed.get(blockForKey(key));
    return block == null ? base.getElement(key) : block.getElement(key);
  }

  @Override
  public V get(@NonNull IndexKey key) {
    if (changed.isEmpty()) return base.get(key);
    var block = changed.get(blockForKey(key));
    return block == null ? base.get(key) : block.get(key);
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    if (changed.isEmpty()) return base.containsElement(key);
    var block = changed.get(blockForKey(key));
    return block == null ? base.containsElement(key) : block.containsElement(key);
  }

  @Override
  public boolean contains(IndexKey key) {
    if (changed.isEmpty()) return base.contains(key);
    var block = changed.get(blockForKey(key));
    return block == null ? base.contains(key) : block.contains(key);
  }

  @Override
  public boolean hasElements() {
    return count != 0;
  }

  private int blocks() {
    return Math.max(1, base.blockCount);
  }

  @Override
  public IndexKey first() {
    for (int i = 0; i < blocks(); i++) {
      var block = changed.get(i);
      if (block != null) {
        var first = block.first();
        if (first != null) return first;
      } else if (base.blockCount != 0) return base.directoryKey(i);
    }
    return null;
  }

  @Override
  public IndexKey last() {
    for (int i = blocks() - 1; i >= 0; i--) {
      var block = changed.get(i);
      if (block != null) {
        var last = block.last();
        if (last != null) return last;
      } else if (base.blockCount != 0)
        return base.blockElement(i, base.blockEntryCount(i) - 1).key();
    }
    return null;
  }

  @Override
  public List<IndexKey> asKeyList() {
    var result = new ArrayList<IndexKey>(count);
    elementIterator().forEachRemaining(element -> result.add(element.key()));
    return result;
  }

  private Iterator<InternalIndexElement<V>> blockElements(int block, boolean reverse) {
    var mutable = changed.get(block);
    if (mutable != null)
      return reverse ? mutable.reverseElementIterator() : mutable.elementIterator();
    return base.blockCount == 0 ? emptyIterator() : base.blockElements(block, reverse);
  }

  private Iterator<InternalIndexElement<V>> iterate(
      IndexKey lower, IndexKey higher, boolean reverse) {
    checkArgument(
        lower == null || higher == null || lower.compareTo(higher) <= 0, "Invalid bounds");
    boolean prefix = lower != null && lower.equals(higher);
    checkArgument(!reverse || !prefix, "Reverse prefix queries unsupported");
    return new AbstractIterator<>() {
      int block =
          reverse
              ? higher == null ? blocks() - 1 : blockForKey(higher)
              : lower == null ? 0 : blockForKey(lower);
      Iterator<InternalIndexElement<V>> entries;

      @Override
      protected InternalIndexElement<V> computeNext() {
        while (block >= 0 && block < blocks()) {
          if (entries == null) entries = blockElements(block, reverse);
          if (!entries.hasNext()) {
            block += reverse ? -1 : 1;
            entries = null;
            continue;
          }
          var element = entries.next();
          var key = element.key();
          if (reverse) {
            if (higher != null && key.compareTo(higher) > 0) continue;
            if (lower != null && key.compareTo(lower) < 0) return endOfData();
          } else {
            if (lower != null && key.compareTo(lower) < 0) continue;
            if (prefix ? !key.startsWith(lower) : higher != null && key.compareTo(higher) > 0)
              return endOfData();
          }
          return element;
        }
        return endOfData();
      }
    };
  }

  @Override
  public Iterator<InternalIndexElement<V>> elementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    return iterate(lower, higher, false);
  }

  @Override
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    return iterate(lower, higher, true);
  }

  private void appendChunks(List<Chunk> chunks, V2IndexReader<V> reader) {
    for (int i = 0; i < reader.blockCount; i++) appendChunk(chunks, reader, i);
  }

  private void appendChunk(List<Chunk> chunks, V2IndexReader<V> reader, int block) {
    var key = reader.serializedDirectoryKey(block).slice();
    int prefix = 0;
    if (options.encoding() == V2IndexWriter.DirectoryEncoding.PREFIX
        && chunks.size() % options.directoryRestart() != 0) {
      var previous = chunks.getLast().key;
      int length = Math.min(previous.remaining(), key.remaining());
      while (prefix < length && previous.get(prefix) == key.get(prefix)) prefix++;
      if (prefix < 2) prefix = 0;
    }
    chunks.add(new Chunk(reader.blockBytes(block), key, reader.blockEntryCount(block), prefix));
  }

  private Plan plan() {
    if (plan != null) return plan;
    var chunks = new ArrayList<Chunk>();
    for (int i = 0; i < blocks(); i++) {
      var mutable = changed.get(i);
      if (mutable != null) {
        // Only dirty blocks use temporary serialized storage; all other bodies are source slices.
        appendChunks(chunks, new V2IndexReader<>(mutable.serialize(), serializer));
      } else if (base.blockCount != 0) appendChunk(chunks, base, i);
    }
    int directory = 1 + varIntLen(count) + 4;
    int directoryBytes = 6 + Math.multiplyExact(chunks.size(), 12);
    int entries = 0;
    for (var chunk : chunks) {
      directory = Math.addExact(directory, chunk.body.remaining());
      int suffix = chunk.key.remaining() - chunk.prefix;
      directoryBytes = Math.addExact(directoryBytes, suffix);
      if (options.encoding() == V2IndexWriter.DirectoryEncoding.PREFIX)
        directoryBytes = Math.addExact(directoryBytes, varIntLen(chunk.prefix) + varIntLen(suffix));
      entries += chunk.count;
    }
    checkState(entries == count, "Entry count mismatch");
    plan = new Plan(chunks, directory, Math.addExact(directory, directoryBytes));
    return plan;
  }

  @Override
  public int estimatedSerializedSize() {
    if (serialized != null) return serialized.remaining();
    if (base.blockCount <= 1) return changed.get(0).estimatedSerializedSize();
    return plan().size;
  }

  @Override
  public @NonNull ByteBuffer serialize() {
    if (serialized != null) return serialized.duplicate();
    if (base.blockCount <= 1) {
      // New or single-block indexes have no untouched blocks to splice into another buffer.
      serialized = changed.get(0).serialize();
      return serialized.duplicate();
    }
    var layout = plan();
    var output = ByteBuffer.allocate(layout.size);
    output.put((byte) 2);
    putVarInt(output, count);
    output.putInt(layout.directory);
    int body = output.position();
    for (var chunk : layout.chunks) output.put(chunk.body.duplicate());
    output.putInt(layout.chunks.size());
    output.put((byte) options.encoding().ordinal());
    output.put(
        (byte)
            (options.encoding() == V2IndexWriter.DirectoryEncoding.FULL
                ? 1
                : options.directoryRestart()));
    int ordinal = 0;
    int keyOffset = layout.directory + 6 + layout.chunks.size() * 12;
    for (var chunk : layout.chunks) {
      output.putInt(body).putInt(ordinal).putInt(keyOffset);
      body += chunk.body.remaining();
      ordinal += chunk.count;
      int suffix = chunk.key.remaining() - chunk.prefix;
      keyOffset +=
          suffix
              + (options.encoding() == V2IndexWriter.DirectoryEncoding.FULL
                  ? 0
                  : varIntLen(chunk.prefix) + varIntLen(suffix));
    }
    for (var chunk : layout.chunks) {
      int suffix = chunk.key.remaining() - chunk.prefix;
      if (options.encoding() == V2IndexWriter.DirectoryEncoding.PREFIX) {
        putVarInt(output, chunk.prefix);
        putVarInt(output, suffix);
      }
      output.put(chunk.key.duplicate().position(chunk.prefix));
    }
    checkState(!output.hasRemaining(), "Size mismatch");
    serialized = output.flip().asReadOnlyBuffer();
    return serialized.duplicate();
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long target) {
    checkArgument(target > 0, "Positive split target required");
    if (count == 0) return List.of();
    if (count == 1 || estimatedSerializedSize() <= target) return List.of(this);
    // Keep the existing full V2 splitter as the explicit oversized-stripe fallback.
    return new V2IndexImpl<>(serialize(), serializer, options).splitByTargetSize(target);
  }

  int changedBlocks() {
    return changed.size();
  }

  int materializedBlocks() {
    return base.materializedBlocks();
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

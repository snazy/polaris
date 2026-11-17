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
import static org.apache.polaris.persistence.nosql.impl.indexes.V2IndexFormat.DEFAULT;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Exclusively owned mutable V2 view. Null delta elements remove physical entries; non-null elements
 * with null values retain tombstones. The immutable base and earlier serialized snapshots are never
 * modified. Reads merge the ordered delta without materializing untouched values.
 */
final class MutableV2IndexImpl<V> implements IndexSpi<V> {
  private record Delta<V>(@Nullable InternalIndexElement<V> element) {}

  private final IndexSpi<V> base;
  private final @Nullable ReadOnlyV2IndexImpl<V> encodedBase;
  private final IndexValueSerializer<V> serializer;
  private final V2IndexFormat.Options options;
  private final NavigableMap<IndexKey, Delta<V>> delta = new TreeMap<>();
  private int count;
  private boolean modified;
  private ObjRef objRef;
  private V2IndexWriter.Prepared<V> prepared;
  private List<V2IndexWriter.Change<V>> preparedChanges;
  private int[] preparedSizes;

  MutableV2IndexImpl(ReadOnlyV2IndexImpl<V> base) {
    this(base, DEFAULT);
  }

  MutableV2IndexImpl(ReadOnlyV2IndexImpl<V> base, V2IndexFormat.Options options) {
    this(base, base, base.serializer, options);
  }

  MutableV2IndexImpl(ReadOnlyIndexImpl<V> base, IndexValueSerializer<V> codec) {
    this(base, null, codec, DEFAULT);
  }

  private MutableV2IndexImpl(
      IndexSpi<V> base,
      @Nullable ReadOnlyV2IndexImpl<V> encodedBase,
      IndexValueSerializer<V> serializer,
      V2IndexFormat.Options options) {
    this.base = requireNonNull(base);
    this.encodedBase = encodedBase;
    this.serializer = requireNonNull(serializer);
    this.options = requireNonNull(options);
    count = base.asKeyList().size();
    objRef = base.getObjId();
  }

  int originalSerializedSize() {
    return base.estimatedSerializedSize();
  }

  static <V> MutableV2IndexImpl<V> empty(IndexValueSerializer<V> codec) {
    var bytes =
        ByteBuffer.allocate(12)
            .put((byte) V2IndexFormat.VERSION)
            .put((byte) 0)
            .putInt(6)
            .putInt(0)
            .put((byte) 0)
            .put((byte) 1)
            .flip();
    return new MutableV2IndexImpl<>(new ReadOnlyV2IndexImpl<>(bytes, codec));
  }

  @Override
  public boolean hasElements() {
    return count != 0;
  }

  @Override
  public boolean isMutable() {
    return true;
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
  public List<IndexSpi<V>> stripes() {
    return List.of(this);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    return this;
  }

  @Override
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    var changed = delta.get(key);
    return changed == null ? base.getElement(key) : changed.element();
  }

  @Override
  public V get(@NonNull IndexKey key) {
    var changed = delta.get(key);
    return changed == null
        ? base.get(key)
        : changed.element() == null ? null : changed.element().valueNullable();
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

  private void changed() {
    modified = true;
    prepared = null;
    preparedChanges = null;
    preparedSizes = null;
  }

  @Override
  public AddResult add(@NonNull InternalIndexElement<V> entry) {
    requireNonNull(entry);
    var key = requireNonNull(entry.key());
    checkArgument(key.serializedSize() <= IndexImpl.MAX_KEY_BYTES, "Key too large");
    checkArgument(entry.contentSerializedSize(serializer) >= 0, "Negative value size");
    var previous = getElement(key);
    var result =
        previous == null
            ? AddResult.NEW_KEY
            : previous.hasValue() ? AddResult.UPDATED : AddResult.UPDATED_NO_VALUE;
    if (previous == null) count = Math.addExact(count, 1);
    delta.put(key, new Delta<>(entry));
    changed();
    return result;
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    if (!containsElement(key)) return false;
    if (base.containsElement(key)) delta.put(key, new Delta<>(null));
    else delta.remove(key);
    count--;
    changed();
    return true;
  }

  @Override
  public IndexKey first() {
    var it = elementIterator();
    return it.hasNext() ? it.next().key() : null;
  }

  @Override
  public IndexKey last() {
    var it = reverseElementIterator();
    return it.hasNext() ? it.next().key() : null;
  }

  @Override
  public List<IndexKey> asKeyList() {
    return new AbstractList<>() {
      @Override
      public int size() {
        return count;
      }

      @Override
      public IndexKey get(int ordinal) {
        checkIndex(ordinal, count);
        if (delta.isEmpty()) return base.asKeyList().get(ordinal);
        var it = elementIterator();
        for (int i = 0; i < ordinal; i++) it.next();
        return it.next().key();
      }

      @Override
      public Iterator<IndexKey> iterator() {
        return Iterators.transform(elementIterator(), InternalIndexElement::key);
      }
    };
  }

  @Override
  public Iterator<InternalIndexElement<V>> elementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    return merged(lower, higher, prefetch, false);
  }

  @Override
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      IndexKey lower, IndexKey higher, boolean prefetch) {
    checkArgument(lower == null || !lower.equals(higher), "Reverse prefix queries are unsupported");
    return merged(lower, higher, prefetch, true);
  }

  private Iterator<InternalIndexElement<V>> merged(
      IndexKey lower, IndexKey higher, boolean prefetch, boolean reverse) {
    checkArgument(
        lower == null || higher == null || lower.compareTo(higher) <= 0, "Invalid bounds");
    boolean prefix = lower != null && lower.equals(higher);
    var original =
        Iterators.peekingIterator(
            reverse
                ? base.reverseElementIterator(lower, higher, prefetch)
                : base.elementIterator(lower, higher, prefetch));
    var range = lower == null ? delta : delta.tailMap(lower, true);
    if (higher != null && !prefix) range = range.headMap(higher, true);
    var entries = (reverse ? range.descendingMap() : range).entrySet().iterator();
    var updates = Iterators.peekingIterator(entries);
    return new AbstractIterator<>() {
      @Override
      protected InternalIndexElement<V> computeNext() {
        while (original.hasNext() || updates.hasNext()) {
          int comparison =
              original.hasNext()
                  ? (updates.hasNext()
                      ? original.peek().key().compareTo(updates.peek().getKey())
                      : -1)
                  : 1;
          if (reverse && original.hasNext() && updates.hasNext()) comparison = -comparison;
          if (comparison < 0) return original.next();
          if (comparison == 0) original.next();
          var update = updates.next();
          if (prefix && !update.getKey().startsWith(lower)) return endOfData();
          var element = update.getValue().element();
          if (element != null) return element;
        }
        return endOfData();
      }
    };
  }

  private List<V2IndexWriter.Change<V>> changes() {
    var result = new ArrayList<V2IndexWriter.Change<V>>(delta.size());
    delta.forEach((key, value) -> result.add(new V2IndexWriter.Change<>(key, value.element())));
    return result;
  }

  private V2IndexWriter.Prepared<V> prepare() {
    if (prepared != null) {
      boolean valid = true;
      for (int i = 0; i < preparedChanges.size(); i++) {
        var entry = preparedChanges.get(i).element();
        if (entry != null && entry.contentSerializedSize(serializer) != preparedSizes[i]) {
          valid = false;
          break;
        }
      }
      if (valid) return prepared;
    }
    // Recheck sizes even without add(): a caller-provided value may have grown or shrunk.
    var changes = changes();
    var sizes = new int[changes.size()];
    for (int i = 0; i < changes.size(); i++) {
      var entry = changes.get(i).element();
      sizes[i] = entry == null ? 0 : entry.contentSerializedSize(serializer);
    }
    var plan =
        encodedBase == null
            ? V2IndexWriter.prepareWrite(this, serializer, options)
            : V2IndexWriter.prepare(encodedBase, changes, options);
    prepared = plan;
    preparedChanges = changes;
    preparedSizes = sizes;
    return plan;
  }

  @Override
  public int estimatedSerializedSize() {
    return delta.isEmpty() && encodedBase != null
        ? base.estimatedSerializedSize()
        : prepare().serializedSize();
  }

  @Override
  public @NonNull ByteBuffer serialize() {
    return delta.isEmpty() && encodedBase != null ? base.serialize() : prepare().serialize();
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long target) {
    checkArgument(target > 0, "Positive split target required");
    if (count == 0) return List.of();
    // Splitting explicitly rewrites a V1-backed view, even when it fits in a single V2 part.
    if (encodedBase == null) modified = true;
    if (count == 1 || estimatedSerializedSize() <= target) return List.of(this);
    var splitBase =
        encodedBase == null ? new ReadOnlyV2IndexImpl<>(serialize(), serializer) : encodedBase;
    var parts =
        V2IndexWriter.mergeSplit(
            splitBase, encodedBase == null ? List.of() : changes(), options, target);
    if (parts.size() == 1) return List.of(this);
    var result = new ArrayList<IndexSpi<V>>(parts.size());
    for (var bytes : parts) {
      var part = new MutableV2IndexImpl<>(new ReadOnlyV2IndexImpl<>(bytes, serializer), options);
      part.modified = true;
      result.add(part);
    }
    return result;
  }
}

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
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;

/** Shared read-contract adapter for the isolated C/D experiments, not a production reader. */
abstract class IndexReadPrototype<V> implements IndexSpi<V> {
  final ByteBuffer source;
  final IndexValueSerializer<V> serializer;
  final int count;
  final int bodyOffset;
  private static final VarHandle ARRAY_ELEMENT =
      MethodHandles.arrayElementVarHandle(Object[].class);
  private static final int PAGE_SHIFT = 6;
  private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
  // Decoded values belong to this view, never to its shared layout.
  private final Object[][] values;
  private ObjRef objRef;

  IndexReadPrototype(ByteBuffer bytes, IndexValueSerializer<V> serializer) {
    source = bytes.slice().asReadOnlyBuffer();
    this.serializer = serializer;
    var header = source.duplicate();
    checkArgument(header.get() == 1, "Expected v1 index");
    count = readVarInt(header);
    checkArgument(count >= 0, "Negative entry count");
    bodyOffset = header.position();
    values = new Object[(int) (((long) count + PAGE_SIZE - 1) >>> PAGE_SHIFT)][];
  }

  abstract InternalIndexElement<V> entryAt(int ordinal);

  abstract Iterator<InternalIndexElement<V>> forward(IndexKey lower);

  abstract Iterator<InternalIndexElement<V>> backward(IndexKey higher);

  abstract long metadataBytes();

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
  public IndexSpi<V> asMutableIndex() {
    return IndexImpl.deserializeStoreIndex(source.duplicate(), serializer).setObjId(objRef);
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
    var page = (Object[]) ARRAY_ELEMENT.getVolatile(values, pageIndex);
    if (page == null && create) {
      page = new Object[PAGE_SIZE];
      var existing = (Object[]) ARRAY_ELEMENT.compareAndExchange(values, pageIndex, null, page);
      if (existing != null) page = existing;
    }
    return page;
  }

  private Object readValueState(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, true);
    int slot = ordinal & (PAGE_SIZE - 1);
    var value = ARRAY_ELEMENT.getVolatile(page, slot);
    if (value == null) {
      V decoded = serializer.deserialize(valueBuffer(valueOffset, endOffset));
      value = decoded == null ? NULL : decoded;
      // Concurrent misses may decode twice; all readers use the published winner.
      var existing = ARRAY_ELEMENT.compareAndExchange(page, slot, null, value);
      if (existing != null) value = existing;
    }
    return value;
  }

  final V readValue(int ordinal, int valueOffset, int endOffset) {
    var value = readValueState(ordinal, valueOffset, endOffset);
    return value == NULL ? null : cast(value);
  }

  final boolean hasValue(int ordinal, int valueOffset, int endOffset) {
    var page = valuePage(ordinal, false);
    var value = page == null ? null : ARRAY_ELEMENT.getVolatile(page, ordinal & (PAGE_SIZE - 1));
    return value == null
        ? !serializer.isNullSerialized(valueBuffer(valueOffset, endOffset))
        : value != NULL;
  }

  private ByteBuffer valueBuffer(int valueOffset, int endOffset) {
    return source.duplicate().position(valueOffset).limit(endOffset);
  }

  final InternalIndexElement<V> element(IndexKey key, int ordinal, int valueOffset, int endOffset) {
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
            ? IndexReadPrototype.this.hasValue(ordinal, valueOffset, endOffset)
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
        return IndexReadPrototype.this.valueBuffer(valueOffset, endOffset);
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

  /** A private forward cursor; key bytes are overwritten only on its next advance. */
  final class Scan {
    final ByteBuffer bytes;
    byte[] keyBytes = new byte[0];
    int keyLength;
    int ordinal = -1;
    int valueOffset;
    int endOffset;
    private boolean anchorPending;
    private IndexKey key;

    Scan() {
      bytes = source.duplicate().position(bodyOffset);
    }

    Scan(int ordinal, byte[] keyBytes, int valueOffset, int endOffset) {
      bytes = source.duplicate().position(endOffset);
      this.ordinal = ordinal - 1;
      this.keyBytes = keyBytes;
      keyLength = keyBytes.length;
      this.valueOffset = valueOffset;
      this.endOffset = endOffset;
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
      int keyOffset = bytes.position();
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
      return IndexReadPrototype.this.element(key(), ordinal, valueOffset, endOffset);
    }
  }
}

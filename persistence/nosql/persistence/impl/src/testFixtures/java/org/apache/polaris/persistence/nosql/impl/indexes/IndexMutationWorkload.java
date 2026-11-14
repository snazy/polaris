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
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/** Fixture model of embedded cleanup and sorted physical-stripe spill, without persistence I/O. */
final class IndexMutationWorkload {
  enum Variant {
    BASELINE,
    V2,
    V2_REUSE
  }

  private record Source(ByteBuffer v1, ByteBuffer v2, IndexKey first, IndexKey last) {}

  record Change(IndexKey key, ObjRef value) {}

  record EmbeddedView(
      IndexSpi<ObjRef> reference, IndexSpi<ObjRef> embedded, long observed, int added) {}

  record SpillView(IndexSpi<ObjRef> stripe, long observed) {}

  private final V2IndexWriter.Options options;
  private final Source reference;
  private final Source embedded;
  private final Source pending;
  final List<Change> changes;
  final int pendingBytes;

  IndexMutationWorkload(
      int entries,
      String shape,
      int embeddedEntries,
      int mutations,
      String locality,
      String operation,
      V2IndexWriter.Options options) {
    checkArgument(embeddedEntries > 0 && embeddedEntries < entries && mutations > 0);
    checkArgument(locality.equals("CLUSTERED") || locality.equals("SCATTERED"));
    this.options = options;
    var data = PrototypeIndexData.create(entries, shape);
    var original = IndexImpl.deserializeStoreIndex(data.serialized(), OBJ_REF_SERIALIZER);
    // Physical stripes do not retain removal markers after a spill.
    for (int i = 0; i < data.keys().size(); i++) {
      var key = data.keys().get(i);
      if (!original.contains(key)) original.put(key, ObjRef.objRef("value", i + 100L, 1 + i % 3));
    }
    reference = source(original);
    var live = new ArrayList<IndexKey>();
    for (var key : data.keys()) if (original.contains(key)) live.add(key);
    checkArgument(mutations < live.size() / 2);
    var updates = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < embeddedEntries; i++) {
      if (i % 17 == 0) {
        // Unnecessary marker: production's no-spill cleanup must discard it.
        updates.add(indexElement(IndexKey.key("~unreferenced-%06d".formatted(i)), null));
      } else {
        var key = live.get(i * live.size() / embeddedEntries);
        updates.add(indexElement(key, i % 19 == 0 ? null : original.get(key)));
      }
    }
    embedded = source(updates);
    var batch = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var selected = new ArrayList<Change>(mutations);
    for (int i = 0; i < mutations; i++) {
      int ordinal =
          locality.equals("CLUSTERED")
              ? live.size() / 2 + i
              : (int) ((long) (i + 1) * live.size() / (mutations + 1));
      var key = live.get(ordinal);
      var previous = requireNonNull(original.get(key));
      ObjRef value = ObjRef.objRef("other", previous.id(), previous.numParts());
      String op =
          operation.equals("MIXED")
              ? switch (i % 3) {
                case 0 -> "REMOVE";
                case 1 -> "UPDATE";
                default -> "INSERT";
              }
              : operation;
      switch (op) {
        case "UPDATE" -> {}
        case "GROW" ->
            value =
                ObjRef.objRef(
                    "substantially-longer-value-label", previous.id(), previous.numParts());
        case "REMOVE" -> value = null;
        case "INSERT" -> {
          var raw = key.asByteBuffer();
          key =
              IndexKey.key(
                  ByteBuffer.allocate(raw.remaining() + 1).put(raw).put((byte) 127).flip());
          checkArgument(!original.containsElement(key), "Insertion key already exists");
        }
        default -> throw new IllegalArgumentException(operation);
      }
      selected.add(new Change(key, value));
      batch.add(indexElement(key, value));
    }
    changes = List.copyOf(selected);
    pending = source(batch);
    pendingBytes = pending.v1.remaining();
  }

  private Source source(IndexSpi<ObjRef> index) {
    var v1 =
        FullKeyCheckpoints.rewrite(
            index.serialize(),
            OBJ_REF_SERIALIZER,
            FullKeyCheckpoints.Policy.NONE,
            options.blockEntries());
    return new Source(
        v1, V2IndexWriter.write(index, OBJ_REF_SERIALIZER, options), index.first(), index.last());
  }

  private IndexSpi<ObjRef> open(Variant variant, Source source) {
    return switch (variant) {
      case BASELINE -> IndexImpl.deserializeStoreIndex(source.v1.duplicate(), OBJ_REF_SERIALIZER);
      case V2 -> new V2IndexImpl<>(source.v2, OBJ_REF_SERIALIZER, options);
      case V2_REUSE -> new V2BlockReuseIndex<>(source.v2, OBJ_REF_SERIALIZER, options);
    };
  }

  private IndexSpi<ObjRef> read(Variant variant, Source source) {
    return variant == Variant.BASELINE
        ? IndexImpl.deserializeStoreIndex(source.v1.duplicate(), OBJ_REF_SERIALIZER)
        : new V2IndexReader<>(source.v2, OBJ_REF_SERIALIZER);
  }

  private IndexSpi<ObjRef> empty(Variant variant) {
    return switch (variant) {
      case BASELINE -> new IndexImpl<>(OBJ_REF_SERIALIZER);
      case V2 -> new V2IndexImpl<>(OBJ_REF_SERIALIZER, options);
      case V2_REUSE -> new V2BlockReuseIndex<>(OBJ_REF_SERIALIZER, options);
    };
  }

  IndexSpi<ObjRef> initialReference(Variant variant) {
    return new LazyIndexImpl<>(() -> read(variant, reference), reference.first, reference.last);
  }

  IndexSpi<ObjRef> initialEmbedded(Variant variant) {
    return open(variant, embedded);
  }

  IndexSpi<ObjRef> initialPending() {
    return open(Variant.BASELINE, pending);
  }

  EmbeddedView updateEmbedded(Variant variant) {
    var ref = initialReference(variant);
    var update = initialEmbedded(variant);
    var logical = new ReadOnlyLayeredIndexImpl<>(ref, update);
    long observed = 0;
    int added = 0;
    for (var change : changes) {
      var previous = logical.get(change.key);
      if (previous != null) observed += previous.id();
      if (change.value == null) {
        var existing = update.getElement(change.key);
        if (existing != null && !existing.hasValue()) continue;
        if (ref.containsElement(change.key)) update.add(indexElement(change.key, null));
        else update.remove(change.key);
      } else {
        var result = update.add(indexElement(change.key, change.value));
        if (result == IndexSpi.AddResult.UPDATED_NO_VALUE
            || (result == IndexSpi.AddResult.NEW_KEY && !ref.containsElement(change.key))) added++;
      }
    }
    return new EmbeddedView(ref, update, observed, added);
  }

  ByteBuffer serializeEmbedded(EmbeddedView view, Variant variant, boolean rebuild) {
    var result = rebuild ? empty(variant) : view.embedded;
    var removals = new ArrayList<IndexKey>();
    for (var iterator = view.embedded.elementIterator(); iterator.hasNext(); ) {
      var element = iterator.next();
      if (element.valueNullable() == null && !view.reference.contains(element.key())) {
        if (!rebuild) removals.add(element.key());
      } else if (rebuild) result.add(element);
    }
    // Preserve iteration semantics by removing unnecessary markers after the traversal.
    for (var key : removals) result.remove(key);
    result.estimatedSerializedSize();
    return result.serialize();
  }

  SpillView spill(Variant variant, int preReads) {
    var stripe = open(variant, reference);
    long observed = 0;
    for (int i = 0; i < preReads; i++) {
      var value = stripe.get(changes.get(i % changes.size()).key);
      if (value != null) observed += value.id();
    }
    var batch = read(variant, pending);
    for (var iterator = batch.elementIterator(); iterator.hasNext(); ) {
      var element = iterator.next();
      if (element.valueNullable() == null) stripe.remove(element.key());
      else stripe.add(element);
    }
    return new SpillView(stripe, observed);
  }
}

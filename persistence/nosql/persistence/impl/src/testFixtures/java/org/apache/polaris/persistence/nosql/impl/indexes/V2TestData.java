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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

/** Deterministic V1 source data for SplitV2 contract tests. */
record V2TestData(ByteBuffer serialized, List<IndexKey> keys) {
  static V2TestData create(int entries, String shape) {
    var ordered = new TreeSet<IndexKey>();
    var random = new Random(42);
    var words = shape.equals("WORDS") ? KeyIndexTestSet.Words.words() : List.<String>of();
    if (shape.equals("WORDS") && entries > words.size()) {
      throw new IllegalArgumentException("Too many word keys");
    }
    for (int i = 0; ordered.size() < entries; i++) {
      ordered.add(
          switch (shape) {
            case "HIERARCHICAL" ->
                IndexKey.key("catalog/namespace-%03d/table-%06d".formatted(i / 100, i));
            case "WORDS" -> IndexKey.key(words.get((int) ((long) i * words.size() / entries)));
            case "UUID" -> IndexKey.key(new UUID(random.nextLong(), random.nextLong()).toString());
            case "BINARY" -> {
              byte[] bytes = new byte[1 + random.nextInt(60)];
              random.nextBytes(bytes);
              yield IndexKey.key(ByteBuffer.wrap(bytes));
            }
            default -> throw new IllegalArgumentException(shape);
          });
    }
    var keys = new ArrayList<>(ordered);
    var index = new IndexImpl<>(OBJ_REF_SERIALIZER);
    for (int i = 0; i < keys.size(); i++) {
      index.add(
          indexElement(keys.get(i), i % 17 == 0 ? null : objRef("value", i + 100L, 1 + i % 3)));
    }
    return new V2TestData(index.serialize().asReadOnlyBuffer(), List.copyOf(keys));
  }

  IndexSpi<ObjRef> mutable() {
    return IndexesInternal.deserializeStoreIndex(serialized, OBJ_REF_SERIALIZER, true, 1);
  }
}

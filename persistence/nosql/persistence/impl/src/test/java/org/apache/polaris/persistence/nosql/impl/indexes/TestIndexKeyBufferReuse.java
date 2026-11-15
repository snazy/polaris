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

import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIndexKeyBufferReuse {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "COMPACT",
        "COMPACT3",
        "V2",
        "FRONT_ENDS",
        "FRONT_LENGTHS",
        "PREFIX_ENDS",
        "FRONT_PREFIX_ENDS"
      })
  void decodedKeysSurviveScratchReuse(String variant) {
    var data = PrototypeIndexData.create(129, "HIERARCHICAL");
    var expected = data.fresh("BASELINE", 64);
    IndexSpi<ObjRef> index;
    if (variant.startsWith("FRONT") || variant.equals("PREFIX_ENDS")) {
      var options =
          new SplitV2Format.Options(
              SplitV2Format.Layout.valueOf(variant), 64, V2IndexWriter.DirectoryEncoding.FULL);
      index =
          new SplitV2IndexReader<>(
              SplitV2IndexWriter.write(expected, OBJ_REF_SERIALIZER, options), OBJ_REF_SERIALIZER);
    } else index = data.encodedFor(variant, 64).fresh(variant, 64);
    var keys = expected.asKeyList();
    var first = index.getElement(keys.get(31));
    assertThat(first).isNotNull();
    var savedKey = first.key();
    for (int i = keys.size() - 1; i >= 0; i--) {
      var scratch = IndexKeyBufferPool.acquire();
      try {
        Arrays.fill(
            scratch.array(),
            scratch.arrayOffset(),
            scratch.arrayOffset() + scratch.capacity(),
            (byte) 0x7f);
      } finally {
        IndexKeyBufferPool.release(scratch);
      }
      assertThat(requireNonNull(index.getElement(keys.get(i))).key()).isEqualTo(keys.get(i));
      assertThat(index.get(keys.get(i))).isEqualTo(expected.get(keys.get(i)));
      assertThat(first.key()).isEqualTo(keys.get(31)).isSameAs(savedKey);
    }
    assertThat(index.asKeyList()).containsExactlyElementsOf(keys);
  }

  @ParameterizedTest
  @EnumSource(SplitV2Format.Layout.class)
  void failedMergeReleasesScratch(SplitV2Format.Layout layout) {
    var data = PrototypeIndexData.create(129, "HIERARCHICAL");
    var options = new SplitV2Format.Options(layout, 64, V2IndexWriter.DirectoryEncoding.FULL);
    var index =
        new SplitV2IndexReader<>(
            SplitV2IndexWriter.write(data.fresh("BASELINE", 64), OBJ_REF_SERIALIZER, options),
            OBJ_REF_SERIALIZER);
    var keys = data.keys();
    var scratch = IndexKeyBufferPool.acquire();
    IndexKeyBufferPool.release(scratch);
    assertThatThrownBy(() ->
            SplitV2IndexWriter.merge(
                index,
                List.of(
                    new SplitV2IndexWriter.Change<>(keys.get(90), null),
                    new SplitV2IndexWriter.Change<>(keys.get(10), null)),
                options))
        .isInstanceOf(IllegalArgumentException.class);
    var reacquired = IndexKeyBufferPool.acquire();
    try {
      assertThat(reacquired).isSameAs(scratch);
    } finally {
      IndexKeyBufferPool.release(reacquired);
    }
  }

  @Test
  void failedBlockReadReleasesScratch() {
    var data = PrototypeIndexData.create(129, "HIERARCHICAL").encodedFor("V2", 64);
    var codec =
        new IndexValueSerializer<ObjRef>() {
          @Override
          public int serializedSize(@Nullable ObjRef value) {
            return OBJ_REF_SERIALIZER.serializedSize(value);
          }

          @Override
          public @NonNull ByteBuffer serialize(@Nullable ObjRef value, @NonNull ByteBuffer target) {
            return OBJ_REF_SERIALIZER.serialize(value, target);
          }

          @Override
          public @Nullable ObjRef deserialize(@NonNull ByteBuffer source) {
            return OBJ_REF_SERIALIZER.deserialize(source);
          }

          @Override
          public boolean isNullSerialized(@NonNull ByteBuffer source) {
            return OBJ_REF_SERIALIZER.isNullSerialized(source);
          }

          @Override
          public void skip(@NonNull ByteBuffer source) {
            throw new IllegalStateException("test skip failure");
          }
        };
    var index = new V2IndexReader<>(data.serialized(), codec);
    var scratch = IndexKeyBufferPool.acquire();
    IndexKeyBufferPool.release(scratch);
    assertThatThrownBy(() -> index.get(data.keys().get(31)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("test skip failure");
    var reacquired = IndexKeyBufferPool.acquire();
    try {
      assertThat(reacquired).isSameAs(scratch);
    } finally {
      IndexKeyBufferPool.release(reacquired);
    }
  }
}

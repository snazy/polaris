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
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.varint.VarInt.readVarInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class TestFullKeyCheckpoints {
  static Stream<Arguments> cases() {
    return Stream.of(FullKeyCheckpoints.Policy.values())
        .flatMap(policy ->
            Stream.of(1, 16, 64)
                .flatMap(interval ->
                    Stream.of(0, 1, 33, 257)
                        .flatMap(count ->
                            Stream.of("HIERARCHICAL", "UUID", "BINARY")
                                .map(shape -> arguments(policy, interval, count, shape)))));
  }

  @ParameterizedTest
  @EnumSource(FullKeyCheckpoints.Policy.class)
  void longEscapedKeysExerciseMultiByteFullStripLengths(FullKeyCheckpoints.Policy policy) {
    var source = new IndexImpl<>(OBJ_REF_SERIALIZER);
    var keys = new ArrayList<IndexKey>();
    for (int i = 0; i < 200; i++) {
      var key = IndexKey.key("漢字\u0000".repeat(70) + "%03d".formatted(i));
      keys.add(key);
      source.add(indexElement(key, i % 17 == 0 ? null : ObjRef.objRef("long-key", i + 1L)));
    }
    var original = source.serialize();
    var encoded = FullKeyCheckpoints.rewrite(original, OBJ_REF_SERIALIZER, policy, 16);
    var oldReader = IndexImpl.deserializeStoreIndex(encoded.duplicate(), OBJ_REF_SERIALIZER);
    var offset = new OffsetCheckpointIndexImpl<>(encoded, OBJ_REF_SERIALIZER);
    for (var key : keys) {
      assertThat(oldReader.get(key)).isEqualTo(source.get(key));
      assertThat(offset.get(key)).isEqualTo(source.get(key));
    }
    if (policy == FullKeyCheckpoints.Policy.NONE) assertThat(encoded).isEqualTo(original);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void v1AnchorsPreserveKeysValuesBoundsAndRollback(
      FullKeyCheckpoints.Policy policy, int interval, int count, String shape) {
    var data = PrototypeIndexData.create(count, shape);
    var encoded =
        FullKeyCheckpoints.rewrite(data.serialized(), OBJ_REF_SERIALIZER, policy, interval);
    if (policy == FullKeyCheckpoints.Policy.NONE) assertThat(encoded).isEqualTo(data.serialized());
    var anchors = FullKeyCheckpoints.anchors(count, policy, interval);
    var scan = encoded.duplicate();
    assertThat(scan.get()).isEqualTo((byte) 1);
    assertThat(readVarInt(scan)).isEqualTo(count);
    int previousLength = 0;
    int compressedRun = 0;
    for (int i = 0; i < count; i++) {
      int strip = i == 0 ? 0 : readVarInt(scan);
      int prefix = previousLength - strip;
      if (anchors.get(i)) assertThat(prefix).isZero();
      compressedRun = prefix == 0 ? 0 : compressedRun + 1;
      if (policy != FullKeyCheckpoints.Policy.NONE)
        assertThat(compressedRun).isLessThanOrEqualTo(interval);
      int start = scan.position();
      IndexKey.skip(scan);
      previousLength = prefix + scan.position() - start;
      OBJ_REF_SERIALIZER.skip(scan);
    }
    assertThat(scan.hasRemaining()).isFalse();
    var original = data.fresh("BASELINE", interval);
    // The unchanged production reader is the old-version/rollback reader for the new bytes.
    var oldReader = IndexImpl.deserializeStoreIndex(encoded.duplicate(), OBJ_REF_SERIALIZER);
    var newReader = new OffsetCheckpointIndexImpl<>(encoded, OBJ_REF_SERIALIZER);
    for (var key : data.keys()) {
      assertThat(oldReader.get(key)).isEqualTo(original.get(key));
      assertThat(newReader.get(key)).isEqualTo(original.get(key));
    }
    var extra = IndexKey.key("new-after-rollback");
    var value = ObjRef.objRef("rollback", 123456L);
    oldReader.put(extra, value);
    var rollbackBytes = oldReader.serialize();
    assertThat(new OffsetCheckpointIndexImpl<>(rollbackBytes, OBJ_REF_SERIALIZER).get(extra))
        .isEqualTo(value);
    for (String kind : List.of("SLICE", "READ_ONLY", "DIRECT")) {
      var padded =
          kind.equals("DIRECT")
              ? ByteBuffer.allocateDirect(encoded.remaining() + 8)
              : ByteBuffer.allocate(encoded.remaining() + 8);
      padded.position(3).put(encoded.duplicate()).flip().position(3);
      if (kind.equals("SLICE")) padded = padded.slice();
      if (kind.equals("READ_ONLY")) padded = padded.asReadOnlyBuffer();
      int position = padded.position();
      var rewritten = FullKeyCheckpoints.rewrite(padded, OBJ_REF_SERIALIZER, policy, interval);
      assertThat(padded.position()).isEqualTo(position);
      var rewrittenReader =
          IndexImpl.deserializeStoreIndex(rewritten.duplicate(), OBJ_REF_SERIALIZER);
      for (var key : data.keys()) assertThat(rewrittenReader.get(key)).isEqualTo(original.get(key));
    }
  }
}

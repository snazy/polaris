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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestSplitV2Workload {
  static Stream<Arguments> cases() {
    return Stream.of("HIERARCHICAL", "WORDS")
        .flatMap(shape ->
            Stream.of("SINGLE_PART", "MIXED_PARTS")
                .flatMap(values ->
                    Stream.of("CLUSTERED", "SCATTERED")
                        .flatMap(locality ->
                            Stream.of("UPDATE", "MIXED", "GROW")
                                .flatMap(operation ->
                                    Stream.of(V2IndexWriter.DirectoryEncoding.values())
                                        .map(directory ->
                                            arguments(
                                                shape, values, locality, operation, directory))))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void sixImplementationsHaveEqualReadAndSpillResults(
      String shape,
      String values,
      String locality,
      String operation,
      V2IndexWriter.DirectoryEncoding directory) {
    var data = new SplitV2Workload(513, shape, values, 32, locality, operation, 16, directory);
    for (int from = 0; from < data.queries.length; from += 16)
      assertThat(new HashSet<>(Arrays.asList(Arrays.copyOfRange(data.queries, from, from + 16))))
          .hasSize(16);
    var expected = data.read(SplitV2Workload.Variant.BASELINE);
    for (var change : data.changes) {
      if (change.element() == null) expected.remove(change.key());
      else expected.add(change.element());
    }
    Long observed = null;
    for (var variant : SplitV2Workload.Variant.values()) {
      for (boolean split : new boolean[] {false, true}) {
        var outcome = data.spill(variant, 4, split, 2000);
        if (observed == null) observed = outcome.observed();
        assertThat(outcome.observed()).isEqualTo(observed);
        var keys = new ArrayList<IndexKey>();
        for (var bytes : outcome.parts()) {
          IndexSpi<ObjRef> read =
              variant.layout == null
                  ? V2IndexReader.read(bytes, OBJ_REF_SERIALIZER)
                  : new SplitV2IndexReader<>(bytes, OBJ_REF_SERIALIZER);
          keys.addAll(read.asKeyList());
          if (split && read.asKeyList().size() > 1)
            assertThat(bytes.remaining()).isLessThanOrEqualTo(2000);
          for (var key : read.asKeyList()) assertThat(read.get(key)).isEqualTo(expected.get(key));
        }
        assertThat(keys).containsExactlyElementsOf(expected.asKeyList());
      }
      assertThat(data.read(variant).asKeyList()).containsExactlyElementsOf(data.keys);
      assertThat(data.serializedSize(variant)).isPositive();
    }
  }
}

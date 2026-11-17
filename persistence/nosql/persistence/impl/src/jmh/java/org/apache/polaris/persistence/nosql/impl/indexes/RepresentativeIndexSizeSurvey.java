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

import static com.google.common.base.Preconditions.checkState;

import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.databind.json.JsonMapper;

/** Exact byte attribution, without timing or allocation claims. */
public final class RepresentativeIndexSizeSurvey {
  private RepresentativeIndexSizeSurvey() {}

  public static void main(String[] args) {
    int entries = args.length == 0 ? 4096 : Integer.parseInt(args[0]);
    String[] variants =
        (args.length < 2 ? "PREFIX_ENDS,FRONT_ENDS,FRONT_PREFIX_ENDS" : args[1]).split(",");
    var mapper = JsonMapper.builder().build();
    for (var family : RepresentativeKeyDataset.Family.values()) {
      for (long seed : RepresentativeKeyDataset.BENCHMARK_SEEDS) {
        for (String variant : variants) {
          var workload =
              RepresentativeIndexWorkload.create(
                  family,
                  seed,
                  RepresentativeIndexWorkload.Variant.valueOf(variant),
                  entries,
                  0,
                  RepresentativeIndexWorkload.History.FRESH,
                  RepresentativeIndexWorkload.Partitioning.NATIVE);
          System.out.println(mapper.writeValueAsString(measure(workload)));
        }
      }
    }
  }

  private static <V> Map<String, Object> measure(RepresentativeIndexWorkload<V> workload) {
    var reader = (SplitV2IndexReader<V>) workload.fresh();
    int fileHeader = reader.blockCount == 0 ? reader.directoryOffset : reader.blockStart(0);
    int directoryHeader = 6;
    int descriptors = 12 * reader.blockCount;
    int directoryKeys =
        reader.source.limit() - reader.directoryOffset - directoryHeader - descriptors;
    int headers = 0, keyEnds = 0, keyPayload = 0, valueLocators = 0, values = 0;
    int strideBlocks = 0, prefixBlocks = 0;
    for (int i = 0; i < reader.blockCount; i++) {
      var block = reader.block(i);
      headers += block.keyTable() - block.start();
      keyEnds += block.keyPayload() - block.keyTable();
      keyPayload += block.keyBytes();
      valueLocators += block.values() - block.valueMeta();
      values += block.end() - block.values();
      if (block.mode() == SplitV2Format.STRIDE) strideBlocks++;
      if (block.prefix() > 0) prefixBlocks++;
    }
    int total =
        fileHeader
            + directoryHeader
            + descriptors
            + directoryKeys
            + headers
            + keyEnds
            + keyPayload
            + valueLocators
            + values;
    checkState(total == workload.input.remaining(), "Byte attribution mismatch");
    var row = new LinkedHashMap<String, Object>();
    row.put("family", workload.dataset.config().family().name());
    row.put("seed", workload.dataset.config().seed());
    row.put("variant", workload.variant.name());
    row.put("entries", workload.keys.size());
    row.put("blocks", reader.blockCount);
    row.put("sourceBytes", total);
    row.put("fileHeaderBytes", fileHeader);
    row.put("directoryHeaderBytes", directoryHeader);
    row.put("descriptorBytes", descriptors);
    row.put("directoryKeyBytes", directoryKeys);
    row.put("blockHeaderBytes", headers);
    row.put("keyEndBytes", keyEnds);
    row.put("keyPayloadBytes", keyPayload);
    row.put("valueLocatorBytes", valueLocators);
    row.put("valuePayloadBytes", values);
    row.put("fixedStrideBlocks", strideBlocks);
    row.put("commonPrefixBlocks", prefixBlocks);
    return row;
  }
}

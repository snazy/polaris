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

import java.nio.ByteBuffer;

/** Untimed serialized-size survey. This entry point never invokes the JMH harness. */
public final class SplitV2SizeSurvey {
  private SplitV2SizeSurvey() {}

  public static void main(String[] args) {
    System.out.println(
        "entries,shape,valueShape,blockEntries,directory,keyRestart,variant,sourceBytes,mergedBytes,splitBytes,splitParts,blocks,fixedStrideBlocks");
    for (String shape : new String[] {"HIERARCHICAL", "WORDS", "UUID", "BINARY"})
      for (String values : new String[] {"SINGLE_PART", "MIXED_PARTS"})
        for (int blockEntries : new int[] {16, 64})
          for (var directory : V2IndexWriter.DirectoryEncoding.values())
            for (int keyRestart : new int[] {8, 16, 32}) {
              var data =
                  new SplitV2Workload(
                      4096,
                      shape,
                      values,
                      256,
                      "SCATTERED",
                      "MIXED",
                      blockEntries,
                      directory,
                      keyRestart);
              for (var variant : SplitV2Workload.Variant.values()) {
                var merged = data.spill(variant, 4, false, 65536);
                var split = data.spill(variant, 4, true, 65536);
                int mergedBytes = merged.parts().stream().mapToInt(ByteBuffer::remaining).sum();
                int splitBytes = split.parts().stream().mapToInt(ByteBuffer::remaining).sum();
                int blocks = 0, fixed = 0;
                var view = data.read(variant);
                if (view instanceof SplitV2IndexReader<?> index) {
                  blocks = index.blockCount;
                  for (int i = 0; i < blocks; i++)
                    if (index.block(i).mode() == SplitV2Format.STRIDE) fixed++;
                } else if (view instanceof V2IndexReader<?> index) blocks = index.blockCount;
                System.out.printf(
                    "4096,%s,%s,%d,%s,%d,%s,%d,%d,%d,%d,%d,%d%n",
                    shape,
                    values,
                    blockEntries,
                    directory,
                    keyRestart,
                    variant,
                    data.serializedSize(variant),
                    mergedBytes,
                    splitBytes,
                    split.parts().size(),
                    blocks,
                    fixed);
              }
            }
  }
}

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

import static org.apache.polaris.persistence.nosql.impl.indexes.ReadOnlyIndexJCStressSupport.KEYS;
import static org.apache.polaris.persistence.nosql.impl.indexes.ReadOnlyIndexJCStressSupport.valid;

import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZZ_Result;

/**
 * Racing slots in two pages and compressed-directory blocks must preserve every published winner.
 * Payload fields are deliberately non-final; page and slot CAS/volatile access must publish them.
 * Both actors race the null sentinel too. Concurrent mutation is outside this contract.
 */
@JCStressTest
@Outcome(
    id = "true, true, true",
    expect = Expect.ACCEPTABLE,
    desc = "Complete payloads and stable winners across page, block and directory publication.")
@Outcome(
    expect = Expect.FORBIDDEN,
    desc = "Partial payload, lost page/slot, wrong key or null publication.")
@State
public class ReadOnlyV2IndexPagesPublicationStress {
  private final AtomicInteger decodes = new AtomicInteger();
  private final IndexSpi<ReadOnlyIndexJCStressSupport.Value> index =
      ReadOnlyIndexJCStressSupport.readV2(decodes);
  private ReadOnlyIndexJCStressSupport.Value a, b, c, d;

  @Actor
  public void first(ZZZ_Result r) {
    a = index.get(KEYS[0]);
    b = index.get(KEYS[64]);
    r.r1 =
        valid(a, 0)
            && valid(b, 64)
            && index.get(KEYS[63]) == null
            && index.asKeyList().get(64).equals(KEYS[64]);
  }

  @Actor
  public void second(ZZZ_Result r) {
    c = index.get(KEYS[62]);
    d = index.get(KEYS[65]);
    r.r2 =
        valid(c, 62)
            && valid(d, 65)
            && index.get(KEYS[63]) == null
            && index.asKeyList().get(62).equals(KEYS[62]);
  }

  @Arbiter
  public void check(ZZZ_Result r) {
    int before = decodes.get();
    r.r3 =
        a == index.get(KEYS[0])
            && b == index.get(KEYS[64])
            && c == index.get(KEYS[62])
            && d == index.get(KEYS[65])
            && index.get(KEYS[63]) == null
            && !index.contains(KEYS[63])
            && before >= 5
            && before <= 10
            && decodes.get() == before;
  }
}

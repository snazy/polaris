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
 * Both actors install slots in both memo pages. Page CAS must select one published page so a late
 * page installation cannot discard another actor's slots; volatile slot access publishes payloads.
 */
@JCStressTest
@Outcome(
    id = "true, true, true",
    expect = Expect.ACCEPTABLE,
    desc = "All slots retain their complete canonical values across page-install races.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Lost page/slot or partial value publication.")
@State
public class ReadOnlyIndexPagesPublicationStress {
  private final IndexSpi<ReadOnlyIndexJCStressSupport.Value> index =
      ReadOnlyIndexJCStressSupport.read(new AtomicInteger());
  private ReadOnlyIndexJCStressSupport.Value a;
  private ReadOnlyIndexJCStressSupport.Value b;
  private ReadOnlyIndexJCStressSupport.Value c;
  private ReadOnlyIndexJCStressSupport.Value d;

  @Actor
  public void actor1(ZZZ_Result r) {
    a = index.get(KEYS[0]);
    b = index.get(KEYS[64]);
    r.r1 = valid(a, 0) && valid(b, 64);
  }

  @Actor
  public void actor2(ZZZ_Result r) {
    c = index.get(KEYS[62]);
    d = index.get(KEYS[65]);
    r.r2 = valid(c, 62) && valid(d, 65);
  }

  @Arbiter
  public void arbiter(ZZZ_Result r) {
    r.r3 =
        a == index.get(KEYS[0])
            && b == index.get(KEYS[64])
            && c == index.get(KEYS[62])
            && d == index.get(KEYS[65]);
  }
}

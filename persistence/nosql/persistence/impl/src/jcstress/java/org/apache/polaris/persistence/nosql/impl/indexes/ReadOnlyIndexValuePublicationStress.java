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
 * Memo page/slot CAS and volatile reads must publish complete key/value state. Duplicate decoding
 * is allowed, but both callers must return the published winner, including its non-final fields.
 */
@JCStressTest
@Outcome(
    id = "true, true, true",
    expect = Expect.ACCEPTABLE,
    desc = "Complete payload and key publication with a canonical value winner.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Partial publication or a noncanonical decoded value.")
@State
public class ReadOnlyIndexValuePublicationStress {
  private final IndexSpi<ReadOnlyIndexJCStressSupport.Value> index =
      ReadOnlyIndexJCStressSupport.read(new AtomicInteger());
  private ReadOnlyIndexJCStressSupport.Value first;
  private ReadOnlyIndexJCStressSupport.Value second;

  @Actor
  public void actor1(ZZZ_Result r) {
    first = index.get(KEYS[64]);
    r.r1 = valid(first, 64);
  }

  @Actor
  public void actor2(ZZZ_Result r) {
    var element = index.getElement(KEYS[64]);
    second = element.valueNullable();
    r.r2 = valid(second, 64) && element.key().equals(KEYS[64]);
  }

  @Arbiter
  public void arbiter(ZZZ_Result r) {
    r.r3 = first != null && first == second && first == index.get(KEYS[64]);
  }
}

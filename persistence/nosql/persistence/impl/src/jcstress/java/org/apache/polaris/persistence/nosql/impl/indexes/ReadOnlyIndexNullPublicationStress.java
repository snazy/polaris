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

import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZZ_Result;

/**
 * The null sentinel is published through the same page/slot CAS as non-null values. It must remain
 * distinct from an unread slot; two racing initial decodes are allowed, later decodes are not.
 */
@JCStressTest
@Outcome(
    id = "true, true, true",
    expect = Expect.ACCEPTABLE,
    desc = "Concurrent readers see null and subsequent probes reuse the published null sentinel.")
@Outcome(
    expect = Expect.FORBIDDEN,
    desc = "Wrong null state or repeated decoding after publication.")
@State
public class ReadOnlyIndexNullPublicationStress {
  private final AtomicInteger decodes = new AtomicInteger();
  private final IndexSpi<ReadOnlyIndexJCStressSupport.Value> index =
      ReadOnlyIndexJCStressSupport.read(decodes);

  @Actor
  public void actor1(ZZZ_Result r) {
    r.r1 = index.get(KEYS[63]) == null;
  }

  @Actor
  public void actor2(ZZZ_Result r) {
    r.r2 = index.getElement(KEYS[63]).valueNullable() == null;
  }

  @Arbiter
  public void arbiter(ZZZ_Result r) {
    var before = decodes.get();
    var consistent =
        index.get(KEYS[63]) == null
            && !index.contains(KEYS[63])
            && !index.getElement(KEYS[63]).hasValue();
    r.r3 = consistent && before >= 1 && before <= 2 && decodes.get() == before;
  }
}

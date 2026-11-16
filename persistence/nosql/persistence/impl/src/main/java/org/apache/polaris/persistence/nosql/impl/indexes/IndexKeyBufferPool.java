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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Bounded, striped scratch buffers for operation-scoped index-key reconstruction. */
final class IndexKeyBufferPool {
  private IndexKeyBufferPool() {}

  /**
   * Used to drastically reduce the amount of 4k {@link ByteBuffer} allocations during key
   * serialization/deserialization operations, which are very frequent.
   *
   * <p>Uses a striped {@link AtomicReferenceArray} indexed by thread ID to avoid contention. Each
   * thread maps to a slot via {@code threadId % POOL_SIZE}. Acquire uses {@link
   * AtomicReferenceArray#getAndSet} (single atomic op) and release uses {@link
   * AtomicReferenceArray#lazySet} (store-release, essentially free on x86). In the common case of
   * no slot collisions, this achieves zero-allocation buffer reuse with minimal overhead.
   *
   * <p>Works efficiently with both platform and virtual threads: the pool size is bounded and does
   * not grow with the number of virtual threads. Slot collisions simply cause a fresh allocation.
   *
   * <p>Callers must use {@link #acquire()} and {@link #release(ByteBuffer)} in a try/finally
   * pattern.
   *
   * <p>JMH results for the different approaches are as follows. Invoked via {@code java -jar
   * persistence/nosql/persistence/impl/build/libs/polaris-persistence-nosql-impl-1.1.0-incubating-SNAPSHOT-jmh.jar
   * RealisticKeyIndexImplBench -p namespaceLevels=3 -p foldersPerLevel=5 -p tablesPerNamespace=5
   * -prof gc -prof perf}.
   *
   * <table>
   *     <tr>
   *         <th>Benchmark</th>
   *         <th><code>Deque&lt;ByteBuffer&gt;</code></th>
   *         <th><code>ThreadLocal&lt;ByteBuffer&gt;</code></th>
   *         <th><code>Long2ObjectHashMap</code></th>
   *         <th><code>AtomicReferenceArray</code></th>
   *         <th>unit</th>
   *     </tr>
   *     <tr>
   *         <td>deserializeAdd</td>
   *         <td>90</td>
   *         <td>22</td>
   *         <td>25</td>
   *         <td>22</td>
   *         <td>µs/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>deserializeAdd</td>
   *         <td>500k</td>
   *         <td>100k</td>
   *         <td>110k</td>
   *         <td>111k</td>
   *         <td>bytes/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>deserializeAdd</td>
   *         <td>1.2</td>
   *         <td>4.5</td>
   *         <td>4.2</td>
   *         <td>3.8</td>
   *         <td>insn/clk (higher is better)</td>
   *     </tr>
   *     <tr>
   *         <td>deserializeAddSerialize</td>
   *         <td>100</td>
   *         <td>45</td>
   *         <td>50</td>
   *         <td>50</td>
   *         <td>µs/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>deserializeAddSerialize</td>
   *         <td>530k</td>
   *         <td>130k</td>
   *         <td>130k</td>
   *         <td>135k</td>
   *         <td>bytes/op (lower is better)</td>
   *     </tr>
   *     <tr>
   *         <td>deserializeAddSerialize</td>
   *         <td>2.3</td>
   *         <td>5.0</td>
   *         <td>4.7</td>
   *         <td>4.4</td>
   *         <td>insn/clk (higher is better)</td>
   *     </tr>
   * </table>
   *
   * <p>The {@link java.util.ArrayDeque}-based approach is the slowest due to CAS contention and
   * per-operation node allocations. {@link ThreadLocal} is the fastest, but adds a permanent GC
   * root that is never cleaned up. The previous {@code Long2ObjectHashMap} approach (keyed by
   * thread ID) performed well but accumulated entries from dead threads, causing the cache to
   * degrade over time &mdash; especially with virtual threads, where short-lived threads would fill
   * the map and the eviction policy preferentially retained dead threads' entries.
   *
   * <p>The {@link AtomicReferenceArray} approach matches the {@code Long2ObjectHashMap} performance
   * while avoiding the dead thread accumulation problem entirely: the pool has a fixed number of
   * slots, and slot collisions simply cause a fresh allocation with no bookkeeping overhead.
   */
  @VisibleForTesting static final int POOL_SIZE = Runtime.getRuntime().availableProcessors() * 4;

  private static final AtomicReferenceArray<ByteBuffer> SCRATCH_POOL =
      new AtomicReferenceArray<>(POOL_SIZE);

  static ByteBuffer acquire() {
    var slot = (int) (Thread.currentThread().threadId() % POOL_SIZE);
    var buf = SCRATCH_POOL.getAndSet(slot, null);
    if (buf != null) {
      buf.clear();
      return buf;
    }
    return ByteBuffer.allocate(IndexImpl.MAX_KEY_BYTES);
  }

  static void release(ByteBuffer buf) {
    var slot = (int) (Thread.currentThread().threadId() % POOL_SIZE);
    SCRATCH_POOL.lazySet(slot, buf);
  }

  @VisibleForTesting
  static int size() {
    var count = 0;
    for (var i = 0; i < SCRATCH_POOL.length(); i++) {
      if (SCRATCH_POOL.get(i) != null) {
        count++;
      }
    }
    return count;
  }

  @VisibleForTesting
  static void clear() {
    for (var i = 0; i < SCRATCH_POOL.length(); i++) {
      SCRATCH_POOL.set(i, null);
    }
  }
}

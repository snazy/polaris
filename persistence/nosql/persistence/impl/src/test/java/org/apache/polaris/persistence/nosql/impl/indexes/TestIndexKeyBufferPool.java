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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class TestIndexKeyBufferPool {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void scratchPoolAcquireRelease() {
    IndexKeyBufferPool.clear();
    var buf = IndexKeyBufferPool.acquire();
    soft.assertThat(buf).isNotNull();
    soft.assertThat(buf.position()).isEqualTo(0);

    // Write some data, then release
    buf.put((byte) 42);
    buf.limit(1);
    IndexKeyBufferPool.release(buf);
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);

    // Re-acquire from the same thread should return the same buffer, cleared
    var buf2 = IndexKeyBufferPool.acquire();
    try {
      soft.assertThat(buf2).isSameAs(buf);
      soft.assertThat(buf2.position()).isEqualTo(0);
      soft.assertThat(buf2.limit()).isEqualTo(IndexImpl.MAX_KEY_BYTES);
      soft.assertThat(buf2.arrayOffset()).isZero();
    } finally {
      IndexKeyBufferPool.release(buf2);
    }
  }

  @Test
  void scratchPoolNestedAcquire() {
    IndexKeyBufferPool.clear();

    // First acquire takes the buffer from the slot
    var outer = IndexKeyBufferPool.acquire();
    // Second acquire from the same thread hits the same (now empty) slot, must allocate fresh
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(0);
    var inner = IndexKeyBufferPool.acquire();

    soft.assertThat(inner).isNotSameAs(outer);

    // Both buffers are independently usable
    outer.put((byte) 1);
    inner.put((byte) 2);
    soft.assertThat(outer.get(0)).isEqualTo((byte) 1);
    soft.assertThat(inner.get(0)).isEqualTo((byte) 2);

    // Release inner first, then outer — outer wins the slot (last-write-wins)
    IndexKeyBufferPool.release(inner);
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);
    IndexKeyBufferPool.release(outer);
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);

    // Re-acquire should get the outer buffer (last released to this slot)
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);
    var reacquired = IndexKeyBufferPool.acquire();
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(0);
    soft.assertThat(reacquired).isSameAs(outer);
    IndexKeyBufferPool.release(reacquired);
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);
    IndexKeyBufferPool.release(outer);
    soft.assertThat(IndexKeyBufferPool.size()).isEqualTo(1);
  }

  @Test
  void scratchPoolConcurrentAccess() throws Exception {
    IndexKeyBufferPool.clear();

    var threads = Runtime.getRuntime().availableProcessors() * 2;
    var iterationsPerThread = 1000;
    var latch = new CountDownLatch(threads);
    var start = new Semaphore(0);

    try (var executor = Executors.newFixedThreadPool(threads)) {
      var futures =
          IntStream.range(0, threads)
              .mapToObj(i ->
                  CompletableFuture.runAsync(
                      () -> {
                        latch.countDown();
                        try {
                          start.acquire();
                        } catch (InterruptedException e) {
                          throw new RuntimeException(e);
                        }
                        for (var iter = 0; iter < iterationsPerThread; iter++) {
                          var buf = IndexKeyBufferPool.acquire();
                          // Write a marker to verify no buffer is shared concurrently
                          var marker = (byte) (Thread.currentThread().hashCode() & 0xFF);
                          buf.put(0, marker);
                          // Simulate work
                          for (var j = 1; j < 16; j++) {
                            buf.put(j, marker);
                          }
                          // Verify our marker is still intact (no other thread overwrote it)
                          for (var j = 0; j < 16; j++) {
                            if (buf.get(j) != marker) {
                              throw new AssertionError(
                                  "Buffer was concurrently modified at position " + j);
                            }
                          }
                          IndexKeyBufferPool.release(buf);
                        }
                      },
                      executor))
              .toArray(CompletableFuture[]::new);

      latch.await();
      start.release(threads);

      CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
    }

    // Pool should still be bounded
    soft.assertThat(IndexKeyBufferPool.size()).isLessThanOrEqualTo(IndexKeyBufferPool.POOL_SIZE);
  }
}

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

package org.apache.polaris.storage.files.impl;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.PrimitiveSink;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.polaris.storage.files.api.FileOperations;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.ImmutablePurgeStats;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.ms.ImmutableMarkResult;
import org.apache.polaris.storage.files.api.ms.ImmutableSweepResult;
import org.apache.polaris.storage.files.api.ms.MarkAndSweep;
import org.apache.polaris.storage.files.api.ms.MarkAndSweepSpec;
import org.apache.polaris.storage.files.api.ms.MarkResult;
import org.apache.polaris.storage.files.api.ms.SweepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

record MarkAndSweepImpl(@Nonnull Clock clock) implements MarkAndSweep {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarkAndSweepImpl.class);

  @Override
  public MarkPhase startMarkPhase(MarkAndSweepSpec spec) {
    return new MarkPhaseImpl(spec);
  }

  @SuppressWarnings("UnstableApiUsage")
  private class MarkPhaseImpl implements MarkPhase {
    private final MarkAndSweepSpec spec;
    private final Instant startTime;
    private volatile MarkResult result;
    private final AtomicLong identifiedFiles = new AtomicLong();

    /**
     * Access to this field is guarded using `synchronized` to allow concurrent access to allow
     * marking referenced files concurrently.
     */
    private final BloomFilter<FileSpec> filesFilter;

    MarkPhaseImpl(MarkAndSweepSpec spec) {
      this.spec = spec;
      this.startTime = clock.instant();
      this.filesFilter =
          BloomFilter.create(
              this::locationFunnel,
              spec.expectedFileCount().orElse(MarkAndSweepSpec.DEFAULT_EXPECTED_FILE_COUNT),
              spec.filterInitializedFpp().orElse(MarkAndSweepSpec.DEFAULT_INITIALIZED_FPP));
    }

    private void locationFunnel(FileSpec fileSpec, PrimitiveSink sink) {
      sink.putString(fileSpec.location(), StandardCharsets.UTF_8);
    }

    @Override
    public void mark(Stream<FileSpec> files) {
      checkState(result == null, "Mark phase already finished");
      var iter = files.iterator();
      var cnt = 0;
      while (iter.hasNext()) {
        var fileSpec = iter.next();
        synchronized (filesFilter) {
          // Synchronize here, as `Iterator.hasNext()`/`.next()` may perform expensive operations
          // (I/O).
          // The synchronized only needs to protect the filesFilter property.
          filesFilter.put(fileSpec);
        }
        cnt++;
      }
      identifiedFiles.addAndGet(cnt);
    }

    @Override
    public MarkResult finish() {
      checkState(result == null, "Mark phase already finished");
      synchronized (filesFilter) {
        var expectedFpp = filesFilter.expectedFpp();
        var identified = identifiedFiles.get();
        var maxAcceptableFpp =
            spec.maxAcceptableFilterFpp().orElse(MarkAndSweepSpec.DEFAULT_MAX_ACCEPTABLE_FPP);
        var withinFppConstraint = expectedFpp <= maxAcceptableFpp;

        var info =
            format(
                "Bloom filter reports expected-FPP of %f, max-acceptable FPP is set to %f.",
                expectedFpp, maxAcceptableFpp);

        (withinFppConstraint ? LOGGER.atInfo() : LOGGER.atWarn())
            .log("Mark phase finished with {} files identified. %s", identified, info);
        result =
            ImmutableMarkResult.builder()
                .identifiedFiles(identified)
                .finishedTime(clock.instant())
                .startTime(startTime)
                .canSweep(withinFppConstraint)
                .information(info)
                .build();
        return result;
      }
    }

    @Override
    public SweepPhase startSweepPhase(PurgeSpec purgeSpec) throws MarkPhaseOverflowException {
      checkState(result != null, "Mark phase not finished");
      if (!result.canSweep()) {
        throw new MarkPhaseOverflowException(
            format(
                "Sweep not possible, mark-and-sweep constraints exceeded. Identified %d files. %s",
                result.identifiedFiles(), result.information()));
      }

      var graceTimeMillis =
          spec.createdAtGraceTime()
              .orElse(MarkAndSweepSpec.DEFAULT_CREATED_AT_GRACE_TIME)
              .toMillis();
      graceTimeMillis = Math.max(Duration.ofMinutes(5).toMillis(), graceTimeMillis);

      synchronized (filesFilter) {
        return new SweepPhaseImpl(
            filesFilter, result.startTime().toEpochMilli() - graceTimeMillis, purgeSpec);
      }
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  private class SweepPhaseImpl implements SweepPhase {
    private final PurgeSpec purgeSpec;
    private final long maxCreateAtMillis;
    private final Instant startTime;
    private final AtomicLong foundFiles = new AtomicLong();
    private final AtomicLong filteredFiles = new AtomicLong();
    private final AtomicLong purgeFileRequests = new AtomicLong();
    private final AtomicLong failedFiles = new AtomicLong();
    private final Predicate<FileSpec> bloomFilterPredicate;
    private SweepResult result;

    SweepPhaseImpl(BloomFilter<FileSpec> filesFilter, long maxCreateAtMillis, PurgeSpec purgeSpec) {
      this.bloomFilterPredicate = filesFilter.negate();
      this.startTime = clock.instant();
      this.maxCreateAtMillis = maxCreateAtMillis;
      this.purgeSpec = purgeSpec;
    }

    @Override
    public void foundFiles(Stream<FileSpec> files, FileOperations fromOperations) {
      checkState(result == null, "Sweep phase already finished");
      var purgeResult =
          fromOperations.purge(
              files
                  .peek(ignore -> foundFiles.incrementAndGet())
                  .filter(
                      fileSpec ->
                          fileSpec.createdAtMillis().orElse(Long.MAX_VALUE) < maxCreateAtMillis)
                  .filter(bloomFilterPredicate)
                  .peek(ignore -> filteredFiles.incrementAndGet()),
              purgeSpec);
      purgeFileRequests.addAndGet(purgeResult.purgeFileRequests());
      failedFiles.addAndGet(purgeResult.failedFilePurges());
    }

    @Override
    public SweepResult finish() {
      checkState(result == null, "Sweep phase already finished");
      var finishedTime = clock.instant();
      result =
          ImmutableSweepResult.builder()
              .startTime(startTime)
              .finishedTime(finishedTime)
              .foundFiles(foundFiles.get())
              .filteredFiles(filteredFiles.get())
              .purgeStats(
                  ImmutablePurgeStats.builder()
                      .purgeFileRequests(purgeFileRequests.get())
                      .failedFilePurges(failedFiles.get())
                      .duration(Duration.between(startTime, finishedTime))
                      .build())
              .build();
      return result;
    }
  }
}

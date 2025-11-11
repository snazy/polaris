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

import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.apache.polaris.storage.files.api.ms.MarkAndSweep;
import org.apache.polaris.storage.files.api.ms.MarkAndSweepSpec;
import org.apache.polaris.storage.files.api.ms.MarkResult;
import org.apache.polaris.storage.files.api.ms.SweepResult;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMarkAndSweepImpl {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void nothingMarkedNothingSwept() {
    var markAndSweep = new MarkAndSweepImpl(Clock.systemUTC());

    var markPhase = markAndSweep.startMarkPhase(MarkAndSweepSpec.builder().build());

    var markResult = markPhase.finish();
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> markPhase.mark(Stream.of()))
        .withMessage("Mark phase already finished");
    soft.assertThat(markResult)
        .extracting(MarkResult::canSweep, MarkResult::identifiedFiles)
        .containsExactly(true, 0L);
    soft.assertThat(markResult.startTime()).isBefore(markResult.finishedTime());

    var sweepPhase = markPhase.startSweepPhase(PurgeSpec.DEFAULT_INSTANCE);
    var fileOps = new FileOperationsImpl(mock(SupportsBulkOperations.class));
    sweepPhase.foundFiles(Stream.of(), fileOps);

    var sweepResult = sweepPhase.finish();
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> sweepPhase.foundFiles(Stream.of(), fileOps))
        .withMessage("Sweep phase already finished");

    soft.assertThat(sweepResult)
        .extracting(SweepResult::foundFiles, SweepResult::filteredFiles)
        .containsExactly(0L, 0L);
    soft.assertThat(sweepResult.purgeStats())
        .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
        .containsExactly(0L, 0L);
    soft.assertThat(sweepResult.startTime()).isBefore(sweepResult.finishedTime());
  }

  @Test
  public void nothingMarkedAllSwept() {
    var markAndSweep = new MarkAndSweepImpl(Clock.systemUTC());

    var markPhase = markAndSweep.startMarkPhase(MarkAndSweepSpec.builder().build());
    var markResult = markPhase.finish();

    soft.assertThat(markResult)
        .extracting(MarkResult::canSweep, MarkResult::identifiedFiles)
        .containsExactly(true, 0L);
    soft.assertThat(markResult.startTime()).isBefore(markResult.finishedTime());
    var sweepPhase = markPhase.startSweepPhase(PurgeSpec.DEFAULT_INSTANCE);
    var fileOps = new FileOperationsImpl(mock(SupportsBulkOperations.class));

    sweepPhase.foundFiles(
        Stream.concat(
            IntStream.range(0, 13).mapToObj(i -> dummyFileSpecOld("old/" + i)),
            IntStream.range(0, 13).mapToObj(i -> dummyFileSpecTooNew("new/" + i))),
        fileOps);

    var sweepResult = sweepPhase.finish();

    soft.assertThat(sweepResult)
        .extracting(SweepResult::foundFiles, SweepResult::filteredFiles)
        .containsExactly(26L, 13L);
    soft.assertThat(sweepResult.purgeStats())
        .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
        .containsExactly(13L, 0L);
  }

  @Test
  public void someMarkedAllSwept() {
    var markAndSweep = new MarkAndSweepImpl(Clock.systemUTC());

    var markPhase = markAndSweep.startMarkPhase(MarkAndSweepSpec.builder().build());
    markPhase.mark(IntStream.range(0, 13).mapToObj(i -> dummyFileSpecOld("identified/" + i)));
    var markResult = markPhase.finish();

    soft.assertThat(markResult)
        .extracting(MarkResult::canSweep, MarkResult::identifiedFiles)
        .containsExactly(true, 13L);
    soft.assertThat(markResult.startTime()).isBefore(markResult.finishedTime());
    var sweepPhase = markPhase.startSweepPhase(PurgeSpec.DEFAULT_INSTANCE);
    var fileOps = new FileOperationsImpl(mock(SupportsBulkOperations.class));

    sweepPhase.foundFiles(
        Stream.concat(
            IntStream.range(0, 13).mapToObj(i -> dummyFileSpecOld("identified/" + i)),
            Stream.concat(
                IntStream.range(0, 13).mapToObj(i -> dummyFileSpecOld("old/" + i)),
                IntStream.range(0, 13).mapToObj(i -> dummyFileSpecTooNew("new/" + i)))),
        fileOps);

    var sweepResult = sweepPhase.finish();

    soft.assertThat(sweepResult)
        .extracting(SweepResult::foundFiles, SweepResult::filteredFiles)
        .containsExactly(39L, 13L);
    soft.assertThat(sweepResult.purgeStats())
        .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
        .containsExactly(13L, 0L);
  }

  @Test
  public void fppConstraintPreventsSweepPhaseFromFileCount() {
    var markAndSweep = new MarkAndSweepImpl(Clock.systemUTC());

    var markPhase =
        markAndSweep.startMarkPhase(MarkAndSweepSpec.builder().expectedFileCount(1L).build());
    markPhase.mark(IntStream.range(0, 1_000).mapToObj(i -> dummyFileSpecOld("identified/" + i)));
    var markResult = markPhase.finish();

    soft.assertThat(markResult.canSweep()).isFalse();
    soft.assertThatThrownBy(() -> markPhase.startSweepPhase(PurgeSpec.DEFAULT_INSTANCE))
        .isInstanceOf(MarkAndSweep.MarkPhaseOverflowException.class)
        .hasMessage(
            "Sweep not possible, mark-and-sweep constraints exceeded. Identified 1000 files. Bloom filter reports expected-FPP of 1.000000, max-acceptable FPP is set to 0.000050.");
  }

  @Test
  public void fppConstraintPreventsSweepPhaseFromFPP() {
    var markAndSweep = new MarkAndSweepImpl(Clock.systemUTC());

    var markPhase =
        markAndSweep.startMarkPhase(
            MarkAndSweepSpec.builder().maxAcceptableFilterFpp(1E-100d).build());
    markPhase.mark(IntStream.range(0, 100_000).mapToObj(i -> dummyFileSpecOld("identified/" + i)));
    var markResult = markPhase.finish();

    soft.assertThat(markResult.canSweep()).isFalse();
    soft.assertThatThrownBy(() -> markPhase.startSweepPhase(PurgeSpec.DEFAULT_INSTANCE))
        .isInstanceOf(MarkAndSweep.MarkPhaseOverflowException.class)
        .hasMessage(
            "Sweep not possible, mark-and-sweep constraints exceeded. Identified 100000 files. Bloom filter reports expected-FPP of 0.000000, max-acceptable FPP is set to 0.000000.");
  }

  static FileSpec dummyFileSpecOld(String path) {
    return FileSpec.fromLocation(path).createdAtMillis(1L).build();
  }

  static FileSpec dummyFileSpecTooNew(String path) {
    return FileSpec.fromLocation(path).createdAtMillis(Long.MAX_VALUE).build();
  }
}

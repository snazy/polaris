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

package org.apache.polaris.storage.files.api.ms;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableMarkAndSweepSpec.class)
@JsonDeserialize(as = ImmutableMarkAndSweepSpec.class)
public interface MarkAndSweepSpec {
  default MarkAndSweepSpec applyMarkResult(MarkResult result) {
    var builder = builder().from(this);
    builder.expectedFileCount(
        (long)
            (result.identifiedFiles()
                * countFromLastRunMultiplier().orElse(DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER)));
    return builder.build();
  }

  long DEFAULT_EXPECTED_FILE_COUNT = 1_000_000L;

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalLong expectedFileCount();

  double DEFAULT_COUNT_FROM_LAST_RUN_MULTIPLIER = 1.1;

  /**
   * The sizes the bloom-filters used to hold the identified files according to the expression
   * {@code lastRun.numberOfIdentified * countFromLastRunMultiplier}. The default is to add 10% to
   * the number of identified items.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble countFromLastRunMultiplier();

  double DEFAULT_INITIALIZED_FPP = 0.00001;

  /** False-positive-probability (FPP) used to initialize the bloom-filters for identified files. */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble filterInitializedFpp();

  double DEFAULT_MAX_ACCEPTABLE_FPP = 0.00005;

  /**
   * Expected maximum false-positive-probability (FPP) used to check the bloom-filters for
   * identified files.
   *
   * <p>If the FPP of a bloom filter exceeds this value, no individual references or objects will be
   * purged.
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  OptionalDouble maxAcceptableFilterFpp();

  String DEFAULT_CREATED_AT_GRACE_TIME_STRING = "PT3H";
  Duration DEFAULT_CREATED_AT_GRACE_TIME = Duration.parse(DEFAULT_CREATED_AT_GRACE_TIME_STRING);

  /**
   * Files that have been created <em>after</em> a maintenance run has started are never purged.
   * This option defines an additional grace time to when the maintenance run has started.
   *
   * <p>This value is a safety net for two reasons:
   *
   * <ul>
   *   <li>Respect the wall-clock drift between Polaris nodes and the object store.
   *   <li>Respect the order of writes. Files are written <em>before</em> those become reachable via
   *       a table-metadata commit. Commits may take a little time (milliseconds, up to a few
   *       seconds, depending on the system load) to complete. Therefore, implementations enforce a
   *       minimum of 5 minutes.
   * </ul>
   */
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> createdAtGraceTime();

  static ImmutableMarkAndSweepSpec.Builder builder() {
    return ImmutableMarkAndSweepSpec.builder();
  }

  @Value.Check
  default void check() {
    expectedFileCount().ifPresent(v -> checkState(v > 0, "expectedFileCount must be positive"));
    countFromLastRunMultiplier()
        .ifPresent(v -> checkState(v > 1d, "countFromLastRunMultiplier must be greater than 1.0d"));
    filterInitializedFpp()
        .ifPresent(
            v -> checkState(v > 0d && v <= 1d, "filterInitializedFpp must be > 0.0d and <= 1.0d"));
    maxAcceptableFilterFpp()
        .ifPresent(
            v ->
                checkState(v > 0d && v <= 1d, "maxAcceptableFilterFpp must be > 0.0d and <= 1.0d"));
    createdAtGraceTime()
        .ifPresent(v -> checkState(!v.isNegative(), "createdAtGraceTime must not be negative"));
  }
}

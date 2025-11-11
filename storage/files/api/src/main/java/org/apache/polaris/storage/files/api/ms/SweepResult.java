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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;

@PolarisImmutable
@JsonSerialize(as = ImmutableSweepResult.class)
@JsonDeserialize(as = ImmutableSweepResult.class)
public interface SweepResult {
  PurgeStats purgeStats();

  /** The total number of found files. */
  long foundFiles();

  /**
   * Number of files that are eligible to be purged. The value includes files that are ignored by
   * {@link PurgeSpec#fileFilter()}.
   */
  long filteredFiles();

  Instant startTime();

  Instant finishedTime();
}

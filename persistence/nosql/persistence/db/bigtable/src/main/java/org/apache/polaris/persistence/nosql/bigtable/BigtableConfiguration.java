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
package org.apache.polaris.persistence.nosql.bigtable;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

/** Polaris persistence, Bigtable backend specific configuration. */
@ConfigMapping(prefix = "polaris.backend.bigtable")
@StaticInitSafe
public interface BigtableConfiguration {

  Optional<String> projectId();

  Optional<String> tablePrefix();

  /** Total timeout (including retries) for Bigtable API calls. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> totalApiTimeout();

  @WithDefault("false")
  Optional<Boolean> noTableAdminClient();

  /** Sets the instance-id to be used with Google BigTable. */
  @WithDefault("polaris")
  Optional<String> instanceId();

  /** Sets the profile-id to be used with Google BigTable. */
  Optional<String> appProfileId();

  /** Google BigTable quota project ID (optional). */
  Optional<String> quotaProjectId();

  /** Google BigTable endpoint (if not default). */
  Optional<String> endpoint();

  /** Google BigTable MTLS endpoint (if not default). */
  Optional<String> mtlsEndpoint();

  /** When using the BigTable emulator, used to configure the host. */
  Optional<String> emulatorHost();

  /** When using the BigTable emulator, used to configure the port. */
  @WithDefault("8086")
  OptionalInt emulatorPort();

  /** Initial retry delay. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> initialRetryDelay();

  /** Max retry-delay. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> maxRetryDelay();

  OptionalDouble retryDelayMultiplier();

  /** Maximum number of attempts for each Bigtable API call (including retries). */
  OptionalInt maxAttempts();

  /** Initial RPC timeout. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> initialRpcTimeout();

  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> maxRpcTimeout();

  OptionalDouble rpcTimeoutMultiplier();

  /** Total timeout (including retries) for Bigtable API calls. */
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> totalTimeout();

  /** Minimum number of gRPC channels. Refer to Google docs for details. */
  OptionalInt minChannelCount();

  /** Maximum number of gRPC channels. Refer to Google docs for details. */
  OptionalInt maxChannelCount();

  /** Initial number of gRPC channels. Refer to Google docs for details */
  OptionalInt initialChannelCount();

  /** Minimum number of RPCs per channel. Refer to Google docs for details. */
  OptionalInt minRpcsPerChannel();

  /** Maximum number of RPCs per channel. Refer to Google docs for details. */
  OptionalInt maxRpcsPerChannel();
}

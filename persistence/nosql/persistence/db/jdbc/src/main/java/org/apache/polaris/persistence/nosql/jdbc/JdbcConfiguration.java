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
package org.apache.polaris.persistence.nosql.jdbc;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;

/** Polaris persistence, JDBC backend specific configuration. */
@ConfigMapping(prefix = "polaris.backend.jdbc")
@StaticInitSafe
public interface JdbcConfiguration {
  String url();

  Optional<String> username();

  Optional<String> password();

  int DEFAULT_INITIAL_POOL_SIZE = 2;

  @WithDefault("" + DEFAULT_INITIAL_POOL_SIZE)
  OptionalInt initialPoolSize();

  int DEFAULT_MIN_POOL_SIZE = 2;

  @WithDefault("" + DEFAULT_MIN_POOL_SIZE)
  OptionalInt minPoolSize();

  int DEFAULT_MAX_POOL_SIZE = 5;

  @WithDefault("" + DEFAULT_MAX_POOL_SIZE)
  OptionalInt maxPoolSize();

  String DEFAULT_MAX_LIFETIME_STRING = "PT5M";
  Duration DEFAULT_MAX_LIFETIME = Duration.parse(DEFAULT_MAX_LIFETIME_STRING);

  @WithDefault("PT5M")
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> maxLifetime();

  String DEFAULT_ACQUISITION_TIMEOUT_STRING = "PT20S";
  Duration DEFAULT_ACQUISITION_TIMEOUT = Duration.parse(DEFAULT_ACQUISITION_TIMEOUT_STRING);

  @WithDefault(DEFAULT_ACQUISITION_TIMEOUT_STRING)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Optional<Duration> acquisitionTimeout();

  String DEFAULT_ISOLATION_STRING = "READ_COMMITTED";
  JdbcIsolation DEFAULT_ISOLATION = JdbcIsolation.valueOf(DEFAULT_ISOLATION_STRING);

  @WithDefault(DEFAULT_ISOLATION_STRING)
  Optional<JdbcIsolation> transactionIsolation();
}

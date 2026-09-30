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

import static org.assertj.core.util.Preconditions.checkState;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;

public abstract class BaseJdbcBackendTestFactory implements BackendTestFactory {
  public abstract String jdbcUrl();

  public abstract String jdbcUser();

  public abstract String jdbcPass();

  public JdbcIsolation jdbcIsolation() {
    return JdbcIsolation.READ_COMMITTED;
  }

  @Override
  public Backend createNewBackend() throws SQLException {
    checkState(jdbcUrl() != null, "Must set JDBC URL first");

    var factory = new JdbcBackendFactory();
    var config =
        factory.buildConfiguration(
            new JdbcConfiguration() {
              @Override
              public String url() {
                return jdbcUrl();
              }

              @Override
              public Optional<String> username() {
                return Optional.ofNullable(jdbcUser());
              }

              @Override
              public Optional<String> password() {
                return Optional.ofNullable(jdbcPass());
              }

              @Override
              public OptionalInt initialPoolSize() {
                return OptionalInt.empty();
              }

              @Override
              public OptionalInt minPoolSize() {
                return OptionalInt.empty();
              }

              @Override
              public OptionalInt maxPoolSize() {
                return OptionalInt.empty();
              }

              @Override
              public Optional<Duration> maxLifetime() {
                return Optional.empty();
              }

              @Override
              public Optional<Duration> acquisitionTimeout() {
                return Optional.empty();
              }

              @Override
              public Optional<JdbcIsolation> transactionIsolation() {
                return Optional.of(jdbcIsolation());
              }
            });

    return factory.buildBackend(config);
  }

  @Override
  public void start() throws Exception {}

  @Override
  public void stop() throws Exception {}
}

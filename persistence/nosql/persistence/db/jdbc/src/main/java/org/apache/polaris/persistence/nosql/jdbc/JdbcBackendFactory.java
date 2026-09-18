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

import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_ACQUISITION_TIMEOUT;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_INITIAL_POOL_SIZE;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_ISOLATION;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_MAX_LIFETIME;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_MAX_POOL_SIZE;
import static org.apache.polaris.persistence.nosql.jdbc.JdbcConfiguration.DEFAULT_MIN_POOL_SIZE;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import jakarta.annotation.Nonnull;
import java.sql.SQLException;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;

public class JdbcBackendFactory implements BackendFactory<JdbcBackendConfig, JdbcConfiguration> {
  public static final String NAME = "JDBC";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull JdbcBackendConfig backendConfig) {
    DatabaseSpecific databaseSpecific = DatabaseSpecifics.detect(backendConfig.dataSource());
    return new JdbcBackend(backendConfig, databaseSpecific);
  }

  @Override
  public Class<JdbcConfiguration> configurationInterface() {
    return JdbcConfiguration.class;
  }

  @Override
  public JdbcBackendConfig buildConfiguration(JdbcConfiguration config) {
    var dataSourceConfiguration = new AgroalDataSourceConfigurationSupplier();
    var poolConfiguration = dataSourceConfiguration.connectionPoolConfiguration();
    var connectionFactoryConfiguration = poolConfiguration.connectionFactoryConfiguration();

    // configure pool
    poolConfiguration
        .initialSize(config.initialPoolSize().orElse(DEFAULT_INITIAL_POOL_SIZE))
        .maxSize(config.maxPoolSize().orElse(DEFAULT_MAX_POOL_SIZE))
        .minSize(config.minPoolSize().orElse(DEFAULT_MIN_POOL_SIZE))
        .maxLifetime(config.maxLifetime().orElse(DEFAULT_MAX_LIFETIME))
        .acquisitionTimeout(config.acquisitionTimeout().orElse(DEFAULT_ACQUISITION_TIMEOUT));

    // configure supplier
    connectionFactoryConfiguration.jdbcUrl(config.url());

    config
        .username()
        .ifPresent(
            u -> {
              connectionFactoryConfiguration.credential(new NamePrincipal(u));
              connectionFactoryConfiguration.credential(
                  new SimplePassword(
                      config
                          .password()
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "Must specify JDBC password if username is provided"))));
            });

    connectionFactoryConfiguration.jdbcTransactionIsolation(
        AgroalConnectionFactoryConfiguration.TransactionIsolation.valueOf(
            config.transactionIsolation().orElse(DEFAULT_ISOLATION).name()));
    connectionFactoryConfiguration.autoCommit(false);

    AgroalDataSource dataSource;
    try {
      dataSource = AgroalDataSource.from(dataSourceConfiguration.get());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return new JdbcBackendConfig(dataSource, true);
  }
}

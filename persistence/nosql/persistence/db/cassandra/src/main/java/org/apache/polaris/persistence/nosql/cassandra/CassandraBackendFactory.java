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
package org.apache.polaris.persistence.nosql.cassandra;

import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_CONNECT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONTROL_CONNECTION_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.HEARTBEAT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_LOG_WARNINGS;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_TIMEOUT;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import jakarta.annotation.Nonnull;
import java.net.InetSocketAddress;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;

public class CassandraBackendFactory
    implements BackendFactory<CassandraBackendConfig, CassandraConfiguration> {
  public static final String NAME = "Cassandra";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull CassandraBackendConfig backendConfig) {
    return new CassandraBackend(backendConfig);
  }

  @Override
  public Class<CassandraConfiguration> configurationInterface() {
    return CassandraConfiguration.class;
  }

  @Override
  public CassandraBackendConfig buildConfiguration(CassandraConfiguration config) {
    var clientBuilder = CqlSession.builder();

    for (String endpoint : config.endpoints()) {
      var lastColon = endpoint.lastIndexOf(':');
      if (lastColon > 0) {
        try {
          var specificPort = Integer.parseInt(endpoint.substring(lastColon + 1));
          var host = endpoint.substring(0, lastColon);
          clientBuilder.addContactPoint(InetSocketAddress.createUnresolved(host, specificPort));
          continue;
        } catch (NumberFormatException nfe) {
          // ignore
        }
      }
      clientBuilder.addContactPoint(InetSocketAddress.createUnresolved(endpoint, config.port()));
    }

    config.localDc().ifPresent(clientBuilder::withLocalDatacenter);

    config
        .username()
        .ifPresent(
            u ->
                clientBuilder.withAuthCredentials(
                    u,
                    config
                        .password()
                        .orElseThrow(
                            () ->
                                new IllegalArgumentException(
                                    "Must specify C* password if username is provided"))));

    var options = OptionsMap.driverDefaults();

    options.put(CONNECTION_CONNECT_TIMEOUT, config.connectTimeout());
    options.put(CONTROL_CONNECTION_TIMEOUT, config.connectTimeout());
    options.put(CONNECTION_INIT_QUERY_TIMEOUT, config.dmlTimeout());
    options.put(CONNECTION_SET_KEYSPACE_TIMEOUT, config.dmlTimeout());
    options.put(REQUEST_TIMEOUT, config.dmlTimeout());
    options.put(HEARTBEAT_TIMEOUT, config.dmlTimeout());
    options.put(METADATA_SCHEMA_REQUEST_TIMEOUT, config.ddlTimeout());

    // Disable warnings due to tombstone_warn_threshold
    options.put(REQUEST_LOG_WARNINGS, false);

    var client = clientBuilder.withConfigLoader(DriverConfigLoader.fromMap(options)).build();

    return new CassandraBackendConfig(
        client, config.keyspace(), config.ddlTimeout(), config.dmlTimeout(), true);
  }
}

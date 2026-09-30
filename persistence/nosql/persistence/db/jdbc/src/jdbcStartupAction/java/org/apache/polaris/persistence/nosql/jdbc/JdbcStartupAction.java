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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import java.util.Map;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class JdbcStartupAction implements PolarisServerStartupAction {
  private PostgreSQLContainer container;

  @Override
  public void start(PolarisServerStartupContext context) {
    var image =
        containerSpecHelper("postgres", PostgreSqlBackendTestFactory.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("postgres");
    container = new PostgreSQLContainer(image);
    container.start();
    context
        .getSystemProperties()
        .putAll(
            Map.ofEntries(
                Map.entry("polaris.persistence.type", "nosql"),
                Map.entry("polaris.persistence.auto-bootstrap-types", "nosql"),
                Map.entry("polaris.persistence.nosql.backend", "JDBC"),
                Map.entry("polaris.backend.jdbc.datasource", "polaris-nosql"),
                Map.entry("quarkus.datasource.polaris-nosql.db-kind", "postgresql"),
                Map.entry("quarkus.datasource.polaris-nosql.jdbc.url", container.getJdbcUrl()),
                Map.entry("quarkus.datasource.polaris-nosql.username", container.getUsername()),
                Map.entry("quarkus.datasource.polaris-nosql.password", container.getPassword())));
  }

  @Override
  public void close() {
    if (container != null) {
      try {
        container.stop();
      } finally {
        container = null;
      }
    }
  }
}

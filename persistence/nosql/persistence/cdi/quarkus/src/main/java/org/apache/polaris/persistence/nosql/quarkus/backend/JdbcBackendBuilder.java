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
package org.apache.polaris.persistence.nosql.quarkus.backend;

import io.quarkus.arc.All;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.datasource.runtime.DataSourceBuildTimeConfig;
import io.quarkus.datasource.runtime.DataSourcesBuildTimeConfig;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.List;
import javax.sql.DataSource;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.jdbc.JdbcBackendConfig;
import org.apache.polaris.persistence.nosql.jdbc.JdbcBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@BackendType(JdbcBackendFactory.NAME)
@Dependent
class JdbcBackendBuilder implements BackendBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackendBuilder.class);

  @Inject DataSourcesBuildTimeConfig dataSourcesConfig;

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<InstanceHandle<DataSource>> dataSources;

  @Inject QuarkusJdbcConfig config;

  @PostConstruct
  void checkDataSourcesConfiguration() {
    var dataSourceName = dataSourceName();
    var dataSourceConfig = dataSourcesConfig.dataSources().get(dataSourceName);
    if (dataSourceConfig == null) {
      throw new IllegalStateException("No datasource configured with name: " + dataSourceName);
    }
    checkDatabaseKind(dataSourceName, dataSourceConfig);
  }

  @Override
  public Backend buildBackend() {
    var dataSource = selectDataSource();
    return new JdbcBackendFactory().buildBackend(new JdbcBackendConfig(dataSource, true));
  }

  public static String unquoteDataSourceName(String dataSourceName) {
    if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
      dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
    }
    return dataSourceName;
  }

  private void checkDatabaseKind(String dataSourceName, DataSourceBuildTimeConfig config) {
    if (config.dbKind().isEmpty()) {
      throw new IllegalArgumentException(
          "Database kind not configured for datasource " + dataSourceName);
    }
    var databaseKind = config.dbKind().get();
    if (!DatabaseKind.isPostgreSQL(databaseKind)
        && !DatabaseKind.isH2(databaseKind)
        && !DatabaseKind.isMariaDB(databaseKind)) {
      throw new IllegalArgumentException(
          "Database kind for datasource "
              + dataSourceName
              + " is configured to '"
              + databaseKind
              + "', which Apache Polaris does not support yet; "
              + "currently PostgreSQL, H2, MariaDB (and MySQL via MariaDB driver) are supported. "
              + "Feel free to raise a pull request to support your database of choice.");
    }
  }

  private DataSource selectDataSource() {
    var dataSourceName = dataSourceName();
    var dataSource = findDataSourceByName(dataSourceName);
    LOGGER.info("Selected datasource: {}", dataSourceName);
    return dataSource;
  }

  private String dataSourceName() {
    return config
        .datasourceName()
        .map(JdbcBackendBuilder::unquoteDataSourceName)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Mandatory configuration option 'polaris.backend.jdbc.datasource' is missing"));
  }

  private DataSource findDataSourceByName(String dataSourceName) {
    for (var handle : dataSources) {
      var bean = handle.getBean();
      var name = bean.getName();
      if (name != null) {
        name = unquoteDataSourceName(name);
        if (name.equals(dataSourceName)) {
          return handle.get();
        }
      }
    }
    throw new IllegalStateException("No datasource configured with name: " + dataSourceName);
  }
}

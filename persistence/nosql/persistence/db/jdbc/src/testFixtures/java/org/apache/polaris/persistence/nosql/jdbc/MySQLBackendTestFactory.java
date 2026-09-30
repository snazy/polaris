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

import jakarta.annotation.Nonnull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class MySQLBackendTestFactory extends BaseJdbcContainerBackendTestFactory {

  public static final String NAME = JdbcBackendFactory.NAME + "-MySQL";

  @Override
  public String name() {
    return NAME;
  }

  @Nonnull
  @Override
  protected JdbcDatabaseContainer<?> createContainer() {
    return new MariaDBDriverMySQLContainer(dockerImage("mysql"));
  }

  private static class MariaDBDriverMySQLContainer extends MySQLContainer {

    MariaDBDriverMySQLContainer(DockerImageName dockerImage) {
      super(dockerImage.asCompatibleSubstituteFor("mysql"));
    }

    @Override
    public String getDriverClassName() {
      return "org.mariadb.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
      return super.getJdbcUrl().replace("jdbc:mysql", "jdbc:mariadb");
    }
  }
}

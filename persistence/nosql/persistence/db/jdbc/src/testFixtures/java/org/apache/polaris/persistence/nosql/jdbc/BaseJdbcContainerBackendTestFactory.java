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
import static org.assertj.core.util.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public abstract class BaseJdbcContainerBackendTestFactory extends BaseJdbcBackendTestFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BaseJdbcContainerBackendTestFactory.class);

  private JdbcDatabaseContainer<?> container;

  protected static DockerImageName dockerImage(String dbName) {
    return containerSpecHelper(dbName, BaseJdbcContainerBackendTestFactory.class)
        .dockerImageName(null);
  }

  @Override
  public String jdbcUrl() {
    checkState(container != null, "Container not started");
    return container.getJdbcUrl();
  }

  @Override
  public String jdbcUser() {
    checkState(container != null, "Container not started");
    return container.getUsername();
  }

  @Override
  public String jdbcPass() {
    checkState(container != null, "Container not started");
    return container.getPassword();
  }

  @Override
  public void start(Optional<String> containerNetworkId) throws Exception {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    for (int retry = 0; ; retry++) {
      @SuppressWarnings("resource")
      JdbcDatabaseContainer<?> c = createContainer().withLogConsumer(new Slf4jLogConsumer(LOGGER));
      containerNetworkId.ifPresent(c::withNetworkMode);
      try {
        c.start();
        container = c;
        break;
      } catch (ContainerLaunchException e) {
        c.close();
        if (e.getCause() != null && retry < 3) {
          LOGGER.warn("Launch of container {} failed, will retry...", c.getDockerImageName(), e);
          continue;
        }
        LOGGER.error("Launch of container {} failed", c.getDockerImageName(), e);
        throw new RuntimeException(e);
      }
    }

    super.start();
  }

  @Override
  public void start() throws Exception {
    start(Optional.empty());
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      try {
        if (container != null) {
          container.stop();
        }
      } finally {
        container = null;
      }
    }
  }

  @Nonnull
  protected abstract JdbcDatabaseContainer<?> createContainer();
}

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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class BigtableBackendContainerTestFactory extends AbstractBigtableBackendTestFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BigtableBackendContainerTestFactory.class);

  public static final String NAME = "Bigtable";
  public static final int BIGTABLE_PORT = 8086;

  private GenericContainer<?> container;
  private String emulatorHost;
  private int emulatorPort;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public int getEmulatorPort() {
    return emulatorPort;
  }

  @Override
  public String getEmulatorHost() {
    return emulatorHost;
  }

  @Override
  public void start() {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    var imageName =
        containerSpecHelper("google-cloud-sdk", BigtableBackendContainerTestFactory.class)
            .dockerImageName(null);

    for (int retry = 0; ; retry++) {
      @SuppressWarnings("resource")
      var c =
          new GenericContainer<>(imageName)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withExposedPorts(BIGTABLE_PORT)
              .withCommand(
                  "gcloud",
                  "beta",
                  "emulators",
                  "bigtable",
                  "start",
                  "--verbosity=info", // debug, info, warning, error, critical, none
                  "--host-port=0.0.0.0:" + BIGTABLE_PORT);
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

    emulatorPort = container.getFirstMappedPort();
    emulatorHost = container.getHost();

    projectId = "test-project";
    instanceId = "test-instance";
  }

  @Override
  public void stop() {
    try {
      if (container != null) {
        container.stop();
      }
    } finally {
      container = null;
    }
  }
}

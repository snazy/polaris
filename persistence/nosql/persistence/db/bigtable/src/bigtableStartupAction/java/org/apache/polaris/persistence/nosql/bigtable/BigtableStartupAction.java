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

import java.util.Map;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;
import org.testcontainers.containers.GenericContainer;

public class BigtableStartupAction implements PolarisServerStartupAction {
  private GenericContainer<?> container;

  @Override
  public void start(PolarisServerStartupContext context) {
    var imageName =
        containerSpecHelper("google-cloud-sdk", BigtableBackendContainerTestFactory.class)
            .dockerImageName(null);
    container =
        new GenericContainer<>(imageName)
            .withExposedPorts(BigtableBackendContainerTestFactory.BIGTABLE_PORT)
            .withCommand(
                "gcloud",
                "beta",
                "emulators",
                "bigtable",
                "start",
                "--verbosity=info",
                "--host-port=0.0.0.0:" + BigtableBackendContainerTestFactory.BIGTABLE_PORT);
    container.start();

    context
        .getSystemProperties()
        .putAll(
            Map.ofEntries(
                Map.entry("polaris.persistence.type", "nosql"),
                Map.entry("polaris.persistence.auto-bootstrap-types", "nosql"),
                Map.entry("polaris.persistence.nosql.backend", "Bigtable"),
                Map.entry("polaris.persistence.nosql.bigtable.emulator-host", container.getHost()),
                Map.entry(
                    "polaris.persistence.nosql.bigtable.emulator-port",
                    String.valueOf(
                        container.getMappedPort(
                            BigtableBackendContainerTestFactory.BIGTABLE_PORT))),
                Map.entry("polaris.persistence.nosql.bigtable.project-id", "test-project"),
                Map.entry("polaris.persistence.nosql.bigtable.instance-id", "test-instance")));
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

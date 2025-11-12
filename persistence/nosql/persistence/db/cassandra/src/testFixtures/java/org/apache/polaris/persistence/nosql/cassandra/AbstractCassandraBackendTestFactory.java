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

import static java.lang.String.format;
import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_KEYSPACE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public abstract class AbstractCassandraBackendTestFactory implements BackendTestFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCassandraBackendTestFactory.class);

  public static final Integer CQL_PORT = 9042;

  private final String dbName;
  private final List<String> args;
  private final Duration timeout;

  private CassandraContainer container;
  private String localDc;
  private String host;
  private Integer port;

  public AbstractCassandraBackendTestFactory(String dbName, List<String> args) {
    this.dbName = dbName;
    this.args = args;

    // Increase some timeouts to avoid flakiness
    this.timeout = Duration.ofSeconds(15);
  }

  @Override
  public Backend createNewBackend() {
    var factory = new CassandraBackendFactory();
    var config =
        factory.buildConfiguration(
            new CassandraConfiguration() {
              @Override
              public Optional<String> localDc() {
                return Optional.of(localDc);
              }

              @Override
              public List<String> endpoints() {
                return List.of(host);
              }

              @Override
              public int port() {
                return port;
              }

              @Override
              public String keyspace() {
                return DEFAULT_KEYSPACE;
              }

              @Override
              public Duration connectTimeout() {
                return timeout;
              }

              @Override
              public Duration ddlTimeout() {
                return timeout;
              }

              @Override
              public Duration dmlTimeout() {
                return timeout;
              }

              @Override
              public Optional<String> username() {
                return Optional.empty();
              }

              @Override
              public Optional<String> password() {
                return Optional.empty();
              }
            });

    maybeCreateKeyspace(config.client());

    return factory.buildBackend(config);
  }

  private void maybeCreateKeyspace(CqlSession session) {
    var replicationFactor = 1;

    var metadata = session.getMetadata();

    var datacenters =
        metadata.getNodes().values().stream()
            .map(Node::getDatacenter)
            .distinct()
            .map(dc -> format("'%s': %d", dc, replicationFactor))
            .collect(Collectors.joining(", "));

    // Disable tablets in ScyllaDB, because those are not compatible with LWTs (yet?). See
    // https://opensource.docs.scylladb.com/stable/architecture/tablets.html#limitations-and-unsupported-features
    var scyllaClause = "scylladb".equals(dbName) ? " AND tablets = {'enabled': false}" : "";

    session.execute(
        format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', %s}%s;",
            DEFAULT_KEYSPACE, datacenters, scyllaClause));

    session.refreshSchema();
  }

  @Override
  public void start(Optional<String> containerNetworkId) {
    if (container != null) {
      throw new IllegalStateException("Already started");
    }

    var dockerImageName =
        containerSpecHelper(dbName, AbstractCassandraBackendTestFactory.class)
            .dockerImageName(null)
            .asCompatibleSubstituteFor("cassandra");

    for (var retry = 0; ; retry++) {
      var c =
          new CassandraContainer(dockerImageName)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withCommand(args.toArray(new String[0]));
      configureContainer(c);
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

    port = containerNetworkId.isPresent() ? CQL_PORT : container.getMappedPort(CQL_PORT);
    host =
        containerNetworkId.isPresent()
            ? container.getCurrentContainerInfo().getConfig().getHostName()
            : container.getHost();

    localDc = container.getLocalDatacenter();
  }

  protected abstract void configureContainer(CassandraContainer c);

  @Override
  public void start() {
    start(Optional.empty());
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

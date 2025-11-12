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

import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_CONNECT_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DDL_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_DML_TIMEOUT;
import static org.apache.polaris.persistence.nosql.cassandra.CassandraBackendConfig.DEFAULT_KEYSPACE;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Polaris persistence, Cassandra backend specific configuration. */
@ConfigMapping(prefix = "polaris.backend.cassandra")
@StaticInitSafe
public interface CassandraConfiguration {

  Optional<String> localDc();

  List<String> endpoints();

  @WithDefault("9042")
  int port();

  @WithDefault(DEFAULT_KEYSPACE)
  String keyspace();

  @WithDefault(DEFAULT_CONNECT_TIMEOUT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Duration connectTimeout();

  /** Timeout used when creating tables. */
  @WithDefault(DEFAULT_DDL_TIMEOUT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Duration ddlTimeout();

  /** Timeout used for queries and updates. */
  @WithDefault(DEFAULT_DML_TIMEOUT)
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  Duration dmlTimeout();

  Optional<String> username();

  Optional<String> password();
}

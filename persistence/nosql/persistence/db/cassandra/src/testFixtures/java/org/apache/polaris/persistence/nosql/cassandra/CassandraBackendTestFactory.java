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

import static java.util.Collections.emptyList;

import org.testcontainers.cassandra.CassandraContainer;

public class CassandraBackendTestFactory extends AbstractCassandraBackendTestFactory {

  private static final String JVM_OPTS_TEST =
      "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
          + "-Dcassandra.num_tokens=1 "
          + "-Dcassandra.initial_token=0";
  public static final String NAME = "Cassandra";

  public CassandraBackendTestFactory() {
    super("cassandra", emptyList());
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  protected void configureContainer(CassandraContainer c) {
    c.withEnv("JVM_OPTS", JVM_OPTS_TEST);
  }
}

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
package org.apache.polaris.persistence.nosql.rocksdb;

import jakarta.annotation.Nonnull;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;

public class RocksDbBackendFactory
    implements BackendFactory<RocksDbBackendConfig, RocksDbConfiguration> {
  public static final String NAME = "RocksDb";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  @Nonnull
  public Backend buildBackend(@Nonnull RocksDbBackendConfig backendConfig) {
    return new RocksDbBackend(backendConfig);
  }

  @Override
  public Class<RocksDbConfiguration> configurationInterface() {
    return RocksDbConfiguration.class;
  }

  @Override
  public RocksDbBackendConfig buildConfiguration(RocksDbConfiguration config) {
    return new RocksDbBackendConfig(
        config
            .databaseDirectory()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Mandatory config option polaris.backend.rocksdb.database-directory is missing")));
  }
}

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

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.rocksdb.RocksDbBackendFactory;

@BackendType(RocksDbBackendFactory.NAME)
@Dependent
class RocksDbBackendBuilder implements BackendBuilder {
  @Inject Instance<QuarkusRocksDbConfiguration> config;

  @Override
  public Backend buildBackend() {
    var factory = new RocksDbBackendFactory();
    var config = this.config;
    var cfg = factory.buildConfiguration(config.get());
    return factory.buildBackend(cfg);
  }
}

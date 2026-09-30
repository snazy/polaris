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
package org.apache.polaris.persistence.nosql.bigtable.quarkus;

import com.google.api.gax.core.CredentialsProvider;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.bigtable.BigtableBackendFactory;
import org.apache.polaris.persistence.nosql.bigtable.BigtableConfiguration;
import org.apache.polaris.persistence.nosql.quarkus.backend.BackendBuilder;
import org.apache.polaris.persistence.nosql.quarkus.backend.BackendType;

@BackendType(BigtableBackendFactory.NAME)
@Dependent
class BigtableBackendBuilder implements BackendBuilder {
  @Inject Instance<BigtableConfiguration> config;
  @Inject Instance<CredentialsProvider> credentialsProvider;

  @Override
  public Backend buildBackend() {
    var configuration = config.get();
    var factory = new BigtableBackendFactory();
    if (configuration.emulatorHost().isPresent()) {
      return factory.buildBackend(factory.buildConfiguration(configuration));
    }
    if (!credentialsProvider.isResolvable()) {
      throw new IllegalStateException("No Google CredentialsProvider available");
    }
    return factory.buildBackend(
        factory.buildConfiguration(configuration, credentialsProvider.get()));
  }
}

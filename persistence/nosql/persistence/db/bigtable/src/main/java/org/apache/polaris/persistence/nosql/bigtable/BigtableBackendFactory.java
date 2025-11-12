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

import static org.apache.polaris.persistence.nosql.bigtable.BigtableClientsFactory.createDataClient;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableClientsFactory.createTableAdminClient;
import static org.apache.polaris.persistence.nosql.bigtable.BigtableConstants.DEFAULT_BULK_READ_TIMEOUT;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import org.apache.polaris.persistence.nosql.api.backend.BackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigtableBackendFactory
    implements BackendFactory<BigtableBackendConfig, BigtableConfiguration> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigtableBackendFactory.class);

  public static final String NAME = "Bigtable";

  @Override
  @Nonnull
  public String name() {
    return NAME;
  }

  @Override
  public BigtableBackendConfig buildConfiguration(BigtableConfiguration config) {
    try {
      var credentialsProvider = NoCredentialsProvider.create();

      var dataClient = createDataClient(config, credentialsProvider);

      var tableAdminClient = (BigtableTableAdminClient) null;
      if (config.noTableAdminClient().orElse(false)) {
        LOGGER.info("Google BigTable table admin client creation disabled.");
      } else {
        tableAdminClient = createTableAdminClient(config, credentialsProvider);

        // Check whether the admin client actually works (Google cloud API access could be
        // disabled). If not, we cannot even check whether tables need to be created, if necessary.
        try {
          tableAdminClient.listTables();
        } catch (PermissionDeniedException e) {
          LOGGER.warn(
              "Google BigTable table admin client cannot list tables due to {}.", e.toString());
          try {
            tableAdminClient.close();
          } finally {
            tableAdminClient = null;
          }
        }
      }

      return new BigtableBackendConfig(
          true,
          dataClient,
          tableAdminClient,
          config.tablePrefix(),
          config.totalApiTimeout().orElse(DEFAULT_BULK_READ_TIMEOUT).toMillis());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<BigtableConfiguration> configurationInterface() {
    return BigtableConfiguration.class;
  }

  @Nonnull
  @Override
  public org.apache.polaris.persistence.nosql.api.backend.Backend buildBackend(
      @Nonnull BigtableBackendConfig config) {
    return new BigtableBackend(config);
  }
}

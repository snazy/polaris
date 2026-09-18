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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

final class BigtableClientsFactory {
  private static final int EMULATOR_PORT = 8086;
  private static final String INSTANCE = "polaris";

  private BigtableClientsFactory() {}

  static BigtableDataClient createDataClient(
      BigtableConfiguration config, @Nullable CredentialsProvider credentialsProvider)
      throws IOException {

    var projectId = projectId(config);
    var dataSettings =
        (config.emulatorHost().isPresent()
            ? BigtableDataSettings.newBuilderForEmulator(
                    config.emulatorHost().get(), config.emulatorPort().orElse(EMULATOR_PORT))
                .setCredentialsProvider(NoCredentialsProvider.create())
            : BigtableDataSettings.newBuilder().setCredentialsProvider(credentialsProvider));
    dataSettings.setProjectId(projectId).setInstanceId(config.instanceId().orElse(INSTANCE));
    config.appProfileId().ifPresent(dataSettings::setAppProfileId);
    config.mtlsEndpoint().ifPresent(dataSettings.stubSettings()::setMtlsEndpoint);
    config.quotaProjectId().ifPresent(dataSettings.stubSettings()::setQuotaProjectId);
    config.endpoint().ifPresent(dataSettings.stubSettings()::setEndpoint);

    var defaultPoolSettings = ChannelPoolSettings.builder().build();

    var poolSettings =
        ChannelPoolSettings.builder()
            .setMinChannelCount(
                config.minChannelCount().orElse(defaultPoolSettings.getMinChannelCount()))
            .setMaxChannelCount(
                config.maxChannelCount().orElse(defaultPoolSettings.getMaxChannelCount()))
            .setInitialChannelCount(
                config.initialChannelCount().orElse(defaultPoolSettings.getInitialChannelCount()))
            .setMinRpcsPerChannel(
                config.minRpcsPerChannel().orElse(defaultPoolSettings.getMinRpcsPerChannel()))
            .setMaxRpcsPerChannel(
                config.maxRpcsPerChannel().orElse(defaultPoolSettings.getMaxRpcsPerChannel()))
            .setPreemptiveRefreshEnabled(true)
            .build();

    var stubSettings = dataSettings.stubSettings();

    for (var retrySettings :
        List.of(
            stubSettings.readRowSettings().retrySettings(),
            stubSettings.readRowsSettings().retrySettings(),
            stubSettings.bulkReadRowsSettings().retrySettings(),
            stubSettings.mutateRowSettings().retrySettings(),
            stubSettings.bulkMutateRowsSettings().retrySettings(),
            stubSettings.readChangeStreamSettings().retrySettings())) {
      configureDuration(config.totalTimeout(), retrySettings::setTotalTimeout);
      configureDuration(config.initialRpcTimeout(), retrySettings::setInitialRpcTimeout);
      configureDuration(config.maxRpcTimeout(), retrySettings::setMaxRpcTimeout);
      config.rpcTimeoutMultiplier().ifPresent(retrySettings::setRpcTimeoutMultiplier);
      configureDuration(config.initialRetryDelay(), retrySettings::setInitialRetryDelay);
      configureDuration(config.maxRetryDelay(), retrySettings::setMaxRetryDelay);
      config.retryDelayMultiplier().ifPresent(retrySettings::setRetryDelayMultiplier);
      config.maxAttempts().ifPresent(retrySettings::setMaxAttempts);
    }

    var transportChannelProvider =
        (InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider();
    var transportChannelProviderBuilder = transportChannelProvider.toBuilder();
    stubSettings.setTransportChannelProvider(
        transportChannelProviderBuilder.setChannelPoolSettings(poolSettings).build());

    applyCommonDataClientSettings(dataSettings);

    return BigtableDataClient.create(dataSettings.build());
  }

  /**
   * Apply settings that are relevant to both production and test usage. Also called from test
   * factories.
   */
  static void applyCommonDataClientSettings(BigtableDataSettings.Builder settings) {
    var stubSettings = settings.stubSettings();

    stubSettings
        .bulkMutateRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigtableConstants.MAX_BULK_MUTATIONS)
                .build());

    stubSettings
        .bulkReadRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkReadRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigtableConstants.MAX_BULK_READS)
                .build());
  }

  static BigtableTableAdminClient createTableAdminClient(
      BigtableConfiguration config, @Nullable CredentialsProvider credentialsProvider)
      throws IOException {

    var projectId = projectId(config);
    var adminSettings =
        config.emulatorHost().isPresent()
            ? BigtableTableAdminSettings.newBuilderForEmulator(
                    config.emulatorHost().get(), config.emulatorPort().orElse(EMULATOR_PORT))
                .setCredentialsProvider(NoCredentialsProvider.create())
            : BigtableTableAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider);
    adminSettings.setProjectId(projectId).setInstanceId(config.instanceId().orElse(INSTANCE));
    config.mtlsEndpoint().ifPresent(adminSettings.stubSettings()::setMtlsEndpoint);
    config.quotaProjectId().ifPresent(adminSettings.stubSettings()::setQuotaProjectId);
    config.endpoint().ifPresent(adminSettings.stubSettings()::setEndpoint);

    return BigtableTableAdminClient.create(adminSettings.build());
  }

  private static String projectId(BigtableConfiguration config) {
    return config.projectId().orElse("polaris");
  }

  private static void configureDuration(
      Optional<Duration> config, Consumer<org.threeten.bp.Duration> configurable) {
    config.map(Duration::toMillis).map(org.threeten.bp.Duration::ofMillis).ifPresent(configurable);
  }
}

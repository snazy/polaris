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
package org.apache.polaris.persistence.nosql.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.util.Map;
import org.apache.polaris.persistence.nosql.api.commit.RetryConfig.BuildableRetryConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPersistenceParams {
  private static PersistenceParams mapping(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withMapping(PersistenceParams.class)
        .addDiscoveredConverters()
        .withSources(new PropertiesConfigSource(properties, "test"))
        .build()
        .getConfigMapping(PersistenceParams.class);
  }

  @Test
  void preferredIndexWriteVersionDefaultsToV1() {
    assertThat(
            PersistenceParams.BuildablePersistenceParams.builder()
                .build()
                .preferredIndexWriteVersion())
        .isEqualTo(1);
    assertThat(mapping(Map.of()).preferredIndexWriteVersion()).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void preferredIndexWriteVersionAllowsSupportedVersions(int version) {
    assertThat(
            PersistenceParams.BuildablePersistenceParams.builder()
                .preferredIndexWriteVersion(version)
                .build()
                .preferredIndexWriteVersion())
        .isEqualTo(version);
    assertThat(
            mapping(
                    Map.of(
                        "polaris.persistence.preferred-index-write-version",
                        Integer.toString(version)))
                .preferredIndexWriteVersion())
        .isEqualTo(version);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 3, Integer.MAX_VALUE})
  void preferredIndexWriteVersionRejectsUnsupportedProgrammaticValues(int version) {
    assertThatThrownBy(() ->
            PersistenceParams.BuildablePersistenceParams.builder()
                .preferredIndexWriteVersion(version)
                .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("preferredIndexWriteVersion");
  }

  @ParameterizedTest
  @ValueSource(strings = {"-1", "0", "3", "2147483648", "not-a-number"})
  void preferredIndexWriteVersionRejectsUnsupportedMappedValues(String version) {
    assertThatThrownBy(
            () -> mapping(Map.of("polaris.persistence.preferred-index-write-version", version)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("preferred-index-write-version");
  }

  @Test
  public void retryConfig() {
    var params =
        new SmallRyeConfigBuilder()
            .withMapping(PersistenceParams.class)
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(
                    Map.of("polaris.persistence.retry.timeout", "PT45S"), "configMap"))
            .build()
            .getConfigMapping(PersistenceParams.class);

    assertThat(BuildableRetryConfig.builder().from(params.retryConfig()).build())
        .isEqualTo(BuildableRetryConfig.builder().timeout(Duration.ofSeconds(45)).build());
  }
}

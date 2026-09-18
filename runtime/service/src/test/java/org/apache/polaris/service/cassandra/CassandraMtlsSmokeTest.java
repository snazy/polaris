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
package org.apache.polaris.service.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.persistence.nosql.cassandra.CassandraAuthentication;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = CassandraMtlsTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(CassandraMtlsSmokeTest.Profile.class)
class CassandraMtlsSmokeTest {

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(Profiles.DEFAULT_PROFILE_CONFIG_OVERRIDES);
      overrides.put("polaris.persistence.nosql.cassandra.auth.provider-name", "test");
      return overrides;
    }
  }

  @Inject CqlSession cqlSession;
  @Inject MeterRegistry meterRegistry;

  @Inject
  @CassandraAuthentication("test")
  TestAuthProvider authProvider;

  @Test
  void connectsWithMutualTlsAndPublishesDriverMetrics() {
    assertThat(cqlSession.execute("SELECT release_version FROM system.local").one()).isNotNull();

    assertThat(meterRegistry.getMeters())
        .extracting(meter -> meter.getId().getName())
        .anyMatch(name -> name.startsWith("cassandra"));
    assertThat(authProvider.missingChallenge()).isTrue();
  }

  @ApplicationScoped
  @CassandraAuthentication("test")
  public static class TestAuthProvider implements AuthProvider {
    private final AtomicBoolean missingChallenge = new AtomicBoolean();

    @Override
    public Authenticator newAuthenticator(EndPoint endPoint, String serverAuthenticator) {
      throw new AssertionError("The mTLS test Cassandra instance does not require authentication.");
    }

    @Override
    public void onMissingChallenge(EndPoint endPoint) {
      missingChallenge.set(true);
    }

    @Override
    public void close() {}

    boolean missingChallenge() {
      return missingChallenge.get();
    }
  }
}

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
package org.apache.polaris.service.nosql.cassandra;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.cassandra.CassandraAuthentication;
import org.apache.polaris.service.Profiles;
import org.eclipse.microprofile.config.ConfigProvider;
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
  @Inject @SystemPersistence Persistence persistence;

  @Inject
  @CassandraAuthentication("test")
  TestAuthProvider authProvider;

  @Test
  void connectsWithMutualTlsAndPublishesDriverMetrics() {
    assertThat(cqlSession.execute("SELECT release_version FROM system.local").one()).isNotNull();

    var referenceName = "mtls-smoke-" + UUID.randomUUID();
    var createdReference = persistence.createReference(referenceName, Optional.empty());
    var fetchedReference = persistence.fetchReference(referenceName);

    assertThat(fetchedReference.name()).isEqualTo(referenceName);
    assertThat(fetchedReference.pointer()).isEmpty();
    assertThat(fetchedReference.createdAtMicros()).isEqualTo(createdReference.createdAtMicros());

    assertThat(meterRegistry.getMeters())
        .extracting(meter -> meter.getId().getName())
        .anyMatch(name -> name.startsWith("cassandra"));
    given()
        .port(quarkusManagementPort())
        .when()
        .get("/q/health/ready")
        .then()
        .statusCode(200)
        .body("status", equalTo("UP"))
        .body("checks.find { it.name == 'Cassandra health check' }.status", equalTo("UP"));
    assertThat(authProvider.missingChallenge()).isTrue();
  }

  private static int quarkusManagementPort() {
    return ConfigProvider.getConfig().getValue("quarkus.management.port", Integer.class);
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

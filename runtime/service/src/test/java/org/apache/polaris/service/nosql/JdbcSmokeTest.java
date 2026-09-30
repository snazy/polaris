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
package org.apache.polaris.service.nosql;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.service.Profiles;

@QuarkusTest
@TestProfile(JdbcSmokeTest.Profile.class)
class JdbcSmokeTest extends NoSqlBackendSmokeTest {

  public static class Profile extends Profiles.DefaultProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("polaris.persistence.type", "nosql");
      overrides.put("polaris.persistence.auto-bootstrap-types", "nosql");
      overrides.put("polaris.persistence.nosql.backend", "JDBC");
      overrides.put("polaris.backend.jdbc.datasource", "nosql");
      overrides.put("quarkus.datasource.nosql.db-kind", "h2");
      overrides.put(
          "quarkus.datasource.nosql.jdbc.url",
          "jdbc:h2:mem:nosql-smoke;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE");
      return overrides;
    }
  }
}

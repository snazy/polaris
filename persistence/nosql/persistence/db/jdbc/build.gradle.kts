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

plugins {
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
}

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))

  implementation(libs.agrona)

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  // Do not leak Agroal by default, it is only needed when JDBC data-sources are not provided by a
  // framework.
  compileOnly(libs.agroal.pool)

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")

  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-impl")))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesImplementation(project(":polaris-container-spec-helper"))
  testFixturesImplementation(platform(libs.testcontainers.bom))
  testFixturesImplementation("org.testcontainers:testcontainers-postgresql")
  testFixturesImplementation("org.testcontainers:testcontainers-mysql")
  testFixturesImplementation("org.testcontainers:testcontainers-mariadb")
  testFixturesImplementation("org.testcontainers:testcontainers-cockroachdb")
  testFixturesImplementation("org.testcontainers:testcontainers-mariadb")
  testFixturesImplementation("org.testcontainers:testcontainers-mysql")

  testFixturesRuntimeOnly(libs.agroal.pool)
  testFixturesRuntimeOnly(libs.h2)
  testFixturesRuntimeOnly(libs.postgresql)
  testFixturesRuntimeOnly(libs.mariadb.java.client)
}

testing {
  suites {
    val intTest by registering(JvmTestSuite::class)
  }
}

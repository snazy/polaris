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

import org.gradle.api.component.AdhocComponentWithVariants

plugins {
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
}

val quarkusRuntimeOnly =
  configurations.dependencyScope("quarkusRuntimeOnly") {
    extendsFrom(configurations.implementation.get(), configurations.runtimeOnly.get())
  }
val quarkusRuntimeElements =
  configurations.consumable("quarkusRuntimeElements") {
    extendsFrom(quarkusRuntimeOnly.get())
    attributes {
      addAllLater(configurations.runtimeElements.get().attributes)
    }
    outgoing {
      artifact(tasks.named("jar"))
      capability("$group:${project.name}-quarkus:$version")
    }
  }

(components["java"] as AdhocComponentWithVariants).addVariantsFromConfiguration(
  quarkusRuntimeElements.get()
) {}

dependencies {
  implementation(project(":polaris-persistence-nosql-api"))
  implementation(project(":polaris-persistence-nosql-impl"))
  implementation(project(":polaris-idgen-api"))
  compileOnly(project(":polaris-persistence-nosql-cdi-quarkus"))

  implementation(libs.rocksdb.jni)
  runtimeOnly(libs.rocksdb.jni) { artifact { classifier = "linux64" } }
  runtimeOnly(libs.rocksdb.jni) { artifact { classifier = "linux64-musl" } }
  runtimeOnly(libs.rocksdb.jni) { artifact { classifier = "osx" } }

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson3.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  implementation("tools.jackson.core:jackson-databind")

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.smallrye.config.core)
  compileOnly(platform(libs.quarkus.bom))
  compileOnly("io.quarkus:quarkus-core")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testFixturesApi(testFixtures(project(":polaris-persistence-nosql-impl")))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesCompileOnly(platform(libs.jackson3.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))
}

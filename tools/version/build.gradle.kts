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

import org.apache.tools.ant.filters.ReplaceTokens

plugins {
  id("polaris-client")
  `java-library`
  `java-test-fixtures`
  `jvm-test-suite`
}

dependencies { testFixturesApi(libs.assertj.core) }

val syncNoticeAndLicense by
  tasks.registering(Sync::class) {
    // Have to manually declare the inputs of this task here on top of the from/include below
    inputs.files(rootProject.layout.files("NOTICE*", "LICENSE*"))
    inputs.property("version", project.version)
    destinationDir = project.layout.buildDirectory.dir("notice-licenses").get().asFile
    from(rootProject.rootDir) {
      include("NOTICE*", "LICENSE*")
      eachFile { path = "META-INF/resources/apache-polaris/${file.name}.txt" }
    }
  }

val versionProperties by
  tasks.registering(Sync::class) {
    destinationDir = project.layout.buildDirectory.dir("version").get().asFile
    from(project.layout.files("src/main/version"))
    eachFile { path = "org/apache/polaris/version/$path" }
    inputs.property("projectVersion", project.version)
    filter(ReplaceTokens::class, mapOf("tokens" to mapOf("projectVersion" to project.version)))
  }

sourceSets.main.configure {
  resources {
    srcDir(syncNoticeAndLicense)
    srcDir(versionProperties)
  }
}

// Add a special test-task that runs against the built polaris-version*.jar, not the classes/
testing {
  suites {
    withType<JvmTestSuite> { useJUnitJupiter(libs.junit.bom.map { it.version!! }) }

    register<JvmTestSuite>("jarTest") {
      dependencies {
        compileOnly(project())
        var jarTask = tasks.named<Jar>("jar")
        runtimeOnly(files(jarTask.get().archiveFile.get().asFile))
        implementation(libs.assertj.core)
      }

      targets.all {
        testTask.configure {
          dependsOn("jar")
          systemProperty("rootProjectDir", rootProject.rootDir.relativeTo(project.projectDir))
        }
      }
    }
  }
}

tasks.named("check") { dependsOn("jarTest") }

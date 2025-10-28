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

import org.apache.rat.config.exclusion.StandardCollection
import java.net.URI

buildscript { repositories { maven { url = java.net.URI("https://plugins.gradle.org/m2/") } } }

plugins {
  id("idea")
  id("eclipse")
  id("polaris-root")
  id("org.apache.rat.gradle-plugin")
  alias(libs.plugins.jetbrains.changelog)
}

val projectName = rootProject.file("ide-name.txt").readText().trim()
val ideName = "$projectName ${rootProject.version.toString().replace("^([0-9.]+).*", "\\1")}"

if (System.getProperty("idea.sync.active").toBoolean()) {
  // There's no proper way to set the name of the IDEA project (when "just importing" or
  // syncing the Gradle project)
  val ideaDir = rootProject.layout.projectDirectory.dir(".idea")
  ideaDir.asFile.mkdirs()
  ideaDir.file(".name").asFile.writeText(ideName)

  val icon = ideaDir.file("icon.png").asFile
  if (!icon.exists()) {
    copy {
      from("site/static/img/logos/polaris-brandmark.png")
      into(ideaDir)
      rename { _ -> "icon.png" }
    }
  }
}

eclipse { project { name = ideName } }

rat {
  // excludes `.gitignore` contents
  inputExcludeParsedScms.add(StandardCollection.GIT)

  // excludes standard file patterns (like .git)
  inputExcludeStds.addAll(
      StandardCollection.MISC,
      StandardCollection.MAC,
      StandardCollection.GIT,
      StandardCollection.GRADLE,
      StandardCollection.IDEA,
      StandardCollection.ECLIPSE)

  inputExcludes.addAll("/ide-name.txt",
      "/version.txt")

  // Manifest files do not allow comments
  inputExcludes.add("/tools/version/src/jarTest/resources/META-INF/FAKE_MANIFEST.MF")

  // Polaris service startup banner
  inputExcludes.add("/runtime/service/src/**/banner.txt")

  // Binary files
  inputExcludes.add(
    "/persistence/nosql/persistence/index/src/testFixtures/resources/org/apache/polaris/persistence/indexes/words.gz"
  )

  // Files copied from Docsy (ASLv2 licensed) don't have header
  inputExcludes.addAll(
      "/site/layouts/docs/baseof.html",
      "/site/layouts/shortcodes/redoc-polaris.html",
      "/site/layouts/community/list.html",
      "/site/layouts/partials/navbar.html",
      "/site/layouts/partials/head.html",
      "/site/layouts/partials/community_links.html",
      "/site/layouts/partials/head.html")

  // Files copied from OpenAPI Generator (ASLv2 licensed) don't have header
  inputExcludes.add("/server-templates/*.mustache")

  // Web site
  inputExcludes.add("/site/layouts/robots.txt")

  // Go stuff
  inputExcludes.add("**/go.sum")

  // Jupyter
  inputExcludes.add("**/*.ipynb")

  // regtests
  inputExcludes.addAll(
      "/regtests/**/py.typed",
      "/regtests/**/*.ref",
      "/regtests/.env",
      "/plugins/**/*.ref")
}

tasks.register<Exec>("buildPythonClient") {
  description = "Build the python client"

  workingDir = project.projectDir
  if (project.hasProperty("python.format")) {
    environment("FORMAT", project.property("python.format") as String)
  }
  commandLine("make", "client-build")
}

// Pass environment variables:
//    ORG_GRADLE_PROJECT_apacheUsername
//    ORG_GRADLE_PROJECT_apachePassword
// OR in ~/.gradle/gradle.properties set
//    apacheUsername
//    apachePassword
// Call targets:
//    publishToApache
//    closeApacheStagingRepository
//    releaseApacheStagingRepository
//       or closeAndReleaseApacheStagingRepository
//
// Username is your ASF ID
// Password: your ASF LDAP password - or better: a token generated via
// https://repository.apache.org/
nexusPublishing {
  transitionCheckOptions {
    // default==60 (10 minutes), wait up to 120 minutes
    maxRetries = 720
    // default 10s
    delayBetween = java.time.Duration.ofSeconds(10)
  }

  repositories {
    register("apache") {
      nexusUrl = URI.create("https://repository.apache.org/service/local/")
      snapshotRepositoryUrl =
        URI.create("https://repository.apache.org/content/repositories/snapshots/")
    }
  }
}

copiedCodeChecks {
  addDefaultContentTypes()

  licenseFile = project.layout.projectDirectory.file("LICENSE")

  scanDirectories {
    register("build-logic") { srcDir("build-logic/src") }
    register("misc") {
      srcDir(".github")
      srcDir("codestyle")
      srcDir("getting-started")
      srcDir("k8")
      srcDir("regtests")
      srcDir("server-templates")
      srcDir("spec")
    }
    register("gradle") {
      srcDir("gradle")
      exclude("wrapper/*.jar")
      exclude("wrapper/*.sha256")
    }
    register("site") {
      srcDir("site")
      exclude("build/**")
      exclude(".hugo_build.lock")
    }
    register("root") {
      srcDir(".")
      include("*")
    }
  }
}

changelog {
  repositoryUrl.set("https://github.com/apache/polaris")
  title.set("Apache Polaris Changelog")
  versionPrefix.set("apache-polaris-")
  header.set(provider { "${version.get()}" })
  groups.set(
    listOf(
      "Highlights",
      "Upgrade notes",
      "Breaking changes",
      "New Features",
      "Changes",
      "Deprecations",
      "Fixes",
      "Commits",
    )
  )
  version.set(provider { project.version.toString() })
}

tasks.register("showVersion") {
  actions.add {
    logger.lifecycle(
      "Polaris version is ${project.file("version.txt").readText(Charsets.UTF_8).trim()}"
    )
  }
}

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

import app.cash.licensee.LicenseeExtension
import app.cash.licensee.LicenseePlugin
import app.cash.licensee.LicenseeTask
import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class VerifyLicenseAttribution : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseeReport: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sbom: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseFile: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val noticeFile: RegularFileProperty

  @get:Input abstract val projectGroup: Property<String>

  @get:OutputFile abstract val validationReport: RegularFileProperty

  @TaskAction
  fun verify() {
    val licenseeCoordinates =
      (JsonSlurper().parse(licenseeReport.get().asFile) as List<*>)
        .map { it as Map<*, *> }
        .map { component -> coordinate(component["groupId"], component["artifactId"], component["version"]) }
        .toSortedSet()

    val sbomComponents =
      (JsonSlurper().parse(sbom.get().asFile) as Map<*, *>)["components"] as? List<*>
        ?: throw GradleException("CycloneDX SBOM does not contain components")
    val shippedCoordinates =
      sbomComponents
        .map { it as Map<*, *> }
        .filter { component ->
          (component["purl"] as? String)?.startsWith("pkg:maven/") == true &&
            component["scope"] != "excluded" && component["group"] != projectGroup.get()
        }
        .map { component -> coordinate(component["group"], component["name"], component["version"]) }
        .toSortedSet()

    val licenseMarkers =
      licenseFile
        .get()
        .asFile
        .readLines()
        .mapNotNull { line ->
          Regex("^\\* Maven group:artifact IDs: ([^:]+:[^:]+)$").matchEntire(line)?.groupValues?.get(1)
        }
        .toSortedSet()
    val shippedGroupArtifacts = shippedCoordinates.map { it.substringBeforeLast(':') }.toSortedSet()
    val missingLicensee = shippedCoordinates - licenseeCoordinates
    val missingMarkers = shippedGroupArtifacts - licenseMarkers
    val licenseeOnly = licenseeCoordinates - shippedCoordinates
    val noticeExists = noticeFile.get().asFile.length() > 0

    val report =
      buildString {
        appendLine("External runtime components in CycloneDX SBOM: ${shippedCoordinates.size}")
        appendLine("Licensee artifacts: ${licenseeCoordinates.size}")
        appendLine("LICENSE Maven group:artifact markers: ${licenseMarkers.size}")
        appendLine("NOTICE is non-empty: $noticeExists")
        appendSection("Runtime SBOM components not checked by Licensee", missingLicensee)
        appendSection("Runtime SBOM components missing LICENSE markers", missingMarkers)
        appendSection("Licensee artifacts not present in the runtime SBOM", licenseeOnly)
      }
    validationReport.get().asFile.apply {
      parentFile.mkdirs()
      writeText(report)
    }

    if (!noticeExists || missingLicensee.isNotEmpty() || missingMarkers.isNotEmpty()) {
      throw GradleException("License attribution validation failed; see ${validationReport.get().asFile}")
    }
  }

  private fun coordinate(group: Any?, artifact: Any?, version: Any?): String {
    require(group is String && artifact is String && version is String) {
      "Expected Maven component with group, artifact, and version"
    }
    return "$group:$artifact:$version"
  }

  private fun StringBuilder.appendSection(title: String, values: Collection<String>) {
    appendLine()
    appendLine("$title (${values.size}):")
    values.forEach { appendLine("- $it") }
  }
}

buildscript {
  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
  dependencies { classpath("app.cash.licensee:licensee-gradle-plugin:1.14.1") }
}

apply<LicenseePlugin>()

extensions.configure<LicenseeExtension>("licensee") {
  allow("Apache-2.0")
  allow("BSD-2-Clause")
  allow("BSD-3-Clause")
  allow("CDDL-1.0")
  allow("CDDL-1.1")
  allow("CC0-1.0")
  allow("EPL-1.0")
  allow("EPL-2.0")
  allow("GPL-2.0-with-classpath-exception")
  allow("LGPL-2.1")
  allow("MIT")
  allow("MIT-0")
  allow("Unicode-3.0")
  allow("UPL-1.0")

  // Licensee receives these non-SPDX URL variants from the effective Maven POM. Each maps to an
  // SPDX identifier allowed above; retain the mapping explicitly rather than parsing POMs here.
  mapOf(
      "https://raw.githubusercontent.com/auth0/java-jwt/master/LICENSE" to "MIT",
      "https://github.com/googleapis/google-cloud-java/blob/main/LICENSE" to "BSD-3-Clause",
      "https://golang.org/LICENSE" to "Go License",
      "http://apache.org/licenses/LICENSE-2.0" to "Apache-2.0",
      "https://apache.org/licenses/LICENSE-2.0.txt" to "Apache-2.0",
      "http://www.apache.org/licenses/" to "Apache-2.0",
      "https://repository.jboss.org/licenses/apache-2.0.txt" to "Apache-2.0",
      "http://repository.jboss.org/licenses/apache-2.0.txt" to "Apache-2.0",
      "https://aws.amazon.com/apache2.0" to "Apache-2.0",
      "http://www.eclipse.org/org/documents/edl-v10.php" to "EDL-1.0",
      "http://repository.jboss.org/licenses/cc0-1.0.txt" to "CC0-1.0",
      "https://jdbc.postgresql.org/about/license.html" to "BSD-2-Clause",
      "https://spdx.org/licenses/MIT.txt" to "MIT",
      "https://opensource.org/license/mit" to "MIT",
      "https://raw.githubusercontent.com/ThreeTen/threeten-extra/main/LICENSE.txt" to
        "BSD-3-Clause",
      "https://raw.githubusercontent.com/ThreeTen/threetenbp/main/LICENSE.txt" to "BSD-3-Clause",
    )
    .forEach { (url, license) ->
      allowUrl(url) { because("Metadata URL for approved $license license") }
    }

  // These two POMs declare an otherwise-approved MIT license without a URL, which Licensee cannot
  // allow generically. Pinning the exception to the resolved version makes upgrades reviewable.
  allowDependency("com.microsoft.azure", "msal4j", "1.23.1") {
    because("POM declares MIT License without a URL")
  }
  allowDependency("com.microsoft.azure", "msal4j-persistence-extension", "1.3.0") {
    because("POM declares MIT License without a URL")
  }
  allowDependency("javax.servlet.jsp", "jsp-api", "2.1") {
    because("The published POM has no license metadata; retained legacy exception.")
  }
}

tasks.named<LicenseeTask>("licensee") {
  configurationToCheck(configurations.named("quarkusProdRuntimeClasspathConfiguration"))
  dependsOn("quarkusAppPartsBuild")
}

tasks.register<VerifyLicenseAttribution>("verifyLicenseAttribution") {
  group = "verification"
  description = "Verifies that shipped runtime dependencies have Licensee checks and LICENSE attribution"
  dependsOn(tasks.named("licensee"))
  licenseeReport.set(layout.buildDirectory.file("reports/licensee/artifacts.json"))
  sbom.set(layout.buildDirectory.file("quarkus-run-cyclonedx.json"))
  licenseFile.set(layout.projectDirectory.file("distribution/LICENSE"))
  noticeFile.set(layout.projectDirectory.file("distribution/NOTICE"))
  projectGroup.set("org.apache.polaris")
  validationReport.set(layout.buildDirectory.file("reports/licensee/attribution-validation.txt"))
}

tasks.named("check") { dependsOn("verifyLicenseAttribution") }

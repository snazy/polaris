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

package org.apache.polaris.gradle.sbom

import org.apache.polaris.gradle.sbom.PackageKind.QUARKUS_FAST_JAR
import org.apache.polaris.gradle.sbom.PackageKind.QUARKUS_UBER_JAR
import org.apache.polaris.gradle.sbom.PackageKind.SHADED_JAR
import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.gradle.CyclonedxPlugin
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFile
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.provider.Provider
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType

/** Polaris producer and publication wiring; keep separate. */
@Suppress("unused")
fun Project.configureDistributionSbom() {
  // The producers own SBOM generation. One finalizer supplies the final artifact
  // identity/selection.
  val subject = subjectForGradleProject(project)
  if (subject.packageKind == SHADED_JAR) {
    pluginManager.apply(CyclonedxPlugin::class.java)
    tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom") {
      includeConfigs.set(listOf("runtimeClasspath"))
      includeBomSerialNumber.set(false)
      includeBuildSystem.set(false)
      xmlOutput.unsetConvention()
    }
  }

  val shadowInputs = extra

  val finalSbom =
    tasks.register<FinalizeDistributionSbom>("finalizeDistributionSbom") {
      group = "build"
      description = "Adds final subject identity and actual assembly inputs to the producer SBOM"
      this.subject.set(subject)
      when (subject.packageKind) {
        SHADED_JAR -> {
          dependsOn("createPolarisSparkJar")
          val sbomShadow = SbomShadow.sbomShadowFromExtra(shadowInputs)
          subjectFile.set(sbomShadow.archive)
          selectedFiles.from(sbomShadow.selected)
          metadataByPath.set(sbomShadow.metadata)
          minimized.set(sbomShadow.minimized)
          producer.set(
            tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom").flatMap { it.jsonOutput },
          )
        }
        QUARKUS_FAST_JAR -> {
          dependsOn("quarkusBuild")
          subjectTree.set(layout.buildDirectory.dir("quarkus-app"))
          treeDescriptor.set(layout.buildDirectory.file("reports/sbom/subject.json"))
          producer.set(layout.buildDirectory.file("quarkus-run-cyclonedx.json"))
        }
        QUARKUS_UBER_JAR -> {
          dependsOn("quarkusBuild")
          subjectFile.set(layout.buildDirectory.file("${subject.nameAndVersion}-runner.jar"))
          producer.set(
            layout.buildDirectory.file("${subject.nameAndVersion}-runner-cyclonedx.json"),
          )
        }
        else -> error("Binary archives use their own adapter")
      }
      document.set(layout.buildDirectory.file("reports/sbom/${subject.sbomFilename}"))
    }

  val distributionLicenseCheck = tasks.named("checkDistributionLicenses")

  afterEvaluate {
    // Realize publications outside lazy publication actions, and retain the existing license gate.
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>().forEach {
      publication ->
      if (subject.packageKind.uberLike) {
        publication.artifact(finalSbom.flatMap { it.document }) {
          classifier = subject.sbomClassifier
          extension = "json"
        }
      }
      publication.artifacts.configureEach { builtBy(distributionLicenseCheck) }
    }
  }
}

private fun subjectForGradleProject(project: Project): SbomSubject =
  SbomSubject(
    project.group.toString(),
    project.name,
    project.version.toString(),
    if (project.plugins.hasPlugin("polaris-spark-license-checks")) {
      SHADED_JAR
    } else {
      QUARKUS_FAST_JAR
    },
    if (project.plugins.hasPlugin("polaris-spark-license-checks")) "bundle" else "",
  )

private class SbomShadow(
  val archive: Provider<RegularFile>,
  val selected: Provider<ConfigurableFileCollection>,
  val metadata: Provider<Map<String, Map<String, String>>>,
  val minimized: Provider<Boolean>,
) {
  companion object {
    @Suppress("UNCHECKED_CAST")
    fun sbomShadowFromExtra(extra: ExtraPropertiesExtension): SbomShadow {
      // Produced in build-logic/src/main/kotlin/polaris-spark-license-checks.gradle.kts
      val sbomShadow = extra["sbomShadow"] as Map<String, Any>
      return SbomShadow(
        sbomShadow["archive"] as Provider<RegularFile>,
        sbomShadow["selected"] as Provider<ConfigurableFileCollection>,
        sbomShadow["metadata"] as Provider<Map<String, Map<String, String>>>,
        sbomShadow["minimized"] as Provider<Boolean>,
      )
    }
  }
}

/** Binary archive sidecars are explicit release inputs; ordinary check stays license-only. */
fun Project.configureBinaryArchiveSboms() {
  val documents =
    listOf("Zip" to PackageKind.BINARY_ZIP, "Tar" to PackageKind.BINARY_TAR).map {
      (kind, packageKind) ->
      val archive = tasks.named<AbstractArchiveTask>("dist$kind")
      tasks.register<FinalizeBinaryArchiveSbom>("finalizeDist${kind}Sbom") {
        group = "build"
        description = "Finalizes the SBOM for the actual binary $kind archive"
        dependsOn("checkDistributionLicenses")
        this.archive.set(archive.flatMap { it.archiveFile })
        adminTree.fileProvider(
          configurations.getByName("adminDistribution").elements.map { it.single().asFile },
        )
        serverTree.fileProvider(
          configurations.getByName("serverDistribution").elements.map { it.single().asFile },
        )
        adminProducer.fileProvider(
          configurations.getByName("adminSbom").elements.map { it.single().asFile },
        )
        serverProducer.fileProvider(
          configurations.getByName("serverSbom").elements.map { it.single().asFile },
        )
        subject.set(
          SbomSubject(
            project.group.toString(),
            "polaris-bin",
            project.version.toString(),
            packageKind,
          ),
        )
        document.set(
          layout.buildDirectory.file(
            "distributions/polaris-bin-${project.version}${packageKind.sbomBasenameSuffix}",
          ),
        )
      }
    }
  tasks.register("assembleDistributionSboms") {
    group = "build"
    description = "Produces SBOM sidecars for binary ZIP and TAR distributions"
    dependsOn(documents)
  }
}

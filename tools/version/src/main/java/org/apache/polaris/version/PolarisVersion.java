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
package org.apache.polaris.version;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

public final class PolarisVersion {
  private PolarisVersion() {}

  /** The version string as in the file {@code version.txt} in the project's root directory. */
  public static String polarisVersionString() {
    return POLARIS_VERSION;
  }

  /**
   * Flag whether the build includes extended version information like HEAD commit ID of the build.
   */
  public static boolean hasGitInfo() {
    return BUILD_INFO.containsKey(MF_IS_RELEASE);
  }

  /** Flag whether the build is a released version. */
  public static boolean isReleasedVersion() {
    return "true".equals(BUILD_INFO.get(MF_IS_RELEASE));
  }

  /**
   * Returns the version as in the jar manifest, if {@linkplain #hasGitInfo() build-time Git
   * information} is available.
   *
   * <p>Example value: {@code 1.0.0-incubating-SNAPSHOT}
   */
  public static Optional<String> getBuildReleasedVersion() {
    return Optional.ofNullable(BUILD_INFO.get(MF_VERSION));
  }

  /**
   * Returns the commit ID as in the jar manifest, if {@linkplain #hasGitInfo() build-time Git
   * information} is available.
   *
   * <p>Example value: {@code d417725ec7c88c1ee8f940ffb2ce72aec7fb2a17}
   */
  public static Optional<String> getBuildGitHead() {
    return Optional.ofNullable(BUILD_INFO.get(MF_BUILD_GIT_HEAD));
  }

  /**
   * Returns the {@code git describe} as in the jar manifest, if {@linkplain #hasGitInfo()
   * build-time Git information} is available.
   *
   * <p>Example value: {@code d417725ec-dirty}
   */
  public static Optional<String> getBuildGitDescribe() {
    return Optional.ofNullable(BUILD_INFO.get(MF_BUILD_GIT_DESCRIBE));
  }

  /**
   * Returns the Java version used during the build as in the jar manifest, if {@linkplain
   * #hasGitInfo() build-time Git information} is available.
   *
   * <p>Example value: {@code 21.0.5}
   */
  public static Optional<String> getBuildJavaVersion() {
    return Optional.ofNullable(BUILD_INFO.get(MF_BUILD_JAVA_VERSION));
  }

  /**
   * Returns information about the system that performed the build, if {@linkplain #hasGitInfo()
   * build-time Git information} is available.
   *
   * <p>Example value: {@code 21.0.5}
   */
  public static Optional<String> getBuildSystem() {
    return Optional.ofNullable(BUILD_INFO.get(MF_BUILD_SYSTEM));
  }

  /**
   * Returns the build timestamp as in the jar manifest, if {@linkplain #hasGitInfo() build-time Git
   * information} is available.
   *
   * <p>Example value: {@code 2024-12-16-11:54:05+01:00}
   */
  public static Optional<String> getBuildTimestamp() {
    return Optional.ofNullable(BUILD_INFO.get(MF_BUILD_TIMESTAMP));
  }

  public static String readNoticeFile() {
    return readResource("NOTICE");
  }

  public static String readSourceLicenseFile() {
    return readResource("LICENSE");
  }

  public static String readBinaryLicenseFile() {
    return readResource("LICENSE-BINARY-DIST");
  }

  private static final String POLARIS_VERSION;

  private static final Map<String, String> BUILD_INFO;

  private static final String MF_VERSION = "Apache-Polaris-Version";
  private static final String MF_IS_RELEASE = "Apache-Polaris-Is-Release";
  private static final String MF_BUILD_GIT_HEAD = "Apache-Polaris-Build-Git-Head";
  private static final String MF_BUILD_GIT_DESCRIBE = "Apache-Polaris-Build-Git-Describe";
  private static final String MF_BUILD_TIMESTAMP = "Apache-Polaris-Build-Timestamp";
  private static final String MF_BUILD_SYSTEM = "Apache-Polaris-Build-System";
  private static final String MF_BUILD_JAVA_VERSION = "Apache-Polaris-Build-Java-Version";
  private static final List<String> MF_ALL =
      List.of(
          MF_VERSION, MF_IS_RELEASE, MF_BUILD_GIT_HEAD, MF_BUILD_GIT_DESCRIBE, MF_BUILD_TIMESTAMP);

  private static String readResource(String resource) {
    var fullResource = format("/META-INF/resources/apache-polaris/%s.txt", resource);
    var resourceUrl =
        requireNonNull(
            PolarisVersion.class.getResource(fullResource),
            "Resource " + fullResource + " does not exist");
    try (InputStream in = resourceUrl.openConnection().getInputStream()) {
      return new String(in.readAllBytes(), UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource " + fullResource + " resource", e);
    }
  }

  static {
    var polarisVersionResource =
        requireNonNull(
            PolarisVersion.class.getResource("polaris-version.properties"),
            "Resource org/apache/polaris/version/polaris-version.properties containing the Apache Polaris version does not exist");
    try (InputStream in = polarisVersionResource.openConnection().getInputStream()) {
      Properties p = new Properties();
      p.load(in);
      POLARIS_VERSION = p.getProperty("polaris.version");
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to load Apache Polaris version from org/apache/polaris/version/polaris-version.properties resource",
          e);
    }

    if ("jar".equals(polarisVersionResource.getProtocol())) {
      var path = polarisVersionResource.toString();
      var jarSep = path.lastIndexOf('!');
      if (jarSep == -1) {
        throw new IllegalStateException(
            "Could not determine the jar of the Apache Polaris version artifact: " + path);
      }
      var manifestPath = path.substring(0, jarSep + 1) + "/META-INF/MANIFEST.MF";
      try {
        try (InputStream in = new URL(manifestPath).openConnection().getInputStream()) {
          var manifest = new Manifest(in);
          var attributes = manifest.getMainAttributes();

          BUILD_INFO =
              MF_ALL.stream()
                  .filter(attributes::containsKey)
                  .collect(Collectors.toMap(Function.identity(), attributes::getValue));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      BUILD_INFO = Map.of();
    }
  }
}

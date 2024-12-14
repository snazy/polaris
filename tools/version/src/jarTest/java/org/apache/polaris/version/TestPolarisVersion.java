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
import static org.apache.polaris.version.PolarisVersion.getBuildGitDescribe;
import static org.apache.polaris.version.PolarisVersion.getBuildGitHead;
import static org.apache.polaris.version.PolarisVersion.getBuildJavaVersion;
import static org.apache.polaris.version.PolarisVersion.getBuildReleasedVersion;
import static org.apache.polaris.version.PolarisVersion.getBuildSystem;
import static org.apache.polaris.version.PolarisVersion.getBuildTimestamp;
import static org.apache.polaris.version.PolarisVersion.hasGitInfo;
import static org.apache.polaris.version.PolarisVersion.isReleasedVersion;
import static org.apache.polaris.version.PolarisVersion.polarisVersionString;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPolarisVersion {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void versionAvailable() {
    soft.assertThat(polarisVersionString()).isNotBlank();
    soft.assertThat(isReleasedVersion()).isFalse();
    soft.assertThat(hasGitInfo()).isFalse();
    soft.assertThat(getBuildReleasedVersion()).isEmpty();
    soft.assertThat(getBuildTimestamp()).isEmpty();
    soft.assertThat(getBuildGitHead()).isEmpty();
    soft.assertThat(getBuildGitDescribe()).isEmpty();
    soft.assertThat(getBuildJavaVersion()).isEmpty();
    soft.assertThat(getBuildSystem()).isEmpty();
  }

  @ParameterizedTest
  @MethodSource
  public void noticeLicense(String name, Supplier<String> supplier) throws Exception {
    var supplied = supplier.get();
    var expected =
        Files.readString(Paths.get(format("%s/%s", System.getProperty("rootProjectDir"), name)));
    soft.assertThat(supplied).isEqualTo(expected);
  }

  static Stream<Arguments> noticeLicense() {
    return Stream.of(
        Arguments.arguments("NOTICE", (Supplier<String>) PolarisVersion::readNoticeFile),
        Arguments.arguments("LICENSE", (Supplier<String>) PolarisVersion::readSourceLicenseFile),
        Arguments.arguments(
            "LICENSE-BINARY-DIST", (Supplier<String>) PolarisVersion::readBinaryLicenseFile));
  }
}

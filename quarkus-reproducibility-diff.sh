#! /bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

DATE="$(date +'%Y-%m-%d-%H-%M-%S')"
DIRECTORY="comparisons/${DATE}"

mkdir -p "${DIRECTORY}"

args=(
"--init-script"
"gradle/quarkus-local.init.gradle.kts"
)

# Build "everything", so the subsequent builds don't have to do it again
./gradlew \
  "${args[@]}" \
  :polaris-server:quarkusBuild \
  :polaris-admin:quarkusBuild

./gradlew \
  "${args[@]}" \
  --no-build-cache \
  :polaris-server:clean \
  :polaris-server:quarkusBuild \
  :polaris-admin:clean \
  :polaris-admin:quarkusBuild
mv runtime/server/build/quarkus-app "${DIRECTORY}"/polaris-server-1
mv runtime/admin/build/quarkus-app "${DIRECTORY}"/polaris-admin-1

./gradlew \
  "${args[@]}" \
  --no-build-cache \
  :polaris-server:clean \
  :polaris-server:quarkusBuild \
  :polaris-admin:clean \
  :polaris-admin:quarkusBuild
mv runtime/server/build/quarkus-app "${DIRECTORY}"/polaris-server-2
mv runtime/admin/build/quarkus-app "${DIRECTORY}"/polaris-admin-2

diffoscope \
  "${DIRECTORY}"/polaris-server-1 \
  "${DIRECTORY}"/polaris-server-2 \
  --html-dir "${DIRECTORY}"/polaris-server-diff \
  --exclude-directory-metadata=recursive || true

diffoscope \
  "${DIRECTORY}"/polaris-admin-1 \
  "${DIRECTORY}"/polaris-admin-2 \
  --html-dir "${DIRECTORY}"/polaris-admin-diff \
  --exclude-directory-metadata=recursive || true

xdg-open "${DIRECTORY}"/polaris-server-diff/index.html
xdg-open "${DIRECTORY}"/polaris-admin-diff/index.html

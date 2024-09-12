#!/usr/bin/env bash
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
bin_dir="$(dirname "$0")"
command_name="$(basename "$0")"
. "${bin_dir}/_releases_lib.sh"

function usage {
  cat << EOF
${command_name} [--major MAJOR_VERSION] [--minor MINOR_VERSION] [--commit GIT_COMMIT] [--recreate] [--dry-run] [--help | -h]

  Promotes a release candidate to a final version.

  This command looks up the latest release candidate for the latest patch version of the major/minor version
  and promote it. Promotion will fail, if a promoted tag (no '-RCx' suffix) for the latest patch version
  already exists.

  Options:
    --major MAJOR_VERSION
        Major version number, must be specified when the command is called on the main branch.
    --minor MINOR_VERSION
        Minor version number, must be specified when the command is called on the main branch
        or on a major version branch.
    --commit GIT_COMMIT
        The Git commit to draft the release from. Defaults to the current HEAD.
    --recreate
        Recreates the draft release if it already exists.
    --dry-run
        Do not update Git. Gradle will publish to the local Maven repository, but still sign the artifacts.
    -h --help
        Print usage information.
EOF
}

dry_run=
new_major_version="${version_major}"
new_minor_version="${version_minor}"
while [[ $# -gt 0 ]]; do
  case $1 in
    --major)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --major, aborting" > /dev/stderr
        exit 1
      fi
      new_major_version="$1"
      shift
      ;;
    --minor)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --minor, aborting" > /dev/stderr
        exit 1
      fi
      new_minor_version="$1"
      shift
      ;;
    --recreate)
      recreate=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option/argument $1" > /dev/stderr
      usage > /dev/stderr
      exit 1
      ;;
  esac
done

new_tag_name=""
from_tag_name=""
version_full=""
case "${branch_type}" in
  "main")
    if [[ -z ${new_major_version} ]]; then
      echo "On the main branch, but specified no major version using the '--major' argument, aborting" > /dev/stderr
      exit 1
    fi
    if [[ -z ${new_minor_version} ]]; then
      echo "On the main branch, but specified no minor version using the '--minor' argument, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  "major")
    if [[ ${version_major} -ne ${new_major_version} ]]; then
      echo "On the major version branch ${version_major}, but specified '--major ${new_major_version}', must be on a the matching version branch, aborting" > /dev/stderr
      exit 1
    fi
    if [[ -z ${new_minor_version} ]]; then
      echo "On the major version branch ${version_major}, but specified no minor version using the '--minor' argument, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  "minor")
    if [[ ${version_major} -ne ${new_major_version} || ${version_minor} -ne ${new_minor_version} ]]; then
      echo "On the minor version branch ${version_major}, but specified '--major ${new_major_version}', must be on a the matching version branch, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  *)
    echo "Unexpected branch type ${branch_type}" > /dev/stderr
    exit 1
esac

patch_version="$(get_max_patch_version) ${new_major_version} ${new_minor_version}"
rc_iteration=
if [[ $patch_version -eq -1 ]]; then
  # that patch version is released
  echo "Version ${new_major_version}.${new_minor_version}.x has no drafted patch release, aborting" > /dev/stderr
  exit 1
else
  rc_iteration="$(get_next_rc_iteration)"
  version_full="${new_major_version}.${new_minor_version}.${patch_version}"
  if [[ $rc_iteration -eq -2 ]]; then
    # that patch version is released
    echo "Version ${version_full} is already released, aborting" > /dev/stderr
    exit 1
  elif [[ $rc_iteration -eq -1 ]]; then
    echo "Unexpected result -1 from get_next_rc_iteration function, aborting" > /dev/stderr
    exit 1
  fi
fi
from_tag_name="${tag_prefix}${version_full}-RC${rc_iteration}"
new_tag_name="${tag_prefix}${version_full}"

echo ""
if [[ ${dry_run} ]]; then
  echo "Dry run, no changes will be made - except the versioned docs updates for local inspection"
else
  echo "Non-dry run, will update Git"
fi

cd "${bin_dir}/../.."
echo "Changed to directory $(pwd)"

echo ""
echo "From tag name is: ${from_tag_name}"
echo "New tag name is:  ${new_tag_name}"
echo ""
git log -n1 "${from_tag_name}"
echo ""

if [[ -z ${new_tag_name} || -z ${from_tag_name} ]]; then
  echo "Empty tag to create - internal error, aborting" > /dev/stderr
  exit 1
fi

exec_process 0 git checkout "${create_from_commit}"
trap "git checkout ${current_branch}" EXIT



[[ ${CI} ]] && echo "::group::Releasing staging repository"
if [[ ${CI} ]]; then

  if [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} || -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] ; then
    echo "One or more of the following required environment variables are missing:" > /dev/stderr
    [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} ]] && echo "  ORG_GRADLE_PROJECT_sonatypeUsername" > /dev/stderr
    [[ -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] && echo "  ORG_GRADLE_PROJECT_sonatypePassword" > /dev/stderr
    exit 1
  fi

  stagingRepositoryId="$(cat releases/current-release-staging-repository-id)"
  exec_process ${dry_run} ./gradlew releaseApacheStagingRepository --staging-repository-id "${stagingRepositoryId}" --stacktrace
else
  # Don't release anything when running locally, just print the statement
  exec_process 1 ./gradlew releaseApacheStagingRepository --staging-repository-id "<staging-repository-id>" --stacktrace
fi
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Create Git tag ${new_tag_name}"
exec_process ${dry_run} git tag "${new_tag_name}" "${from_tag_name}"
exec_process ${dry_run} git push "${upstream_name}" "${new_tag_name}"
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Generate release version docs"
cd "${bin_dir}/../../site"
echo "Changed to directory $(pwd)"

echo "Checking out released version docs..."
bin/checkout-releases.sh

# Copy release docs content
rel_docs_dir="content/releases/${version_full}"
mkdir "${rel_docs_dir}"
echo "Copying docs from content/in-dev/unreleased to ${rel_docs_dir}..."
(cd content/in-dev/unreleased ; tar cf - .) | (cd "${rel_docs_dir}" ; tar xf -)

# Copy release_index.md template as _index.md for the new release
versioned_docs_index_md="content/releases/${version_full}/_index.md"
echo "Updating released version ${versioned_docs_index_md}..."
had_marker=
while read -r ln ; do
  if [[ "${ln}" == "# RELEASE_INDEX_MARKER_DO_NOT_CHANGE" ]]; then
    had_marker=1
    cat << EOF
---
$(cat ../codestyle/copyright-header-hash.txt)
title: 'Polaris v${version_full}'
date: $(date --iso-8601=seconds)
params:
  release_version: "${version_full}"
cascade:
  exclude_search: false
EOF
  fi
  [[ ${had_marker} ]] && echo "${ln}"
done < content/in-dev/release_index.md > "${versioned_docs_index_md}"
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Announcement email"
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo "Suggested announcement email subject:"
echo "====================================="
echo ""
echo "[ANNOUNCE] Apache Polaris release ${version_full}"
echo ""
echo ""
echo ""
echo "Suggested announcement email body:"
echo "=================================="
echo ""
cat << EOF
Hi everyone,

I'm pleased to announce the release of Apache Polaris ${version_full}!

Apache Polaris is an open-source, fully-featured catalog for Apache Iceberg™. It implements Iceberg's REST API,
enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris™,
Apache Flink®, Apache Spark™, StarRocks, and Trino.

This release can be downloaded from: https://dlcdn.apache.org/iceberg/apache-polaris-${version_full}/apache-polaris-${version_full}.tar.gz

Release notes: https://polaris.apache.org/release/${version_full}

Java artifacts are available from Maven Central.

Thanks to everyone for contributing!
EOF
echo ""
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo ""

cd content/releases
echo "Changed to directory $(pwd)"

exec_process ${dry_run} git add .
exec_process ${dry_run} git commit -m "Add versioned docs for release ${version_full}"
exec_process ${dry_run} git push
[[ ${CI} ]] && echo "::endgroup::"




[[ ${CI} ]] && echo "::group::GitHub release"
if [[ ${CI} ]]; then
  exec_process ${dry_run} gh release create "${new_tag_name}" \
    --notes-file "${version_full}/release-notes.md" \
    --title "Apache Polaris ${version_full}"
else
  # GitHub release only created from CI
  exec_process 1 gh release create "${new_tag_name}" \
    --notes-file "${version_full}/release-notes.md" \
    --title "Apache Polaris ${version_full}"
fi
[[ ${CI} ]] && echo "::endgroup::"

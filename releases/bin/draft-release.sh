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

  Creates a release candidate.

  A new release candidate tag is created for the latest patch version for the major/minor version.
  If no RC tag (following the '${tag_prefix}<major>.<minor>.<patch>-RCx' pattern) exists, an RC1
  will be created, otherwise the RC number will be incremented. If the latest patch version is
  already promoted to GA, an RC1 for the next patch version will be created.

  Performs the Maven artifacts publication, Apache source tarball upload. Artifacts are signed, make
  sure to have a compatible GPG key present.

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
recreate=
need_checkout=
new_major_version="${version_major}"
new_minor_version="${version_minor}"
create_from_commit="$(git rev-parse HEAD)"
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
    --commit)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --commit, aborting" > /dev/stderr
        exit 1
      fi
      create_from_commit="$1"
      need_checkout=1
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
version_full=""
case "${branch_type}" in
  "main")
    if [[ -z ${new_major_version} || -z ${new_minor_version} ]]; then
      echo "On the main branch, but specified no major and/or minor version using the '--major'/'--minor' arguments, aborting" > /dev/stderr
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

max_patch="$(get_max_patch_version ${new_major_version} ${new_minor_version})"
patch_version=
rc_iteration=
if [[ $max_patch -eq -1 ]]; then
  # No previous patch release
  patch_version=0
  rc_iteration=1
else
  max_rc="$(get_next_rc_iteration)"
  if [[ $max_rc -eq -2 ]]; then
    # that patch version is released, increase patch version
    patch_version="$(( $max_patch + 1))"
    rc_iteration=1
  elif [[ $max_rc -eq -1 ]]; then
    echo "Unexpected result -1 from get_next_rc_iteration function, aborting" > /dev/stderr
    exit 1
  else
    rc_iteration="$(( $max_rc + 1 ))"
  fi
fi

version_full="${new_major_version}.${new_minor_version}.${patch_version}"
new_tag_name="${tag_prefix}${version_full}-RC${rc_iteration}"

echo ""
gradleDryRunSigning=""
gradleReleaseArgs=""
if [[ ${dry_run} ]]; then
  echo "Dry run, no changes will be made"

  # Only sign + use the GPG agent locally
  [[ ${CI} ]] || gradleDryRunSigning="-PsignArtifacts -PuseGpgAgent"
else
  echo "Non-dry run, will update Git"

  # Verify that the required secrets for the Maven publication are present.
  if [[ ${CI} ]]; then
    # Only publish to "Apache" from CI
    gradleReleaseArgs="-Prelease publishToApache closeApacheStagingRepository"

    if [[ -z ${ORG_GRADLE_PROJECT_signingKey} || -z ${ORG_GRADLE_PROJECT_signingPassword} || -z ${ORG_GRADLE_PROJECT_sonatypeUsername} || -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] ; then
      echo "One or more of the following required environment variables are missing:" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_signingKey} ]] && echo "  ORG_GRADLE_PROJECT_signingKey" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_signingPassword} ]] && echo "  ORG_GRADLE_PROJECT_signingPassword" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} ]] && echo "  ORG_GRADLE_PROJECT_sonatypeUsername" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] && echo "  ORG_GRADLE_PROJECT_sonatypePassword" > /dev/stderr
      exit 1
    fi
  else
    # Only publish to "Apache" from CI, otherwise publish to local Maven repo
    gradleReleaseArgs="-PsignArtifacts -PuseGpgAgent publishToMavenLocal"
  fi
fi

echo ""
echo "New version is:  ${version_full}"
echo "RC iteration:    ${rc_iteration}"
echo "New tag name is: ${new_tag_name}"
echo "From commit:     ${create_from_commit}"
echo ""
git log -n1 "${create_from_commit}"
echo ""

if [[ ${need_checkout} ]]; then
  exec_process 0 git checkout "${create_from_commit}"
  trap "git checkout ${current_branch}" EXIT
fi

if [[ -z ${new_tag_name} ]]; then
  echo "Empty tag to create - internal error, aborting" > /dev/stderr
  exit 1
fi

do_recreate=
if [[ ${recreate} ]]; then
  if list_release_tags "" "" | grep --quiet "${new_tag_name}"; then
    do_recreate=1
  fi
fi

cd "${bin_dir}/../.."
echo "Changed to directory $(pwd)"



[[ ${CI} ]] && echo "::group::Update + commit version.txt"
echo "Executing 'echo -n "${version_full}" > version.txt'"
echo -n "${version_full}" > version.txt
[[ ${dry_run} ]] && trap "git checkout version.txt > /dev/null" EXIT
exec_process ${dry_run} git add version.txt
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Create Git tag ${new_tag_name}"
[[ ${do_recreate} ]] && exec_process ${dry_run} git tag -d "${new_tag_name}"
exec_process ${dry_run} git tag "${new_tag_name}" "${create_from_commit}"
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Gradle publication"
if [[ ${dry_run} ]]; then
  exec_process 0 ./gradlew publishToMavenLocal sourceTarball -PjarWithGitInfo ${gradleDryRunSigning} --stacktrace
  stagingRepositoryId="DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY ID!"
  stagingRepositoryUrl="DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY URL!"
else
  exec_process ${dry_run} ./gradlew ${gradleReleaseArgs} sourceTarball --stacktrace | tee build/gradle-release-build.log

  # Extract staging repository ID from log
  # ... look for the log message similar to 'Created staging repository 'orgprojectnessie-1214' at https://oss.sonatype.org/service/local/repositories/orgprojectnessie-1214/content/'
  stagingLogMsg="$(grep 'Created staging repository' build/gradle-release-build.log)"
  stagingRepositoryId="$(echo $stagingLogMsg | sed --regexp-extended "s/^Created staging repository .([a-z0-9-]+). at (.*)/\1/")"
  stagingRepositoryUrl="$(echo $stagingLogMsg | sed --regexp-extended "s/^Created staging repository .([a-z0-9-]+). at (.*)/\2/")"

  # Memoize the commit-ID, staging-repository-ID+URL for later use and reference information
  # The staging-repository-ID is required for the 'publish-release.sh' script to release the staging repository.
  echo -n "${create_from_commit}" > releases/current-release-commit-id
  echo -n "${stagingRepositoryId}" > releases/current-release-staging-repository-id
  echo -n "${stagingRepositoryUrl}" > releases/current-release-staging-repository-url

  exec_process ${dry_run} git add releases/current-release-*
fi
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Upload source tarball"
# TODO Upload source tarball
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Create release notes"
releaseNotesFile="site/content/in-dev/unreleasd/release-notes.md"
# TODO release notes
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Commit changes to Git"
exec_process ${dry_run} git commit -m "[RELEASE] Version ${version_full}-RC${rc_iteration}

Base release commit ID: ${create_from_commit}
Staging repository ID: ${stagingRepositoryId}
Staging repository URL: ${stagingRepositoryUrl}
"
tag_commit_id="$(git rev-parse HEAD)"
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Release vote email"
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo "Suggested release vote email subject:"
echo "====================================="
echo ""
echo "[VOTE] Release Apache Polaris (Incubating) ${version_full}-RC${rc_iteration}"
echo ""
echo ""
echo ""
echo "Suggested Release vote email body:"
echo "=================================="
echo ""
cat << EOF
Hi everyone,

I propose that we release the following RC as the official
Apache Polaris (Incubating) ${version_full} release.

The commit ID is ${tag_commit_id}
* This corresponds to the tag: ${new_tag_name}
* https://github.com/apache/polaris/commits/${new_tag_name}
* https://github.com/apache/polaris/tree/${tag_commit_id}

The release tarball, signature, and checksums are here:
* https://dist.apache.org/repos/dist/dev/polaris/apache-polaris-${version_full}-RC${rc_iteration}

You can find the KEYS file here:
* https://dist.apache.org/repos/dist/dev/polaris/KEYS

Convenience binary artifacts are staged on Nexus. The Maven repository URL is:
* ${stagingRepositoryUrl}

Please download, verify, and test.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Polaris ${version_full}
[ ] +0
[ ] -1 Do not release this because...

Only PMC members have binding votes, but other community members are
encouraged to cast non-binding votes. This vote will pass if there are
3 binding +1 votes and more binding +1 votes than -1 votes.

Thanks
Regards
EOF
echo ""
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo ""
[[ ${CI} ]] && echo "::endgroup::"



[[ ${CI} ]] && echo "::group::Push Git tag ${new_tag_name}"
[[ ${do_recreate} ]] && exec_process ${dry_run} git push "${upstream_name}" --delete "${new_tag_name}"
exec_process ${dry_run} git push "${upstream_name}" "${new_tag_name}"
[[ ${CI} ]] && echo "::endgroup::"



echo ""
if [[ ${dry_run} ]]; then
  echo "Draft-release finished - but dry run was enabled, no changes were made to Git"
else
  echo "Draft-release finished"
fi

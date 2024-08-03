#!/usr/bin/env bash
#
# Copyright (c) 2024 Snowflake Computing Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

set -e

cd "$(dirname "$0")"

docker_bin="$(command -v docker || command -v podman)"

user_gradle_dir="${HOME}/.gradle"

uid="$(id -u)"
gid="$(id -g)"

##  --build-arg USER_UID="$(id -u)" --build-arg USER_GID="$(id -g)" \

${docker_bin} build \
  --userns keep-id \
  --volume="${user_gradle_dir}:/home/default/.gradle:O" \
  "${@}"

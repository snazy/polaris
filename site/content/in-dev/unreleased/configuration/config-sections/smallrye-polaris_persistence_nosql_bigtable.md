---
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
title: smallrye-polaris_persistence_nosql_bigtable
build:
  list: never
  render: never
---

Polaris persistence, Bigtable backend specific configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.bigtable.project-id` |  | `string` |  |
| `polaris.persistence.nosql.bigtable.table-prefix` |  | `string` |  |
| `polaris.persistence.nosql.bigtable.total-api-timeout` |  | `duration` | Total timeout (including retries) for Bigtable API calls.  |
| `polaris.persistence.nosql.bigtable.no-table-admin-client` | `false` | `boolean` |  |
| `polaris.persistence.nosql.bigtable.instance-id` | `polaris` | `string` | Sets the instance-id to be used with Google BigTable.  |
| `polaris.persistence.nosql.bigtable.app-profile-id` |  | `string` | Sets the profile-id to be used with Google BigTable.  |
| `polaris.persistence.nosql.bigtable.quota-project-id` |  | `string` | Google BigTable quota project ID (optional).  |
| `polaris.persistence.nosql.bigtable.endpoint` |  | `string` | Google BigTable endpoint (if not default).  |
| `polaris.persistence.nosql.bigtable.mtls-endpoint` |  | `string` | Google BigTable MTLS endpoint (if not default).  |
| `polaris.persistence.nosql.bigtable.emulator-host` |  | `string` | When using the BigTable emulator, used to configure the host.  |
| `polaris.persistence.nosql.bigtable.emulator-port` | `8086` | `int` | When using the BigTable emulator, used to configure the port.  |
| `polaris.persistence.nosql.bigtable.initial-retry-delay` |  | `duration` | Initial retry delay.  |
| `polaris.persistence.nosql.bigtable.max-retry-delay` |  | `duration` | Max retry-delay.  |
| `polaris.persistence.nosql.bigtable.retry-delay-multiplier` |  | `double` |  |
| `polaris.persistence.nosql.bigtable.max-attempts` |  | `int` | Maximum number of attempts for each Bigtable API call (including retries).  |
| `polaris.persistence.nosql.bigtable.initial-rpc-timeout` |  | `duration` | Initial RPC timeout.  |
| `polaris.persistence.nosql.bigtable.max-rpc-timeout` |  | `duration` |  |
| `polaris.persistence.nosql.bigtable.rpc-timeout-multiplier` |  | `double` |  |
| `polaris.persistence.nosql.bigtable.total-timeout` |  | `duration` | Total timeout (including retries) for Bigtable API calls.  |
| `polaris.persistence.nosql.bigtable.min-channel-count` |  | `int` | Minimum number of gRPC channels. Refer to Google docs for details. |
| `polaris.persistence.nosql.bigtable.max-channel-count` |  | `int` | Maximum number of gRPC channels. Refer to Google docs for details. |
| `polaris.persistence.nosql.bigtable.initial-channel-count` |  | `int` | Initial number of gRPC channels. Refer to Google docs for details |
| `polaris.persistence.nosql.bigtable.min-rpcs-per-channel` |  | `int` | Minimum number of RPCs per channel. Refer to Google docs for details. |
| `polaris.persistence.nosql.bigtable.max-rpcs-per-channel` |  | `int` | Maximum number of RPCs per channel. Refer to Google docs for details. |

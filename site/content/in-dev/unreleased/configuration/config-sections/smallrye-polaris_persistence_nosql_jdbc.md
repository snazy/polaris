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
title: smallrye-polaris_persistence_nosql_jdbc
build:
  list: never
  render: never
---

Polaris persistence, JDBC backend specific configuration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.persistence.nosql.jdbc.url` |  | `string` |  |
| `polaris.persistence.nosql.jdbc.username` |  | `string` |  |
| `polaris.persistence.nosql.jdbc.password` |  | `string` |  |
| `polaris.persistence.nosql.jdbc.initial-pool-size` | `2` | `int` |  |
| `polaris.persistence.nosql.jdbc.min-pool-size` | `2` | `int` |  |
| `polaris.persistence.nosql.jdbc.max-pool-size` | `5` | `int` |  |
| `polaris.persistence.nosql.jdbc.max-lifetime` | `PT5M` | `duration` |  |
| `polaris.persistence.nosql.jdbc.acquisition-timeout` | `PT20S` | `duration` |  |
| `polaris.persistence.nosql.jdbc.transaction-isolation` | `READ_COMMITTED` | <span title="NONE, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE"><code>enum (NONE, READ_UNCOMMITTED, READ_COMMITTED, ...)</code></span> |  |

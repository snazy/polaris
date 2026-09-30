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
title: smallrye-polaris_backend_jdbc
build:
  list: never
  render: never
---

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.backend.jdbc.datasource` |  | `string` | The name of the datasource to use. Must correspond to a configured datasource under `quarkus.datasource.<name>` . Supported values are: `postgresql` `mariadb`, `mysql` and `h2`. If not provided, the default Quarkus datasource, defined using the  `quarkus.datasource.*` configuration keys, will be used (the corresponding driver is  PostgresQL). Note that it is recommended to define "named" JDBC datasources, see [Quarkus JDBC config  reference ](https://quarkus.io/guides/datasource#jdbc-configuration). |

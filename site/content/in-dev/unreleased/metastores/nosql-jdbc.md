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
title: NoSQL JDBC
type: docs
weight: 700
---

{{< alert note >}}
The NoSQL JDBC backend is experimental.
{{< /alert >}}

The NoSQL JDBC backend stores Polaris metadata through a Quarkus-managed JDBC datasource. It is
distinct from Polaris's relational JDBC metastore: select `nosql` persistence and `JDBC` as the
NoSQL backend.

## Basic configuration

The packaged server and admin tool predeclare the `polaris-nosql` PostgreSQL datasource. It
remains inactive until you configure its JDBC URL. Select it for the NoSQL JDBC backend:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=JDBC
polaris.backend.jdbc.datasource=polaris-nosql

quarkus.datasource.polaris-nosql.db-kind=postgresql
quarkus.datasource.polaris-nosql.username=<username>
quarkus.datasource.polaris-nosql.password=<password>
quarkus.datasource.polaris-nosql.jdbc.url=jdbc:postgresql://<host>:5432/<database>
```

The backend supports PostgreSQL, H2, MariaDB, and MySQL (via the MariaDB driver). The packaged
runtime contribution includes PostgreSQL support. For another database, build a deployment that
also includes its matching Quarkus JDBC extension and driver. Datasource names and database kinds
must be declared when building the deployment. See the [Quarkus datasource
guide](https://quarkus.io/guides/datasource) for datasource credentials, pooling, TLS, and driver
configuration.

## Bootstrapping and maintenance

Run the [Admin Tool]({{% ref "../admin-tool" %}}) with the same named datasource configuration as
the service. The database account must be able to create and manage the Polaris tables during
bootstrap, then read and write them at runtime. Use a dedicated database or schema for the
metastore and scope its privileges accordingly.

The generated [JDBC configuration reference]({{% relref
"../configuration/config-sections/smallrye-polaris_backend_jdbc" %}}) covers the Quarkus
datasource selection. Programmatic/standalone use has a separate
`polaris.persistence.nosql.jdbc.*` mapping. Run regular NoSQL maintenance against the same
datasource.

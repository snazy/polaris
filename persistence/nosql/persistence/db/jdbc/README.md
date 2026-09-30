<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# JDBC persistence backend

This module implements the Polaris NoSQL persistence backend backed by a JDBC datasource. For
deployment configuration and bootstrap instructions, see the
[NoSQL JDBC metastore documentation](../../../../../site/content/in-dev/unreleased/metastores/nosql-jdbc.md).

## Runtime integration

`JdbcBackendFactory` supports programmatic use and can create an Agroal datasource from
`polaris.persistence.nosql.jdbc.*` settings. In a Quarkus runtime,
`JdbcBackendBuilder` instead uses a named Quarkus-managed datasource selected by
`polaris.backend.jdbc.datasource`. Configure the datasource under
`quarkus.datasource.<name>.*`; Polaris does not own or close that datasource independently.

The runtime-service smoke test uses a named in-memory H2 datasource. Module tests exercise H2,
PostgreSQL, MariaDB, MySQL, and CockroachDB where applicable:

```bash
./gradlew :polaris-persistence-nosql-jdbc:check
```

## Supported databases

The Quarkus backend accepts PostgreSQL, H2, MariaDB, and MySQL (through the MariaDB driver).
The packaged runtime contribution supplies the PostgreSQL Quarkus JDBC extension; deployments
using another database need a distribution that includes its matching Quarkus JDBC extension and
driver.

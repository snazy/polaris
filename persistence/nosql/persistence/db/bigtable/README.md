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

# Bigtable persistence backend

This module implements the Polaris NoSQL persistence backend backed by Google Cloud Bigtable. For
deployment configuration and bootstrap instructions, see the
[Bigtable metastore documentation](../../../../../site/content/in-dev/unreleased/metastores/nosql-bigtable.md).

## Runtime integration

`BigtableBackendFactory` supports programmatic use. In a Quarkus runtime,
`BigtableBackendBuilder` obtains credentials from the Quarkus Google Cloud Bigtable extension and
constructs the data and table-admin clients from
`polaris.persistence.nosql.bigtable.*` configuration. When `emulator-host` is configured, the
backend uses the Bigtable emulator and deliberately does not require credentials.

The module integration tests and the runtime-service smoke test start the emulator from the Google
Cloud SDK image:

```bash
./gradlew :polaris-persistence-nosql-bigtable:intTest
```

## Bootstrapping

With the table-admin client enabled (the default), Polaris creates and manages its Bigtable tables
during NoSQL bootstrap. The runtime identity needs the corresponding Bigtable data and table-admin
permissions for the configured project and instance. Set `no-table-admin-client=true` only when
tables are provisioned separately and the deployment does not need Polaris to validate or create
them.

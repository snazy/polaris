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

# RocksDB persistence backend

This module implements the Polaris NoSQL persistence backend backed by a local RocksDB database.
For deployment configuration and bootstrap instructions, see the
[RocksDB metastore documentation](../../../../../site/content/in-dev/unreleased/metastores/nosql-rocksdb.md).

## Runtime integration

`RocksDbBackendFactory` supports programmatic use. In a Quarkus runtime,
`RocksDbBackendBuilder` uses `polaris.backend.rocksdb.database-directory` to select the local
database directory. The generic standalone configuration mapping instead uses
`polaris.persistence.nosql.rocksdb.database-directory`; this distinction exists because the
Quarkus mapping is a runtime-specific workaround.

The runtime-service smoke test creates a temporary directory and verifies the Quarkus backend can
bootstrap and use it. The module tests cover the backend directly:

```bash
./gradlew :polaris-persistence-nosql-rocksdb:check
```

## Deployment ownership

The configured directory is the durable state of the metastore. A single Polaris process must own
it, and its containing volume must be persistent across restarts. Do not place the same RocksDB
directory on multiple service instances.

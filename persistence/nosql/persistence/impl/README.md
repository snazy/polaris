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

# NoSQL persistence implementation

This module provides the database-independent implementation of the NoSQL persistence
API. It connects the persistence API to a storage `Backend`; database-specific backend
implementations live in separate modules.

## Components

The main sources are under
[`org.apache.polaris.persistence.nosql.impl`](src/main/java/org/apache/polaris/persistence/nosql/impl/).

| Component | Responsibility |
| --- | --- |
| `PersistenceImplementation` | Implement persistence operations over a backend, object serialization, and parameter validation. |
| `indexes` | Ordered indexes with embedded updates, persisted stripes, and lazy loading. |
| `commits` | Commit construction, synchronization, and retry handling. |
| `cache` | Persistence caching, cache backends, and invalidation support. |

Consumers use the contracts in the
[NoSQL persistence API module](../api/src/main/java/org/apache/polaris/persistence/nosql/api/).
Implementation types are not an application extension API.

## Index documentation

- [Index implementation guide](INDEXES.md): architecture, ownership, configuration,
  version selection, and upgrade behavior.
- [V1 serialization format](V1-FORMAT.md): sequential key compression and value encoding.
- [V2 serialization format](V2-FORMAT.md): block directory, key and value encodings,
  and selective access.

## Configuration

[`PersistenceParams`](../api/src/main/java/org/apache/polaris/persistence/nosql/api/PersistenceParams.java)
defines persistence settings and defaults. The
[index guide](INDEXES.md#version-selection-and-upgrades) explains the compatibility
implications of `preferredIndexWriteVersion()`; its
[related settings](INDEXES.md#related-persistence-settings) section describes index
size and loading controls.

## Build and validation

From the repository root, run the module checks:

```shell
./gradlew :polaris-persistence-nosql-impl:check
```

The checks include unit tests, style verification, and JCStress concurrency tests.
JMH benchmarks live in `src/jmh`; build their executable jar with
`./gradlew :polaris-persistence-nosql-impl:jmhJar`.

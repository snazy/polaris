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
title: NoSQL RocksDB
type: docs
weight: 600
---

{{< alert note >}}
The RocksDB backend is experimental.
{{< /alert >}}

The RocksDB backend stores Polaris metadata in a local RocksDB database. It is suitable only when
a single Polaris service process owns a persistent local volume.

## Basic configuration

In a Quarkus deployment, select the backend and configure a durable directory:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=RocksDb
polaris.backend.rocksdb.database-directory=/var/lib/polaris/rocksdb
```

The directory is the metastore's durable state. Mount it on persistent storage and do not share it
between service instances. Sharing a RocksDB database directory is not a substitute for a
distributed metastore.

The generated [RocksDB configuration reference]({{% relref
"../configuration/config-sections/smallrye-polaris_backend_rocksdb" %}}) documents the Quarkus
runtime mapping shown above. The standalone backend mapping instead uses
`polaris.persistence.nosql.rocksdb.*`.

## Bootstrapping and maintenance

Run the [Admin Tool]({{% ref "../admin-tool" %}}) with the same directory mounted at the same
path as the service. Bootstrap creates the initial metastore state. Run regular NoSQL maintenance
against that same directory; never run the service and maintenance process concurrently against a
shared directory without ensuring exclusive ownership.

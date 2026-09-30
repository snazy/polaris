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
title: NoSQL Bigtable
type: docs
weight: 500
---

{{< alert note >}}
The Bigtable backend is experimental.
{{< /alert >}}

The Bigtable backend stores Polaris metadata in Google Cloud Bigtable. Polaris uses credentials
provided by the Quarkus Google Cloud Bigtable extension; configure Application Default Credentials,
workload identity, or another supported Google credential source for the service identity.

## Basic configuration

Configure the NoSQL backend, Google Cloud project, and Bigtable instance:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=Bigtable
polaris.persistence.nosql.bigtable.project-id=<project-id>
polaris.persistence.nosql.bigtable.instance-id=polaris
```

`table-prefix` optionally prefixes the tables owned by this Polaris deployment. The generated
[Bigtable configuration reference]({{% relref
"../configuration/config-sections/smallrye-polaris_persistence_nosql_bigtable" %}}) describes
timeouts, retry settings, endpoint selection, and the remaining Polaris-specific options.

For local development, configure a Bigtable emulator instead of cloud credentials:

```properties
polaris.persistence.nosql.bigtable.emulator-host=localhost
polaris.persistence.nosql.bigtable.emulator-port=8086
polaris.persistence.nosql.bigtable.project-id=test-project
polaris.persistence.nosql.bigtable.instance-id=test-instance
```

## Bootstrapping and maintenance

By default, Polaris uses the Bigtable table-admin client to create and manage its tables during
NoSQL bootstrap. Run the [Admin Tool]({{% ref "../admin-tool" %}}) with the same project,
instance, and credentials as the service. The identity needs Bigtable data access and table-admin
permission for the configured instance.

Set `no-table-admin-client=true` only when an administrator provisions the tables separately. In
that mode Polaris cannot create or validate them through the table-admin API. Run regular NoSQL
maintenance with an identity that can access the Polaris tables.

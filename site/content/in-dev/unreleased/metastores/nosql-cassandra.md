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
title: NoSQL Cassandra
type: docs
weight: 300
---

The Cassandra backend stores Polaris metadata in Apache Cassandra and uses the Apache Cassandra
Java driver directly. Polaris configures the driver from its normal configuration sources; a
driver `application.conf` file is neither required nor consulted.

## Basic configuration

Configure the NoSQL backend and its Java-driver contact points and local data center:

```properties
polaris.persistence.type=nosql
polaris.persistence.nosql.backend=Cassandra
polaris.persistence.nosql.cassandra.driver.basic.contact-points=cassandra-1.example:9042,cassandra-2.example:9042
polaris.persistence.nosql.cassandra.driver.basic.load-balancing-policy.local-datacenter=dc1
polaris.persistence.nosql.cassandra.keyspace=polaris
```

For plaintext username/password authentication, configure both properties. Polaris uses its
default authentication provider automatically:

```properties
polaris.persistence.nosql.cassandra.auth.username=<username>
polaris.persistence.nosql.cassandra.auth.password=<password>
```

`auth.authorization-id` is available for servers that support proxy authentication. Integrators
that need another mechanism can provide a CDI Java-driver `AuthProvider` qualified with
`@CassandraAuthentication("name")`, then set `auth.provider-name=name`.

The generated [Cassandra configuration reference]({{% relref "../configuration/config-sections/smallrye-polaris_persistence_nosql_cassandra" %}})
lists Polaris-specific configuration. Additional Java-driver options can be set under
`polaris.persistence.nosql.cassandra.driver` using their exact driver option paths. See the
[Apache Cassandra Java Driver configuration manual](https://github.com/apache/cassandra-java-driver/blob/4.x/manual/core/configuration/README.md)
for their semantics. The Polaris-owned request timeout, metric, authentication, and TLS options
cannot be overridden through this subsection.

## TLS and mTLS with Quarkus

In a Quarkus deployment, use the Quarkus TLS Registry. Set the Cassandra
`tls-configuration-name` and configure that named TLS bucket with the CA certificate(s) trusted
for the Cassandra server:

```properties
polaris.persistence.nosql.cassandra.tls-configuration-name=cassandra
quarkus.tls.cassandra.trust-store.pem.certs=/etc/cassandra-tls/ca.crt
```

For mTLS, additionally mount the client certificate and key from a secret and configure them in
the same bucket:

```properties
quarkus.tls.cassandra.key-store.pem.client.cert=/etc/cassandra-tls/client.crt
quarkus.tls.cassandra.key-store.pem.client.key=/etc/cassandra-tls/client.key
```

Quarkus accepts these values from normal runtime configuration sources, including environment
variables and mounted configuration files. Keep Cassandra hostname validation enabled (the
default) and configure a real trust store; `trust-all` and disabled hostname validation are only
appropriate for controlled tests. Do not also configure Java-driver TLS options under
`polaris.persistence.nosql.cassandra.driver`.

For details of the TLS Registry and supported PEM formats, see the
[Quarkus TLS Registry reference](https://quarkus.io/guides/tls-registry-reference/).

## Bootstrapping

Before using the backend, bootstrap the metastore with the Polaris Admin Tool. See the
[Admin Tool]({{% ref "../admin-tool" %}}) documentation for the bootstrap command and other
administrative operations.

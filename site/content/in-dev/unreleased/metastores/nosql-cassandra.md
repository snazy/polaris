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

Before using the backend, a DBA must create the configured Cassandra keyspace with the
deployment-appropriate replication strategy. The backend creates its tables but does not create
the keyspace.

The managed schema requires Apache Cassandra 5.0 or later and uses the Unified Compaction
Strategy (UCS). Run schema initialization from exactly one administrative process at a time on
Cassandra versions before 6.0; `CREATE TABLE IF NOT EXISTS` does not make concurrent DDL safe on
those versions.

The following are the complete table definitions that Polaris creates. Replace `<keyspace>` with
the configured keyspace name when running them manually.

```sql
CREATE TABLE IF NOT EXISTS <keyspace>.refs
(
  r text,
  n text,
  p blob,
  c bigint,
  t blob,
  PRIMARY KEY ((r, n))
) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4'};

CREATE TABLE IF NOT EXISTS <keyspace>.objs
(
  r text,
  i blob,
  t text,
  v text,
  d blob,
  c bigint,
  q int,
  PRIMARY KEY ((r, i))
) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'T4'};
```

`objs` primarily stores immutable metadata objects retrieved by primary key. `refs` stores small,
frequently read metadata rows that are conditionally updated. Neither table is a TTL-based
time-series workload. `T4` is the UCS tiered baseline for these point-lookup workloads. Operators
should review the [UCS tuning guidance](https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/ucs.html)
against their read/write ratio, read amplification, and compaction capacity; a more leveled
setting can be appropriate when read latency dominates.

### Cassandra 4.x compatibility

Cassandra 4.x does not provide UCS. To use the backend with Cassandra 4.x, create both tables
yourself before running the Polaris Admin Tool, using the definitions above with the compaction
clause replaced by:

```sql
WITH compaction = {'class': 'SizeTieredCompactionStrategy'};
```

When the tables already exist, Polaris validates the required columns and primary key but does
not issue its Cassandra 5.0 UCS DDL. This is a compatibility path; Cassandra 5.0 is the supported
baseline for new deployments.

Then bootstrap the metastore with the Polaris Admin Tool. See the
[Admin Tool]({{% ref "../admin-tool" %}}) documentation for the bootstrap command and other
administrative operations.

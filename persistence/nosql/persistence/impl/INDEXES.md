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

# NoSQL persistence indexes

The index implementation maps ordered `IndexKey` keys to values encoded by an
`IndexValueSerializer`. Indexes support point lookups, bounded forward and reverse
iteration, and forward prefix queries. Persistence exposes read-only views through
`Persistence.buildReadIndex` and updates through `Persistence.buildWriteIndex`.

## Reading the index implementation

The index implementation is in
[`src/main/java/org/apache/polaris/persistence/nosql/impl/indexes/`](src/main/java/org/apache/polaris/persistence/nosql/impl/indexes/).

An `IndexContainer` contains serialized embedded entries and may reference additional
persisted index stripes. Embedded entries override referenced entries. A null-valued
embedded entry can act as a deletion marker for a referenced entry; physically removing
an entry and retaining a deletion marker are different operations.

| Component | Responsibility |
| --- | --- |
| `IndexesProvider`, `IndexesInternal` | Build views, select readers by stored version, and select writers by write preference. |
| `ReadOnlyIndexImpl`, `IndexImpl` | Read-only and mutable implementations of [serialization version 1](V1-FORMAT.md). |
| `ReadOnlyV2IndexImpl`, `MutableV2IndexImpl`, `V2IndexWriter` | Read-only view, mutation overlay, and block writer for [serialization version 2](V2-FORMAT.md). |
| `ReadOnlyLayeredIndexImpl`, `AbstractLayeredIndexImpl` | Combine referenced entries and overriding updates. |
| `StripedIndexImpl`, `LazyIndexImpl`, `IndexLoader` | Locate and load persisted stripes, including bulk loading. |
| `UpdatableIndexImpl`, `LazyUpdatableIndex` | Coordinate updates, embedded data, spill thresholds, and persistence. |
| `IndexSpi`, `InternalIndexElement` | Implementation contracts for index views and entries. |

Serialization versions describe individual embedded indexes or stripes, not the entire
container. A container can reference stripes using different supported versions.
The format documents specify the bytes; array layouts, caches, block targets, and
mutation strategies are implementation choices.

## Ownership and lazy decoding

The read-only implementations borrow the supplied serialized storage. They use
independent buffer positions, but a read-only buffer does not make its backing storage
immutable. Callers must keep those bytes unchanged for the lifetime of the view.
Both heap and direct buffers are supported.

Read-only views support concurrent reads and publish lazily decoded state with atomic
or volatile access. Decoded keys and values are memoized within a view. Mutable views
require exclusive ownership. Do not mutate a view while another thread reads it, or
share a single iterator between threads.

The V1 read-only constructor scans every entry to locate keys and values, retaining
compact metadata while deferring object decoding. V2 uses a persisted directory and
per-block offset tables to locate entries without scanning every serialized value.
Its mutable implementation tracks changes over an immutable base. Serialization can
copy untouched blocks and encoded values, and rebuild only affected portions.

## Version selection and upgrades

[`PersistenceParams`](../api/src/main/java/org/apache/polaris/persistence/nosql/api/PersistenceParams.java)
defines `preferredIndexWriteVersion()`, exposed in deployment configuration as
`polaris.persistence.preferred-index-write-version`. It accepts `1` or `2` and defaults
to `1`. This is a write preference, not a limit on readable versions: this
implementation always reads both versions. Internal factory callers pass the
preference explicitly.

| Preference | New indexes | Rewrites with V1-only inputs | Rewrites inheriting V2 |
| --- | --- | --- | --- |
| `1` (default) | V1 | V1 | V2 |
| `2` | V2 | V2 | V2 |

Changing the preference does not scan or migrate stored indexes. V1-to-V2 promotion
occurs when a write requires rewriting an affected component. Unchanged containers
and stripes may retain their original bytes and identities. During a spill, V2
embedded data also raises the write version of rewritten stripes to V2, even with
preference `1`. A container can therefore retain a mixture of V1 and V2 stripes.

Keep preference `1` while any active node only supports V1. Enable V2 writes only
after every active cluster node and every binary intended for rollback can read V2.
The preference does not coordinate cluster readiness. Reverting it to `1` does not
convert stored V2 data back to V1, including when entries are deleted, and does not
restore compatibility with V1-only binaries.

See [NoSQL index formats](../../../../site/content/in-dev/unreleased/metastores/nosql-index-format.md)
for deployment and rollback guidance.

## Related persistence settings

The following `PersistenceParams` methods control index sizing or related persistence
operations. Consult
the linked interface for current defaults and types rather than treating this guide
as a separate configuration reference.

| Method | Effect |
| --- | --- |
| `maxEmbeddedIndexSize()` | Size threshold used to decide when embedded updates spill into persisted stripes. |
| `maxIndexStripeSize()` | Target serialized size used to split oversized stripes. It is separate from the number of entries in a V2 block. |
| `bucketizedBulkFetchSize()` | Batch size for calls to `Persistence.bucketizedBulkFetches`; it does not set V2 block size. |
| `maxSerializedValueSize()` | Backend row-value size limit used by persistence; it is distinct from the index stripe target. |

`maxIndexStripes()` is also defined in `PersistenceParams`, but the current index
implementation does not enforce it as a stripe-count limit. Neither the embedded nor
stripe size setting selects the serialization version or changes the V2 wire format.

## Validation and maintenance

Tests in this module cover format compatibility, malformed inputs, positioned and
direct buffers, layered and striped indexes, mutations, and V1-to-V2 promotion.
Concurrent read publication is also exercised by JCStress. JMH benchmarks cover read,
mutation, and serialization workloads.

Run the module checks from the repository root:

```shell
./gradlew :polaris-persistence-nosql-impl:check
```

When changing a format or reader, update its format document and compatibility tests.
Keep writer defaults distinct from requirements imposed on all valid serialized data.

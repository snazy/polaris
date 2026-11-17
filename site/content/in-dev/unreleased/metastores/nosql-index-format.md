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
title: NoSQL index formats
type: docs
weight: 205
---

Polaris can read NoSQL index formats V1 and V2. Index writes use
`polaris.persistence.preferred-index-write-version`, which accepts `1` or `2`
and defaults to `1`. This setting applies to NoSQL persistence.

```properties
polaris.persistence.preferred-index-write-version=1
```

| Preference | New indexes | Rewrites with V1-only inputs | Rewrites inheriting V2 |
|---|---|---|---|
| `1` (default) | V1 | V1 | V2 |
| `2` | V2 | V2 | V2 |

Reads support both formats with either preference. Changing the preference does
not scan or migrate stored indexes. Existing data is converted when a write
requires rewriting the affected component; untouched containers and stripes can
retain their original bytes. Stored indexes can therefore contain components
using different supported versions.

Rewrites inherit the format of contributing data. During a spill, a stored V2
embedded index raises the write floor for rewritten stripes. A V1 stripe can
therefore become V2 even with preference `1` when rewritten together with V2
data. Untouched stripes can retain their original format and identity.

## Rolling upgrades and rollback

Keep preference `1` while any active node only understands V1. Before enabling
`2`, deploy a V2-capable binary to every active node and ensure that every intended
rollback binary can also read V2. Then explicitly set the preference to `2`
through deployment configuration and restart nodes as usual.

Lowering the preference to `1` keeps new indexes and rewrites with V1-only inputs
on V1. Rewrites inheriting V2 still use V2, including V1 stripes merged with V2
embedded data. It does not restore compatibility with a
V1-only binary. Deleting all entries from an index also does not guarantee that
its stored format is downgraded. Once V2 data has been persisted, rolling restart
and rollback require V2-capable readers.

This option does not coordinate cluster readiness or provide a reverse migration.
Operators must choose when the cluster and its rollback targets are ready for V2.

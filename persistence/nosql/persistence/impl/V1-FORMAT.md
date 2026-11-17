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

# Index serialization version 1

This document describes one serialized index, whether embedded in an `IndexContainer`
or stored as an index stripe. Container framing and value-specific encodings are
outside this format. See the [index implementation guide](INDEXES.md) for implementation and
upgrade behavior.

## Primitive encodings

`u8` is an unsigned byte. `varint` encodes a nonnegative integer in groups of seven
payload bits, least significant group first. Bit `0x80` indicates that another byte
follows. Counts and lengths must fit the implementation's nonnegative Java `int` range.

A full encoded key is the output of `IndexKey.serialize`: its underlying bytes,
with the following escaping, followed by the unescaped terminator `0x01`.

| Underlying byte | Encoded bytes |
| --- | --- |
| `0x01` | `0x02 0x03` |
| `0x02` | `0x02 0x04` |
| Other bytes, including `0x00` | Unchanged |

Keys are byte sequences, not necessarily text. String keys use UTF-8, and composite
names can contain zero separators. Escaping and termination apply to the whole key.
`IndexKey` limits decoded keys to 500 bytes; the index implementation also bounds a
reconstructed encoded key to 4096 bytes. Keys in an index are non-null, unique, and
ordered by `IndexKey.compareTo`.

Values use the `IndexValueSerializer` supplied by the caller. The index does not store
a serializer identifier. The caller must use the serializer appropriate to that index,
including its null encoding.

## Layout

```text
u8       version = 1
varint   entry_count

if entry_count > 0:
  bytes  first full encoded key, including terminator
  bytes  first encoded value

for each remaining entry, in key order:
  varint strip
  bytes  encoded key suffix, including terminator
  bytes  encoded value
```

An empty index contains only the version and a zero entry count.
There is no directory, per-entry length table, or trailing checksum.

Let `previous` be the complete encoded previous key. To reconstruct a subsequent key:

```text
prefix_length = length(previous) - strip
current = previous[0 : prefix_length] + suffix
```

`strip` must be nonnegative and no greater than the previous encoded key length.
The suffix runs through its unescaped key terminator. Compression operates on encoded
bytes, including any escape bytes, rather than on characters or name components.
A writer can store a full subsequent key by stripping the entire previous encoded key.
The first key has no preceding `strip` field.

For example, ignoring the final terminators in this explanation, changing `abc` to
`abd` retains `ab`: strip the previous `c` and terminator, then append `d` and a new
terminator. The encoded record is `02 64 01`.

Value boundaries are determined by the serializer's `skip` or `deserialize` method.
Exactly `entry_count` entries must consume the index payload; trailing bytes are not
part of a valid index.

## Access implications

Locating arbitrary entries requires an initial scan because key and value offsets are
not stored. The read-only implementation records compact offsets and key dependencies
during that scan. It then reconstructs keys and decodes values on demand, memoizing
results per view. These metadata and memoization choices do not change the V1 bytes.

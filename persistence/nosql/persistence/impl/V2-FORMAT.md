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

# Index serialization version 2

V2 divides an ordered index into blocks and stores a directory of their first keys.
This allows readers to locate a block without first scanning all entries. Per-block
key and value boundaries support selective decoding. Writers can reuse unchanged
blocks and values when applying a small set of updates.

These properties can reduce read latency and allocation, particularly for large
indexes accessed at a few keys. They add directory and offset metadata, and some
write workloads require more work than the simpler V1 stream. V2 does not guarantee
smaller serialized data or faster execution for every workload.

This document specifies a single embedded index or stripe. See the
[index implementation guide](INDEXES.md) for ownership, version selection, and upgrade behavior,
and [V1](V1-FORMAT.md#primitive-encodings) for the shared `varint`, full encoded key,
key limits, ordering, and value serializer conventions.

## Primitive encodings and overall layout

`u8`, `u16`, and `i32` are fixed-width big-endian integers. The first two are unsigned;
`i32` fields used as counts or offsets must be nonnegative. File offsets are relative
to the first version byte, independent of the input buffer's original position.

```text
u8       version = 2
varint   total_entry_count
i32      directory_offset
bytes    blocks, in key order
bytes    directory
```

There is no magic field or checksum. The supplied buffer limit defines the end of the
index. An empty index still contains the header and an empty directory.

### Why fixed-width fields?

`directory_offset` is an `i32` so a writer can reserve four bytes before writing the
blocks and fill in the final directory position without changing the header size.
With a varint, a larger final offset could require more header bytes, moving the
blocks and changing the very offset being encoded. Precomputing sizes could resolve
that dependency, but a fixed-width pointer also permits writers that discover the
directory position as they emit blocks. Readers decode the entry count once, then
read the pointer directly.

The directory descriptors use three `i32` fields so each descriptor is exactly
12 bytes. Readers can access any descriptor or field by arithmetic during binary
search, without decoding preceding descriptors or building an offset table in memory.
`block_count` is also fixed-width, keeping the directory header a constant size.
Its width is a simplicity choice rather than a requirement for binary search.
Nonnegative `i32` values match the implementation's `ByteBuffer` offsets and ordinal
range.

These choices spend a bounded amount of metadata per block to support direct access.
The entry count is read sequentially only once, so a varint is appropriate there.
Within blocks, the potentially larger key-end and value-end tables instead use a
uniform width of one, two, or four bytes per table. That preserves arithmetic access
while avoiding four-byte fields when the payload fits a narrower width.

## Directory

```text
i32      block_count
u8       directory_key_encoding   # 0 = full, 1 = prefix-compressed
u8       directory_restart        # 1 for full, otherwise 1..255

repeat block_count times:
  i32    block_offset
  i32    first_entry_ordinal
  i32    directory_key_offset

bytes    directory key records, in block order
```

Ordinals are zero-based. The first block starts immediately after the header and its
first ordinal is zero. Subsequent block offsets, first ordinals, and key offsets
strictly increase. The next block offset gives the previous block's end; the final
block ends at `directory_offset`. Ordinal differences give block entry counts, with
the final count derived from `total_entry_count`. Each block contains 1..4096 entries.
An empty index has no blocks, block bytes, descriptors, or key records.

Directory key records follow the descriptors immediately. Each record ends at the
next key offset, or the index limit for the last record. A record represents the
block's first full encoded key; that key is not duplicated in the block payload.

For encoding `0`, each record is a full encoded key. For encoding `1`, each record is:

```text
varint   retained_prefix_length
varint   suffix_length
bytes    suffix[suffix_length]
```

Reconstruction retains that many bytes from the previous reconstructed directory key
and appends the suffix. Every `directory_restart`th record, starting with record zero,
has zero retained prefix. Other records may also use zero prefix. Suffixes are
nonempty and consume the rest of the record.

Directory keys permit a first binary search across blocks. A second search locates
an entry within the selected block.

## Blocks

For a block with `N` entries:

```text
u8       flags
u16      common_prefix_length    # only if flag 0x01 or 0x80 is set
uint     key_ends[N - 1]          # width selected by flags
bytes    key_records
uint     value_ends[N]           # only for ENDS value mode
bytes    encoded_values
```

The flags have these meanings:

| Bits | Meaning |
| --- | --- |
| `0x01` | Independent key suffixes sharing a block prefix. |
| `0x80` | Front-coded key suffixes sharing a block prefix. |
| `0x02` / `0x20` | Key-end width 4 / 1 byte; neither means 2 bytes. |
| `0x04` / `0x40` | Value-end width 4 / 1 byte; neither means 2 bytes. |
| `0x18` | Value mode: `0x00` = ENDS, `0x10` = STRIDE. Other modes are unsupported. |

The two key encoding flags are mutually exclusive. Each pair of width flags is also
mutually exclusive. One- and two-byte ends are unsigned; four-byte ends must be
nonnegative. Ends are cumulative byte offsets relative to their own payload section,
not file offsets, and must remain within that section.

There are no key records or key ends for entry zero: its key comes from the directory.
For entries 1 through `N - 1`, successive key ends delimit each record. The final key
end gives the size of the key payload. A singleton block has no key payload.

### Key encodings

With flag `0x01`, let `P` be `common_prefix_length`. A key record is the remaining
encoded key bytes, without a length or prefix field. Reconstruct it by prepending the
first `P` bytes of the directory key. No previous entry needs to be reconstructed.

With neither key encoding flag set, each record contains:

```text
varint   retained_prefix_length
bytes    nonempty encoded key suffix
```

The retained length refers to the previous full encoded key, unlike V1's count of
bytes to strip. Zero denotes a full-key restart. The offset table supplies record
boundaries, so there is no separate suffix length.

With flag `0x80`, `common_prefix_length` must be positive. Each record has the same
shape as a front-coded record, but the retained prefix length is additional to the
block's common prefix. Reconstruction retains `common_prefix_length +
retained_prefix_length` bytes from the previous key, then appends the suffix.
A zero retained length restarts from the common prefix of the directory key.

Every reconstructed key must be a valid full encoded `IndexKey`. Prefix lengths must
not exceed the available predecessor bytes. Restart placement is a writer choice:
blocks have no stored restart interval and need not have equal entry counts.

### Value encodings

Values remain opaque serializer bytes; they are not prefix-compressed across entries.
Their boundaries use one of two modes:

- **ENDS:** `N` cumulative value ends precede the value payload. The first value
  starts at offset zero, and the final end equals the value payload length.
- **STRIDE:** There is no value-end table. All entries have the same encoded length,
  derived by dividing the bytes remaining in the block by `N`. Division must be exact.
  Equal lengths are a property of this block's bytes, not a requirement on the
  serializer's type. The value-width bits do not add metadata in this mode.

The first key is stored in the directory, but its value is the first value in the
block. Null values use the caller's serializer, just as in V1.

## Writer choices and reuse

The current writer defaults to front coding without a common-prefix header, a target
of 64 entries per block, key restarts every eight entries, and full directory keys.
Readers also accept the other encodings described above. These defaults do not impose
additional format restrictions.

Writers can split blocks with added entries during serialization. Removing every
entry removes that block. Directory descriptors are rebuilt to reflect the resulting
offsets and ordinals. Unchanged block bytes can be copied intact. Value-only changes
can retain the key encoding; structural changes can require rebuilding key records
and their boundaries.

## Validation and compatibility

Construction validates the header and directory structure. Blocks and key records
are checked as accessed, so successful construction alone does not validate every
payload byte. Serializers remain responsible for interpreting value bytes.

V2 is distinguished by the leading version byte. V1-only readers cannot read V2;
selecting V1 as the preferred write version does not demote an existing V2 index.

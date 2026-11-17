/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.persistence.nosql.impl.indexes;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import java.nio.ByteBuffer;

/**
 * V2 wire contract. All fixed-width integers are big-endian and offsets are relative to the index
 * start. The file is: version byte 2, nonnegative varint entry count, i32 directory offset, block
 * bytes, then directory. Empty V2 indexes still have this framing.
 *
 * <p>The directory is i32 block count, u8 encoding (0 full / 1 prefix), u8 restart (1 for full,
 * otherwise 1..255), then 12-byte descriptors (i32 block offset, first ordinal, directory-key
 * offset). First ordinal is zero; subsequent offsets/ordinals strictly increase. The final block
 * ends at the directory and final directory key ends at the file limit. Full directory keys are
 * serialized IndexKeys. Compressed records contain nonnegative prefix/suffix varints and suffix
 * bytes; restart records have zero prefix. A block's first key occurs only in the directory.
 *
 * <p>A block has 1..4096 entries, u8 flags, optional u16 common-prefix length, N-1 cumulative key
 * ends, key payload, value metadata and value payload. Flags: 0x01 independent suffix keys, 0x80
 * common prefix plus front coding (mutually exclusive), 0x02/0x20 key-end width 4/1, 0x04/0x40
 * value-end width 4/1 (default width 2; conflicting width bits invalid), and bits 3..4 select
 * ENDS=0 or STRIDE=2. Other value modes are unsupported. The prefix length exists only with
 * 0x01/0x80; 0x80 requires a positive prefix. Unflagged keys use front coding without that field.
 * Front-coded records contain a nonnegative prefix varint followed by nonempty suffix; independent
 * suffix records use the common prefix. Full reconstructed keys obey the V1 key limit.
 *
 * <p>ENDS has N cumulative value ends; STRIDE divides the entire remaining payload evenly among N
 * entries and has no value metadata. Widths describe unsigned 1/2-byte or nonnegative 4-byte
 * offsets. All bounds remain within the corresponding section. Key restarts are writer choices,
 * encoded by zero front-prefix length, not a required interval or a header field.
 *
 * <p>Parsing is lazy: directory structure is checked at construction; block and record bounds are
 * checked when accessed. Index storage must remain unchanged while borrowed by a view. Earlier
 * experimental version-2 layouts are not supported compatibility formats.
 */
final class V2IndexFormat {
  static final int VERSION = 2;
  static final int HEADER = 1;
  static final int DIRECTORY_HEADER = 6;
  static final int DESCRIPTOR = 12;
  static final int DIRECTORY_POINTER = 4;
  static final int FRONT_PREFIX = 128;
  static final int PREFIX = 1;
  static final int KEY_WIDE = 2;
  static final int VALUE_WIDE = 4;
  static final int KEY_BYTE = 32;
  static final int VALUE_BYTE = 64;
  static final int MODE_SHIFT = 3;
  static final int ENDS = 0;
  static final int STRIDE = 2;

  static final int GROUP = 8;

  enum DirectoryEncoding {
    FULL,
    PREFIX
  }

  enum Layout {
    PREFIX_ENDS,
    FRONT_ENDS,
    FRONT_PREFIX_ENDS
  }

  enum BlockSplit {
    MAXIMUM_REMAINDER,
    BALANCED
  }

  record Options(
      Layout layout,
      int blockEntries,
      DirectoryEncoding directory,
      int keyRestart,
      BlockSplit split) {
    Options(Layout layout, int blockEntries, DirectoryEncoding directory, int keyRestart) {
      this(layout, blockEntries, directory, keyRestart, BlockSplit.MAXIMUM_REMAINDER);
    }

    Options {
      checkArgument(blockEntries > 0 && blockEntries <= 4096, "Invalid block target");
      checkArgument(keyRestart > 0 && keyRestart <= 4096, "Invalid key restart");
      requireNonNull(layout);
      requireNonNull(directory);
      requireNonNull(split);
    }
  }

  static final Options DEFAULT = new Options(Layout.FRONT_ENDS, 64, DirectoryEncoding.FULL, 8);

  /**
   * Conservative singleton bound for the production FULL-directory preset, including ENDS metadata.
   */
  static long singleEntrySerializedSizeUpperBound(int keyBytes, int valueBytes) {
    return (long) fileHeaderSize(1)
        + DIRECTORY_HEADER
        + DESCRIPTOR
        + headerSize(PREFIX)
        + Integer.BYTES
        + keyBytes
        + valueBytes;
  }

  static int fileHeaderSize(int count) {
    return 1 + varIntLen(count) + DIRECTORY_POINTER;
  }

  static int width(int size) {
    return size <= 255 ? 1 : size <= 65535 ? 2 : 4;
  }

  static void putOffset(ByteBuffer output, int value, int width) {
    checkArgument(width == 1 || width == 2 || width == 4, "Invalid offset width");
    checkArgument(
        value >= 0 && (width == 4 || value < (1 << (width * 8))), "Offset does not fit width");
    if (width == 1) output.put((byte) value);
    else if (width == 2) output.putShort((short) value);
    else output.putInt(value);
  }

  static int commonPrefix(byte[] first, byte[] second) {
    int length = Math.min(first.length, second.length), prefix = 0;
    while (prefix < length && first[prefix] == second[prefix]) prefix++;
    return prefix;
  }

  private V2IndexFormat() {}

  static int headerSize(int flags) {
    return HEADER + ((flags & (PREFIX | FRONT_PREFIX)) == 0 ? 0 : 2);
  }

  static int offset(ByteBuffer source, int at, int width) {
    return width == 1
        ? Byte.toUnsignedInt(source.get(at))
        : width == 2 ? Short.toUnsignedInt(source.getShort(at)) : source.getInt(at);
  }

  static void copy(ByteBuffer output, ByteBuffer source, int at, int length) {
    int position = output.position();
    output.put(position, source, at, length);
    output.position(position + length);
  }

  // The upper word is the next position, the lower word the decoded nonnegative value.
  static long varIntAt(ByteBuffer source, int at) {
    int value = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      int next = Byte.toUnsignedInt(source.get(at++));
      checkArgument(shift != 28 || next <= 7, "Oversized integer");
      value |= (next & 127) << shift;
      if (next < 128) return ((long) at << 32) | value;
    }
    throw new IllegalArgumentException("Invalid integer");
  }
}

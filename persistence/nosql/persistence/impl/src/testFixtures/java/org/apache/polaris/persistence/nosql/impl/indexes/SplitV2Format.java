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

import java.nio.ByteBuffer;

/** Shared wire mechanics for the isolated greenfield V2 experiments, not a production format. */
final class SplitV2Format {
  static final int MAGIC = 0x53505632;
  static final int HEADER = 7;
  static final int GROUP = 8;
  static final int PREFIX = 1;
  static final int KEY_WIDE = 2;
  static final int VALUE_WIDE = 4;
  static final int KEY_BYTE = 32;
  static final int VALUE_BYTE = 64;
  static final int MODE_SHIFT = 3;
  static final int ENDS = 0;
  static final int LENGTHS = 1;
  static final int STRIDE = 2;

  enum Layout {
    FRONT_ENDS,
    FRONT_LENGTHS,
    PREFIX_ENDS
  }

  record Options(
      Layout layout,
      int blockEntries,
      V2IndexWriter.DirectoryEncoding directory,
      int keyRestart,
      boolean partitionAtPrefix) {
    Options(
        Layout layout,
        int blockEntries,
        V2IndexWriter.DirectoryEncoding directory,
        int keyRestart) {
      this(layout, blockEntries, directory, keyRestart, true);
    }

    Options(Layout layout, int blockEntries, V2IndexWriter.DirectoryEncoding directory) {
      this(layout, blockEntries, directory, GROUP);
    }

    Options {
      checkArgument(blockEntries > 0 && blockEntries <= 4096, "Invalid block target");
      checkArgument(keyRestart > 0 && keyRestart <= 4096, "Invalid key restart");
    }
  }

  private SplitV2Format() {}

  static int width(int size) {
    return size <= 255 ? 1 : size <= 65535 ? 2 : 4;
  }

  static int offset(ByteBuffer source, int at, int width) {
    return width == 1
        ? Byte.toUnsignedInt(source.get(at))
        : width == 2 ? Short.toUnsignedInt(source.getShort(at)) : source.getInt(at);
  }

  static void putOffset(ByteBuffer output, int value, int width) {
    if (width == 1) output.put((byte) value);
    else if (width == 2) output.putShort((short) value);
    else output.putInt(value);
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

  static int commonPrefix(byte[] first, byte[] second) {
    int length = Math.min(first.length, second.length);
    int prefix = 0;
    while (prefix < length && first[prefix] == second[prefix]) prefix++;
    return prefix;
  }
}

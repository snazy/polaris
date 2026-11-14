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
import static org.apache.polaris.persistence.varint.VarInt.putVarInt;
import static org.apache.polaris.persistence.varint.VarInt.varIntLen;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;

/** Fixture-only v2 writer. No production format selection or configuration is installed. */
final class V2IndexWriter {
  enum DirectoryEncoding {
    FULL,
    PREFIX
  }

  record Options(
      int blockEntries, boolean uneven, DirectoryEncoding encoding, int directoryRestart) {
    Options {
      checkArgument(blockEntries > 0, "Positive block target required");
      checkArgument(directoryRestart > 0 && directoryRestart <= 255, "Invalid directory restart");
    }

    static Options defaults(int blockEntries) {
      return new Options(blockEntries, false, DirectoryEncoding.FULL, 8);
    }
  }

  private record Block(int offset, int ordinal, byte[] key) {}

  private V2IndexWriter() {}

  static <V> ByteBuffer write(IndexSpi<V> index, IndexValueSerializer<V> codec, Options options) {
    var entries = new ArrayList<InternalIndexElement<V>>();
    index.elementIterator().forEachRemaining(entries::add);
    var blocks = new ArrayList<Block>();
    var previous = ByteBuffer.allocate(IndexImpl.MAX_KEY_BYTES);
    var current = ByteBuffer.allocate(IndexImpl.MAX_KEY_BYTES);
    int position = 1 + varIntLen(entries.size()) + Integer.BYTES;
    int inBlock = 0;
    // Size once, then serialize directly into one exactly sized output array.
    for (int ordinal = 0; ordinal < entries.size(); ordinal++) {
      var entry = entries.get(ordinal);
      entry.key().serialize(current.clear());
      current.flip();
      int shared =
          ordinal == 0
              ? 0
              : commonPrefix(previous.array(), previous.limit(), current.array(), current.limit());
      boolean start =
          ordinal == 0
              || inBlock >= options.blockEntries
              || (options.uneven
                  && inBlock >= Math.max(1, options.blockEntries / 2)
                  && shared * 2 < Math.min(previous.limit(), current.limit()));
      if (start) {
        blocks.add(new Block(position, ordinal, Arrays.copyOf(current.array(), current.limit())));
        inBlock = 0;
      } else {
        position = Math.addExact(position, varIntLen(previous.limit() - shared));
        position = Math.addExact(position, current.limit() - shared);
      }
      position = Math.addExact(position, entry.contentSerializedSize(codec));
      var swap = previous;
      previous = current;
      current = swap;
      inBlock++;
    }
    int directory = position;
    position = Math.addExact(position, Math.addExact(6, Math.multiplyExact(blocks.size(), 12)));
    var keyOffsets = new int[blocks.size()];
    var prefixes = options.encoding == DirectoryEncoding.PREFIX ? new int[blocks.size()] : null;
    byte[] preceding = new byte[0];
    for (int i = 0; i < blocks.size(); i++) {
      var key = blocks.get(i).key;
      keyOffsets[i] = position;
      int prefix = 0;
      if (prefixes != null) {
        prefix =
            i % options.directoryRestart == 0
                ? 0
                : commonPrefix(preceding, preceding.length, key, key.length);
        if (prefix < 2) prefix = 0;
        prefixes[i] = prefix;
        position = Math.addExact(position, varIntLen(prefix) + varIntLen(key.length - prefix));
      }
      position = Math.addExact(position, key.length - prefix);
      preceding = key;
    }
    var output = ByteBuffer.allocate(position);
    output.put((byte) 2);
    putVarInt(output, entries.size());
    output.putInt(directory);
    int nextBlock = 0;
    for (int ordinal = 0; ordinal < entries.size(); ordinal++) {
      var entry = entries.get(ordinal);
      entry.key().serialize(current.clear());
      current.flip();
      if (nextBlock < blocks.size() && ordinal == blocks.get(nextBlock).ordinal) {
        checkArgument(output.position() == blocks.get(nextBlock).offset, "Block size mismatch");
        nextBlock++;
      } else {
        int shared =
            commonPrefix(previous.array(), previous.limit(), current.array(), current.limit());
        putVarInt(output, previous.limit() - shared);
        output.put(current.array(), shared, current.limit() - shared);
      }
      entry.serializeContent(codec, output);
      var swap = previous;
      previous = current;
      current = swap;
    }
    checkArgument(output.position() == directory, "Body size mismatch");
    output.putInt(blocks.size());
    output.put((byte) options.encoding.ordinal());
    output.put((byte) (prefixes == null ? 1 : options.directoryRestart));
    for (int i = 0; i < blocks.size(); i++) {
      var block = blocks.get(i);
      output.putInt(block.offset).putInt(block.ordinal).putInt(keyOffsets[i]);
    }
    for (int i = 0; i < blocks.size(); i++) {
      var key = blocks.get(i).key;
      int prefix = prefixes == null ? 0 : prefixes[i];
      if (prefixes != null) {
        putVarInt(output, prefix);
        putVarInt(output, key.length - prefix);
      }
      output.put(key, prefix, key.length - prefix);
    }
    checkArgument(!output.hasRemaining(), "Directory size mismatch");
    return output.flip().asReadOnlyBuffer();
  }

  private static int commonPrefix(byte[] a, int aLength, byte[] b, int bLength) {
    int limit = Math.min(aLength, bLength);
    int shared = 0;
    while (shared < limit && a[shared] == b[shared]) shared++;
    return shared;
  }
}

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
package org.apache.polaris.persistence.nosql.bigtable;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;

import com.google.protobuf.ByteString;
import java.time.Duration;

final class BigtableConstants {
  static final String FAMILY_REFS = "r";
  static final String FAMILY_OBJS = "o";

  static final ByteString QUALIFIER_OBJ_TYPE = copyFromUtf8(COL_OBJ_TYPE);
  static final ByteString QUALIFIER_OBJ_VALUE = copyFromUtf8(COL_OBJ_VALUE);
  static final ByteString QUALIFIER_OBJ_VERSION = copyFromUtf8(COL_OBJ_VERSION);
  static final ByteString QUALIFIER_OBJ_CREATED_AT = copyFromUtf8(COL_OBJ_CREATED_AT);
  static final ByteString QUALIFIER_OBJ_REAL_PART_NUM = copyFromUtf8(COL_OBJ_REAL_PART_NUM);
  static final ByteString QUALIFIER_REF_POINTER = copyFromUtf8(COL_REF_POINTER);
  static final ByteString QUALIFIER_REF_CREATED_AT = copyFromUtf8(COL_REF_CREATED_AT);
  static final ByteString QUALIFIER_REF_PREVIOUS = copyFromUtf8(COL_REF_PREVIOUS);

  // Tue Apr 7 08:14:21 2020 +0200
  static final long CELL_TIMESTAMP = 1586232861000L;

  static final int MAX_PARALLEL_READS = 5;
  static final int MAX_BULK_READS = 100;
  static final int MAX_BULK_MUTATIONS = 1000;

  static final Duration DEFAULT_BULK_READ_TIMEOUT = Duration.ofSeconds(5);

  private BigtableConstants() {}
}

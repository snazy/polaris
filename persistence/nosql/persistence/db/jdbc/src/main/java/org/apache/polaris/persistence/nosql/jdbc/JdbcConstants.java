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
package org.apache.polaris.persistence.nosql.jdbc;

import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_ID;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_REAL_PART_NUM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_TYPE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VALUE;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_OBJ_VERSION;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REALM;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_CREATED_AT;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_NAME;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_POINTER;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.COL_REF_PREVIOUS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_OBJS;
import static org.apache.polaris.persistence.nosql.impl.Identifiers.TABLE_REFS;

import java.util.List;

final class JdbcConstants {
  private JdbcConstants() {}

  static final int MAX_BATCH_SIZE = 50;

  static final String ADD_REFERENCE =
      "INSERT INTO "
          + TABLE_REFS
          + " ("
          + COL_REALM
          + ", "
          + COL_REF_NAME
          + ", "
          + COL_REF_POINTER
          + ", "
          + COL_REF_CREATED_AT
          + ", "
          + COL_REF_PREVIOUS
          + ") VALUES (?, ?, ?, ?, ?)";
  static final String UPDATE_REFERENCE_POINTER =
      "UPDATE "
          + TABLE_REFS
          + " SET "
          + COL_REF_POINTER
          + "=?, "
          + COL_REF_PREVIOUS
          + "=? WHERE "
          + COL_REALM
          + "=? AND "
          + COL_REF_NAME
          + "=? AND "
          + COL_REF_POINTER
          + "*PTR* AND "
          + COL_REF_CREATED_AT
          + "=?";
  static final String FIND_REFERENCES =
      "SELECT "
          + COL_REF_NAME
          + ", "
          + COL_REF_POINTER
          + ", "
          + COL_REF_CREATED_AT
          + ", "
          + COL_REF_PREVIOUS
          + " FROM "
          + TABLE_REFS
          + " WHERE "
          + COL_REALM
          + "=? AND "
          + COL_REF_NAME
          + " IN (?)";

  static final List<String> ALL_OBJ_KEY_LIST = List.of(COL_REALM, COL_OBJ_ID);

  static final List<String> ALL_OBJ_COLS_LIST =
      List.of(
          COL_OBJ_TYPE, COL_OBJ_VERSION, COL_OBJ_VALUE, COL_OBJ_CREATED_AT, COL_OBJ_REAL_PART_NUM);

  static final String ALL_OBJ_KEYS = String.join(", ", ALL_OBJ_KEY_LIST);

  static final String ALL_OBJ_COLS = String.join(", ", ALL_OBJ_COLS_LIST);

  static final String ALL_OBJ_COLS_WITH_PK = ALL_OBJ_KEYS + ", " + ALL_OBJ_COLS;

  static final String OBJ_WHERE_CLAUSE = "WHERE " + COL_REALM + "=? AND " + COL_OBJ_ID + "=?";

  static final String OBJ_WHERE_CLAUSE_CONDITIONAL = " AND " + COL_OBJ_VERSION + "=?";

  static final String FIND_OBJS =
      "SELECT "
          + ALL_OBJ_COLS_WITH_PK
          + " FROM "
          + TABLE_OBJS
          + " WHERE "
          + COL_REALM
          + "=? AND "
          + COL_OBJ_ID
          + " IN (?)";

  static final String INSERT_OBJ =
      "INSERT INTO " + TABLE_OBJS + " (" + ALL_OBJ_COLS_WITH_PK + ") VALUES (?, ?, ?, ?, ?, ?, ?)";

  static final String UPDATE_OBJ =
      "UPDATE "
          + TABLE_OBJS
          + " SET "
          + COL_OBJ_VERSION
          + "=?, "
          + COL_OBJ_VALUE
          + "=?, "
          + COL_OBJ_CREATED_AT
          + "=?, "
          + COL_OBJ_REAL_PART_NUM
          + "=? "
          + OBJ_WHERE_CLAUSE;

  static final String UPDATE_OBJ_CONDITIONAL = UPDATE_OBJ + OBJ_WHERE_CLAUSE_CONDITIONAL;

  static final String DELETE_OBJ = "DELETE FROM " + TABLE_OBJS + " " + OBJ_WHERE_CLAUSE;

  static final String DELETE_OBJ_CONDITIONAL = DELETE_OBJ + OBJ_WHERE_CLAUSE_CONDITIONAL;

  static final String PURGE_REALMS_OBJS =
      "DELETE FROM " + TABLE_OBJS + " WHERE " + COL_REALM + " IN (?)";
  static final String PURGE_REALMS_REFS =
      "DELETE FROM " + TABLE_REFS + " WHERE " + COL_REALM + " IN (?)";

  static final String BULK_DELETE_OBJS =
      "DELETE FROM " + TABLE_OBJS + " WHERE " + COL_REALM + " = ? AND " + COL_OBJ_ID + " IN (?)";
  static final String BULK_DELETE_REFS =
      "DELETE FROM " + TABLE_REFS + " WHERE " + COL_REALM + " = ? AND " + COL_REF_NAME + " IN (?)";

  static final String SCAN_REFS =
      "SELECT "
          + COL_REALM
          + ", "
          + COL_REF_NAME
          + ", "
          + COL_REF_CREATED_AT
          + " FROM "
          + TABLE_REFS;
  static final String SCAN_OBJS =
      "SELECT "
          + COL_REALM
          + ", "
          + COL_OBJ_ID
          + ", "
          + COL_OBJ_TYPE
          + ", "
          + COL_OBJ_CREATED_AT
          + " FROM "
          + TABLE_OBJS;
}

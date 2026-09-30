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

import jakarta.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public final class DatabaseSpecifics {
  private DatabaseSpecifics() {}

  public static DatabaseSpecific detect(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection()) {
      return detect(conn);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private static DatabaseSpecific detect(Connection conn) {
    try {
      String productName = conn.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
      switch (productName) {
        case "h2":
          return new H2DatabaseSpecific();
        case "postgresql":
          try (ResultSet rs = conn.getMetaData().getSchemas(conn.getCatalog(), "crdb_internal")) {
            if (rs.next()) {
              return new CockroachDatabaseSpecific();
            } else {
              return new PostgresDatabaseSpecific();
            }
          }
        case "mysql":
          return new MySqlDatabaseSpecific();
        case "mariadb":
          return new MariaDBDatabaseSpecific();
        default:
          throw new IllegalStateException(
              "Could not select specifics to use for database product '" + productName + "'");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  abstract static class BasePostgresDatabaseSpecific implements DatabaseSpecific {

    /** Integrity constraint violation error code, as returned by H2, Postgres &amp; Cockroach. */
    private static final String CONSTRAINT_VIOLATION_SQL_CODE = "23505";

    /** Deadlock error, returned by Postgres. */
    private static final String DEADLOCK_SQL_STATE_POSTGRES = "40P01";

    /** Already exists error, returned by Postgres, H2 and Cockroach. */
    private static final String ALREADY_EXISTS_STATE_POSTGRES = "42P07";

    /**
     * Cockroach "retry, write too old" error, see <a
     * href="https://www.cockroachlabs.com/docs/v21.1/transaction-retry-error-reference.html#retry_write_too_old">Cockroach's
     * Transaction Retry Error Reference</a>, and Postgres may return a "deadlock" error.
     */
    private static final String RETRY_SQL_STATE_COCKROACH = "40001";

    private final Map<JdbcColumnType, String> typeMap;
    private final Map<JdbcColumnType, Integer> typeIdMap;

    BasePostgresDatabaseSpecific(String varcharType, int objIdType) {
      typeMap = new EnumMap<>(JdbcColumnType.class);
      typeIdMap = new EnumMap<>(JdbcColumnType.class);
      typeMap.put(JdbcColumnType.NAME, varcharType);
      typeIdMap.put(JdbcColumnType.NAME, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID, "BYTEA");
      typeIdMap.put(JdbcColumnType.OBJ_ID, objIdType);
      typeMap.put(JdbcColumnType.BOOL, "BOOLEAN");
      typeIdMap.put(JdbcColumnType.BOOL, Types.BOOLEAN);
      typeMap.put(JdbcColumnType.VARBINARY, "BYTEA");
      typeIdMap.put(JdbcColumnType.VARBINARY, Types.BINARY);
      typeMap.put(JdbcColumnType.BIGINT, "BIGINT");
      typeIdMap.put(JdbcColumnType.BIGINT, Types.BIGINT);
      typeMap.put(JdbcColumnType.INT, "INT");
      typeIdMap.put(JdbcColumnType.INT, Types.INTEGER);
      typeMap.put(JdbcColumnType.VARCHAR, varcharType);
      typeIdMap.put(JdbcColumnType.VARCHAR, Types.VARCHAR);
    }

    @Override
    public Map<JdbcColumnType, String> columnTypes() {
      return typeMap;
    }

    @Override
    public Map<JdbcColumnType, Integer> columnTypeIds() {
      return typeIdMap;
    }

    @Override
    public boolean isConstraintViolation(SQLException e) {
      return CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
    }

    @Override
    public boolean isRetryTransaction(SQLException e) {
      if (e.getSQLState() == null) {
        return false;
      }
      return switch (e.getSQLState()) {
        case DEADLOCK_SQL_STATE_POSTGRES, RETRY_SQL_STATE_COCKROACH -> true;
        default -> false;
      };
    }

    @Override
    public boolean isAlreadyExists(SQLException e) {
      return ALREADY_EXISTS_STATE_POSTGRES.equals(e.getSQLState());
    }

    @Override
    public String wrapInsert(String sql) {
      return sql + " ON CONFLICT DO NOTHING";
    }

    @Override
    public String wrapUpsert(String sql, List<String> keyCols, List<String> columns) {
      return sql
          + " ON CONFLICT ("
          + String.join(", ", keyCols)
          + ")  DO UPDATE SET "
          + columns.stream().map(c -> c + "=EXCLUDED." + c).collect(Collectors.joining(", "));
    }

    @Override
    public String primaryKeyCol(String col, JdbcColumnType columnType) {
      return col;
    }
  }

  static class H2DatabaseSpecific extends BasePostgresDatabaseSpecific {
    H2DatabaseSpecific() {
      super("VARCHAR", Types.VARBINARY);
    }

    @Override
    public String wrapUpsert(String sql, List<String> keyCols, List<String> columns) {
      return super.wrapInsert(sql);
    }
  }

  static class PostgresDatabaseSpecific extends BasePostgresDatabaseSpecific {
    // Use 'ucs_basic' collation for PostgreSQL, otherwise multiple spaces would be collapsed and
    // result in wrong results. Assume the following strings:
    // 'ref-    1'
    // 'ref-    2'
    // 'ref-    3'
    // 'ref-    8'
    // 'ref-    9'
    // 'ref-   10'
    // 'ref-   11'
    // 'ref-   19'
    // 'ref-   20'
    // 'ref-   21'
    // With ucs_basic, the above (expected) order is maintained, but the default behavior could
    // choose a collation in which 'ref-    2' is sorted _after_ 'ref-   19', which is unexpected
    // and wrong.
    PostgresDatabaseSpecific() {
      super("VARCHAR COLLATE ucs_basic", Types.BINARY);
    }
  }

  static class CockroachDatabaseSpecific extends BasePostgresDatabaseSpecific {
    CockroachDatabaseSpecific() {
      super("VARCHAR", Types.BINARY);
    }
  }

  abstract static class BaseMariaDBDatabaseSpecific implements DatabaseSpecific {

    private static final String OBJ_ID = "TINYBLOB";
    private static final String VARCHAR = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";

    private static final String MYSQL_CONSTRAINT_VIOLATION_SQL_STATE = "23000";
    private static final String MYSQL_LOCK_DEADLOCK_SQL_STATE = "40001";
    private static final String MYSQL_ALREADY_EXISTS_SQL_STATE = "42S01";

    private final Map<JdbcColumnType, String> typeMap;
    private final Map<JdbcColumnType, Integer> typeIdMap;

    BaseMariaDBDatabaseSpecific() {
      typeMap = new EnumMap<>(JdbcColumnType.class);
      typeIdMap = new EnumMap<>(JdbcColumnType.class);
      typeMap.put(JdbcColumnType.NAME, VARCHAR);
      typeIdMap.put(JdbcColumnType.NAME, Types.VARCHAR);
      typeMap.put(JdbcColumnType.OBJ_ID, OBJ_ID);
      typeIdMap.put(JdbcColumnType.OBJ_ID, Types.VARBINARY);
      typeMap.put(JdbcColumnType.BOOL, "BIT(1)");
      typeIdMap.put(JdbcColumnType.BOOL, Types.BIT);
      typeMap.put(JdbcColumnType.VARBINARY, "LONGBLOB");
      typeIdMap.put(JdbcColumnType.VARBINARY, Types.BLOB);
      typeMap.put(JdbcColumnType.BIGINT, "BIGINT");
      typeIdMap.put(JdbcColumnType.BIGINT, Types.BIGINT);
      typeMap.put(JdbcColumnType.INT, "INT");
      typeIdMap.put(JdbcColumnType.INT, Types.INTEGER);
      typeMap.put(JdbcColumnType.VARCHAR, VARCHAR);
      typeIdMap.put(JdbcColumnType.VARCHAR, Types.VARCHAR);
    }

    @Override
    public Map<JdbcColumnType, String> columnTypes() {
      return typeMap;
    }

    @Override
    public Map<JdbcColumnType, Integer> columnTypeIds() {
      return typeIdMap;
    }

    @Override
    public boolean isConstraintViolation(SQLException e) {
      return MYSQL_CONSTRAINT_VIOLATION_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public boolean isRetryTransaction(SQLException e) {
      return MYSQL_LOCK_DEADLOCK_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public boolean isAlreadyExists(SQLException e) {
      return MYSQL_ALREADY_EXISTS_SQL_STATE.equals(e.getSQLState());
    }

    @Override
    public String wrapInsert(String sql) {
      return sql.replace("INSERT INTO", "INSERT IGNORE INTO");
    }

    @Override
    public String primaryKeyCol(String col, JdbcColumnType columnType) {
      if (Objects.requireNonNull(columnType) == JdbcColumnType.OBJ_ID) {
        return col + "(255)";
      }
      return col;
    }
  }

  static class MariaDBDatabaseSpecific extends BaseMariaDBDatabaseSpecific {
    @Override
    public String wrapUpsert(String sql, List<String> keyCols, List<String> columns) {
      return sql
          + " ON DUPLICATE KEY UPDATE "
          + columns.stream().map(c -> c + "=VALUE(" + c + ')').collect(Collectors.joining(", "));
    }
  }

  static class MySqlDatabaseSpecific extends BaseMariaDBDatabaseSpecific {
    @Override
    public String wrapUpsert(String sql, List<String> keyCols, List<String> columns) {
      return sql
          + " AS new ON DUPLICATE KEY UPDATE "
          + columns.stream().map(c -> c + "=new." + c).collect(Collectors.joining(", "));
    }
  }
}

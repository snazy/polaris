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
package org.apache.polaris.persistence.nosql.cassandra;

import static org.apache.polaris.persistence.nosql.cassandra.CassandraConstants.SELECT_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.junit.jupiter.api.Test;

class CassandraBackendTest {

  @Test
  void casClosedConnectionHasUnknownOutcome() {
    var exception = new ClosedConnectionException("connection closed");

    var mapped = CassandraBackend.unhandledCasException(exception);

    assertThat(mapped).isInstanceOf(UnknownOperationResultException.class);
    assertThat(mapped.getCause()).isSameAs(exception);
  }

  @Test
  void casClosedConnectionNestedInAllNodesFailedHasUnknownOutcome() {
    var exception =
        AllNodesFailedException.fromErrors(
            List.of(
                Map.entry(mock(Node.class), new ClosedConnectionException("connection closed"))));

    var mapped = CassandraBackend.unhandledCasException(exception);

    assertThat(mapped).isInstanceOf(UnknownOperationResultException.class);
    assertThat(mapped.getCause()).isSameAs(exception);
  }

  @Test
  void batchedQueryCollectsResultsFromConcurrentQueries() {
    var firstResult = resultSet(0);
    var secondResult = resultSet(SELECT_BATCH_SIZE);
    var firstQuery = new CompletableFuture<AsyncResultSet>();
    var secondQuery = new CompletableFuture<AsyncResultSet>();
    var queries = new ArrayList<>(List.of(firstQuery, secondQuery));
    var query =
        backend()
            .newBatchedQuery(
                ignored -> queries.removeFirst(),
                row -> row.getInt("id"),
                row -> row.getInt("id"),
                SELECT_BATCH_SIZE * 2);

    for (var id = 0; id < SELECT_BATCH_SIZE * 2; id++) {
      query.add(id);
    }

    CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> firstQuery.complete(firstResult)),
            CompletableFuture.runAsync(() -> secondQuery.complete(secondResult)))
        .join();

    assertThat(query.finish())
        .containsExactlyInAnyOrderEntriesOf(
            IntStream.range(0, SELECT_BATCH_SIZE * 2)
                .boxed()
                .collect(Collectors.toMap(id -> id, id -> id)));
  }

  @Test
  void batchedQueryPropagatesSynchronousSubmissionFailure() {
    var query =
        backend()
            .newBatchedQuery(
                ignored -> {
                  throw new IllegalStateException("prepare failed");
                },
                row -> row,
                row -> row,
                1);

    query.add(mock(Row.class));

    assertThatIllegalStateException().isThrownBy(query::finish).withMessage("prepare failed");
  }

  @Test
  void batchedQueryDoesNotMaskFailureWhenClosedAfterFinish() {
    assertThatIllegalStateException()
        .isThrownBy(
            () -> {
              try (var query =
                  backend()
                      .newBatchedQuery(
                          ignored -> {
                            throw new IllegalStateException("prepare failed");
                          },
                          row -> row,
                          row -> row,
                          1)) {
                query.add(mock(Row.class));
                query.finish();
              }
            })
        .withMessage("prepare failed");
  }

  private static CassandraBackend backend() {
    return new CassandraBackend(
        new CassandraBackendConfig(
            mock(CqlSession.class),
            "polaris",
            Duration.ofSeconds(5),
            Duration.ofSeconds(3),
            false));
  }

  private static AsyncResultSet resultSet(int firstId) {
    List<Row> rows = new ArrayList<>();
    for (var id = firstId; id < firstId + SELECT_BATCH_SIZE; id++) {
      Row row = mock(Row.class);
      when(row.getInt("id")).thenReturn(id);
      rows.add(row);
    }
    AsyncResultSet resultSet = mock(AsyncResultSet.class);
    when(resultSet.currentPage()).thenReturn(rows);
    return resultSet;
  }
}

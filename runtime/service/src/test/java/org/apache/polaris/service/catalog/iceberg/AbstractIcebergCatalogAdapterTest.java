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

package org.apache.polaris.service.catalog.iceberg;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.polaris.service.catalog.ImmutableFakeRestIcebergCatalog;
import org.apache.polaris.service.catalog.RequestScopedRunner;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEventType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public abstract class AbstractIcebergCatalogAdapterTest extends AbstractIcebergCatalogTestBase {
  protected @Inject IcebergCatalogAdapter icebergCatalogAdapter;
  protected @Inject RequestScopedRunner requestScopedRunner;

  protected String restPrefix() {
    return requireNonNull(catalog().properties().get("prefix"), "'prefix' property must be set");
  }

  @Override
  protected RESTCatalog createCatalog(String catalogName, Map<String, String> properties) {
    return ImmutableFakeRestIcebergCatalog.builder()
        .icebergCatalogAdapter(icebergCatalogAdapter)
        .realmContext(realmContext)
        .securityContext(securityContext)
        .name(catalogName)
        .putAllProperties(properties)
        .requestScopedRunner(requestScopedRunner)
        .build()
        .newRestCatalog();
  }

  @Test
  public void testPaginatedListTables() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");

    var catalog = catalog();

    catalog.createNamespace(NS);

    for (int i = 0; i < 5; i++) {
      catalog.buildTable(TableIdentifier.of(NS, "pagination_table_" + i), SCHEMA).create();
    }

    try {
      // List without pagination
      Assertions.assertThat(catalog.listTables(NS)).isNotNull().hasSize(5);

      // List with a limit:
      var firstListResult =
          icebergCatalogAdapter
              .listTables(restPrefix(), NS_ENCODED, null, 2, realmContext, securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(firstListResult.identifiers().size()).isEqualTo(2);
      Assertions.assertThat(firstListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the previously obtained token:
      var secondListResult =
          icebergCatalogAdapter
              .listTables(
                  restPrefix(),
                  NS_ENCODED,
                  firstListResult.nextPageToken(),
                  2,
                  realmContext,
                  securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(secondListResult.identifiers().size()).isEqualTo(2);
      Assertions.assertThat(secondListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the final token:
      var finalListResult =
          icebergCatalogAdapter
              .listTables(
                  restPrefix(),
                  NS_ENCODED,
                  secondListResult.nextPageToken(),
                  2,
                  realmContext,
                  securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(finalListResult.identifiers().size()).isEqualTo(1);
      Assertions.assertThat(finalListResult.nextPageToken()).isNull();
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropTable(TableIdentifier.of(NS, "pagination_table_" + i));
      }
    }
  }

  @Test
  public void testPaginatedListViews() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");

    var catalog = catalog();

    catalog.createNamespace(NS);

    for (int i = 0; i < 5; i++) {
      catalog
          .buildView(TableIdentifier.of(NS, "pagination_view_" + i))
          .withQuery("a_" + i, "SELECT 1 id")
          .withSchema(SCHEMA)
          .withDefaultNamespace(NS)
          .create();
    }

    try {
      // List without pagination
      Assertions.assertThat(catalog.listViews(NS)).isNotNull().hasSize(5);

      // List with a limit:
      var firstListResult =
          icebergCatalogAdapter
              .listViews(restPrefix(), NS_ENCODED, null, 2, realmContext, securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(firstListResult.identifiers().size()).isEqualTo(2);
      Assertions.assertThat(firstListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the previously obtained token:
      var secondListResult =
          icebergCatalogAdapter
              .listViews(
                  restPrefix(),
                  NS_ENCODED,
                  firstListResult.nextPageToken(),
                  2,
                  realmContext,
                  securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(secondListResult.identifiers().size()).isEqualTo(2);
      Assertions.assertThat(secondListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the final token:
      var finalListResult =
          icebergCatalogAdapter
              .listViews(
                  restPrefix(),
                  NS_ENCODED,
                  secondListResult.nextPageToken(),
                  2,
                  realmContext,
                  securityContext)
              .readEntity(ListTablesResponse.class);
      Assertions.assertThat(finalListResult.identifiers().size()).isEqualTo(1);
      Assertions.assertThat(finalListResult.nextPageToken()).isNull();
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropTable(TableIdentifier.of(NS, "pagination_view_" + i));
      }
    }
  }

  @Test
  public void testPaginatedListNamespaces() {
    var catalog = catalog();
    for (int i = 0; i < 5; i++) {
      catalog.createNamespace(Namespace.of("pagination_namespace_" + i));
    }

    try {
      // List without pagination
      Assertions.assertThat(catalog.listNamespaces()).isNotNull().hasSize(5);

      var emptyNamespaceEncoded = (String) null;

      // List with a limit:
      var firstListResult =
          icebergCatalogAdapter
              .listNamespaces(
                  restPrefix(), null, 2, emptyNamespaceEncoded, realmContext, securityContext)
              .readEntity(ListNamespacesResponse.class);
      Assertions.assertThat(firstListResult.namespaces().size()).isEqualTo(2);
      Assertions.assertThat(firstListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the previously obtained token:
      var secondListResult =
          icebergCatalogAdapter
              .listNamespaces(
                  restPrefix(),
                  firstListResult.nextPageToken(),
                  2,
                  emptyNamespaceEncoded,
                  realmContext,
                  securityContext)
              .readEntity(ListNamespacesResponse.class);
      Assertions.assertThat(secondListResult.namespaces().size()).isEqualTo(2);
      Assertions.assertThat(secondListResult.nextPageToken()).isNotNull().isNotEmpty();

      // List using the final token:
      var finalListResult =
          icebergCatalogAdapter
              .listNamespaces(
                  restPrefix(),
                  secondListResult.nextPageToken(),
                  2,
                  emptyNamespaceEncoded,
                  realmContext,
                  securityContext)
              .readEntity(ListNamespacesResponse.class);
      Assertions.assertThat(finalListResult.namespaces().size()).isEqualTo(1);
      Assertions.assertThat(finalListResult.nextPageToken()).isNull();

      // List with page size matching the amount of data, no more pages
      var firstExactListResult =
          icebergCatalogAdapter
              .listNamespaces(
                  restPrefix(), null, 5, emptyNamespaceEncoded, realmContext, securityContext)
              .readEntity(ListNamespacesResponse.class);
      Assertions.assertThat(firstExactListResult.namespaces().size()).isEqualTo(5);
      Assertions.assertThat(firstExactListResult.nextPageToken()).isNull();

      // List with huge page size:
      var bigListResult =
          icebergCatalogAdapter
              .listNamespaces(
                  restPrefix(), null, 9999, emptyNamespaceEncoded, realmContext, securityContext)
              .readEntity(ListNamespacesResponse.class);
      Assertions.assertThat(bigListResult.namespaces().size()).isEqualTo(5);
      Assertions.assertThat(bigListResult.nextPageToken()).isNull();
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropNamespace(Namespace.of("pagination_namespace_" + i));
      }
    }
  }

  @Test
  // TODO We don't have the event-delegator in this test, need to move this elsewhere or refactory
  //  this test stuff
  void testEventsForSuccessfulTransaction() {
    var catalog = catalog();
    catalog.createNamespace(NS);

    String table1Name = "test-table-1";
    String table2Name = "test-table-2";
    executeTransactionTest(false, table1Name, table2Name);

    // Verify that all (Before/After)CommitTransaction and (Before/After)UpdateTable events were
    // emitted
    assertThat(testPolarisEventListener.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    assertThat(
            testPolarisEventListener
                .getLatest(PolarisEventType.BEFORE_UPDATE_TABLE)
                .attributes()
                .getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);

    assertThat(testPolarisEventListener.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isNotNull();
    assertThat(
            testPolarisEventListener
                .getLatest(PolarisEventType.AFTER_UPDATE_TABLE)
                .attributes()
                .getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);
  }

  @Test
  // TODO We don't have the event-delegator in this test, need to move this elsewhere or refactory
  //  this test stuff
  void testEventsForUnSuccessfulTransaction() {
    var catalog = catalog();
    catalog.createNamespace(NS);

    String table3Name = "test-table-3";
    String table4Name = "test-table-4";
    executeTransactionTest(true, table3Name, table4Name);

    // Verify that all (Before)CommitTable events were emitted

    // Verify that all BeforeCommitTransaction and BeforeUpdateTable events were emitted,
    // and that the AfterCommitTransaction and AfterUpdateTable events were not emitted
    assertThat(testPolarisEventListener.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    assertThat(
            testPolarisEventListener
                .getLatest(PolarisEventType.BEFORE_UPDATE_TABLE)
                .attributes()
                .getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table4Name);

    assertThatThrownBy(
            () -> testPolarisEventListener.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () ->
                testPolarisEventListener
                    .getLatest(PolarisEventType.AFTER_UPDATE_TABLE)
                    .attributes()
                    .getRequired(EventAttributes.TABLE_NAME))
        .isInstanceOf(IllegalStateException.class);
  }

  private void createTable(String tableName) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(
                DEFAULT_BASE_STORAGE_LOCATION
                    + "/"
                    + String.join("/", NS.levels())
                    + "/"
                    + tableName)
            .withSchema(SCHEMA)
            .build();
    icebergCatalogAdapter.createTable(
        restPrefix(), NS_ENCODED, createTableRequest, null, realmContext, securityContext);
  }

  /**
   * Executes a transaction test with the specified parameters.
   *
   * @param table1Name name of the first table
   * @param table2Name name of the second table
   */
  private void executeTransactionTest(boolean shouldFail, String table1Name, String table2Name) {
    // Set up the test tables
    createTable(table1Name);
    createTable(table2Name);

    // Ignore any errors that occur during transaction commit
    try {
      icebergCatalogAdapter.commitTransaction(
          restPrefix(),
          generateCommitTransactionRequest(shouldFail, table1Name, table2Name),
          realmContext,
          securityContext);
    } catch (Exception ignored) {
    }
  }

  private CommitTransactionRequest generateCommitTransactionRequest(
      boolean shouldFail, String table1Name, String table2Name) {
    List<UpdateRequirement> updateRequirements;
    if (shouldFail) {
      // Schema ID does not exist, therefore call will fail
      updateRequirements = List.of(new UpdateRequirement.AssertCurrentSchemaID(-1));
    } else {
      updateRequirements = List.of();
    }
    return new CommitTransactionRequest(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(NS, table1Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of("custom-property-1", "value1")))),
            UpdateTableRequest.create(
                TableIdentifier.of(NS, table2Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of("custom-property-1", "value2"))))));
  }
}

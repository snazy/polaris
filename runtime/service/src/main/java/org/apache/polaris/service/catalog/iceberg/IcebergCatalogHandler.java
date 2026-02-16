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

import java.util.EnumSet;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * Authorization-aware adapter between REST stubs and shared Iceberg SDK CatalogHandlers.
 *
 * <p>We must make authorization decisions based on entity resolution at this layer instead of the
 * underlying PolarisIcebergCatalog layer, because this REST-adjacent layer captures intent of
 * different REST calls that share underlying catalog calls (e.g. updateTable will call loadTable
 * under the hood), and some features of the REST API aren't expressed at all in the underlying
 * Catalog interfaces (e.g. credential-vending in createTable/loadTable).
 *
 * <p>We also want this layer to be independent of API-endpoint-specific idioms, such as dealing
 * with jakarta.ws.rs.core.Response objects, and other implementations that expose different HTTP
 * stubs or even tunnel the protocol over something like gRPC can still normalize on the Iceberg
 * model objects used in this layer to still benefit from the shared implementation of
 * authorization-aware catalog protocols.
 */
public interface IcebergCatalogHandler extends CatalogHandler, AutoCloseable {
  ListNamespacesResponse listNamespaces(Namespace parent, String pageToken, Integer pageSize);

  default ListNamespacesResponse listNamespaces(Namespace parent) {
    return listNamespaces(parent, null, null);
  }

  CreateNamespaceResponse createNamespace(CreateNamespaceRequest request);

  GetNamespaceResponse loadNamespaceMetadata(Namespace namespace);

  void namespaceExists(Namespace namespace);

  void dropNamespace(Namespace namespace);

  UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request);

  ListTablesResponse listTables(Namespace namespace, String pageToken, Integer pageSize);

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  LoadTableResponse createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint);

  LoadTableResponse createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint);

  /**
   * Register a table.
   *
   * @param namespace The namespace to register the table in
   * @param request the register table request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request);

  void reportMetrics(TableIdentifier identifier, ReportMetricsRequest request);

  Optional<LoadTableResponse> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint);

  LoadTableResponse updateTable(TableIdentifier tableIdentifier, UpdateTableRequest request);

  LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request);

  void tableExists(TableIdentifier tableIdentifier);

  void renameTable(RenameTableRequest request);

  void dropTable(TableIdentifier tableIdentifier, boolean purgeRequested);

  void commitTransaction(CommitTransactionRequest commitTransactionRequest);

  ListTablesResponse listViews(Namespace namespace, String pageToken, Integer pageSize);

  LoadViewResponse createView(Namespace namespace, CreateViewRequest request);

  LoadViewResponse loadView(TableIdentifier viewIdentifier);

  LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request);

  void dropView(TableIdentifier viewIdentifier);

  void viewExists(TableIdentifier viewIdentifier);

  void renameView(RenameTableRequest request);

  ConfigResponse getConfig();

  boolean sendNotification(TableIdentifier identifier, NotificationRequest request);
}

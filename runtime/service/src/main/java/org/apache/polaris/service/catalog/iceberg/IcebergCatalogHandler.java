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

import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING;
import static org.apache.polaris.core.config.FeatureConfiguration.LIST_PAGINATION_ENABLED;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.inject.Instance;
import java.io.Closeable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.catalog.common.CatalogAccess;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.http.IcebergHttpUtil;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class IcebergCatalogHandler extends CatalogHandler<Catalog> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogHandler.class);

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_NAMESPACE_EXISTS)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_TABLE_EXISTS)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  private static final Set<Endpoint> VIEW_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_VIEW_EXISTS)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .build();

  private final CatalogPrefixParser prefixParser;
  private final ReservedProperties reservedProperties;
  private final CatalogHandlerUtils catalogHandlerUtils;
  private final StorageAccessConfigProvider storageAccessConfigProvider;

  // Catalog instance will be initialized after authorizing resolver successfully resolves
  // the catalog entity.
  protected Catalog baseCatalog = null;
  protected SupportsNamespaces namespaceCatalog = null;
  protected ViewCatalog viewCatalog = null;

  public static final String SNAPSHOTS_ALL = "all";
  public static final String SNAPSHOTS_REFS = "refs";

  public IcebergCatalogHandler(
      CatalogPrefixParser prefixParser,
      CatalogAccess<Catalog> catalogAccess,
      RealmConfig realConfig,
      PolarisCredentialManager credentialManager,
      ReservedProperties reservedProperties,
      CatalogHandlerUtils catalogHandlerUtils,
      Instance<ExternalCatalogFactory> externalCatalogFactories,
      StorageAccessConfigProvider storageAccessConfigProvider) {
    super(realConfig, catalogAccess, credentialManager, externalCatalogFactories);
    this.prefixParser = prefixParser;
    this.reservedProperties = reservedProperties;
    this.catalogHandlerUtils = catalogHandlerUtils;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
  }

  private CatalogEntity getResolvedCatalogEntity() {
    return catalogAccess().catalogEntity();
  }

  private boolean shouldDecodeToken() {
    return realmConfig().getConfig(LIST_PAGINATION_ENABLED, catalogAccess().catalogProperties());
  }

  public ListNamespacesResponse listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, parent);
    initializeCatalog();

    if (baseCatalog instanceof PolarisIcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<Namespace> results = polarisCatalog.listNamespaces(parent, pageRequest);
      return ListNamespacesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else {
      return catalogHandlerUtils.listNamespaces(namespaceCatalog, parent, pageToken, pageSize);
    }
  }

  @Override
  protected void initializeCatalog() {
    var connectionConfigInfoDpo = catalogAccess().catalogConnectionConfigInfo();
    if (connectionConfigInfoDpo != null) {
      LOGGER
          .atInfo()
          .addKeyValue("remoteUrl", connectionConfigInfoDpo.getUri())
          .log("Initializing federated catalog");
      FeatureConfiguration.enforceFeatureEnabledOrThrow(
          realmConfig(), FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      Catalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      // Use the unified factory pattern for all external catalog types
      Instance<ExternalCatalogFactory> externalCatalogFactory =
          externalCatalogFactories()
              .select(Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (externalCatalogFactory.isResolvable()) {
        // Pass through catalog properties (e.g., rest.client.proxy.*, timeout settings)
        // to the external catalog factory for configuration of the underlying HTTP client
        Map<String, String> catalogProperties = catalogAccess().catalogProperties();
        federatedCatalog =
            externalCatalogFactory
                .get()
                .createCatalog(
                    connectionConfigInfoDpo, getPolarisCredentialManager(), catalogProperties);
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      // TODO: if the remote catalog is not RestCatalog, the corresponding table operation will use
      // environment to load the table metadata, the env may not contain credentials to access the
      // storage. In the future, we could leverage PolarisCredentialManager to inject storage
      // credentials for non-rest remote catalog
      this.baseCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.baseCatalog = catalogAccess().initializeCatalog(Map.of());
    }
    this.namespaceCatalog =
        (baseCatalog instanceof SupportsNamespaces) ? (SupportsNamespaces) baseCatalog : null;
    this.viewCatalog = (baseCatalog instanceof ViewCatalog) ? (ViewCatalog) baseCatalog : null;
  }

  public ListNamespacesResponse listNamespaces(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, parent);
    initializeCatalog();

    return catalogHandlerUtils.listNamespaces(namespaceCatalog, parent);
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_NAMESPACE;

    Namespace namespace = request.namespace();
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }
    catalogAuthZ().authorizeCreateNamespaceUnderNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    if (namespaceCatalog instanceof PolarisIcebergCatalog polarisIcebergCatalog) {
      // Note: The CatalogHandlers' default implementation will non-atomically create the
      // namespace and then fetch its properties using loadNamespaceMetadata for the response.
      // However, the latest namespace metadata technically isn't the same authorized instance,
      // so we don't want all cals to loadNamespaceMetadata to automatically use the manifest
      // in "passthrough" mode.
      //
      // For CreateNamespace, we consider this a special case in that the creator is able to
      // retrieve the latest namespace metadata for the duration of the CreateNamespace
      // operation, even if the entityVersion and/or grantsVersion update in the interim.
      var result =
          polarisIcebergCatalog.createNamespaceWithResult(
              namespace, reservedProperties.removeReservedProperties(request.properties()));
      return CreateNamespaceResponse.builder()
          .withNamespace(namespace)
          .setProperties(reservedProperties.removeReservedProperties(result.properties()))
          .build();
    } else {
      return catalogHandlerUtils.createNamespace(namespaceCatalog, request);
    }
  }

  public GetNamespaceResponse loadNamespaceMetadata(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    return catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace);
  }

  public void namespaceExists(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.NAMESPACE_EXISTS;

    // TODO: This authz check doesn't accomplish true authz in terms of blocking the ability
    // for a caller to ascertain whether the namespace exists or not, but instead just behaves
    // according to convention -- if existence is going to be privileged, we must instead
    // add a base layer that throws NotFound exceptions instead of NotAuthorizedException
    // for *all* operations in which we determine that the basic privilege for determining
    // existence is also missing.
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      if (!restCompatibleCatalog.namespaceExists(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      return;
    }

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace);
  }

  public void dropNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    catalogHandlerUtils.dropNamespace(namespaceCatalog, namespace);
  }

  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    return catalogHandlerUtils.updateNamespaceProperties(namespaceCatalog, namespace, request);
  }

  public ListTablesResponse listTables(Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    if (baseCatalog instanceof PolarisIcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<TableIdentifier> results = polarisCatalog.listTables(namespace, pageRequest);
      return ListTablesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else {
      return catalogHandlerUtils.listTables(baseCatalog, namespace, pageToken, pageSize);
    }
  }

  public ListTablesResponse listTables(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    return catalogHandlerUtils.listTables(baseCatalog, namespace);
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirect(Namespace namespace, CreateTableRequest request) {
    return createTableDirect(
        namespace, request, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty());
  }

  /**
   * Create a table.
   *
   * @param namespace the namespace to create the table in
   * @param request the table creation request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse createTableDirectWithWriteDelegation(
      Namespace namespace,
      CreateTableRequest request,
      Optional<String> refreshCredentialsEndpoint) {
    return createTableDirect(
        namespace, request, EnumSet.of(VENDED_CREDENTIALS), refreshCredentialsEndpoint);
  }

  public void authorizeCreateTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
      catalogAuthZ()
          .authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
              PolarisAuthorizableOperation.CREATE_TABLE_DIRECT, identifier);
    } else {
      catalogAuthZ()
          .authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
              PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
              TableIdentifier.of(namespace, request.name()));
    }
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
    checkAllowExternalCatalogCredentialVending(delegationModes);
  }

  public LoadTableResponse createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    authorizeCreateTableDirect(namespace, request, delegationModes);

    request.validate();

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(tableIdentifier)) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      var tableMetadata =
          restCompatibleCatalog.createTable(
              tableIdentifier,
              CreateTableRequest.builder()
                  .withName(request.name())
                  .setProperties(properties)
                  .withSchema(request.schema())
                  .withPartitionSpec(request.spec())
                  .withLocation(request.location())
                  .withWriteOrder(request.writeOrder())
                  .build());
      return buildLoadTableResponseWithDelegationCredentials(
              tableIdentifier,
              tableMetadata,
              delegationModes,
              Set.of(
                  PolarisStorageActions.READ,
                  PolarisStorageActions.WRITE,
                  PolarisStorageActions.LIST),
              refreshCredentialsEndpoint)
          .build();
    }

    Table table =
        baseCatalog
            .buildTable(tableIdentifier, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(properties)
            .create();

    if (table instanceof BaseTable baseTable) {
      TableMetadata tableMetadata = baseTable.operations().current();
      return buildLoadTableResponseWithDelegationCredentials(
              tableIdentifier,
              tableMetadata,
              delegationModes,
              Set.of(
                  PolarisStorageActions.READ,
                  PolarisStorageActions.WRITE,
                  PolarisStorageActions.LIST),
              refreshCredentialsEndpoint)
          .build();
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private TableMetadata stageTableCreateHelper(Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      return restCompatibleCatalog.createTable(
          ident,
          CreateTableRequest.builder()
              .withName(request.name())
              .setProperties(properties)
              .withSchema(request.schema())
              .withPartitionSpec(request.spec())
              .withLocation(request.location())
              .withWriteOrder(request.writeOrder())
              .stageCreate()
              .build());
    }

    String location;
    if (request.location() != null) {
      // Even if the request provides a location, run it through the catalog's TableBuilder
      // to inherit any override behaviors if applicable.
      if (baseCatalog instanceof PolarisIcebergCatalog polarisIcebergCatalog) {
        location = polarisIcebergCatalog.transformTableLikeLocation(ident, request.location());
      } else {
        location = request.location();
      }
    } else {
      location =
          baseCatalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);
    return metadata;
  }

  public LoadTableResponse createTableStaged(Namespace namespace, CreateTableRequest request) {
    return createTableStaged(
        namespace, request, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty());
  }

  public LoadTableResponse createTableStagedWithWriteDelegation(
      Namespace namespace,
      CreateTableRequest request,
      Optional<String> refreshCredentialsEndpoint) {
    return createTableStaged(
        namespace, request, EnumSet.of(VENDED_CREDENTIALS), refreshCredentialsEndpoint);
  }

  private void authorizeCreateTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes) {
    catalogAuthZ()
        .authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
            delegationModes.isEmpty()
                ? PolarisAuthorizableOperation.CREATE_TABLE_STAGED
                : PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
            TableIdentifier.of(namespace, request.name()));
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
    checkAllowExternalCatalogCredentialVending(delegationModes);
  }

  public LoadTableResponse createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    authorizeCreateTableStaged(namespace, request, delegationModes);

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    TableMetadata metadata = stageTableCreateHelper(namespace, request);

    return buildLoadTableResponseWithDelegationCredentials(
            ident,
            metadata,
            delegationModes,
            Set.of(PolarisStorageActions.ALL),
            refreshCredentialsEndpoint)
        .build();
  }

  /**
   * Register a table.
   *
   * @param namespace The namespace to register the table in
   * @param request the register table request
   * @return ETagged {@link LoadTableResponse} to uniquely identify the table metadata
   */
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REGISTER_TABLE;
    catalogAuthZ()
        .authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
            op, TableIdentifier.of(namespace, request.name()));
    initializeCatalog();

    return catalogHandlerUtils.registerTable(baseCatalog, namespace, request);
  }

  public boolean sendNotification(TableIdentifier identifier, NotificationRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.SEND_NOTIFICATIONS;

    // For now, just require the full set of privileges on the base Catalog entity, which we can
    // also express just as the "root" Namespace for purposes of the PolarisIcebergCatalog being
    // able to fetch Namespace.empty() as path key.
    List<TableIdentifier> extraPassthroughTableLikes = List.of(identifier);
    List<Namespace> extraPassthroughNamespaces = new ArrayList<>();
    extraPassthroughNamespaces.add(Namespace.empty());
    for (int i = 1; i <= identifier.namespace().length(); i++) {
      Namespace nsLevel =
          Namespace.of(
              Arrays.stream(identifier.namespace().levels()).limit(i).toArray(String[]::new));
      extraPassthroughNamespaces.add(nsLevel);
    }
    catalogAuthZ()
        .authorizeBasicNamespaceOperationOrThrow(
            op, Namespace.empty(), extraPassthroughNamespaces, extraPassthroughTableLikes, null);
    initializeCatalog();

    CatalogEntity catalog = getResolvedCatalogEntity();
    if (catalogAccess()
        .catalogType()
        .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.INTERNAL)) {
      LOGGER
          .atWarn()
          .addKeyValue("catalog", catalog)
          .addKeyValue("notification", request)
          .log("Attempted notification on internal catalog");
      throw new BadRequestException("Cannot update internal catalog via notifications");
    }
    return baseCatalog instanceof SupportsNotifications notificationCatalog
        && notificationCatalog.sendNotification(identifier, request);
  }

  public LoadTableResponse loadTable(TableIdentifier tableIdentifier, String snapshots) {
    return loadTableIfStale(tableIdentifier, null, snapshots).get();
  }

  /**
   * Attempt to perform a loadTable operation only when the specified set of eTags do not match the
   * current state of the table metadata.
   *
   * @param tableIdentifier The identifier of the table to load
   * @param ifNoneMatch set of entity-tags to check the metadata against for staleness
   * @param snapshots
   * @return {@link Optional#empty()} if the ETag is current, an {@link Optional} containing the
   *     load table response, otherwise
   */
  public Optional<LoadTableResponse> loadTableIfStale(
      TableIdentifier tableIdentifier, IfNoneMatch ifNoneMatch, String snapshots) {
    return loadTable(
        tableIdentifier,
        snapshots,
        ifNoneMatch,
        EnumSet.noneOf(AccessDelegationMode.class),
        Optional.empty());
  }

  public LoadTableResponse loadTableWithAccessDelegation(
      TableIdentifier tableIdentifier,
      String snapshots,
      Optional<String> refreshCredentialsEndpoint) {
    return loadTableWithAccessDelegationIfStale(
            tableIdentifier, null, snapshots, refreshCredentialsEndpoint)
        .get();
  }

  /**
   * Attempt to perform a loadTable operation with access delegation only when the if none of the
   * provided eTags match the current state of the table metadata.
   *
   * @param tableIdentifier The identifier of the table to load
   * @param ifNoneMatch set of entity-tags to check the metadata against for staleness
   * @param snapshots
   * @return {@link Optional#empty()} if the ETag is current, an {@link Optional} containing the
   *     load table response, otherwise
   */
  public Optional<LoadTableResponse> loadTableWithAccessDelegationIfStale(
      TableIdentifier tableIdentifier,
      IfNoneMatch ifNoneMatch,
      String snapshots,
      Optional<String> refreshCredentialsEndpoint) {
    return loadTable(
        tableIdentifier,
        snapshots,
        ifNoneMatch,
        EnumSet.of(VENDED_CREDENTIALS),
        refreshCredentialsEndpoint);
  }

  private Set<PolarisStorageActions> authorizeLoadTable(
      TableIdentifier tableIdentifier, EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      catalogAuthZ()
          .authorizeBasicTableLikeOperationOrThrow(
              PolarisAuthorizableOperation.LOAD_TABLE,
              PolarisEntitySubType.ICEBERG_TABLE,
              tableIdentifier);
      initializeCatalog();
      return Set.of();
    }

    // Here we have a single method that falls through multiple candidate
    // PolarisAuthorizableOperations because instead of identifying the desired operation up-front
    // and
    // failing the authz check if grants aren't found, we find the first most-privileged authz match
    // and respond according to that.
    PolarisAuthorizableOperation read =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION;
    PolarisAuthorizableOperation write =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION;

    Set<PolarisStorageActions> actionsRequested =
        new HashSet<>(Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));
    try {
      // TODO: Refactor to have a boolean-return version of the helpers so we can fallthrough
      // easily.
      catalogAuthZ()
          .authorizeBasicTableLikeOperationOrThrow(
              write, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
      actionsRequested.add(PolarisStorageActions.WRITE);
    } catch (ForbiddenException e) {
      catalogAuthZ()
          .authorizeBasicTableLikeOperationOrThrow(
              read, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    }
    initializeCatalog();

    checkAllowExternalCatalogCredentialVending(delegationModes);

    return actionsRequested;
  }

  public Optional<LoadTableResponse> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    Set<PolarisStorageActions> actionsRequested =
        authorizeLoadTable(tableIdentifier, delegationModes);

    if (ifNoneMatch != null) {
      // Perform freshness-aware table loading if caller specified ifNoneMatch.
      var etag =
          catalogAccess()
              .metadataLocation(tableIdentifier)
              .map(IcebergHttpUtil::generateETagForMetadataFileLocation);
      if (etag.isEmpty()) {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableIdentifier)
            .log("Failed to get ETag for table");
      } else {
        if (ifNoneMatch.anyMatch(etag.get())) {
          return Optional.empty();
        }
      }
    }

    TableMetadata tableMetadata;
    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      // TODO add delegation credentials support
      tableMetadata = restCompatibleCatalog.loadTableMetadata(tableIdentifier);
    } else {
      // TODO: Find a way for the configuration or caller to better express whether to fail or omit
      // when data-access is specified but access delegation grants are not found.
      Table table = baseCatalog.loadTable(tableIdentifier);
      if (table instanceof BaseTable baseTable) {
        tableMetadata = baseTable.operations().current();
      } else if (table instanceof BaseMetadataTable) {
        // metadata tables are loaded on the client side, return NoSuchTableException for now
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
      } else {
        throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
      }
    }

    LoadTableResponse response =
        buildLoadTableResponseWithDelegationCredentials(
                tableIdentifier,
                tableMetadata,
                delegationModes,
                actionsRequested,
                refreshCredentialsEndpoint)
            .build();
    return Optional.of(filterResponseToSnapshots(response, snapshots));
  }

  private LoadTableResponse.Builder buildLoadTableResponseWithDelegationCredentials(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      EnumSet<AccessDelegationMode> delegationModes,
      Set<PolarisStorageActions> actions,
      Optional<String> refreshCredentialsEndpoint) {
    LoadTableResponse.Builder responseBuilder =
        LoadTableResponse.builder().withTableMetadata(tableMetadata);
    Optional<List<PolarisEntity>> resolvedPath = catalogAccess().resolvePath(tableIdentifier);

    if (resolvedPath.isEmpty()) {
      LOGGER.debug(
          "Unable to find storage configuration information for table {}", tableIdentifier);
      return responseBuilder;
    }

    if (baseCatalog instanceof PolarisIcebergCatalog
        || realmConfig()
            .getConfig(
                ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING, catalogAccess().catalogProperties())) {

      Set<String> tableLocations = StorageUtil.getLocationsUsedByTable(tableMetadata);

      // For non polaris' catalog, validate that table locations are within allowed locations
      if (!(baseCatalog instanceof PolarisIcebergCatalog)) {
        validateRemoteTableLocations(tableIdentifier, tableLocations, resolvedPath.get());
      }

      StorageAccessConfig storageAccessConfig =
          storageAccessConfigProvider.getStorageAccessConfig(
              tableIdentifier,
              tableLocations,
              actions,
              refreshCredentialsEndpoint,
              resolvedPath.get());
      Map<String, String> credentialConfig = storageAccessConfig.credentials();
      if (delegationModes.contains(VENDED_CREDENTIALS)) {
        if (!credentialConfig.isEmpty()) {
          responseBuilder.addAllConfig(credentialConfig);
          responseBuilder.addCredential(
              ImmutableCredential.builder()
                  .prefix(tableMetadata.location())
                  .config(credentialConfig)
                  .build());
        } else {
          Boolean skipCredIndirection =
              realmConfig().getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
          Preconditions.checkArgument(
              !storageAccessConfig.supportsCredentialVending() || skipCredIndirection,
              "Credential vending was requested for table %s, but no credentials are available",
              tableIdentifier);
        }
      }
      responseBuilder.addAllConfig(storageAccessConfig.extraProperties());
    }

    return responseBuilder;
  }

  private void validateRemoteTableLocations(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      List<PolarisEntity> resolvedStoragePath) {

    try {
      // Delegate to common validation logic
      CatalogUtils.validateLocationsForTableLike(
          realmConfig(), tableIdentifier, tableLocations, resolvedStoragePath);

      LOGGER
          .atInfo()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Validated federated table locations");
    } catch (ForbiddenException e) {
      LOGGER
          .atError()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Federated table locations validation failed");
      throw new ForbiddenException(
          "Table '%s' in remote catalog has locations outside catalog's allowed locations: %s",
          tableIdentifier, e.getMessage());
    }
  }

  private UpdateTableRequest applyUpdateFilters(UpdateTableRequest request) {
    // Certain MetadataUpdates need to be explicitly transformed to achieve the same behavior
    // as using a local Catalog client via TableBuilder.
    TableIdentifier identifier = request.identifier();
    List<UpdateRequirement> requirements = request.requirements();
    List<MetadataUpdate> updates =
        request.updates().stream()
            .map(
                update -> {
                  if (baseCatalog instanceof PolarisIcebergCatalog polarisIcebergCatalog
                      && update instanceof MetadataUpdate.SetLocation setLocation) {
                    String requestedLocation = setLocation.location();
                    String filteredLocation =
                        polarisIcebergCatalog.transformTableLikeLocation(
                            identifier, requestedLocation);
                    return new MetadataUpdate.SetLocation(filteredLocation);
                  } else {
                    return update;
                  }
                })
            .toList();
    return UpdateTableRequest.create(identifier, requirements, updates);
  }

  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {

    EnumSet<PolarisAuthorizableOperation> authorizableOperations =
        getUpdateTableAuthorizableOperations(request);

    catalogAuthZ()
        .authorizeBasicTableLikeOperationsOrThrow(
            authorizableOperations, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateTable(
        baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    catalogAuthZ().authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateTable(
        baseCatalog, tableIdentifier, applyUpdateFilters(request));
  }

  public void dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    initializeCatalog();

    catalogHandlerUtils.dropTable(baseCatalog, tableIdentifier);
  }

  public void dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot drop table on static-facade external catalogs.");
    }
    catalogHandlerUtils.purgeTable(baseCatalog, tableIdentifier);
  }

  public void tableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      if (!restCompatibleCatalog.tableExists(tableIdentifier)) {
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier.toString());
      }
      return;
    }

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadTable(baseCatalog, tableIdentifier);
  }

  public void renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    catalogAuthZ()
        .authorizeRenameTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot rename table on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameTable(baseCatalog, request);
  }

  public void commitTransaction(CommitTransactionRequest commitTransactionRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.COMMIT_TRANSACTION;
    // TODO: The authz actually needs to detect hidden updateForStagedCreate UpdateTableRequests
    // and have some kind of per-item conditional privilege requirement if we want to make it
    // so that only the stageCreate updates need TABLE_CREATE whereas everything else only
    // needs TABLE_WRITE_PROPERTIES.
    catalogAuthZ()
        .authorizeCollectionOfTableLikeOperationOrThrow(
            op,
            PolarisEntitySubType.ICEBERG_TABLE,
            commitTransactionRequest.tableChanges().stream()
                .map(UpdateTableRequest::identifier)
                .toList());
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }

    if (baseCatalog instanceof PolarisIcebergCatalog icebergCatalog) {
      icebergCatalog.commitTransaction(commitTransactionRequest);
    } else {
      throw new BadRequestException(
          "Unsupported operation: commitTransaction with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }
  }

  public ListTablesResponse listViews(Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    if (baseCatalog instanceof PolarisIcebergCatalog polarisCatalog) {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      Page<TableIdentifier> results = polarisCatalog.listViews(namespace, pageRequest);
      return ListTablesResponse.builder()
          .addAll(results.items())
          .nextPageToken(results.encodedResponseToken())
          .build();
    } else if (baseCatalog instanceof ViewCatalog vc) {
      return catalogHandlerUtils.listViews(vc, namespace, pageToken, pageSize);
    } else {
      throw new BadRequestException(
          "Unsupported operation: listViews with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }
  }

  public ListTablesResponse listViews(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, namespace);
    initializeCatalog();

    return catalogHandlerUtils.listViews(viewCatalog, namespace);
  }

  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    catalogAuthZ()
        .authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
            op, TableIdentifier.of(namespace, request.name()));
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      var meta =
          restCompatibleCatalog.updateView(
              restCompatibleCatalog.updateTableRequestForNewView(namespace, request), true);
      // TODO configs ?
      return ImmutableLoadViewResponse.builder()
          .metadata(meta)
          .metadataLocation(meta.metadataFileLocation())
          .build();
    }

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot create view on static-facade external catalogs.");
    }
    return catalogHandlerUtils.createView(viewCatalog, namespace, request);
  }

  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      var meta = restCompatibleCatalog.loadViewMetadata(viewIdentifier);
      // TODO configs ?
      return ImmutableLoadViewResponse.builder()
          .metadata(meta)
          .metadataLocation(meta.metadataFileLocation())
          .build();
    }

    return catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
  }

  public LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      var meta =
          restCompatibleCatalog.updateView(
              UpdateTableRequest.create(viewIdentifier, request.requirements(), request.updates()),
              false);
      // TODO configs ?
      return ImmutableLoadViewResponse.builder()
          .metadata(meta)
          .metadataLocation(meta.metadataFileLocation())
          .build();
    }

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot replace view on static-facade external catalogs.");
    }
    return catalogHandlerUtils.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request));
  }

  public void dropView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      if (!restCompatibleCatalog.dropView(viewIdentifier)) {
        throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
      }
      return;
    }

    catalogHandlerUtils.dropView(viewCatalog, viewIdentifier);
  }

  public void viewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    initializeCatalog();

    if (baseCatalog instanceof RESTCompatibleCatalog restCompatibleCatalog) {
      if (!restCompatibleCatalog.viewExists(viewIdentifier)) {
        throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
      }
      return;
    }

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
  }

  public void renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    catalogAuthZ()
        .authorizeRenameTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());
    initializeCatalog();

    if (catalogAccess().isStaticFacade()) {
      throw new BadRequestException("Cannot rename view on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameView(viewCatalog, request);
  }

  private @Nonnull LoadTableResponse filterResponseToSnapshots(
      LoadTableResponse loadTableResponse, String snapshots) {
    if (snapshots == null || snapshots.equalsIgnoreCase(SNAPSHOTS_ALL)) {
      return loadTableResponse;
    } else if (snapshots.equalsIgnoreCase(SNAPSHOTS_REFS)) {
      TableMetadata metadata = loadTableResponse.tableMetadata();

      Set<Long> referencedSnapshotIds =
          metadata.refs().values().stream()
              .map(SnapshotRef::snapshotId)
              .collect(Collectors.toSet());

      TableMetadata filteredMetadata =
          metadata.removeSnapshotsIf(s -> !referencedSnapshotIds.contains(s.snapshotId()));

      return LoadTableResponse.builder()
          .withTableMetadata(filteredMetadata)
          .addAllConfig(loadTableResponse.config())
          .addAllCredentials(loadTableResponse.credentials())
          .build();
    } else {
      throw new IllegalArgumentException("Unrecognized snapshots: " + snapshots);
    }
  }

  private EnumSet<PolarisAuthorizableOperation> getUpdateTableAuthorizableOperations(
      UpdateTableRequest request) {
    boolean useFineGrainedOperations =
        realmConfig()
            .getConfig(
                FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES,
                catalogAccess().catalogProperties());

    if (useFineGrainedOperations) {
      EnumSet<PolarisAuthorizableOperation> actions =
          request.updates().stream()
              .map(
                  update ->
                      switch (update) {
                        case MetadataUpdate.AssignUUID assignUuid ->
                            PolarisAuthorizableOperation.ASSIGN_TABLE_UUID;
                        case MetadataUpdate.UpgradeFormatVersion upgradeFormat ->
                            PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION;
                        case MetadataUpdate.AddSchema addSchema ->
                            PolarisAuthorizableOperation.ADD_TABLE_SCHEMA;
                        case MetadataUpdate.SetCurrentSchema setCurrentSchema ->
                            PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA;
                        case MetadataUpdate.AddPartitionSpec addPartitionSpec ->
                            PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC;
                        case MetadataUpdate.AddSortOrder addSortOrder ->
                            PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER;
                        case MetadataUpdate.SetDefaultSortOrder setDefaultSortOrder ->
                            PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER;
                        case MetadataUpdate.AddSnapshot addSnapshot ->
                            PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT;
                        case MetadataUpdate.SetSnapshotRef setSnapshotRef ->
                            PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.RemoveSnapshots removeSnapshots ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS;
                        case MetadataUpdate.RemoveSnapshotRef removeSnapshotRef ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.SetLocation setLocation ->
                            PolarisAuthorizableOperation.SET_TABLE_LOCATION;
                        case MetadataUpdate.SetProperties setProperties ->
                            PolarisAuthorizableOperation.SET_TABLE_PROPERTIES;
                        case MetadataUpdate.RemoveProperties removeProperties ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES;
                        case MetadataUpdate.SetStatistics setStatistics ->
                            PolarisAuthorizableOperation.SET_TABLE_STATISTICS;
                        case MetadataUpdate.RemoveStatistics removeStatistics ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS;
                        case MetadataUpdate.RemovePartitionSpecs removePartitionSpecs ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS;
                        default ->
                            PolarisAuthorizableOperation
                                .UPDATE_TABLE; // Fallback for unknown update types
                      })
              .collect(
                  () -> EnumSet.noneOf(PolarisAuthorizableOperation.class),
                  EnumSet::add,
                  EnumSet::addAll);

      // If there are no MetadataUpdates, then default to the UPDATE_TABLE operation.
      if (actions.isEmpty()) {
        actions.add(PolarisAuthorizableOperation.UPDATE_TABLE);
      }

      return actions;
    } else {
      return EnumSet.of(PolarisAuthorizableOperation.UPDATE_TABLE);
    }
  }

  private void checkAllowExternalCatalogCredentialVending(
      EnumSet<AccessDelegationMode> delegationModes) {

    if (delegationModes.isEmpty()) {
      return;
    }

    var catalogProperties = catalogAccess().catalogProperties();

    LOGGER.info("Catalog type: {}", catalogAccess().catalogType());
    LOGGER.info(
        "allow external catalog credential vending: {}",
        realmConfig()
            .getConfig(
                FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING, catalogProperties));
    if (catalogAccess()
            .catalogType()
            .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.EXTERNAL)
        && !realmConfig()
            .getConfig(
                FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING,
                catalogProperties)) {
      throw new ForbiddenException(
          "Access Delegation is not enabled for this catalog. Please consult applicable "
              + "documentation for the catalog config property '%s' to enable this feature",
          FeatureConfiguration.ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING.catalogConfig());
    }
  }

  @Override
  public void close() throws Exception {
    if (baseCatalog instanceof Closeable closeable) {
      closeable.close();
    }
  }

  public ConfigResponse getConfig() {
    // 'catalogName' is taken from the REST request's 'warehouse' query parameter.
    // 'warehouse' as an output will be treated by the client as a default catalog
    //   storage base location.
    // 'prefix' as an output is the REST subpath that routes to the catalog
    //   resource, which may be URL-escaped catalogName or potentially a different
    //   unique identifier for the catalog being accessed.
    if (catalogName() == null) {
      throw new BadRequestException("Please specify a warehouse");
    }

    Map<String, String> properties = catalogAccess().catalogProperties();

    String prefix = prefixParser.catalogNameToPrefix(catalogName());
    return ConfigResponse.builder()
        .withDefaults(properties) // catalog properties are defaults
        .withOverrides(ImmutableMap.of("prefix", prefix))
        .withEndpoints(
            ImmutableList.<Endpoint>builder()
                .addAll(DEFAULT_ENDPOINTS)
                .addAll(VIEW_ENDPOINTS)
                .addAll(PolarisEndpoints.getSupportedGenericTableEndpoints(realmConfig()))
                .addAll(PolarisEndpoints.getSupportedPolicyEndpoints(realmConfig()))
                .build())
        .build();
  }
}

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

package org.apache.polaris.service.catalog.iceberg.nosql;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.polaris.core.config.BehaviorChangeConfiguration.ALLOW_NAMESPACE_CUSTOM_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.DROP_WITH_PURGE_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.LIST_PAGINATION_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.core.entity.CatalogEntity.REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY;
import static org.apache.polaris.core.entity.NamespaceEntity.PARENT_NAMESPACE_KEY;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.api.obj.ObjTypes.objTypeById;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifier;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifierFor;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.indexKeyToIdentifier;
import static org.apache.polaris.persistence.nosql.metastore.NoSqlPaginationToken.paginationToken;
import static org.apache.polaris.persistence.nosql.metastore.ResolvedPath.resolvePathWithOptionalLeaf;
import static org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexUtils.hasChildren;
import static org.apache.polaris.persistence.nosql.metastore.mutation.MutationAttempt.updateLocationsIndex;
import static org.apache.polaris.service.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;
import static org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils.buildTableMetadataPropertiesMap;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
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
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet;
import org.apache.polaris.persistence.nosql.coretypes.changes.Change;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeAdd;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeRemove;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeRename;
import org.apache.polaris.persistence.nosql.coretypes.changes.ChangeUpdate;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.apache.polaris.persistence.nosql.coretypes.content.GenericTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.NoSqlPaginationToken;
import org.apache.polaris.persistence.nosql.metastore.ResolvedPath;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.common.LocationUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.catalog.validation.IcebergPropertiesValidation;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.apache.polaris.service.types.NotificationRequest;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PolarisImmutable
public abstract class IcebergCatalogHandlerNoSqlImpl extends NoSqlAuthZ
    implements IcebergCatalogHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergCatalogHandlerNoSqlImpl.class);

  private static final Joiner SLASH = Joiner.on("/").skipNulls();

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

  protected abstract StorageAccessConfigProvider storageAccessConfigProvider();

  protected abstract FileIOFactory fileIOFactory();

  protected abstract MonotonicClock monotonicClock();

  protected abstract CatalogPrefixParser prefixParser();

  protected abstract EventAttributeMap eventAttributeMap();

  protected abstract ReservedProperties reservedProperties();

  protected abstract PolarisEventListener polarisEventListener();

  protected abstract PolarisEventMetadataFactory eventMetadataFactory();

  protected abstract PolarisMetricsReporter metricsReporter();

  protected abstract Clock clock();

  @SuppressWarnings("immutables:incompat")
  private FileIO catalogFileIO;

  @SuppressWarnings("immutables:incompat")
  private boolean loggedPrefixOverlapWarning;

  @Value.Default
  protected CloseableGroup closeableGroup() {
    var closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);
    return closeableGroup;
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    authorizeBasicNamespaceOperationOrThrow(PolarisAuthorizableOperation.LIST_NAMESPACES, parent);

    var page =
        listChildren(
            parent,
            pageToken(pageToken, pageSize),
            NamespaceObj.class,
            elemIdent -> Namespace.of(elemIdent.elements().toArray(new String[0])));
    return ListNamespacesResponse.builder()
        .addAll(page.items())
        .nextPageToken(page.encodedResponseToken())
        .build();
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    request.validate();

    var namespace = request.namespace();
    var ident = identifier(namespace.levels());

    authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.CREATE_NAMESPACE, namespace);

    var result =
        store()
            .catalogContentChange(
                (state, ref, byName, byId, changes, locations) -> {
                  var resolvedPath =
                      resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                          .orElseThrow(
                              () ->
                                  new NoSuchNamespaceException(
                                      "Cannot create namespace %s. Parent namespace does not exist.",
                                      namespace));
                  resolvedPath
                      .leafObj()
                      .ifPresent(
                          existingObj -> {
                            throw new AlreadyExistsException(
                                "Cannot create namespace %s. %s already exists",
                                namespace, typeAsName(existingObj));
                          });

                  var properties = new HashMap<>(request.properties());

                  properties.put(
                      ENTITY_BASE_LOCATION,
                      baseLocationFor(
                          resolvedPath.namespaceElements(),
                          ident.leafName(),
                          properties,
                          Optional.empty()));

                  validateLocationForNamespace(
                      ident,
                      resolvedPath,
                      properties,
                      new CommitChangesHandler.ValidationContext(
                          state.persistence(), byName, locations));

                  //

                  var objBuilder =
                      newObjBuilder(state.persistence(), LocalNamespaceObj.builder())
                          .name(ident.leafName())
                          .properties(properties);

                  if (ident.length() > 1) {
                    objBuilder.putInternalProperty(
                        PARENT_NAMESPACE_KEY,
                        RESTUtil.encodeNamespace(
                            Namespace.of(ident.parent().elements().toArray(new String[0]))));
                  }

                  var obj =
                      finishChangeForNewObj(
                          state, byName, byId, changes, locations, resolvedPath, objBuilder, ident);

                  return new ChangeResult.CommitChange<>(
                      new NamespaceResult(obj.stableId(), properties));
                },
                NamespaceResult.class);

    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(result.properties())
        .build();
  }

  record NamespaceResult(long id, Map<String, String> properties) {}

  @Override
  public GetNamespaceResponse loadNamespaceMetadata(Namespace namespace) {

    authorizeBasicNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA, namespace);

    var properties =
        storeNameIndex()
            .map(index -> index.get(identifier(namespace.levels()).toIndexKey()))
            .flatMap(
                objRef ->
                    Optional.ofNullable(store().persistence().fetch(objRef, NamespaceObj.class)))
            .map(NamespaceObj::properties)
            .orElseThrow(() -> noSuchNamespaceException(namespace));
    return GetNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(properties)
        .build();
  }

  @Override
  public void namespaceExists(Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.NAMESPACE_EXISTS, namespace);

    if (storeNameIndex()
        .map(index -> index.get(identifier(namespace.levels()).toIndexKey()))
        .flatMap(
            objRef -> Optional.ofNullable(store().persistence().fetch(objRef, NamespaceObj.class)))
        .isEmpty()) {
      throw noSuchNamespaceException(namespace);
    }
  }

  @Override
  public void dropNamespace(Namespace namespace) {
    var ident = identifier(namespace.levels());
    if (ident.isEmpty()) {
      throw new IllegalArgumentException("Cannot drop root namespace");
    }

    authorizeBasicNamespaceOperationOrThrow(PolarisAuthorizableOperation.DROP_NAMESPACE, namespace);

    store()
        .catalogContentChange(
            (state, ref, byName, byId, changes, locations) ->
                resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                    .orElseThrow(() -> noSuchNamespaceException(namespace))
                    .leafObjAs(NamespaceObj.class)
                    .map(
                        namespaceObj -> {
                          if (hasChildren(byName, ident)) {
                            throw new NamespaceNotEmptyException(
                                "Namespace %s is not empty", namespace);
                          }

                          finishChangeForDeleteObj(
                              byName, byId, changes, locations, namespaceObj, ident);

                          return new ChangeResult.CommitChange<>(true);
                        })
                    .orElseThrow(() -> noSuchNamespaceException(namespace)),
            Boolean.class);
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    request.validate();

    authorizeBasicNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES, namespace);

    var ident = identifier(namespace.levels());

    return store()
        .catalogContentChange(
            (state, ref, byName, byId, changes, locations) -> {
              var resolvedPath =
                  resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                      .orElseThrow(() -> noSuchNamespaceException(namespace));
              return resolvedPath
                  .leafObjAs(NamespaceObj.class)
                  .map(
                      namespaceObj -> {
                        var properties = new HashMap<>(namespaceObj.properties());

                        var response = UpdateNamespacePropertiesResponse.builder();

                        properties.putAll(request.updates());
                        response.addUpdated(request.updates().keySet());
                        for (var removal : request.removals()) {
                          if (properties.remove(removal) != null) {
                            response.addRemoved(removal);
                          } else {
                            response.addMissing(removal);
                          }
                        }

                        if (properties.equals(namespaceObj.properties())) {
                          // Nothing changed, so no need to commit anything
                          return new ChangeResult.NoChange<>(
                              UpdateNamespacePropertiesResponse.builder().build());
                        }

                        validateLocationForNamespace(
                            ident,
                            resolvedPath,
                            properties,
                            new CommitChangesHandler.ValidationContext(
                                state.persistence(), byName, locations));

                        var objBuilder =
                            updateObjBuilder(
                                    state.persistence(), namespaceObj, LocalNamespaceObj.builder())
                                .properties(properties);

                        finishChangeForUpdatedObj(
                            state, byName, changes, locations, namespaceObj, objBuilder, ident);

                        return new ChangeResult.CommitChange<>(response.build());
                      })
                  .orElseThrow(() -> noSuchNamespaceException(namespace));
            },
            UpdateNamespacePropertiesResponse.class);
  }

  @Override
  public ListTablesResponse listTables(Namespace namespace, String pageToken, Integer pageSize) {
    authorizeBasicNamespaceOperationOrThrow(PolarisAuthorizableOperation.LIST_TABLES, namespace);

    var page =
        listChildren(
            namespace,
            pageToken(pageToken, pageSize),
            tableLikeType(PolarisEntitySubType.ICEBERG_TABLE),
            elemIdent -> TableIdentifier.of(namespace, elemIdent.leafName()));
    return ListTablesResponse.builder()
        .addAll(page.items())
        .nextPageToken(page.encodedResponseToken())
        .build();
  }

  @Override
  public LoadTableResponse createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    return createTable(namespace, request, delegationModes, refreshCredentialsEndpoint);
  }

  @Override
  public LoadTableResponse createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    return createTable(namespace, request, delegationModes, refreshCredentialsEndpoint);
  }

  private LoadTableResponse createTable(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    request.validate();

    var tableIdentifier = TableIdentifier.of(namespace, request.name());

    var authzOp =
        delegationModes.isEmpty()
            ? (request.stageCreate()
                ? PolarisAuthorizableOperation.CREATE_TABLE_STAGED
                : PolarisAuthorizableOperation.CREATE_TABLE_DIRECT)
            : (request.stageCreate()
                ? PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION
                : PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION);
    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(authzOp, tableIdentifier);

    // TODO ?
    // var catalog = getResolvedCatalogEntity();
    // if (catalog.isStaticFacade()) {
    //    throw new BadRequestException("Cannot create table on static-facade external
    // catalogs.");
    // }
    // checkAllowExternalCatalogCredentialVending(delegationModes);

    var location = request.location();
    if (request.stageCreate()) {
      // TODO verify create
      location = fixupTableLikeLocation(tableIdentifier, location);
    }

    var spec = request.spec();
    var writeOrder = request.writeOrder();
    var properties = request.properties();
    var metaTemp =
        TableMetadata.newTableMetadata(
            request.schema(),
            spec != null ? spec : PartitionSpec.unpartitioned(),
            writeOrder != null ? writeOrder : SortOrder.unsorted(),
            location,
            properties);

    if (request.stageCreate()) {
      // Namespace
      var resolvedPathOptional = resolveEntity(tableIdentifier);

      // TODO verify left not present??

      return buildLoadTableResponse(
          tableIdentifier,
          metaTemp,
          resolvedPathOptional,
          // TODO ALL does not seem correct - security issue?
          EnumSet.of(PolarisStorageActions.ALL),
          delegationModes,
          refreshCredentialsEndpoint);
    }

    var updates = new ArrayList<MetadataUpdate>(metaTemp.changes().size() + 1);
    var hasUpgradeFormatVersion = false;
    var hasSetProperties = false;
    for (int i = 0; i < metaTemp.changes().size(); i++) {
      var change = metaTemp.changes().get(i);
      if (change instanceof MetadataUpdate.UpgradeFormatVersion) {
        hasUpgradeFormatVersion = true;
      }
      if (change instanceof MetadataUpdate.SetProperties) {
        hasSetProperties = true;
        change = new MetadataUpdate.SetProperties(properties);
      }
      updates.add(change);
    }
    if (!hasSetProperties && !properties.isEmpty()) {
      updates.add(new MetadataUpdate.SetProperties(properties));
    }
    if (!hasUpgradeFormatVersion) {
      updates.add(new MetadataUpdate.UpgradeFormatVersion(metaTemp.formatVersion()));
    }

    return updateTable(
        tableIdentifier,
        UpdateTableRequest.create(
            tableIdentifier, UpdateRequirements.forCreateTable(updates), updates),
        delegationModes,
        refreshCredentialsEndpoint,
        true);
  }

  @Override
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    request.validate();

    var name = request.name();
    var identifier = TableIdentifier.of(namespace, name);

    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.REGISTER_TABLE, identifier);

    var metadataLocation = request.metadataLocation();
    var metadata = registerTableLoadMetadata(identifier, metadataLocation);
    var ident = identifierFor(identifier);
    var resolvedPathResult =
        store()
            .catalogContentChange(
                (state, ref, byName, byId, changes, locations) -> {
                  var resolvedPath =
                      resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                          .orElseThrow(() -> noSuchNamespaceException(namespace));
                  resolvedPath
                      .leafObj()
                      .ifPresent(
                          l -> {
                            throw alreadyExistsException(
                                identifier, l.type(), IcebergTableObj.TYPE);
                          });

                  var objBuilder =
                      newObjBuilder(state.persistence(), IcebergTableObj.builder())
                          .name(ident.leafName())
                          .putProperty(ENTITY_BASE_LOCATION, metadata.location())
                          .internalProperties(buildTableMetadataPropertiesMap(metadata))
                          .metadataLocation(metadataLocation);

                  var created =
                      finishChangeForNewObj(
                          state, byName, byId, changes, locations, resolvedPath, objBuilder, ident);

                  return new ChangeResult.CommitChange<>(resolvedPath.withLeaf(created));
                },
                ResolvedPath.class);

    return buildLoadTableResponse(
        identifier,
        metadata,
        Optional.of(resolvedPathResult),
        // TODO authz on write
        EnumSet.of(
            PolarisStorageActions.READ, PolarisStorageActions.WRITE, PolarisStorageActions.LIST),
        // TODO
        EnumSet.of(AccessDelegationMode.VENDED_CREDENTIALS),
        Optional.empty());
  }

  @Override
  public void reportMetrics(TableIdentifier identifier, ReportMetricsRequest request) {
    request.validate();

    authorizeBasicTableLikeOperationOrThrow(
        request.report() instanceof ScanReport
            ? PolarisAuthorizableOperation.REPORT_READ_METRICS
            : PolarisAuthorizableOperation.REPORT_WRITE_METRICS,
        PolarisEntitySubType.ICEBERG_TABLE,
        identifier);

    metricsReporter().reportMetric(catalogName(), identifier, request.report(), clock().instant());
  }

  @Override
  public Optional<LoadTableResponse> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    var storageActions = authorizeLoadTable(tableIdentifier, delegationModes);

    var resolvedPathOptional = resolveEntity(tableIdentifier, IcebergTableObj.TYPE);
    var metadata =
        loadMetadata(
            tableIdentifier,
            IcebergTableObj.TYPE,
            resolvedPathOptional,
            TableMetadataParser::read,
            PolarisEventType.BEFORE_REFRESH_TABLE,
            PolarisEventType.AFTER_REFRESH_TABLE);
    return Optional.of(
        buildLoadTableResponse(
            tableIdentifier,
            metadata,
            resolvedPathOptional,
            storageActions,
            delegationModes,
            refreshCredentialsEndpoint));
  }

  private LoadTableResponse buildLoadTableResponse(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Optional<ResolvedPath> resolvedPath,
      Set<PolarisStorageActions> storageActions,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    var tableLocations = StorageUtil.getLocationsUsedByTable(tableMetadata);
    var responseBuilder = LoadTableResponse.builder().withTableMetadata(tableMetadata);

    if (resolvedPath.isPresent()) {
      var fullPath = store().resolvedPathToPolarisEntities(resolvedPath.get(), true);

      var actions =
          delegationModes.isEmpty() ? EnumSet.noneOf(PolarisStorageActions.class) : storageActions;
      var storageAccessConfig =
          storageAccessConfigProvider()
              .getStorageAccessConfig(
                  tableIdentifier, tableLocations, actions, refreshCredentialsEndpoint, fullPath);

      var credentialConfig = storageAccessConfig.credentials();
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

    return responseBuilder.build();
  }

  @Override
  public LoadTableResponse updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    request.validate();

    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE, tableIdentifier);

    return updateTable(
        tableIdentifier,
        request,
        EnumSet.noneOf(AccessDelegationMode.class),
        Optional.empty(),
        false);
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    request.validate();

    authorizeBasicTableLikeOperationsOrThrow(
        getUpdateTableAuthorizableOperations(request),
        PolarisEntitySubType.ICEBERG_TABLE,
        tableIdentifier);

    return updateTable(
        tableIdentifier,
        request,
        EnumSet.noneOf(AccessDelegationMode.class),
        Optional.empty(),
        false);
  }

  private LoadTableResponse updateTable(
      TableIdentifier tableIdentifier,
      UpdateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint,
      boolean directUpdateOrCreate) {
    request.validate();

    request = fixUpdateTableRequest(request, tableIdentifier);
    var committedChange =
        commitChange(newContentChange(ContentChange.Target.TABLE, request), directUpdateOrCreate);
    var metadata = (TableMetadata) committedChange.requiredMetadata();
    return buildLoadTableResponse(
        request.identifier(),
        metadata,
        Optional.of(committedChange.resolvedPath()),
        // TODO authz on write?
        EnumSet.of(
            PolarisStorageActions.READ, PolarisStorageActions.WRITE, PolarisStorageActions.LIST),
        delegationModes,
        refreshCredentialsEndpoint);
  }

  private UpdateTableRequest fixUpdateTableRequest(
      UpdateTableRequest request, TableIdentifier identifier) {
    request.validate();

    if (request.identifier() == null) {
      return UpdateTableRequest.create(identifier, request.requirements(), request.updates());
    }
    return request;
  }

  @Override
  public void tableExists(TableIdentifier tableIdentifier) {
    authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.TABLE_EXISTS,
        PolarisEntitySubType.ICEBERG_TABLE,
        tableIdentifier);

    if (resolveEntity(tableIdentifier, IcebergTableObj.TYPE).isEmpty()) {
      throw notFoundExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.ICEBERG_TABLE);
    }
  }

  @Override
  public void renameTable(RenameTableRequest request) {
    request.validate();

    authorizeRenameTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.RENAME_TABLE,
        PolarisEntitySubType.ICEBERG_TABLE,
        request.source(),
        request.destination());

    renameTableLike(PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());
  }

  @Override
  public void dropTable(TableIdentifier tableIdentifier, boolean purgeRequested) {
    authorizeBasicTableLikeOperationOrThrow(
        purgeRequested
            ? PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE
            : PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE,
        PolarisEntitySubType.ICEBERG_TABLE,
        tableIdentifier);

    dropTableLike(PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, purgeRequested);
  }

  @Override
  public void commitTransaction(CommitTransactionRequest request) {
    request.validate();

    authorizeCollectionOfTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.COMMIT_TRANSACTION,
        PolarisEntitySubType.ICEBERG_TABLE,
        request.tableChanges().stream().map(UpdateTableRequest::identifier).toList());

    var commitedChanges =
        commitChanges(
            request.tableChanges().stream()
                .map(c -> newContentChange(ContentChange.Target.TABLE, c))
                .toList(),
            false);
    eventAttributeMap()
        .put(
            EventAttributes.TABLE_METADATAS,
            commitedChanges.stream().map(c -> (TableMetadata) c.requiredMetadata()).toList());
  }

  @Override
  public ListTablesResponse listViews(Namespace namespace, String pageToken, Integer pageSize) {
    var page =
        listChildren(
            namespace,
            pageToken(pageToken, pageSize),
            tableLikeType(PolarisEntitySubType.ICEBERG_VIEW),
            elemIdent -> TableIdentifier.of(namespace, elemIdent.leafName()));
    return ListTablesResponse.builder()
        .addAll(page.items())
        .nextPageToken(page.encodedResponseToken())
        .build();
  }

  @Override
  public LoadViewResponse createView(Namespace namespace, CreateViewRequest request) {
    request.validate();

    authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation.CREATE_VIEW, TableIdentifier.of(namespace, request.name()));

    var updateTableRequest = updateTableRequestForNewView(namespace, request);
    return replaceView(
        updateTableRequest.identifier(), updateTableRequestForNewView(namespace, request));
  }

  @Override
  public LoadViewResponse loadView(TableIdentifier viewIdentifier) {
    authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.LOAD_VIEW, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    var resolvedPathOptional = resolveEntity(viewIdentifier, IcebergViewObj.TYPE);
    var metadata =
        loadMetadata(
            viewIdentifier,
            IcebergViewObj.TYPE,
            resolvedPathOptional,
            ViewMetadataParser::read,
            PolarisEventType.BEFORE_REFRESH_VIEW,
            PolarisEventType.AFTER_REFRESH_VIEW);
    return buildLoadViewResponse(metadata);
  }

  @Override
  public LoadViewResponse replaceView(TableIdentifier viewIdentifier, UpdateTableRequest request) {
    request.validate();

    authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.REPLACE_VIEW,
        PolarisEntitySubType.ICEBERG_VIEW,
        viewIdentifier);

    request = fixUpdateTableRequest(request, viewIdentifier);
    var metadata =
        (ViewMetadata)
            commitChange(newContentChange(ContentChange.Target.VIEW, request), false)
                .requiredMetadata();
    return buildLoadViewResponse(metadata);
  }

  private static LoadViewResponse buildLoadViewResponse(ViewMetadata metadata) {
    return ImmutableLoadViewResponse.builder()
        .metadata(metadata)
        .metadataLocation(
            requireNonNull(
                metadata.metadataFileLocation(), "metadata file location must not be null"))
        .build();
  }

  @Override
  public void dropView(TableIdentifier viewIdentifier) {
    authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.DROP_VIEW, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);

    dropTableLike(PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier, false);
  }

  @Override
  public void viewExists(TableIdentifier viewIdentifier) {
    authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.VIEW_EXISTS,
        PolarisEntitySubType.ICEBERG_VIEW,
        viewIdentifier);

    if (resolveEntity(viewIdentifier, IcebergViewObj.TYPE).isEmpty()) {
      throw notFoundExceptionForTableLikeEntity(viewIdentifier, PolarisEntitySubType.ICEBERG_VIEW);
    }
  }

  @Override
  public void renameView(RenameTableRequest request) {
    request.validate();

    authorizeRenameTableLikeOperationOrThrow(
        PolarisAuthorizableOperation.RENAME_VIEW,
        PolarisEntitySubType.ICEBERG_VIEW,
        request.source(),
        request.destination());

    renameTableLike(PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());
  }

  @Override
  public ConfigResponse getConfig() {
    String prefix = prefixParser().catalogNameToPrefix(catalogName());
    var properties = store().catalog().properties();
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

  @Override
  public boolean sendNotification(TableIdentifier identifier, NotificationRequest request) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws Exception {
    closeableGroup().close();
  }

  public static ImmutableIcebergCatalogHandlerNoSqlImpl.Builder builder() {
    return ImmutableIcebergCatalogHandlerNoSqlImpl.builder();
  }

  private void validateLocationForNamespace(
      ContentIdentifier ident,
      ResolvedPath resolvedPath,
      HashMap<String, String> properties,
      CommitChangesHandler.ValidationContext validationContext) {
    if (!realmConfig().getConfig(ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
      LOGGER.debug("Validating no overlap with sibling tables or namespaces");
      validateNoLocationOverlap(
          ident,
          resolvedPath,
          Optional.ofNullable(properties.get(ENTITY_BASE_LOCATION)),
          validationContext);
    } else {
      LOGGER.debug("Skipping location overlap validation for namespace '{}'", ident);
    }
    if (!realmConfig().getConfig(ALLOW_NAMESPACE_CUSTOM_LOCATION, store().catalog().properties())) {
      if (properties.containsKey(ENTITY_BASE_LOCATION)) {
        validateNamespaceLocation(ident, resolvedPath, properties);
      }
    }
  }

  /** Checks whether the location of a namespace is valid given its parent */
  private void validateNamespaceLocation(
      ContentIdentifier ident, ResolvedPath resolvedPath, HashMap<String, String> properties) {
    StorageLocation namespaceLocation =
        StorageLocation.of(
            baseLocationFor(
                resolvedPath.namespaceElements(), ident.leafName(), properties, Optional.empty()));
    if (resolvedPath.namespaceElements().isEmpty()) {
      var catalogObj = store().catalog();
      LOGGER.debug(
          "Validating namespace {} given parent catalog {}", ident.leafName(), catalogObj.name());
      var storageConfigInfo =
          catalogObj
              .storageConfigurationInfo()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Cannot create namespace without a parent storage configuration"));
      var defaultLocations =
          storageConfigInfo.getAllowedLocations().stream()
              .filter(java.util.Objects::nonNull)
              .map(
                  l ->
                      ensureTrailingSlash(
                              ensureTrailingSlash(new StringBuilder(l)).append(ident.leafName()))
                          .toString())
              .map(StorageLocation::of)
              .toList();
      if (!defaultLocations.contains(namespaceLocation)) {
        throw new IllegalArgumentException(
            "Namespace "
                + ident.leafName()
                + " has a custom location, "
                + "which is not enabled. Expected a location in: ["
                + String.join(
                    ", ", defaultLocations.stream().map(StorageLocation::toString).toList())
                + "]. Got location: "
                + namespaceLocation
                + "]");
      }
    } else {
      var parentNamespace = resolvedPath.namespaceElements().getLast();
      LOGGER.debug(
          "Validating namespace {} given parent namespace {}",
          ident.leafName(),
          parentNamespace.name());
      var parentLocation =
          baseLocationFor(
              resolvedPath.namespaceElements(),
              parentNamespace.name(),
              parentNamespace.properties(),
              Optional.of(ident.leafName()),
              true);
      var defaultLocation = StorageLocation.of(parentLocation);
      if (!defaultLocation.equals(namespaceLocation)) {
        throw new IllegalArgumentException(
            "Namespace "
                + ident.leafName()
                + " has a custom location, "
                + "which is not enabled. Expected location: ["
                + defaultLocation
                + "]. Got location: ["
                + namespaceLocation
                + "]");
      }
    }
  }

  /**
   * Validate no location overlap exists between the entity path and its sibling entities. This
   * resolves all siblings at the same level as the target entity (namespaces if the target entity
   * is a namespace whose parent is the catalog, namespaces and tables otherwise) and checks the
   * base-location property of each. The target entity's base location may not be a prefix or a
   * suffix of any sibling entity's base location.
   */
  private void validateNoLocationOverlap(
      ContentIdentifier ident,
      ResolvedPath resolvedPath,
      Optional<String> baseLocation,
      CommitChangesHandler.ValidationContext validationContext) {

    var location = baseLocation.orElseThrow(() -> new IllegalArgumentException("No location"));

    // Attempt to directly query for siblings
    if (realmConfig().getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK)) {
      store()
          .hasOverlappingSiblings(location)
          .ifPresent(
              reason -> {
                throw new ForbiddenException(
                    "Unable to create entity at location '%s' because it conflicts with existing table or namespace at %s",
                    location, reason);
              });
      return;
    }

    var targetLocation = StorageLocation.of(location);
    store()
        .listChildren(validationContext.byName(), ident.parent())
        .filter(c -> resolvedPath.leafObj().map(l -> l.id() != c.id()).orElse(true))
        .map(c -> c.properties().get(ENTITY_BASE_LOCATION))
        .filter(java.util.Objects::nonNull)
        .map(StorageLocation::of)
        .forEach(
            siblingLocation -> {
              if (targetLocation.isChildOf(siblingLocation)
                  || siblingLocation.isChildOf(targetLocation)) {
                throw new ForbiddenException(
                    "Unable to create table at location '%s' because it conflicts with existing table or namespace at location '%s'",
                    targetLocation, siblingLocation);
              }
            });
  }

  @SuppressWarnings("SameParameterValue") // TODO ?
  private ContentChange.CommittedContentChange commitChange(
      ContentChange contentChange, boolean directUpdateOrCreate) {
    return commitChanges(List.of(contentChange), directUpdateOrCreate).getFirst();
  }

  /**
   * Commits one or multiple changes against tables and/or views.
   *
   * <p>Suitable for single table/view changes and batch changes, even with both tables and views.
   *
   * <p>Implicitly provides a simple form of conflict resolution in cases two concurrent changes
   * update different parts (snapshots, partition specs, sort orders, schema) of a table. Concurrent
   * changes against the same part of a table still yield to conflicts.
   *
   * <p>See {@link CommitChangesHandler} for a description of the inner workings.
   */
  private List<ContentChange.CommittedContentChange> commitChanges(
      List<ContentChange> contentChanges, boolean directUpdateOrCreate) {

    // TODO configurable
    var commitTimeout = Duration.ofSeconds(120);
    @SuppressWarnings("resource")
    var monotonicClock = monotonicClock();
    var deadline = monotonicClock.currentInstant().plus(commitTimeout);

    try (var commitChangesHandler =
        new CommitChangesHandler(
            catalogFileIO(),
            store(),
            directUpdateOrCreate,
            contentChanges,
            (event, contentChange) -> {
              switch (event) {
                case BEFORE_LOAD_METADATA ->
                    emitEvent(
                        contentChange.tableIdentifier(),
                        switch (contentChange.target()) {
                          case TABLE -> PolarisEventType.BEFORE_REFRESH_TABLE;
                          case VIEW -> PolarisEventType.BEFORE_REFRESH_VIEW;
                        });
                case AFTER_LOAD_METADATA ->
                    emitEvent(
                        contentChange.tableIdentifier(),
                        switch (contentChange.target()) {
                          case TABLE -> PolarisEventType.AFTER_REFRESH_TABLE;
                          case VIEW -> PolarisEventType.AFTER_REFRESH_VIEW;
                        });
                default -> {}
              }
            })) {

      // Outer loop (until a commit succeeded or failed):
      for (var attempt = 0; ; attempt++) {
        if (deadline.isBefore(monotonicClock.currentInstant())) {
          throw new CommitFailedException(
              "Commit timed out after %s and %d attempts", commitTimeout, attempt);
        }

        // Load missing table-metadata, memoize additional commit/version state if necessary
        commitChangesHandler.setupMetadata();

        // Store table-metadata to object stores
        commitChangesHandler.storeUpdatedMetadata();

        var commitResult =
            store()
                .catalogContentChange(
                    (state, ref, byName, byId, changes, locations) -> {
                      // Check if all tables are at the same state
                      // If not, don't commit and yield that status to the outer loop
                      if (!commitChangesHandler.verifyState(state.persistence(), byName)) {
                        // Some metadata has changed in the meantime.
                        // Abort the commit-retry loop and try again,
                        // potentially re-fetching metadata from object stores
                        // and writing new metadata.
                        return new ChangeResult.NoChange<>(false);
                      }

                      // Apply table-changes
                      commitChangesHandler.applyToCommit(
                          realmConfig(),
                          new CommitChangesHandler.ValidationContext(
                              state.persistence(), byName, locations),
                          this::validateNoLocationOverlap,
                          (contentChange, resolvedPath, newObjBuilder) -> {
                            switch (contentChange.operation()) {
                              case CREATE ->
                                  finishChangeForNewObj(
                                      state,
                                      byName,
                                      byId,
                                      changes,
                                      locations,
                                      resolvedPath,
                                      newObjBuilder,
                                      contentChange.identifier());
                              case UPDATE ->
                                  finishChangeForUpdatedObj(
                                      state,
                                      byName,
                                      changes,
                                      locations,
                                      resolvedPath.leafObjOrNull(),
                                      newObjBuilder,
                                      contentChange.identifier());
                            }
                          });

                      // Changes applied
                      return new ChangeResult.CommitChange<>(true);
                    },
                    Boolean.class);

        if (commitResult) {
          // All changes successfully committed!

          return commitChangesHandler.committed();
        }
      }
    }
  }

  private String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      var l =
          SLASH.join(defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
      return realmConfig().getConfig(ADD_TRAILING_SLASH_TO_LOCATION)
          ? StorageLocation.ensureTrailingSlash(l)
          : l;
    } else {
      var resolvedPathOptional =
          resolveEntity(tableIdentifier)
              .orElseThrow(() -> noSuchNamespaceException(tableIdentifier));

      return baseLocationFor(
          resolvedPathOptional.namespaceElements(),
          tableIdentifier.name(),
          Map.of(),
          Optional.empty());
    }
  }

  private String baseLocationFor(
      List<NamespaceObj> namespaceElements,
      String objName,
      Map<String, String> properties,
      Optional<String> additionalElement) {
    return baseLocationFor(
        namespaceElements,
        objName,
        properties,
        additionalElement,
        realmConfig().getConfig(ADD_TRAILING_SLASH_TO_LOCATION));
  }

  private String baseLocationFor(
      List<NamespaceObj> namespaceElements,
      String objName,
      Map<String, String> properties,
      Optional<String> additionalElement,
      boolean addFinalTrailingSlash) {

    var loc = new StringBuilder();

    var entityBaseLocation = properties.get(ENTITY_BASE_LOCATION);
    if (entityBaseLocation != null) {
      loc.append(entityBaseLocation);
    } else {
      var fromIndex = namespaceElements.size() - 1;
      var baseLocation = (String) null;
      for (; fromIndex >= 0; fromIndex--) {
        baseLocation = namespaceElements.get(fromIndex).properties().get(ENTITY_BASE_LOCATION);
        if (baseLocation != null) {
          break;
        }
      }

      loc.append(
          baseLocation != null
              ? baseLocation
              : catalogBaseLocation()
                  .orElseThrow(
                      () ->
                          new IllegalStateException("No base location configured on the catalog")));

      for (fromIndex++; fromIndex < namespaceElements.size(); fromIndex++) {
        ensureTrailingSlash(loc).append(namespaceElements.get(fromIndex).name());
      }

      ensureTrailingSlash(loc).append(objName);
    }

    additionalElement.ifPresent(e -> ensureTrailingSlash(loc).append(e));

    if (addFinalTrailingSlash) {
      ensureTrailingSlash(loc);
    }

    return loc.toString();
  }

  private static StringBuilder ensureTrailingSlash(StringBuilder loc) {
    if (!loc.isEmpty() && loc.charAt(loc.length() - 1) != '/') {
      loc.append('/');
    }
    return loc;
  }

  private <R> ContentObj finishChangeForNewObj(
      CommitterState<CatalogStateObj, R> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      ResolvedPath resolvedPath,
      ContentObj.Builder<?, ?> objBuilder,
      ContentIdentifier ident) {

    if (resolvedPath.namespaceElements().isEmpty()) {
      objBuilder.parentStableId(store().catalog().stableId());
    } else {
      objBuilder.parentStableId(resolvedPath.namespaceElements().getLast().stableId());
    }

    var obj = objBuilder.build();

    updateLocationsIndex(locations, null, obj);

    state.writeOrReplace("entity-" + obj.stableId(), obj);

    var entityIdKey = IndexKey.key(obj.stableId());
    var nameKey = ident.toIndexKey();

    byName.put(nameKey, objRef(obj));
    byId.put(entityIdKey, nameKey);

    checkState(
        changes.put(nameKey, ChangeAdd.builder().build()),
        "Entity '%s' updated more than once",
        nameKey);

    return obj;
  }

  private <R> void finishChangeForUpdatedObj(
      CommitterState<CatalogStateObj, R> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      ContentObj currentObj,
      ContentObj.Builder<?, ?> objBuilder,
      ContentIdentifier ident) {

    var obj = objBuilder.build();

    updateLocationsIndex(locations, currentObj, obj);

    state.writeOrReplace("entity-" + obj.stableId(), obj);

    var nameKey = ident.toIndexKey();

    byName.put(nameKey, objRef(obj));

    checkState(
        changes.put(nameKey, ChangeUpdate.builder().build()),
        "Entity '%s' updated more than once",
        nameKey);
  }

  private <R> void finishChangeForRenameObj(
      CommitterState<CatalogStateObj, R> state,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      ObjBase currentObj,
      ObjBase.Builder<?, ?> objBuilder,
      ContentIdentifier fromIdent,
      ContentIdentifier toIdent) {

    var obj = objBuilder.build();

    updateLocationsIndex(locations, currentObj, obj);

    state.writeOrReplace("entity-" + obj.stableId(), obj);

    var fromNameKey = fromIdent.toIndexKey();
    var toNameKey = toIdent.toIndexKey();
    var entityIdKey = IndexKey.key(obj.stableId());

    byName.remove(fromNameKey);
    byName.put(toNameKey, objRef(obj));
    byId.put(entityIdKey, toNameKey);

    checkState(
        changes.put(toNameKey, ChangeRename.builder().renameFrom(fromNameKey).build()),
        "Entity '%s' updated more than once",
        toNameKey);
  }

  private void finishChangeForDeleteObj(
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<IndexKey> byId,
      UpdatableIndex<Change> changes,
      UpdatableIndex<EntityIdSet> locations,
      ContentObj currentObj,
      ContentIdentifier ident) {

    updateLocationsIndex(locations, currentObj, null);

    var nameKey = ident.toIndexKey();

    byName.remove(nameKey);
    byId.remove(IndexKey.key(currentObj.stableId()));

    changes.put(nameKey, ChangeRemove.builder().build());
  }

  private <B extends ObjBase.Builder<?, ?>> B newObjBuilder(Persistence persistence, B builder) {
    var now = persistence.currentInstant();
    builder
        .createTimestamp(now)
        .updateTimestamp(now)
        .id(persistence.generateId())
        .stableId(persistence.generateId());
    return builder;
  }

  private <B extends ObjBase.Builder<?, ?>> B updateObjBuilder(
      Persistence persistence, ObjBase currentObj, B builder) {
    builder
        .from(currentObj)
        .updateTimestamp(persistence.currentInstant())
        .id(persistence.generateId())
        .entityVersion(currentObj.entityVersion() + 1);
    return builder;
  }

  private ContentChange newContentChange(ContentChange.Target target, UpdateTableRequest request) {
    var updates =
        request.updates().stream()
            .map(
                u -> {
                  if (u instanceof MetadataUpdate.SetLocation setLocation) {
                    return new MetadataUpdate.SetLocation(
                        fixupTableLikeLocation(request.identifier(), setLocation.location()));
                  }
                  return u;
                })
            .toList();

    return ContentChange.fromUpdateTableRequest(
        target, UpdateTableRequest.create(request.identifier(), request.requirements(), updates));
  }

  /** Apply default location and catalog rules to a table-like location */
  private String fixupTableLikeLocation(TableIdentifier tableIdentifier, String location) {
    return transformTableLikeLocation(
        tableIdentifier, location != null ? location : defaultWarehouseLocation(tableIdentifier));
  }

  private <EL> Page<EL> listChildren(
      Namespace namespace,
      PageToken pageToken,
      Class<? extends Obj> expectedType,
      Function<ContentIdentifier, EL> elementMapper) {
    return storeNameIndex()
        .map(
            byName -> {
              var ident = identifier(namespace.levels());
              var offset =
                  pageToken.valueAs(NoSqlPaginationToken.class).map(NoSqlPaginationToken::key);

              var indexIter = byName.iterator(offset.orElse(ident.toIndexKey()), null, false);

              // Check that the given namespace refers to a namespace object, unless the requested
              // namespace is the root namespace (that's the catalog).
              if (offset.isEmpty() && !ident.isEmpty()) {
                if (!indexIter.hasNext()
                    || objTypeMismatch(indexIter.next().getValue(), NamespaceObj.class)) {
                  throw noSuchNamespaceException(namespace);
                }
              }

              var limit = pageToken.pageSize().orElse(Integer.MAX_VALUE);
              var result = new ArrayList<EL>();
              while (indexIter.hasNext()) {
                var elem = indexIter.next();
                var elemKey = elem.getKey();
                var elemIdent = indexKeyToIdentifier(elemKey);
                if (!elemIdent.startsWith(ident)) {
                  // Not a child of the requested namespace, stop.
                  break;
                }
                if (elemIdent.length() != ident.length() + 1) {
                  // Not a direct child of the requested namespace, skip.
                  continue;
                }

                if (objTypeMismatch(elem.getValue(), expectedType)) {
                  // Not a namespace, skip.
                  continue;
                }

                if (result.size() == limit) {
                  return Page.page(
                      pageToken,
                      result,
                      paginationToken(
                          objRef(store().catalogContent().refObj().orElseThrow()), elemKey));
                }

                result.add(elementMapper.apply(elemIdent));
              }
              return Page.page(pageToken, result, null);
            })
        .orElseGet(
            () -> {
              // Catalog is empty.
              // Yield an empty result if the root namespace is requested, otherwise throw.
              if (namespace.isEmpty()) {
                return Page.fromItems(Collections.emptyList());
              }
              throw noSuchNamespaceException(namespace);
            });
  }

  private Optional<Index<ObjRef>> storeNameIndex() {
    return store().catalogContent().nameIndex();
  }

  private static boolean objTypeMismatch(ObjRef objRef, Class<? extends Obj> checkedClass) {
    var objType = objTypeById(objRef.type());
    return !checkedClass.isAssignableFrom(objType.targetClass());
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends TableLikeObj> tableLikeType(
      PolarisEntitySubType polarisEntitySubType) {
    return (Class<? extends TableLikeObj>)
        EntityObjMappings.byEntityType(TABLE_LIKE)
            .objTypeForSubType(polarisEntitySubType)
            .targetClass();
  }

  public static RuntimeException noSuchException(TableIdentifier identifier, ObjType leafObjType) {
    if (leafObjType == IcebergViewObj.TYPE) {
      return new NoSuchViewException("View does not exist: %s", identifier);
    }
    return new NoSuchTableException("%s does not exist: %s", typeAsName(leafObjType), identifier);
  }

  public static AlreadyExistsException alreadyExistsException(
      TableIdentifier identifier, ObjType foundObjType, ObjType expectedObjType) {
    var middle = expectedObjType != foundObjType ? "with same name already" : "already";
    return new AlreadyExistsException(
        "%s %s exists: %s", typeAsName(foundObjType), middle, identifier);
  }

  public static String typeAsName(ContentObj existing) {
    return typeAsName(existing.type());
  }

  public static String typeAsName(ObjType type) {
    if (IcebergTableObj.TYPE == type) {
      return "Table";
    } else if (IcebergViewObj.TYPE == type) {
      return "View";
    } else if (GenericTableObj.TYPE == type) {
      return "Generic table";
    } else if (LocalNamespaceObj.TYPE == type) {
      return "Namespace";
    }
    return "Object";
  }

  public void renameTableLike(
      PolarisEntitySubType subType, TableIdentifier from, TableIdentifier to) {
    store()
        .catalogContentChange(
            (state, ref, byName, byId, changes, locations) -> {
              var fromIdent = identifierFor(from);
              var toIdent = identifierFor(to);

              var fromLeaf =
                  resolvePathWithOptionalLeaf(state.persistence(), byName, fromIdent)
                      .flatMap(p -> p.leafObjAs(tableLikeType(subType)))
                      .orElseThrow(
                          () ->
                              subType == PolarisEntitySubType.ICEBERG_VIEW
                                  ? new NoSuchViewException(
                                      "Cannot rename %s to %s. View does not exist", from, to)
                                  : new NoSuchTableException(
                                      "Cannot rename %s to %s. Table does not exist", from, to));

              var toResolved =
                  resolvePathWithOptionalLeaf(state.persistence(), byName, toIdent)
                      .orElseThrow(
                          () ->
                              new NoSuchNamespaceException(
                                  "Cannot rename %s to %s. Namespace does not exist: %s",
                                  from, to, to.namespace()));
              toResolved
                  .leafObj()
                  .ifPresent(
                      toExistingObj -> {
                        throw new AlreadyExistsException(
                            "Cannot rename %s to %s. %s already exists",
                            from, to, typeAsName(toExistingObj));
                      });

              var intProps = new HashMap<>(fromLeaf.internalProperties());
              if (!to.namespace().isEmpty()) {
                intProps.put(PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(to.namespace()));
              } else {
                intProps.remove(PARENT_NAMESPACE_KEY);
              }
              var renamedObj =
                  updateObjBuilder(
                          state.persistence(),
                          fromLeaf,
                          EntityObjMappings.byEntityType(TABLE_LIKE).newObjBuilder(subType))
                      .name(toIdent.leafName())
                      .parentStableId(
                          (toResolved.namespaceElements().isEmpty()
                                  ? store().catalog()
                                  : toResolved.namespaceElements().getLast())
                              .stableId())
                      .internalProperties(intProps);

              finishChangeForRenameObj(
                  state,
                  byName,
                  byId,
                  changes,
                  locations,
                  fromLeaf,
                  renamedObj,
                  fromIdent,
                  toIdent);

              return new ChangeResult.CommitChange<>(true);
            },
            Boolean.class);
  }

  @SuppressWarnings("UnusedReturnValue") // TODO ?
  public boolean dropTableLike(
      PolarisEntitySubType subType, TableIdentifier identifier, boolean purge) {

    // Check that purge is enabled if it is set:
    if (purge) {
      boolean dropWithPurgeEnabled =
          realmConfig().getConfig(DROP_WITH_PURGE_ENABLED, store().catalog().properties());
      if (!dropWithPurgeEnabled) {
        throw new ForbiddenException(
            "Unable to purge entity: %s. To enable this feature, set the Polaris configuration %s "
                + "or the catalog configuration %s",
            identifier.name(),
            DROP_WITH_PURGE_ENABLED.key(),
            DROP_WITH_PURGE_ENABLED.catalogConfig());
      }

      // TODO re-add actual purging for tables (when the content change succeeded)
      // TODO re-add actual purging for views (when the content change succeeded)
    }

    var ident = identifierFor(identifier);
    var objType = EntityObjMappings.byEntityType(TABLE_LIKE).objTypeForSubType(subType);
    var expectedType = objType.targetClass();

    return store()
        .catalogContentChange(
            (state, ref, byName, byId, changes, locations) -> {
              var obj =
                  resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                      .flatMap(ResolvedPath::leafObj)
                      .orElse(null);

              if (expectedType.isInstance(obj)) {
                finishChangeForDeleteObj(byName, byId, changes, locations, obj, ident);

                return new ChangeResult.CommitChange<>(true);
              } else {
                throw noSuchException(identifier, objType);
              }
            },
            Boolean.class);
  }

  private <R> R loadMetadata(
      TableIdentifier identifier,
      ObjType expectedType,
      Optional<ResolvedPath> resolvedPathOptional,
      Function<InputFile, R> metadataLoader,
      PolarisEventType beforeEventType,
      PolarisEventType afterEventType) {
    var latestLocationOptional =
        resolvedPathOptional
            .flatMap(p -> p.leafObjAs(TableLikeObj.class))
            .flatMap(TableLikeObj::metadataLocation);

    emitEvent(identifier, beforeEventType);

    var latestLocation =
        latestLocationOptional.orElseThrow(() -> noSuchException(identifier, expectedType));
    var io = requireNonNull(catalogFileIO());
    var metaInputFile = io.newInputFile(latestLocation);
    R metadata = metadataLoader.apply(metaInputFile);

    emitEvent(identifier, afterEventType);

    return metadata;
  }

  private void emitEvent(TableIdentifier identifier, PolarisEventType eventType) {
    polarisEventListener()
        .onEvent(
            new PolarisEvent(
                eventType,
                eventMetadataFactory().create(),
                new EventAttributeMap()
                    .put(EventAttributes.CATALOG_NAME, catalogName())
                    .put(EventAttributes.TABLE_IDENTIFIER, identifier)));
  }

  private Optional<ResolvedPath> resolveEntity(TableIdentifier identifier, ObjType expectedType) {
    return resolveEntity(identifier).filter(p -> p.leafObjIs(expectedType));
  }

  private Optional<ResolvedPath> resolveEntity(TableIdentifier identifier) {
    return storeNameIndex()
        .flatMap(
            byName ->
                resolvePathWithOptionalLeaf(
                    store().persistence(), byName, identifierFor(identifier)));
  }

  // TODO move up
  public String defaultNamespaceLocation(Namespace namespace) {
    var defaultBaseLocation = defaultBaseLocation();
    if (namespace.isEmpty()) {
      return defaultBaseLocation;
    } else {
      return SLASH.join(defaultBaseLocation, SLASH.join(namespace.levels()));
    }
  }

  // TODO move up
  public PageToken pageToken(String pageToken, Integer pageSize) {
    return PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
  }

  // TODO move up
  public boolean shouldDecodeToken() {
    return getCatalogConfig(LIST_PAGINATION_ENABLED);
  }

  // TODO move up
  public <T> T getCatalogConfig(PolarisConfiguration<T> config) {
    return realmConfig().getConfig(config, store().catalog().properties());
  }

  // TODO move up
  public Optional<String> catalogBaseLocation() {
    return store().catalog().defaultBaseLocation();
  }

  // TODO move up
  public String buildPrefixedLocation(TableIdentifier tableIdentifier) {
    var locationBuilder = new StringBuilder();
    var defaultBaseLocation = defaultBaseLocation();
    locationBuilder.append(defaultBaseLocation);
    if (!defaultBaseLocation.endsWith("/")) {
      locationBuilder.append("/");
    }

    locationBuilder.append(LocationUtils.computeHash(tableIdentifier.toString()));

    for (var ns : tableIdentifier.namespace().levels()) {
      locationBuilder.append("/").append(URLEncoder.encode(ns, Charset.defaultCharset()));
    }
    locationBuilder
        .append("/")
        .append(URLEncoder.encode(tableIdentifier.name(), Charset.defaultCharset()))
        .append("/");
    return locationBuilder.toString();
  }

  /**
   * Based on configuration settings, for callsites that need to handle potentially setting a new
   * base location for a TableLike entity, produces the transformed location if applicable, or else
   * the unaltered specified location.
   *
   * <p>Applies the rules controlled by {@link
   * FeatureConfiguration#DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED} and {@link
   * CatalogEntity#REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY} to a tablelike location
   */
  // TODO move up
  public String transformTableLikeLocation(TableIdentifier tableIdentifier, String location) {
    location = applyReplaceNewLocationWithCatalogDefault(location);

    var prefixEnabled = getCatalogConfig(DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED);
    var allowUnstructuredTableLocation = getCatalogConfig(ALLOW_UNSTRUCTURED_TABLE_LOCATION);
    var allowTableLocationOverlap = getCatalogConfig(ALLOW_TABLE_LOCATION_OVERLAP);
    var optimizedSiblingCheck = getCatalogConfig(OPTIMIZED_SIBLING_CHECK);
    if (location != null) {
      return location;
    } else if (!prefixEnabled) {
      return null;
    } else if (!allowUnstructuredTableLocation) {
      throw new IllegalStateException(
          format(
              "The configuration %s is enabled, but %s is not enabled",
              DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.key(),
              ALLOW_UNSTRUCTURED_TABLE_LOCATION.key()));
    } else if (!allowTableLocationOverlap) {
      // TODO consider doing this check any time ALLOW_EXTERNAL_TABLE_LOCATION is enabled, not just
      // here
      if (!optimizedSiblingCheck) {
        throw new IllegalStateException(
            format(
                "%s and %s are both disabled, which means that table location overlap checks are being"
                    + " performed, but only within each namespace. However, %s is enabled, which indicates"
                    + " that tables may be created outside of their parent namespace. This is not a safe"
                    + " combination of configurations.",
                ALLOW_TABLE_LOCATION_OVERLAP.key(),
                OPTIMIZED_SIBLING_CHECK.key(),
                ALLOW_UNSTRUCTURED_TABLE_LOCATION.key()));
      } else if (!logPrefixOverlapWarning()) {
        LOGGER.warn(
            "A table is being created with {} and {} enabled, but with {} disabled. "
                + "This is a safe combination of configurations which may prevent table overlap, but only if the "
                + "underlying persistence actually implements %s. Exercise caution.",
            DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.key(),
            OPTIMIZED_SIBLING_CHECK.key(),
            ALLOW_TABLE_LOCATION_OVERLAP.key());
      }
      return buildPrefixedLocation(tableIdentifier);
    } else {
      return buildPrefixedLocation(tableIdentifier);
    }
  }

  // TODO move up
  public TableMetadata registerTableLoadMetadata(
      TableIdentifier identifier, String metadataFileLocation) {
    checkArgument(identifier != null, "Invalid identifier: %s", identifier);
    checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    var lastSlashIndex = metadataFileLocation.lastIndexOf("/");
    checkArgument(
        lastSlashIndex != -1,
        "Invalid metadata file location; metadata file location must be absolute and contain a '/': %s",
        metadataFileLocation);

    // Throw an exception if this table already exists in the catalog.
    if (resolveEntity(identifier, IcebergTableObj.TYPE).isPresent()) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    var locationDir = metadataFileLocation.substring(0, lastSlashIndex);

    // TODO  the fileIO isn't added to the closeableGroup and not handled in close() ??
    @SuppressWarnings("resource")
    var fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            store().catalog().properties(),
            Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

    var metadataFile = fileIO.newInputFile(metadataFileLocation);
    return TableMetadataParser.read(metadataFile);
  }

  // TODO move up (interface only)
  public String applyReplaceNewLocationWithCatalogDefault(String specifiedTableLikeLocation) {
    var replaceNewLocationPrefix =
        store().catalog().properties().get(REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY);
    if (specifiedTableLikeLocation != null
        && replaceNewLocationPrefix != null
        && specifiedTableLikeLocation.startsWith(replaceNewLocationPrefix)) {
      return defaultBaseLocation()
          + specifiedTableLikeLocation.substring(replaceNewLocationPrefix.length());
    }
    return specifiedTableLikeLocation;
  }

  // TODO move up (interface only)
  public boolean logPrefixOverlapWarning() {
    var c = loggedPrefixOverlapWarning;
    if (c) {
      return false;
    }
    loggedPrefixOverlapWarning = true;
    return true;
  }

  // TODO move up (interface only)
  public FileIO catalogFileIO() {
    var fileIO = this.catalogFileIO;
    if (fileIO == null) {
      var config = store().catalog().storageConfigurationInfo().orElseThrow();

      var storageAccessConfig =
          storageAccessConfigProvider()
              .getStorageAccessConfig(
                  // The table-identifier is only used for logging purposes.
                  TableIdentifier.of(Namespace.of("foo"), "bar"),
                  Set.copyOf(config.getAllowedLocations()),
                  Set.of(PolarisStorageActions.ALL),
                  Optional.empty(),
                  List.of(store().catalogEntity()));

      fileIO =
          fileIOFactory()
              .loadFileIO(storageAccessConfig, ioImplClassName(), store().catalog().properties());
      addCloseable(fileIO);
      this.catalogFileIO = fileIO;
    }
    return fileIO;
  }

  // TODO move up (interface only)
  // TODO this or catalogFileIO ??
  public FileIO loadFileIOForTableLike(
      TableIdentifier identifier,
      Set<String> locationDirs,
      Map<String, String> tableDefaultProperties,
      Set<PolarisStorageActions> storageActions) {
    var ident = identifierFor(identifier);
    var resolvedPath =
        storeNameIndex()
            .flatMap(byName -> resolvePathWithOptionalLeaf(store().persistence(), byName, ident))
            .orElseThrow(() -> noSuchNamespaceException(identifier));

    var rawFullPath = store().resolvedPathToPolarisEntities(resolvedPath, true);

    var storageAccessConfig =
        storageAccessConfigProvider()
            .getStorageAccessConfig(
                identifier, locationDirs, storageActions, Optional.empty(), rawFullPath);

    // defensive clone
    var tableProperties = new HashMap<>(tableDefaultProperties);

    // Reload fileIO based on table-specific context
    var fileIO =
        fileIOFactory().loadFileIO(storageAccessConfig, ioImplClassName(), tableProperties);
    // ensure the new fileIO is closed when the catalog is closed
    addCloseable(fileIO);
    return fileIO;
  }

  @SuppressWarnings("resource")
  private void addCloseable(FileIO fileIO) {
    closeableGroup().addCloseable(fileIO);
  }

  // TODO move up
  @Value.Lazy
  protected String ioImplClassName() {
    var storageConfigurationInfo = store().catalog().storageConfigurationInfo().orElse(null);
    var properties = store().catalog().properties();
    var ioImplClassName =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfig(), properties, storageConfigurationInfo);

    if (ioImplClassName == null) {
      LOGGER.warn(
          "Cannot resolve property '{}' for null storageConfiguration.",
          CatalogProperties.FILE_IO_IMPL);
    }
    return ioImplClassName;
  }

  // TODO move up
  @Value.Lazy
  protected String defaultBaseLocation() {
    var properties = store().catalog().properties();
    var baseLocation =
        catalogBaseLocation()
            .orElse(
                properties.getOrDefault(
                    DEFAULT_BASE_LOCATION_KEY, properties.getOrDefault(WAREHOUSE_LOCATION, "")));
    return baseLocation.replaceAll("/*$", "");
  }

  // TODO move up?
  public UpdateTableRequest updateTableRequestForNewView(
      Namespace namespace, CreateViewRequest req) {

    var ident = TableIdentifier.of(namespace, req.name());

    var viewMeta =
        ViewMetadata.builder()
            .setLocation(req.location() != null ? req.location() : defaultWarehouseLocation(ident));
    if (req.viewVersion() != null) {
      viewMeta.setCurrentVersion(req.viewVersion(), req.schema());
    } else if (req.schema() != null) {
      viewMeta.addSchema(req.schema());
    }
    if (req.properties() != null) {
      viewMeta.setProperties(req.properties());
    }
    var updates = viewMeta.build().changes();
    var requirements = List.<UpdateRequirement>of(new UpdateRequirement.AssertTableDoesNotExist());
    return UpdateTableRequest.create(ident, requirements, updates);
  }
}

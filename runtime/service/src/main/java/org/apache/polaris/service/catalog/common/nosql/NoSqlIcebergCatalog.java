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

package org.apache.polaris.service.catalog.common.nosql;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.config.BehaviorChangeConfiguration.ALLOW_NAMESPACE_CUSTOM_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.DROP_WITH_PURGE_ENABLED;
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
import static org.apache.polaris.service.catalog.common.CatalogUtils.noSuchNamespaceException;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.ids.api.MonotonicClock;
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
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogContent;
import org.apache.polaris.persistence.nosql.metastore.NoSqlPaginationToken;
import org.apache.polaris.persistence.nosql.metastore.ResolvedPath;
import org.apache.polaris.persistence.nosql.metastore.committers.ChangeResult;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.iceberg.PolarisCatalogHelper;
import org.apache.polaris.service.catalog.iceberg.RESTCompatibleCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NoSqlIcebergCatalog implements RESTCompatibleCatalog {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoSqlIcebergCatalog.class);

  private final RealmConfig realmConfig;
  private final NoSqlCatalogContent store;
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final FileIOFactory fileIOFactory;
  private final MonotonicClock monotonicClock;
  private final PolarisCatalogHelper helper = new PolarisCatalogHelper();
  private final EventAttributeMap eventAttributeMap;

  NoSqlIcebergCatalog(
      RealmConfig realmConfig,
      NoSqlCatalogContent store,
      StorageAccessConfigProvider storageAccessConfigProvider,
      FileIOFactory fileIOFactory,
      MonotonicClock monotonicClock,
      EventAttributeMap eventAttributeMap) {
    this.realmConfig = realmConfig;
    this.store = store;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.fileIOFactory = fileIOFactory;
    this.monotonicClock = monotonicClock;
    this.eventAttributeMap = eventAttributeMap;
  }

  @Override
  public RealmConfig realmConfig() {
    return realmConfig;
  }

  @Nullable
  @Override
  public FileIO catalogFileIO() {
    var fileIO = helper.catalogFileIO();
    if (fileIO == null) {
      var config = store.catalog().storageConfigurationInfo().orElseThrow();

      var storageAccessConfig =
          storageAccessConfigProvider.getStorageAccessConfig(
              // The table-identifier is only used for logging purposes.
              TableIdentifier.of(Namespace.of("foo"), "bar"),
              Set.copyOf(config.getAllowedLocations()),
              Set.of(PolarisStorageActions.ALL),
              Optional.empty(),
              List.of(store.catalogEntity()));

      fileIO =
          fileIOFactory.loadFileIO(
              storageAccessConfig, ioImplClassName(), store.catalog().properties());
      helper.addCloseable(fileIO);
      helper.setCatalogFileIO(fileIO);
    }
    return fileIO;
  }

  @Override
  public void setCatalogFileIO(FileIO fileIO) {
    helper.setCatalogFileIO(fileIO);
  }

  @Override
  public String name() {
    return store.catalog().name();
  }

  @Nonnull
  @Override
  public InitializedConfig initializedConfig() {
    return helper.initializedConfig();
  }

  @Override
  public Optional<String> catalogBaseLocation() {
    return store.catalog().defaultBaseLocation();
  }

  @Override
  public Optional<PolarisStorageConfigurationInfo> catalogStorageConfigurationInfo() {
    return store.catalog().storageConfigurationInfo();
  }

  @Override
  public <T> T getCatalogConfig(PolarisConfiguration<T> config) {
    return realmConfig().getConfig(config, store.catalog().properties());
  }

  @Override
  public void close() throws IOException {
    helper.closeCloseables();
  }

  @Override
  public boolean logPrefixOverlapWarning() {
    return helper.logPrefixOverlapWarning();
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    helper.setInitializedConfig(initializeIcebergCatalog(name, properties));

    // TODO potentially add MetricsReporter (CommitReport), need to generate the metrics
  }

  @Override
  public NamespaceResult createNamespaceWithResult(
      Namespace namespace, Map<String, String> requestedMetadata) {
    var ident = identifier(namespace.levels());

    return store.catalogContentChange(
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

          var properties = new HashMap<>(requestedMetadata);

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
              new CommitChangesHandler.ValidationContext(state.persistence(), byName, locations));

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

          return new ChangeResult.CommitChange<>(new NamespaceResult(obj.stableId(), properties));
        },
        NamespaceResult.class);
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    var ident = identifier(namespace.levels());

    return store.catalogContentChange(
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
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    updateNamespaceProperties(
        namespace, UpdateNamespacePropertiesRequest.builder().updateAll(properties).build());

    // There's no 'false' case in Iceberg!
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    updateNamespaceProperties(
        namespace, UpdateNamespacePropertiesRequest.builder().removeAll(properties).build());

    // There's no 'false' case in Iceberg!
    return true;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    var ident = identifier(namespace.levels());
    if (ident.isEmpty()) {
      throw new IllegalArgumentException("Cannot drop root namespace");
    }

    return store.catalogContentChange(
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
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return storeNameIndex()
        .map(index -> index.get(identifier(namespace.levels()).toIndexKey()))
        .flatMap(
            objRef -> Optional.ofNullable(store.persistence().fetch(objRef, NamespaceObj.class)))
        .map(NamespaceObj::properties)
        .orElseThrow(() -> noSuchNamespaceException(namespace));
  }

  @Override
  public Page<Namespace> listNamespaces(Namespace namespace, PageToken pageToken)
      throws NoSuchNamespaceException {
    return listChildren(
        namespace,
        pageToken,
        NamespaceObj.class,
        elemIdent -> Namespace.of(elemIdent.elements().toArray(new String[0])));
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return resolveEntity(identifier, IcebergTableObj.TYPE).isPresent();
  }

  @Override
  public boolean viewExists(TableIdentifier identifier) {
    return resolveEntity(identifier, IcebergViewObj.TYPE).isPresent();
  }

  @Override
  public TableMetadata loadTableMetadata(TableIdentifier identifier) {
    return loadMetadata(identifier, IcebergTableObj.TYPE, TableMetadataParser::read);
  }

  @Override
  public ViewMetadata loadViewMetadata(TableIdentifier identifier) {
    return loadMetadata(identifier, IcebergViewObj.TYPE, ViewMetadataParser::read);
  }

  private <R> R loadMetadata(
      TableIdentifier identifier, ObjType expectedType, Function<InputFile, R> metadataLoader) {
    var resolvedPathOptional = resolveEntity(identifier, expectedType);

    var latestLocationOptional =
        resolvedPathOptional
            .flatMap(p -> p.leafObjAs(TableLikeObj.class))
            .flatMap(TableLikeObj::metadataLocation);

    var latestLocation =
        latestLocationOptional.orElseThrow(() -> noSuchException(identifier, expectedType));
    var io = requireNonNull(catalogFileIO());
    var metaInputFile = io.newInputFile(latestLocation);
    return metadataLoader.apply(metaInputFile);
  }

  private Optional<ResolvedPath> resolveEntity(TableIdentifier identifier, ObjType expectedType) {
    return storeNameIndex()
        .flatMap(
            byName ->
                resolvePathWithOptionalLeaf(store.persistence(), byName, identifierFor(identifier)))
        .filter(p -> p.leafObjIs(expectedType));
  }

  @Override
  public Page<TableIdentifier> listTableLike(
      PolarisEntitySubType polarisEntitySubType, Namespace namespace, PageToken pageToken) {
    return listChildren(
        namespace,
        pageToken,
        tableLikeType(polarisEntitySubType),
        elemIdent -> TableIdentifier.of(namespace, elemIdent.leafName()));
  }

  @Override
  public FileIO loadFileIOForTableLike(
      TableIdentifier identifier,
      Set<String> locationDirs,
      Map<String, String> tableDefaultProperties,
      Set<PolarisStorageActions> storageActions) {
    var ident = identifierFor(identifier);
    var resolvedPath =
        storeNameIndex()
            .flatMap(byName -> resolvePathWithOptionalLeaf(store.persistence(), byName, ident))
            .orElseThrow(() -> noSuchNamespaceException(identifier));

    var rawFullPath = store.resolvedPathToPolarisEntities(resolvedPath, true);

    var storageAccessConfig =
        storageAccessConfigProvider.getStorageAccessConfig(
            identifier, locationDirs, storageActions, Optional.empty(), rawFullPath);

    // defensive clone
    var tableProperties = new HashMap<>(tableDefaultProperties);

    // Reload fileIO based on table-specific context
    var fileIO = fileIOFactory.loadFileIO(storageAccessConfig, ioImplClassName(), tableProperties);
    // ensure the new fileIO is closed when the catalog is closed
    helper.addCloseable(fileIO);
    return fileIO;
  }

  @Override
  public String applyReplaceNewLocationWithCatalogDefault(String specifiedTableLikeLocation) {
    var replaceNewLocationPrefix =
        store.catalog().properties().get(REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY);
    if (specifiedTableLikeLocation != null
        && replaceNewLocationPrefix != null
        && specifiedTableLikeLocation.startsWith(replaceNewLocationPrefix)) {
      return defaultBaseLocation()
          + specifiedTableLikeLocation.substring(replaceNewLocationPrefix.length());
    }
    return specifiedTableLikeLocation;
  }

  @Override
  public void renameTableLike(
      PolarisEntitySubType subType, TableIdentifier from, TableIdentifier to) {
    store.catalogContentChange(
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
                              ? store.catalog()
                              : toResolved.namespaceElements().getLast())
                          .stableId())
                  .internalProperties(intProps);

          finishChangeForRenameObj(
              state, byName, byId, changes, locations, fromLeaf, renamedObj, fromIdent, toIdent);

          return new ChangeResult.CommitChange<>(true);
        },
        Boolean.class);
  }

  @Override
  public TableMetadata createTable(TableIdentifier tableIdentifier, CreateTableRequest request) {

    var location = request.location();
    if (request.stageCreate()) {
      // location is verified/fixed later for the non-stage variant anyway, no need to do it here
      // as well.
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
      return metaTemp;
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
        UpdateTableRequest.create(
            tableIdentifier, UpdateRequirements.forCreateTable(updates), updates),
        true);
  }

  @Override
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

  @Override
  public boolean dropTableLike(
      PolarisEntitySubType subType, TableIdentifier identifier, boolean purge) {

    // Check that purge is enabled if it is set:
    if (purge) {
      boolean dropWithPurgeEnabled =
          realmConfig().getConfig(DROP_WITH_PURGE_ENABLED, store.catalog().properties());
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

    return store.catalogContentChange(
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

  @Override
  public ViewMetadata updateView(UpdateTableRequest request, boolean directUpdateOrCreate) {
    return (ViewMetadata)
        commitChange(newContentChange(ContentChange.Target.VIEW, request), directUpdateOrCreate)
            .metadata();
  }

  @Override
  public TableMetadata updateTable(UpdateTableRequest request, boolean directUpdateOrCreate) {
    return (TableMetadata)
        commitChange(newContentChange(ContentChange.Target.TABLE, request), directUpdateOrCreate)
            .metadata();
  }

  @Override
  public TableMetadata registerTable(Namespace namespace, String name, String metadataLocation) {
    var identifier = TableIdentifier.of(namespace, name);
    var metadata = registerTableLoadMetadata(identifier, metadataLocation);
    var ident = identifierFor(identifier);
    store.catalogContentChange(
        (state, ref, byName, byId, changes, locations) -> {
          var resolvedPath =
              resolvePathWithOptionalLeaf(state.persistence(), byName, ident)
                  .orElseThrow(() -> noSuchNamespaceException(namespace));
          resolvedPath
              .leafObj()
              .ifPresent(
                  l -> {
                    throw alreadyExistsException(identifier, l.type(), IcebergTableObj.TYPE);
                  });

          var objBuilder =
              newObjBuilder(state.persistence(), IcebergTableObj.builder())
                  .name(ident.leafName())
                  .putProperty(ENTITY_BASE_LOCATION, metadata.location())
                  .internalProperties(CatalogUtils.buildTableMetadataPropertiesMap(metadata))
                  .metadataLocation(metadataLocation);

          finishChangeForNewObj(
              state, byName, byId, changes, locations, resolvedPath, objBuilder, ident);

          return new ChangeResult.CommitChange<>(true);
        },
        Boolean.class);
    return metadata;
  }

  @Override
  public void commitTransaction(CommitTransactionRequest commitTransactionRequest) {
    var commitedChanges =
        commitChanges(
            commitTransactionRequest.tableChanges().stream()
                .map(c -> newContentChange(ContentChange.Target.TABLE, c))
                .toList(),
            false);
    eventAttributeMap.put(
        EventAttributes.TABLE_METADATAS,
        commitedChanges.stream().map(c -> (TableMetadata) c.metadata()).toList());
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
    if (!realmConfig().getConfig(ALLOW_NAMESPACE_CUSTOM_LOCATION, store.catalog().properties())) {
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
      var catalogObj = store.catalog();
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
      store
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
    store
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
    var deadline = monotonicClock.currentInstant().plus(commitTimeout);

    try (var commitChangesHandler =
        new CommitChangesHandler(catalogFileIO(), store, directUpdateOrCreate, contentChanges)) {

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
            store.catalogContentChange(
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

  @Override
  public String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      var l =
          SLASH.join(defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
      return realmConfig.getConfig(ADD_TRAILING_SLASH_TO_LOCATION)
          ? StorageLocation.ensureTrailingSlash(l)
          : l;
    } else {
      var resolvedPathOptional =
          storeNameIndex()
              .flatMap(
                  byName ->
                      resolvePathWithOptionalLeaf(
                          store.persistence(), byName, identifierFor(tableIdentifier)))
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
      objBuilder.parentStableId(store.catalog().stableId());
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
                          objRef(store.catalogContent().refObj().orElseThrow()), elemKey));
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
    return store.catalogContent().nameIndex();
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
}

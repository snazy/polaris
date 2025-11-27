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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.config.BehaviorChangeConfiguration.VALIDATE_VIEW_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.entity.NamespaceEntity.PARENT_NAMESPACE_KEY;
import static org.apache.polaris.core.entity.PolarisEntityConstants.ENTITY_BASE_LOCATION;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY;
import static org.apache.polaris.core.storage.StorageUtil.getLocationsUsedByTable;
import static org.apache.polaris.persistence.nosql.metastore.ResolvedPath.resolvePathWithOptionalLeaf;
import static org.apache.polaris.service.catalog.common.CatalogUtils.buildTableMetadataPropertiesMap;
import static org.apache.polaris.service.catalog.common.CatalogUtils.newTableMetadataFilePath;
import static org.apache.polaris.service.catalog.common.CatalogUtils.newViewMetadataFilePath;
import static org.apache.polaris.service.catalog.common.CatalogUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.CatalogUtils.parseVersionFromMetadataLocation;
import static org.apache.polaris.service.catalog.common.CatalogUtils.validateLocationsForTableLike;
import static org.apache.polaris.service.catalog.common.CatalogUtils.validateMetadataFileInTableDir;
import static org.apache.polaris.service.catalog.common.nosql.NoSqlIcebergCatalog.alreadyExistsException;
import static org.apache.polaris.service.catalog.common.nosql.NoSqlIcebergCatalog.noSuchException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirement.AssertTableDoesNotExist;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.Committer;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.UpdatableIndex;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.EntityIdSet;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogContent;
import org.apache.polaris.persistence.nosql.metastore.ResolvedPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Committing changes to Iceberg tables and views involves reads and writes to object stores, which
 * are supposed to be long-running operations, especially when many tables/views are involved.
 *
 * <p>{@link Committer Persistence committers} are supposed to complete quickly to account
 * concurrently modified reference-pointers, and given that commits against the same reference are
 * locally coordinated as an optimization.
 *
 * <p>This class provides the primitives to perform the individual steps for a metadata related
 * content update operation, which is implemented by {@link NoSqlIcebergCatalog}.
 *
 * <p>A full metadata involving commit retry loop consists of an outer loop for object-storage
 * related parts and an inner loop.
 *
 * <ol>
 *   <li>Outer loop (until a commit succeeded or failed):
 *       <ol>
 *         <li>Check outer-loop retry timeout, abort if exceeded
 *         <li>{@linkplain #setupMetadata() Load missing table-metadata}, memoize additional
 *             commit/version state if necessary.
 *         <li>Check requirements and fail early if requirements aren't met'
 *         <li>Perform table-metadata updates
 *         <li>{@linkplain #storeUpdatedMetadata() Store table-metadata to object stores}
 *         <li>Inner commit-retry-loop:
 *             <ol>
 *               <li>{@linkplain #verifyState(Persistence, UpdatableIndex) Check if all tables are
 *                   still at the same state}. If not, don't commit and yield that status to the
 *                   outer loop
 *               <li>{@linkplain #applyToCommit(RealmConfig, ValidationContext, LocationCheck,
 *                   FinishChange) Apply table-changes}
 *             </ol>
 *         <li>Return the {@linkplain #committed() updated state information}, if successfully
 *             committed
 *       </ol>
 *   <li>Post-commit actions:
 *       <ol>
 *         <li>{@linkplain #close() Delete stale data from object-stores}
 *       </ol>
 */
final class CommitChangesHandler implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitChangesHandler.class);

  private final Map<ContentIdentifier, ChangeElement<?, ?>> fileChanges = new HashMap<>();
  private final FileIO fileIO;
  private final NoSqlCatalogContent store;
  private final boolean directUpdateOrCreate;
  private boolean committed;

  /**
   * Construct a new "content with metadata batch committer".
   *
   * @param fileIO the file IO instance to use
   * @param store the catalog content instance
   * @param directUpdateOrCreate a flag to produce the right exception messages depending on the
   *     call site, this is practically only relevant for tests
   * @param contentChanges the content changes to commit atomically to the catalog
   */
  CommitChangesHandler(
      FileIO fileIO,
      NoSqlCatalogContent store,
      boolean directUpdateOrCreate,
      List<ContentChange> contentChanges) {
    this.fileIO = fileIO;
    this.store = store;
    this.directUpdateOrCreate = directUpdateOrCreate;
    checkArgument(!contentChanges.isEmpty());
    for (var contentChange : contentChanges) {
      var changeElement =
          switch (contentChange.target()) {
            case TABLE -> new TableChangeElement(contentChange);
            case VIEW -> new ViewChangeElement(contentChange);
          };
      if (fileChanges.put(contentChange.identifier(), changeElement) != null) {
        throw new CommitFailedException(
            "Duplicate content changes against the same table %s within a single atomic change are not allowed",
            contentChange.identifier());
      }
    }
  }

  /** Load missing table-metadata, memoize additional commit/version state if necessary. */
  void setupMetadata() {
    fileChanges.values().forEach(ChangeElement::setupMetadata);
  }

  /** Store the updated metadata files. */
  void storeUpdatedMetadata() {
    fileChanges.values().forEach(ChangeElement::saveMetadata);
  }

  /**
   * Called from within a commit-retry loop to verify that the expected state of the entities
   * matches the current state of the entities.
   */
  boolean verifyState(Persistence persistence, UpdatableIndex<ObjRef> byName) {
    return fileChanges.values().stream().allMatch(c -> c.verifyState(persistence, byName));
  }

  /**
   * Called from with a commit-retry loop, after {@link #verifyState(Persistence, UpdatableIndex)},
   * to apply the changes to the catalog.
   */
  void applyToCommit(
      RealmConfig realmConfig,
      ValidationContext validationContext,
      LocationCheck locationCheck,
      FinishChange finishChange) {
    fileChanges
        .values()
        .forEach(
            el -> el.applyToCommit(realmConfig, validationContext, locationCheck, finishChange));
  }

  /**
   * If the changes have been committed, this function yields the effectively used base metadata and
   * updated metadata objects for each content change.
   */
  List<ContentChange.CommittedContentChange> committed() {
    committed = true;

    return fileChanges.values().stream()
        .map(
            c ->
                (ContentChange.CommittedContentChange)
                    ContentChange.CommittedContentChange.builder()
                        .change(c.contentChange)
                        .base(Optional.ofNullable(c.baseMetadata()))
                        .metadata(c.updatedMetadata())
                        .build())
        .toList();
  }

  final class TableChangeElement extends ChangeElement<TableMetadata, IcebergTableObj> {
    TableMetadata baseMetadata;
    TableMetadata updatedMetadata;

    TableChangeElement(ContentChange contentChange) {
      super(IcebergTableObj.TYPE, contentChange);
    }

    @Override
    TableMetadata baseMetadata() {
      return baseMetadata;
    }

    @Override
    TableMetadata updatedMetadata() {
      return updatedMetadata;
    }

    @Override
    String baseMetadataLocation() {
      return baseMetadata != null ? baseMetadata.metadataFileLocation() : null;
    }

    @Override
    void loadBaseMetadata() {
      baseMetadata = TableMetadataParser.read(metadataInputFile());
    }

    @Override
    void checkRequirements() {
      contentChange.requirements().forEach(r -> r.validate(baseMetadata));
    }

    @Override
    void setCreateMetadata() {
      var formatVersion =
          contentChange.updates().stream()
              .filter(MetadataUpdate.UpgradeFormatVersion.class::isInstance)
              .map(MetadataUpdate.UpgradeFormatVersion.class::cast)
              .mapToInt(MetadataUpdate.UpgradeFormatVersion::formatVersion)
              .findFirst()
              .orElse(2);
      var tableMetaBuilder = TableMetadata.buildFromEmpty(formatVersion);
      contentChange.updates().forEach(u -> u.applyTo(tableMetaBuilder));
      updatedMetadata = tableMetaBuilder.build();
    }

    @Override
    void setUpdatedMetadata() {
      var tableMetaBuilder = TableMetadata.buildFrom(baseMetadata);
      contentChange.updates().forEach(u -> u.applyTo(tableMetaBuilder));
      updatedMetadata = tableMetaBuilder.build();
    }

    @Override
    String newMetadataFilePath(int version) {
      return newTableMetadataFilePath(updatedMetadata, version);
    }

    @Override
    void writeMetadata(OutputFile outputFile) {
      TableMetadataParser.overwrite(updatedMetadata, outputFile);
      updatedMetadata =
          TableMetadata.buildFrom(updatedMetadata)
              .withMetadataLocation(outputFile.location())
              .discardChanges()
              .build();
    }

    @Override
    void verifyLocation(
        RealmConfig realmConfig,
        ValidationContext validationContext,
        LocationCheck locationCheck,
        Map<String, String> properties,
        TableIdentifier tableIdentifier,
        ArrayList<PolarisEntity> resolvedFullPath) {

      if (baseMetadata == null
          || !updatedMetadata.location().equals(baseMetadata.location())
          || !Objects.equals(
              baseMetadata.properties().get(USER_SPECIFIED_WRITE_DATA_LOCATION_KEY),
              updatedMetadata.properties().get(USER_SPECIFIED_WRITE_DATA_LOCATION_KEY))) {
        // If location is changing then we must validate that the requested location is valid
        // for the storage configuration inherited under this entity's path.
        var dataLocations =
            getLocationsUsedByTable(updatedMetadata.location(), updatedMetadata.properties());
        validateLocationsForTableLike(
            realmConfig, tableIdentifier, dataLocations, resolvedFullPath);
        // also validate that the table location doesn't overlap an existing table
        dataLocations.forEach(
            location ->
                validateNoLocationOverlap(
                    realmConfig,
                    validationContext,
                    locationCheck,
                    location,
                    tableIdentifier,
                    resolvedFullPath));
        // and that the metadata file points to a location within the table's directory
        // structure
        if (updatedMetadata.metadataFileLocation() != null) {
          validateMetadataFileInTableDir(realmConfig, tableIdentifier, updatedMetadata);
        }
      }
    }

    @Override
    TableLikeObj.Builder<?, ?> newObjBuilder() {
      return IcebergTableObj.builder();
    }

    @Override
    Map<String, String> finalizeProperties(Map<String, String> props) {
      props.put(ENTITY_BASE_LOCATION, updatedMetadata.location());
      return props;
    }

    @Override
    Map<String, String> finalizeInternalProperties(Map<String, String> internalProps) {
      internalProps.putAll(buildTableMetadataPropertiesMap(updatedMetadata));
      return internalProps;
    }
  }

  final class ViewChangeElement extends ChangeElement<ViewMetadata, IcebergViewObj> {
    ViewMetadata baseMetadata;
    ViewMetadata updatedMetadata;

    ViewChangeElement(ContentChange contentChange) {
      super(IcebergViewObj.TYPE, contentChange);
    }

    @Override
    ViewMetadata baseMetadata() {
      return baseMetadata;
    }

    @Override
    ViewMetadata updatedMetadata() {
      return updatedMetadata;
    }

    @Override
    String baseMetadataLocation() {
      return baseMetadata != null ? baseMetadata.metadataFileLocation() : null;
    }

    @Override
    void loadBaseMetadata() {
      baseMetadata = ViewMetadataParser.read(metadataInputFile());
    }

    @Override
    void checkRequirements() {
      contentChange.requirements().forEach(r -> r.validate(baseMetadata));
    }

    @Override
    void setCreateMetadata() {
      var viewMetaBuilder = ViewMetadata.builder();
      contentChange.updates().forEach(u -> u.applyTo(viewMetaBuilder));
      updatedMetadata = viewMetaBuilder.build();
    }

    @Override
    void setUpdatedMetadata() {
      var viewMetaBuilder = ViewMetadata.buildFrom(baseMetadata);
      contentChange.updates().forEach(u -> u.applyTo(viewMetaBuilder));
      updatedMetadata = viewMetaBuilder.build();
    }

    @Override
    String newMetadataFilePath(int version) {
      return newViewMetadataFilePath(updatedMetadata, version);
    }

    @Override
    void writeMetadata(OutputFile outputFile) {
      ViewMetadataParser.overwrite(updatedMetadata, outputFile);
      updatedMetadata =
          ViewMetadata.buildFrom(updatedMetadata)
              .setMetadataLocation(outputFile.location())
              .build();
    }

    @Override
    void verifyLocation(
        RealmConfig realmConfig,
        ValidationContext validationContext,
        LocationCheck locationCheck,
        Map<String, String> properties,
        TableIdentifier tableIdentifier,
        ArrayList<PolarisEntity> resolvedFullPath) {
      if (contentChange.operation() == ContentChange.Operation.CREATE
          || !updatedMetadata.location().equals(baseMetadata.location())) {
        validateNoLocationOverlap(
            realmConfig,
            validationContext,
            locationCheck,
            updatedMetadata.location(),
            tableIdentifier,
            resolvedFullPath);
      }
    }

    @Override
    TableLikeObj.Builder<?, ?> newObjBuilder() {
      return IcebergViewObj.builder();
    }

    @Override
    Map<String, String> finalizeProperties(Map<String, String> props) {
      return props;
    }

    @Override
    Map<String, String> finalizeInternalProperties(Map<String, String> internalProps) {
      return internalProps;
    }
  }

  abstract class ChangeElement<ICEBERG_METADATA, OBJ_CLASS extends TableLikeObj> {
    private final ObjType objType;
    private final Class<OBJ_CLASS> objClass;
    private final List<String> previousLocations = new ArrayList<>();
    final ContentChange contentChange;
    private int version;
    private ResolvedPath resolvedPath;
    private String writtenLocation;
    private boolean needBaseReload;
    private boolean needRequirementsCheck;
    private boolean needsWrite;

    @SuppressWarnings("unchecked")
    ChangeElement(ObjType objType, ContentChange contentChange) {
      this.objType = objType;
      this.objClass = (Class<OBJ_CLASS>) objType.targetClass();
      this.contentChange = contentChange;
      this.needRequirementsCheck = true;
    }

    abstract ICEBERG_METADATA baseMetadata();

    abstract ICEBERG_METADATA updatedMetadata();

    abstract String baseMetadataLocation();

    abstract void loadBaseMetadata();

    abstract void checkRequirements();

    abstract void setCreateMetadata();

    abstract void setUpdatedMetadata();

    Optional<OBJ_CLASS> leafObj() {
      return resolvedPath.leafObjAs(objClass);
    }

    abstract String newMetadataFilePath(int version);

    abstract void writeMetadata(OutputFile outputFile);

    InputFile metadataInputFile() {
      var metadataLocation =
          leafObj()
              .flatMap(TableLikeObj::metadataLocation)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          format(
                              "No metadata location for existing Iceberg table %s",
                              contentChange.identifier())));
      version = parseVersionFromMetadataLocation(metadataLocation);
      return fileIO.newInputFile(metadataLocation);
    }

    void saveMetadata() {
      if (!needsWrite) {
        return;
      }
      if (writtenLocation != null) {
        // add a previously written metadata location to the list of files to delete
        previousLocations.add(writtenLocation);
        writtenLocation = null;
      }
      while (true) {
        var metadataLocation = newMetadataFilePath(version + 1);
        if (previousLocations.contains(metadataLocation)) {
          // It is very unlikely to hit this code path, as metadata file paths contain a
          // random UUID component.
          continue;
        }
        writtenLocation = metadataLocation;
        var outputFile = fileIO.newOutputFile(metadataLocation);
        writeMetadata(outputFile);
        needsWrite = false;
        break;
      }
    }

    ResolvedPath resolvedPath() {
      return requireNonNull(resolvedPath);
    }

    void setupMetadata() {
      if (resolvedPath == null) {
        // Note: the resolvedPath field might be updated in 'verifyState'
        // The 'resolvedPath' function performs CREATE/UPDATE operation specific checks.
        resolvedPath =
            resolvedPath(
                store.catalogContent().nameIndex().orElse(Index.empty()), store.persistence());
      }

      switch (contentChange.operation()) {
        case CREATE -> {
          setCreateMetadata();
          needsWrite = true;
        }
        case UPDATE -> {
          if (baseMetadata() == null) {
            needBaseReload = true;
          } else {
            var currentMetadataLocation =
                leafObj().flatMap(TableLikeObj::metadataLocation).orElse(null);
            var baseMetadataLocation = baseMetadataLocation();
            needBaseReload = !Objects.equals(currentMetadataLocation, baseMetadataLocation);
          }

          if (needBaseReload) {
            loadBaseMetadata();
            needBaseReload = false;
            needRequirementsCheck = true;
          }

          if (needRequirementsCheck) {
            checkRequirements();
            setUpdatedMetadata();
            needRequirementsCheck = false;
            needsWrite = true;
          }
        }
      }
    }

    ResolvedPath resolvedPath(Index<ObjRef> byName, Persistence persistence) {
      return resolvePathWithOptionalLeaf(persistence, byName, contentChange.identifier())
          .map(
              r -> {
                var leafObj = r.leafObj();
                switch (contentChange.operation()) {
                  case CREATE ->
                      leafObj.ifPresent(
                          l -> {
                            if (!directUpdateOrCreate
                                && contentChange.requirements().stream()
                                    .anyMatch(AssertTableDoesNotExist.class::isInstance)
                                && l.type() == contentChange.target().objType) {
                              // Simulate the requirements-check failure, mostly for tests
                              throw new AlreadyExistsException(
                                  "Requirement failed: table already exists");
                            }
                            throw alreadyExistsException(
                                contentChange.tableIdentifier(),
                                l.type(),
                                contentChange.target().objType);
                          });
                  case UPDATE ->
                      leafObj.ifPresentOrElse(
                          l -> {
                            if (!objClass.isInstance(l)) {
                              throw alreadyExistsException(
                                  contentChange.tableIdentifier(),
                                  l.type(),
                                  contentChange.target().objType);
                            }
                          },
                          () -> {
                            throw noSuchException(contentChange.tableIdentifier(), objType);
                          });
                }
                return r;
              })
          .orElseThrow(() -> noSuchNamespaceException(contentChange.tableIdentifier()));
    }

    boolean verifyState(Persistence persistence, UpdatableIndex<ObjRef> byName) {
      // resolvedPath() performs CREATE/UPDATE specific checks.
      var current = resolvedPath(byName, persistence);
      // This captures every little change
      var same = resolvedPath.equals(current);
      if (same) {
        return true;
      }
      switch (contentChange.operation()) {
        case CREATE -> {}
        case UPDATE -> {
          var resolvedObj =
              resolvedPath
                  .leafObjAs(TableLikeObj.class)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              format(
                                  "Resolved path leaf object does not exist for %s",
                                  contentChange.identifier())));
          var currentObj =
              current
                  .leafObjAs(TableLikeObj.class)
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              format(
                                  "Current path leaf object does not exist for %s",
                                  contentChange.identifier())));
          if (!currentObj.metadataLocation().equals(resolvedObj.metadataLocation())) {
            // refresh metadata, if necessary
            needBaseReload = true;
          }
        }
      }
      resolvedPath = current;
      return false;
    }

    void applyToCommit(
        RealmConfig realmConfig,
        ValidationContext validationContext,
        LocationCheck locationCheck,
        FinishChange finishChange) {
      var resolvedNamespace = store.resolvedPathToPolarisEntities(resolvedPath, false);
      var resolvedFullPath = new ArrayList<>(resolvedNamespace);
      resolvedPath.leafObj().ifPresent(l -> resolvedFullPath.add(store.mapToPolarisEntity(l)));

      var props = new HashMap<String, String>();
      var internalProps = new HashMap<String, String>();

      var persistence = validationContext.persistence();
      var now = persistence.currentInstant();
      var builder =
          switch (contentChange.operation()) {
            case CREATE -> {
              if (!contentChange.tableIdentifier().namespace().isEmpty()) {
                internalProps.put(
                    PARENT_NAMESPACE_KEY,
                    RESTUtil.encodeNamespace(contentChange.tableIdentifier().namespace()));
              }
              yield newObjBuilder().stableId(persistence.generateId()).createTimestamp(now);
            }
            case UPDATE -> {
              var l = leafObj().orElseThrow();
              props.putAll(l.properties());
              internalProps.putAll(l.internalProperties());
              yield newObjBuilder().from(l).entityVersion(l.entityVersion() + 1);
            }
          };

      var finalizeProperties = finalizeProperties(props);

      verifyLocation(
          realmConfig,
          validationContext,
          locationCheck,
          finalizeProperties,
          contentChange.tableIdentifier(),
          resolvedFullPath);

      builder
          .updateTimestamp(now)
          .id(persistence.generateId())
          .metadataLocation(writtenLocation)
          .name(contentChange.identifier().leafName())
          .properties(finalizeProperties)
          .internalProperties(finalizeInternalProperties(internalProps));

      finishChange.finishChange(contentChange, this.resolvedPath, builder);
    }

    abstract Map<String, String> finalizeInternalProperties(Map<String, String> internalProps);

    abstract Map<String, String> finalizeProperties(Map<String, String> props);

    abstract TableLikeObj.Builder<?, ?> newObjBuilder();

    abstract void verifyLocation(
        RealmConfig realmConfig,
        ValidationContext validationContext,
        LocationCheck locationCheck,
        Map<String, String> properties,
        TableIdentifier tableIdentifier,
        ArrayList<PolarisEntity> resolvedFullPath);

    void validateNoLocationOverlap(
        RealmConfig realmConfig,
        ValidationContext validationContext,
        LocationCheck locationCheck,
        String location,
        TableIdentifier tableIdentifier,
        ArrayList<PolarisEntity> resolvedFullPath) {
      // If location is changing then we must validate that the requested location is valid
      // for the storage configuration inherited under this entity's path.
      validateLocationsForTableLike(
          realmConfig, tableIdentifier, Set.of(location), resolvedFullPath);
      // also validate that the view location doesn't overlap an existing table
      if (realmConfig.getConfig(ALLOW_TABLE_LOCATION_OVERLAP, store.catalog().properties())) {
        LOGGER.debug(
            "Skipping location overlap validation for identifier '{}'", contentChange.identifier());
      } else if (realmConfig.getConfig(VALIDATE_VIEW_LOCATION_OVERLAP)
          || contentChange.target() == ContentChange.Target.TABLE) {
        LOGGER.debug("Validating no overlap with sibling tables or namespaces");

        locationCheck.validateNoLocationOverlap(
            contentChange.identifier(), resolvedPath(), Optional.of(location), validationContext);
      }
    }
  }

  // Clean up uncommitted files
  @Override
  public void close() {
    var toDelete = fileChanges.values().stream().flatMap(c -> c.previousLocations.stream());
    if (!committed) {
      toDelete =
          Stream.concat(
              toDelete,
              fileChanges.values().stream().map(c -> c.writtenLocation).filter(Objects::nonNull));
    }
    var filesToDelete = toDelete.toList();
    if (filesToDelete.isEmpty()) {
      return;
    }
    try {
      if (fileIO instanceof SupportsBulkOperations bulkOperations) {
        bulkOperations.deleteFiles(filesToDelete);
      } else {
        filesToDelete.forEach(fileIO::deleteFile);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to clean up {} metadata file(s)", filesToDelete.size(), e);
    }
  }

  @FunctionalInterface
  interface FinishChange {
    void finishChange(
        ContentChange contentChange,
        ResolvedPath resolvedPath,
        TableLikeObj.Builder<?, ?> newObjBuilder);
  }

  @FunctionalInterface
  interface LocationCheck {
    void validateNoLocationOverlap(
        ContentIdentifier ident,
        ResolvedPath resolvedPath,
        Optional<String> baseLocation,
        ValidationContext validationContext);
  }

  record ValidationContext(
      Persistence persistence,
      UpdatableIndex<ObjRef> byName,
      UpdatableIndex<EntityIdSet> locations) {}
}

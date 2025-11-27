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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.TABLE_DEFAULT_PREFIX;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED;
import static org.apache.polaris.core.config.FeatureConfiguration.OPTIMIZED_SIBLING_CHECK;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import jakarta.annotation.Nonnull;
import java.io.Closeable;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.service.catalog.common.LocationUtils;
import org.apache.polaris.service.catalog.validation.IcebergPropertiesValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that allows Iceberg catalog implementations in Polaris that are based Iceberg's {@link
 * org.apache.iceberg.view.BaseMetastoreViewCatalog} and those that are not, like {@link
 * RESTCompatibleCatalog}.
 */
public interface PolarisIcebergCatalog extends Catalog, ViewCatalog, SupportsNamespaces, Closeable {

  @Override
  default List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return listNamespaces(namespace, PageToken.readEverything()).items();
  }

  Page<Namespace> listNamespaces(Namespace namespace, PageToken pageToken)
      throws NoSuchNamespaceException;

  @Override
  default List<TableIdentifier> listTables(Namespace namespace) {
    return listTables(namespace, PageToken.readEverything()).items();
  }

  default Page<TableIdentifier> listTables(Namespace namespace, PageToken pageToken) {
    return listTableLike(PolarisEntitySubType.ICEBERG_TABLE, namespace, pageToken);
  }

  @Override
  default List<TableIdentifier> listViews(Namespace namespace) {
    return listViews(namespace, PageToken.readEverything()).items();
  }

  default Page<TableIdentifier> listViews(Namespace namespace, PageToken pageToken) {
    return listTableLike(PolarisEntitySubType.ICEBERG_VIEW, namespace, pageToken);
  }

  Page<TableIdentifier> listTableLike(
      PolarisEntitySubType polarisEntitySubType, Namespace namespace, PageToken pageToken);

  record NamespaceResult(long id, Map<String, String> properties) {}

  @Override
  default void createNamespace(Namespace namespace, Map<String, String> metadata) {
    LOGGER.debug("Creating namespace {} with metadata {}", namespace, metadata);
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }

    createNamespaceWithResult(namespace, metadata);
  }

  NamespaceResult createNamespaceWithResult(Namespace namespace, Map<String, String> metadata);

  @Override
  default boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    return dropTableLike(PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, purge);
  }

  @Override
  default boolean dropView(TableIdentifier identifier) {
    boolean purge = getCatalogConfig(FeatureConfiguration.PURGE_VIEW_METADATA_ON_DROP);

    return dropTableLike(PolarisEntitySubType.ICEBERG_VIEW, identifier, purge);
  }

  @Override
  default void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_VIEW, from, to);
  }

  @Override
  default void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_TABLE, from, to);
  }

  void renameTableLike(PolarisEntitySubType subType, TableIdentifier from, TableIdentifier to);

  boolean dropTableLike(PolarisEntitySubType subType, TableIdentifier identifier, boolean purge);

  void commitTransaction(CommitTransactionRequest commitTransactionRequest);

  // declaration clash in Iceberg types, so we need this override
  @Override
  String name();

  // declaration clash in Iceberg types, so we need this override
  @Override
  void initialize(String name, Map<String, String> properties);

  RealmConfig realmConfig();

  record InitializedConfig(
      String ioImplClassName,
      Map<String, String> tableDefaultProperties,
      String defaultBaseLocation,
      Map<String, String> catalogProperties) {}

  @Nonnull
  InitializedConfig initializedConfig();

  default String defaultBaseLocation() {
    return initializedConfig().defaultBaseLocation();
  }

  default String defaultNamespaceLocation(Namespace namespace) {
    var defaultBaseLocation = defaultBaseLocation();
    if (namespace.isEmpty()) {
      return defaultBaseLocation;
    } else {
      return SLASH.join(defaultBaseLocation, SLASH.join(namespace.levels()));
    }
  }

  default String ioImplClassName() {
    return initializedConfig().ioImplClassName();
  }

  default Map<String, String> tableDefaultProperties() {
    return initializedConfig().tableDefaultProperties();
  }

  default Map<String, String> properties() {
    var catalogProperties = initializedConfig().catalogProperties();
    return catalogProperties == null ? Map.of() : catalogProperties;
  }

  Optional<String> catalogBaseLocation();

  Optional<PolarisStorageConfigurationInfo> catalogStorageConfigurationInfo();

  @VisibleForTesting
  FileIO catalogFileIO();

  @VisibleForTesting
  void setCatalogFileIO(FileIO fileIO);

  <T> T getCatalogConfig(PolarisConfiguration<T> config);

  /**
   * Applies the rule controlled by {@link
   * CatalogEntity#REPLACE_NEW_LOCATION_PREFIX_WITH_CATALOG_DEFAULT_KEY} to a tablelike location
   */
  String applyReplaceNewLocationWithCatalogDefault(String specifiedTableLikeLocation);

  default InitializedConfig initializeIcebergCatalog(String name, Map<String, String> properties) {
    checkState(
        name().equals(name),
        "Tried to initialize catalog as name %s but already constructed with name %s",
        name,
        name());

    // Base location from catalogEntity is the primary source of truth, otherwise fall through
    // to the same key from the properties-map and finally fall through to WAREHOUSE_LOCATION.
    var baseLocation =
        catalogBaseLocation()
            .orElse(
                properties.getOrDefault(
                    DEFAULT_BASE_LOCATION_KEY, properties.getOrDefault(WAREHOUSE_LOCATION, "")));
    var defaultBaseLocation = baseLocation.replaceAll("/*$", "");

    var storageConfigurationInfo = catalogStorageConfigurationInfo().orElse(null);
    var ioImplClassName =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfig(), properties, storageConfigurationInfo);

    if (ioImplClassName == null) {
      LOGGER.warn(
          "Cannot resolve property '{}' for null storageConfiguration.",
          CatalogProperties.FILE_IO_IMPL);
    }

    var tableDefaultProperties = propertiesWithPrefix(properties, TABLE_DEFAULT_PREFIX);

    return new InitializedConfig(
        ioImplClassName, tableDefaultProperties, defaultBaseLocation, properties);
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
  default String transformTableLikeLocation(TableIdentifier tableIdentifier, String location) {
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

  boolean logPrefixOverlapWarning();

  default String buildPrefixedLocation(TableIdentifier tableIdentifier) {
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

  default TableMetadata registerTableLoadMetadata(
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
    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }

    var locationDir = metadataFileLocation.substring(0, lastSlashIndex);

    // TODO  the fileIO isn't added to the closeableGroup and not handled in close() ??
    @SuppressWarnings("resource")
    var fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            properties(),
            Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

    var metadataFile = fileIO.newInputFile(metadataFileLocation);
    return TableMetadataParser.read(metadataFile);
  }

  FileIO loadFileIOForTableLike(
      TableIdentifier identifier,
      Set<String> locationDirs,
      Map<String, String> tableDefaultProperties,
      Set<PolarisStorageActions> storageActions);

  Logger LOGGER = LoggerFactory.getLogger(PolarisIcebergCatalog.class);
  Joiner SLASH = Joiner.on("/");
}

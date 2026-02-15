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

package org.apache.polaris.service.catalog.common;

import static java.lang.String.format;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION;
import static org.apache.iceberg.TableProperties.METADATA_COMPRESSION_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_METADATA_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.CURRENT_SCHEMA_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.CURRENT_SNAPSHOT_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.DEFAULT_SORT_ORDER_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.DEFAULT_SPEC_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.FORMAT_VERSION;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.LAST_COLUMN_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.LAST_PARTITION_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.LAST_SEQUENCE_NUMBER;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.LAST_UPDATED_MILLIS;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.LOCATION;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.NEXT_ROW_ID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.TABLE_UUID;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY;

import jakarta.ws.rs.core.SecurityContext;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewProperties;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.storage.LocationRestrictions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for working with Polaris catalog entities. */
public class CatalogUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogUtils.class);

  public static final String METADATA_FOLDER_NAME = "metadata";

  private CatalogUtils() {}

  /**
   * Find the resolved entity path that may contain storage information
   *
   * @param resolvedEntityView The resolved entity view containing catalog entities.
   * @param tableIdentifier The table identifier for which to find storage information.
   * @return The resolved path wrapper that may contain storage information.
   */
  public static PolarisResolvedPathWrapper findResolvedStorageEntity(
      PolarisResolutionManifestCatalogView resolvedEntityView, TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper resolvedTableEntities =
        resolvedEntityView.getResolvedPath(
            tableIdentifier, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE);
    if (resolvedTableEntities != null) {
      return resolvedTableEntities;
    }
    return resolvedEntityView.getResolvedPath(tableIdentifier.namespace());
  }

  /**
   * Validates that the specified {@code locations} are valid for whatever storage config is found
   * for the given entity's parent hierarchy.
   *
   * @param realmConfig the realm configuration
   * @param identifier the table identifier (for error messages)
   * @param locations the set of locations to validate (base location + write.data.path +
   *     write.metadata.path)
   * @param locationRestrictions location restrictions
   * @throws ForbiddenException if any location is outside the allowed locations or if file
   *     locations are not allowed
   */
  public static void validateLocationsForTableLike(
      RealmConfig realmConfig,
      TableIdentifier identifier,
      Set<String> locations,
      Optional<LocationRestrictions> locationRestrictions) {
    locationRestrictions.ifPresentOrElse(
        restrictions -> restrictions.validate(realmConfig, identifier, locations),
        () -> {
          List<String> allowedStorageTypes =
              realmConfig.getConfig(FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES);
          if (allowedStorageTypes != null
              && !allowedStorageTypes.contains(StorageConfigInfo.StorageTypeEnum.FILE.name())) {
            List<String> invalidLocations =
                locations.stream()
                    .filter(location -> location.startsWith("file:") || location.startsWith("http"))
                    .collect(Collectors.toList());
            if (!invalidLocations.isEmpty()) {
              throw new ForbiddenException(
                  "Invalid locations '%s' for identifier '%s': File locations are not allowed",
                  invalidLocations, identifier);
            }
          }
        });
  }

  public static void validateLocationsForTableLike(
      RealmConfig realmConfig,
      TableIdentifier identifier,
      Set<String> locations,
      List<PolarisEntity> resolvedStorageEntity) {
    validateLocationsForTableLike(
        realmConfig,
        identifier,
        locations,
        PolarisStorageConfigurationInfo.forEntityPath(realmConfig, resolvedStorageEntity));
  }

  /**
   * Deprecated, use {@link #validateLocationsForTableLike(RealmConfig, TableIdentifier, Set,
   * Optional)}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public static void validateLocationsForTableLike(
      RealmConfig realmConfig,
      TableIdentifier identifier,
      Set<String> locations,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    validateLocationsForTableLike(
        realmConfig,
        identifier,
        locations,
        PolarisStorageConfigurationInfo.forEntityPath(
            realmConfig, resolvedStorageEntity.getRawFullPath()));
  }

  public static void validateMetadataFileInTableDir(
      RealmConfig realmConfig, TableIdentifier identifier, TableMetadata metadata) {
    var allowEscape = realmConfig.getConfig(ALLOW_EXTERNAL_TABLE_LOCATION);
    if (!allowEscape && !realmConfig.getConfig(ALLOW_EXTERNAL_METADATA_FILE_LOCATION)) {
      LOGGER.debug(
          "Validating base location {} for table {} in metadata file {}",
          metadata.location(),
          identifier,
          metadata.metadataFileLocation());
      var metadataFileLocation = StorageLocation.of(metadata.metadataFileLocation());
      var baseLocation = StorageLocation.of(metadata.location());
      if (!metadataFileLocation.isChildOf(baseLocation)) {
        throw new BadRequestException(
            "Metadata location %s is not allowed outside of table location %s",
            metadata.metadataFileLocation(), metadata.location());
      }
    }
  }

  public static Namespace decodeNamespace(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  public static String newViewMetadataFilePath(ViewMetadata metadata, int newVersion) {
    String codecName =
        metadata
            .properties()
            .getOrDefault(
                ViewProperties.METADATA_COMPRESSION, ViewProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return viewMetadataFileLocation(
        metadata, format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  private static String viewMetadataFileLocation(ViewMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(ViewProperties.WRITE_METADATA_LOCATION);
    if (metadataLocation != null) {
      return format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format(
          "%s/%s/%s",
          LocationUtil.stripTrailingSlash(metadata.location()), METADATA_FOLDER_NAME, filename);
    }
  }

  public static String tableMetadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  public static String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName = meta.property(METADATA_COMPRESSION, METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return tableMetadataFileLocation(
        meta, format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  /**
   * Parse the version from table/view metadata file name.
   *
   * @param metadataLocation table/view metadata file location
   * @return version of the table/view metadata file in success case and -1 if the version is not
   *     parsable (as a sign that the metadata is not part of this catalog)
   */
  public static int parseVersionFromMetadataLocation(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem object's metadata
      return -1;
    }

    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOGGER.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  public static PolarisAuthorizableOperation determinePolicyMappingOperation(
      PolicyAttachmentTarget target, PolarisResolvedPathWrapper targetWrapper, boolean isAttach) {
    return switch (targetWrapper.getRawLeafEntity().getType()) {
      case CATALOG ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG;
      case NAMESPACE ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE;
      case TABLE_LIKE -> {
        PolarisEntitySubType subType = targetWrapper.getRawLeafEntity().getSubType();
        if (subType == PolarisEntitySubType.ICEBERG_TABLE) {
          yield isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE;
        }
        throw new IllegalArgumentException("Unsupported table-like subtype: " + subType);
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }

  public static Map<String, String> buildTableMetadataPropertiesMap(TableMetadata metadata) {
    Map<String, String> storedProperties = new HashMap<>();
    // Location specific properties
    storedProperties.put(LOCATION, metadata.location());
    if (metadata.properties().containsKey(WRITE_DATA_LOCATION)) {
      storedProperties.put(
          USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, metadata.properties().get(WRITE_DATA_LOCATION));
    }
    if (metadata.properties().containsKey(WRITE_METADATA_LOCATION)) {
      storedProperties.put(
          USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
          metadata.properties().get(WRITE_METADATA_LOCATION));
    }
    storedProperties.put(FORMAT_VERSION, String.valueOf(metadata.formatVersion()));
    storedProperties.put(TABLE_UUID, metadata.uuid());
    storedProperties.put(CURRENT_SCHEMA_ID, String.valueOf(metadata.currentSchemaId()));
    if (metadata.currentSnapshot() != null) {
      storedProperties.put(
          CURRENT_SNAPSHOT_ID, String.valueOf(metadata.currentSnapshot().snapshotId()));
    }
    storedProperties.put(LAST_COLUMN_ID, String.valueOf(metadata.lastColumnId()));
    storedProperties.put(NEXT_ROW_ID, String.valueOf(metadata.nextRowId()));
    storedProperties.put(LAST_SEQUENCE_NUMBER, String.valueOf(metadata.lastSequenceNumber()));
    storedProperties.put(LAST_UPDATED_MILLIS, String.valueOf(metadata.lastUpdatedMillis()));
    if (metadata.sortOrder() != null) {
      storedProperties.put(DEFAULT_SORT_ORDER_ID, String.valueOf(metadata.defaultSortOrderId()));
    }
    if (metadata.spec() != null) {
      storedProperties.put(DEFAULT_SPEC_ID, String.valueOf(metadata.defaultSpecId()));
      storedProperties.put(LAST_PARTITION_ID, String.valueOf(metadata.lastAssignedPartitionId()));
    }
    return storedProperties;
  }

  /**
   * Helper function for when a TABLE_LIKE entity is not found, so we want to throw the appropriate
   * exception. Used in Iceberg APIs, so the Iceberg messages cannot be changed.
   *
   * @param subTypes The subtypes of the entity that the exception should report as non-existing
   */
  public static RuntimeException notFoundExceptionForTableLikeEntity(
      TableIdentifier identifier, List<PolarisEntitySubType> subTypes) {

    // In this case, we assume it's a table
    if (subTypes.size() > 1) {
      return new NoSuchTableException("Table does not exist: %s", identifier);
    } else {
      return notFoundExceptionForTableLikeEntity(identifier, subTypes.getFirst());
    }
  }

  public static RuntimeException notFoundExceptionForTableLikeEntity(
      TableIdentifier identifier, PolarisEntitySubType subType) {
    return switch (subType) {
      case ICEBERG_TABLE -> new NoSuchTableException("Table does not exist: %s", identifier);
      case ICEBERG_VIEW -> new NoSuchViewException("View does not exist: %s", identifier);
      case GENERIC_TABLE ->
          new NoSuchTableException("Generic table does not exist: %s", identifier);
      default ->
          // Assume it's a table
          new NoSuchTableException("Table does not exist: %s", identifier);
    };
  }

  public static NoSuchNamespaceException noSuchNamespaceException(TableIdentifier identifier) {
    return noSuchNamespaceException(identifier.namespace());
  }

  public static NoSuchNamespaceException noSuchNamespaceException(Namespace namespace) {
    // tests assert this
    var ns = namespace.isEmpty() ? "''" : namespace.toString();
    return new NoSuchNamespaceException("Namespace does not exist: %s", ns);
  }

  public static PolarisPrincipal validatePrincipal(SecurityContext securityContext) {
    var authenticatedPrincipal = securityContext.getUserPrincipal();
    if (authenticatedPrincipal instanceof PolarisPrincipal polarisPrincipal) {
      return polarisPrincipal;
    }
    throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
  }
}

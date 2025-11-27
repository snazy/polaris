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
package org.apache.polaris.service.catalog.generic;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.util.LinkedHashSet;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.service.catalog.common.CatalogAccess;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.GenericTable;
import org.apache.polaris.service.types.ListGenericTablesResponse;
import org.apache.polaris.service.types.LoadGenericTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericTableCatalogHandler extends CatalogHandler<GenericTableCatalog> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenericTableCatalogHandler.class);

  private GenericTableCatalog genericTableCatalog;

  public GenericTableCatalogHandler(
      RealmConfig realmConfig,
      CatalogAccess<GenericTableCatalog> catalogAccess,
      PolarisCredentialManager polarisCredentialManager,
      Instance<ExternalCatalogFactory> externalCatalogFactories) {
    super(realmConfig, catalogAccess, polarisCredentialManager, externalCatalogFactories);
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

      GenericTableCatalog federatedCatalog;
      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());

      // Use the unified factory pattern for all external catalog types
      Instance<ExternalCatalogFactory> externalCatalogFactory =
          externalCatalogFactories()
              .select(Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (externalCatalogFactory.isResolvable()) {
        // Pass through catalog properties (e.g., rest.client.proxy.*, timeout settings)
        Map<String, String> catalogProperties = catalogAccess().catalogProperties();
        federatedCatalog =
            externalCatalogFactory
                .get()
                .createGenericCatalog(
                    connectionConfigInfoDpo, getPolarisCredentialManager(), catalogProperties);
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      this.genericTableCatalog = federatedCatalog;
    } else {
      LOGGER.atInfo().log("Initializing non-federated catalog");
      this.genericTableCatalog = catalogAccess().initializeCatalog(Map.of());
    }
  }

  public ListGenericTablesResponse listGenericTables(Namespace parent) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, parent);
    initializeCatalog();

    return ListGenericTablesResponse.builder()
        .setIdentifiers(new LinkedHashSet<>(genericTableCatalog.listGenericTables(parent)))
        .build();
  }

  public LoadGenericTableResponse createGenericTable(
      TableIdentifier identifier,
      String format,
      String baseLocation,
      String doc,
      Map<String, String> properties) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_TABLE_DIRECT;
    catalogAuthZ().authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);
    initializeCatalog();

    GenericTableEntity createdEntity =
        this.genericTableCatalog.createGenericTable(
            identifier, format, baseLocation, doc, properties);
    GenericTable createdTable =
        GenericTable.builder()
            .setName(createdEntity.getName())
            .setFormat(createdEntity.getFormat())
            .setBaseLocation(createdEntity.getBaseLocation())
            .setDoc(createdEntity.getDoc())
            .setProperties(createdEntity.getPropertiesAsMap())
            .build();

    return LoadGenericTableResponse.builder().setTable(createdTable).build();
  }

  public boolean dropGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    catalogAuthZ().authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, identifier);
    initializeCatalog();

    return this.genericTableCatalog.dropGenericTable(identifier);
  }

  public LoadGenericTableResponse loadGenericTable(TableIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_TABLE;
    catalogAuthZ()
        .authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.GENERIC_TABLE, identifier);
    initializeCatalog();

    GenericTableEntity loadedEntity = this.genericTableCatalog.loadGenericTable(identifier);
    GenericTable loadedTable =
        GenericTable.builder()
            .setName(loadedEntity.getName())
            .setFormat(loadedEntity.getFormat())
            .setBaseLocation(loadedEntity.getBaseLocation())
            .setDoc(loadedEntity.getDoc())
            .setProperties(loadedEntity.getPropertiesAsMap())
            .build();

    return LoadGenericTableResponse.builder().setTable(loadedTable).build();
  }
}

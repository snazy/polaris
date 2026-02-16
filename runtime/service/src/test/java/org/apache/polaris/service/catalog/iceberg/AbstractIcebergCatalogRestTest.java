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

import static org.apache.iceberg.rest.RESTSessionCatalog.REST_PAGE_SIZE;

import com.google.common.collect.ImmutableMap;
import io.quarkus.vertx.http.HttpServer;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public abstract class AbstractIcebergCatalogRestTest extends AbstractIcebergCatalogTestBase {
  protected @SuppressWarnings("CdiInjectionPointsInspection") @Inject HttpServer httpServer;

  @Override
  protected RESTCatalog createCatalog(String catalogName, Map<String, String> properties) {
    var catalog = new RESTCatalog();
    var effectiveProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(
                CatalogProperties.URI,
                httpServer.getLocalBaseUri().resolve("/api/catalog/").toString())
            .put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2)
            .put(OAuth2Properties.CREDENTIAL, CLIENT_ID + ':' + CLIENT_SECRET)
            .put("scope", "PRINCIPAL_ROLE:ALL")
            .put("warehouse", catalogName)
            .buildKeepingLast();
    catalog.initialize(catalogName, effectiveProperties);
    return catalog;
  }

  @Test
  public void testPaginatedListTables() {
    additionalCatalogProperty(REST_PAGE_SIZE, "2");
    var catalog = catalog();

    catalog.createNamespace(NS);

    for (int i = 0; i < 5; i++) {
      catalog.buildTable(TableIdentifier.of(NS, "pagination_table_" + i), SCHEMA).create();
    }

    try {
      // List with pagination
      Assertions.assertThat(catalog.listTables(NS)).isNotNull().hasSize(5);
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropTable(TableIdentifier.of(NS, "pagination_table_" + i));
      }
    }
  }

  @Test
  public void testPaginatedListViews() {
    additionalCatalogProperty(REST_PAGE_SIZE, "2");
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
      // List with pagination
      Assertions.assertThat(catalog.listViews(NS)).isNotNull().hasSize(5);
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropTable(TableIdentifier.of(NS, "pagination_view_" + i));
      }
    }
  }

  @Test
  public void testPaginatedListNamespaces() {
    additionalCatalogProperty(REST_PAGE_SIZE, "2");
    var catalog = catalog();
    for (int i = 0; i < 5; i++) {
      catalog.createNamespace(Namespace.of("pagination_namespace_" + i));
    }

    try {
      // List with pagination
      Assertions.assertThat(catalog.listNamespaces()).isNotNull().hasSize(5);
    } finally {
      for (int i = 0; i < 5; i++) {
        catalog.dropNamespace(Namespace.of("pagination_namespace_" + i));
      }
    }
  }
}

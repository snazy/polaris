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
package org.apache.polaris.service.catalog.iceberg.nosql.rest;

import static org.apache.polaris.service.catalog.Profiles.NOSQL_IN_MEM;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogs;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.common.metastore.MetaStoreCatalogAccessFactory;
import org.apache.polaris.service.catalog.common.nosql.NoSqlCatalogAccessFactory;
import org.apache.polaris.service.catalog.iceberg.AbstractIcebergCatalogHandlerAuthzTest;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.events.EventAttributeMap;
import org.jetbrains.annotations.NotNull;

@QuarkusTest
@TestProfile(IcebergCatalogHandlerRestNoSqlAuthzTest.Profile.class)
public class IcebergCatalogHandlerRestNoSqlAuthzTest
    extends AbstractIcebergCatalogHandlerAuthzTest {

  public static class Profile extends PolarisAuthzTestBase.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(NOSQL_IN_MEM)
          .put("polaris.persistence.catalog-store-type", "nosql")
          .build();
    }
  }

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  MonotonicClock monotonicClock;

  @Inject NoSqlCatalogs noSqlCatalogs;

  @Override
  protected void bootstrapRealm(String realmIdentifier) {
    metaStoreManagerFactory.bootstrapRealms(List.of(realmIdentifier), RootCredentialsSet.EMPTY);
  }

  @Override
  protected @NotNull IcebergCatalogHandler newWrapper(
      String catalogName,
      CallContext callContext,
      RealmConfig realmConfig,
      CallContextCatalogFactory factory,
      PolarisPrincipal authenticatedPrincipal) {
    var catalogStore =
        new NoSqlCatalogAccessFactory(
                realmConfig,
                noSqlCatalogs,
                storageAccessConfigProvider,
                fileIOFactory,
                monotonicClock,
                new MetaStoreCatalogAccessFactory(
                    metaStoreManager,
                    callContext,
                    callContextCatalogFactory,
                    resolutionManifestFactory,
                    resolverFactory,
                    polarisAuthorizer,
                    authenticatedPrincipal),
                polarisAuthorizer,
                authenticatedPrincipal,
                new EventAttributeMap())
            .forCatalog(catalogName, Catalog.class);
    return new IcebergCatalogHandler(
        prefixParser,
        catalogStore,
        realmConfig,
        credentialManager,
        reservedProperties,
        catalogHandlerUtils,
        emptyExternalCatalogFactory(),
        storageAccessConfigProvider);
  }
}

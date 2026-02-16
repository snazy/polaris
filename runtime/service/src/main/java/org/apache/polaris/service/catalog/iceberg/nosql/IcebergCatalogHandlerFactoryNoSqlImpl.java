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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogs;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandler;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogHandlerFactory;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;

@RequestScoped
@Identifier("nosql")
class IcebergCatalogHandlerFactoryNoSqlImpl implements IcebergCatalogHandlerFactory {

  @Inject CallContext callContext;
  @Inject CatalogPrefixParser prefixParser;
  @Inject PolarisAuthorizer authorizer;
  @Inject ReservedProperties reservedProperties;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject EventAttributeMap eventAttributeMap;
  @Inject PolarisEventListener polarisEventListener;
  @Inject FileIOFactory fileIOFactory;
  @Inject MonotonicClock monotonicClock;
  @Inject NoSqlCatalogs noSqlCatalogs;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisMetricsReporter metricsReporter;
  @Inject Clock clock;

  // TODO ?
  //  @Inject PolarisCredentialManager credentialManager;
  //  @Inject CallContextCatalogFactory catalogFactory;
  //  @Inject CatalogHandlerUtils catalogHandlerUtils;
  //  @Inject @Any Instance<ExternalCatalogFactory> externalCatalogFactories;

  @Override
  public IcebergCatalogHandler createHandler(String catalogName, PolarisPrincipal principal) {
    var store = noSqlCatalogs.getCatalog(catalogName).orElseThrow();
    return IcebergCatalogHandlerNoSqlImpl.builder()
        .catalogName(catalogName)
        .polarisPrincipal(principal)
        .callContext(callContext)
        .polarisEventListener(polarisEventListener)
        .eventAttributeMap(eventAttributeMap)
        .authorizer(authorizer)
        .storageAccessConfigProvider(storageAccessConfigProvider)
        .store(store)
        .fileIOFactory(fileIOFactory)
        .monotonicClock(monotonicClock)
        .reservedProperties(reservedProperties)
        .prefixParser(prefixParser)
        .eventMetadataFactory(eventMetadataFactory)
        .metricsReporter(metricsReporter)
        .clock(clock)
        .authorizer(authorizer)
        .polarisPrincipal(principal)
        //        .credentialManager(credentialManager)
        //        .catalogFactory(catalogFactory)
        //        .catalogHandlerUtils(catalogHandlerUtils)
        //        .externalCatalogFactories(externalCatalogFactories)
        .build();
  }
}

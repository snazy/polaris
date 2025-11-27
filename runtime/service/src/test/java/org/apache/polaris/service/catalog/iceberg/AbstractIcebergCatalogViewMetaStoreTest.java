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

import java.io.IOException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.events.EventAttributeMap;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;

public abstract class AbstractIcebergCatalogViewMetaStoreTest
    extends AbstractIcebergCatalogViewTest<PolarisIcebergCatalog> {
  private PolarisIcebergCatalog catalog;

  @Override
  protected void setupCatalog(PolarisPrincipal authenticatedRoot) {
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, CATALOG_NAME);

    catalog =
        new IcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            polarisContext,
            passthroughView,
            authenticatedRoot,
            Mockito.mock(),
            storageAccessConfigProvider,
            fileIOFactory,
            polarisEventListener,
            eventMetadataFactory,
            new EventAttributeMap());
  }

  @AfterEach
  public void after() throws IOException {
    catalog().close();
    metaStoreManager.purge(polarisContext);
  }

  @Override
  protected PolarisIcebergCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }
}

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

import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;
import static org.apache.polaris.service.catalog.iceberg.AbstractIcebergCatalogRestTest.TABLE_PREFIXES;

import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import java.io.IOException;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.catalog.FakeRestIcebergCatalog;
import org.apache.polaris.service.catalog.common.CatalogAccessFactory;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractIcebergCatalogViewRestTest
    extends AbstractIcebergCatalogViewTest<RESTCatalog> {
  @Inject CatalogAccessFactory catalogAccessFactory;

  private RESTCatalog catalog;
  private RESTCompatibleCatalog icebergCatalog;

  @Override
  protected void setupCatalog(PolarisPrincipal authenticatedRoot) {
    var catalogStore = catalogAccessFactory.forCatalog(CATALOG_NAME, Catalog.class);
    icebergCatalog =
        (RESTCompatibleCatalog)
            catalogStore.initializeCatalog(
                ImmutableMap.<String, String>builder()
                    .put(FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
                    .putAll(TABLE_PREFIXES)
                    .buildKeepingLast());

    catalog = new FakeRestIcebergCatalog(icebergCatalog);
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
    icebergCatalog.close();
  }

  @Override
  protected RESTCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }
}

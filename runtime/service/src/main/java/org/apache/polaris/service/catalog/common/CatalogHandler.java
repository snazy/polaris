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

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.credentials.PolarisCredentialManager;

/**
 * An ABC for catalog wrappers which provides authorize methods that should be called before a
 * request is actually forwarded to a catalog. Child types must implement `initializeCatalog` which
 * will be called after a successful authorization.
 */
public abstract class CatalogHandler<CATALOG> {

  private final CatalogAccess<CATALOG> catalogAccess;
  private final CatalogAuthZ catalogAuthZ;
  private final PolarisCredentialManager credentialManager;
  private final Instance<ExternalCatalogFactory> externalCatalogFactories;

  private final RealmConfig realmConfig;

  public CatalogHandler(
      RealmConfig realmConfig,
      CatalogAccess<CATALOG> catalogAccess,
      PolarisCredentialManager credentialManager,
      Instance<ExternalCatalogFactory> externalCatalogFactories) {
    this.realmConfig = realmConfig;
    this.catalogAccess = catalogAccess;
    this.catalogAuthZ = catalogAccess.authZ();
    this.credentialManager = credentialManager;
    this.externalCatalogFactories = externalCatalogFactories;
  }

  protected CatalogAccess<CATALOG> catalogAccess() {
    return catalogAccess;
  }

  protected CatalogAuthZ catalogAuthZ() {
    return catalogAuthZ;
  }

  protected RealmConfig realmConfig() {
    return realmConfig;
  }

  protected Instance<ExternalCatalogFactory> externalCatalogFactories() {
    return externalCatalogFactories;
  }

  protected PolarisCredentialManager getPolarisCredentialManager() {
    return credentialManager;
  }

  protected String catalogName() {
    return catalogAccess.catalogName();
  }

  /** Initialize the catalog once authorized. Called after all `authorize...` methods. */
  protected abstract void initializeCatalog();
}

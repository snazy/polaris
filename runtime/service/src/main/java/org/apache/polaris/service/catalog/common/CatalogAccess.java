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

import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;

/** Persistence agnostic catalog access interface. */
public interface CatalogAccess<CATALOG> {
  CatalogAuthZ authZ();

  String catalogName();

  @Nullable
  default ConnectionConfigInfoDpo catalogConnectionConfigInfo() {
    var catalogEntity = catalogEntity();
    return catalogEntity != null ? catalogEntity.getConnectionConfigInfoDpo() : null;
  }

  CATALOG initializeCatalog(Map<String, String> additionalProperties);

  CatalogEntity catalogEntity();

  org.apache.polaris.core.admin.model.Catalog.TypeEnum catalogType();

  boolean isStaticFacade();

  Map<String, String> catalogProperties();

  Optional<String> metadataLocation(TableIdentifier tableIdentifier);

  Optional<List<PolarisEntity>> resolvePath(TableIdentifier tableIdentifier);
}

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

package org.apache.polaris.persistence.nosql.metastore;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.apache.polaris.persistence.nosql.metastore.committers.CatalogChangeCommitter;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess;

/**
 * API to work against the content of a Polaris catalog.
 *
 * <p>This API is considered unstable, function definitions may change without notice.
 */
public interface NoSqlCatalogContent {
  /** NoSQL {@link Persistence} instance used to access the catalog content. */
  Persistence persistence();

  /**
   * The catalog object state this instance refers to.
   *
   * <p>The returned value is constant throughout the lifetime of this object.
   */
  CatalogObj catalog();

  /** Provides read access to the catalog content. */
  IndexedContainerAccess<CatalogStateObj> catalogContent();

  /** Provides write access to the catalog content. */
  <RESULT> RESULT catalogContentChange(
      CatalogChangeCommitter<RESULT> changeCommitter, Class<RESULT> resultClass);

  Optional<String> hasOverlappingSiblings(String location);

  Stream<? extends ContentObj> listChildren(Index<ObjRef> nameIndex, ContentIdentifier parent);

  Optional<PolarisBaseEntity> principal(String principalName);

  List<PolarisBaseEntity> principalRoles(Set<String> roleNames);

  List<PolarisBaseEntity> principalRoles(long[] principalRoleIds);

  List<ResolvedPolarisEntity> resolveEntities(List<? extends PolarisBaseEntity> entities);

  ResolvedPolarisEntity resolveEntity(PolarisBaseEntity entity);

  List<PolarisBaseEntity> catalogRoles(long catalogId, long[] catalogRoleIds);

  PolarisBaseEntity rootEntity();

  PolarisEntity mapToPolarisEntity(ContentObj obj);

  List<PolarisEntity> resolvedPathToPolarisEntities(
      ResolvedPath resolvedPath, boolean includeLeafIfPresent);

  CatalogEntity catalogEntity();
}

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.metastore.committers.CatalogChangeCommitter;
import org.apache.polaris.persistence.nosql.metastore.committers.CatalogChangeCommitterWrapper;
import org.apache.polaris.persistence.nosql.metastore.indexaccess.IndexedContainerAccess;

record NoSqlCatalogsImpl(Persistence persistence, NoSqlMetaStore metaStore)
    implements NoSqlCatalogs {

  @Override
  public Optional<NoSqlCatalogContent> getCatalog(String catalogName) {
    var access =
        metaStore.memoizedIndexedAccess().indexedAccess(0L, PolarisEntityType.CATALOG.getCode());
    return access
        .byNameOnRoot(catalogName)
        .map(CatalogObj.class::cast)
        .map(NoSqlCatalogContentImpl::new);
  }

  final class NoSqlCatalogContentImpl implements NoSqlCatalogContent {
    private final CatalogObj catalog;
    private CatalogEntity catalogEntity;

    NoSqlCatalogContentImpl(CatalogObj catalog) {
      this.catalog = catalog;
    }

    @Override
    public IndexedContainerAccess<CatalogStateObj> catalogContent() {
      return metaStore.memoizedIndexedAccess().catalogContent(catalog.stableId());
    }

    @Override
    public Persistence persistence() {
      return persistence;
    }

    @Override
    public CatalogObj catalog() {
      return catalog;
    }

    @Override
    public <RESULT> RESULT catalogContentChange(
        CatalogChangeCommitter<RESULT> changeCommitter, Class<RESULT> resultClass) {
      try {
        var committer =
            persistence
                .createCommitter(
                    format(CATALOG_STATE_REF_NAME_PATTERN, catalog.stableId()),
                    CatalogStateObj.class,
                    resultClass)
                .synchronizingLocally();
        var commitRetryable = new CatalogChangeCommitterWrapper<>(changeCommitter);
        return committer.commitRuntimeException(commitRetryable).orElseThrow();
      } finally {
        metaStore.memoizedIndexedAccess().invalidateCatalogContent(catalog.stableId());
      }
    }

    @Override
    public ResolvedPolarisEntity resolveEntity(PolarisBaseEntity entity) {
      return resolveEntities(List.of(requireNonNull(entity))).stream().findFirst().orElse(null);
    }

    @Override
    public List<ResolvedPolarisEntity> resolveEntities(List<? extends PolarisBaseEntity> entities) {
      var catalogId = catalog.stableId();
      checkArgument(
          entities.stream()
              .mapToLong(PolarisBaseEntity::getCatalogId)
              .noneMatch(c -> c != 0L && c != catalogId),
          "All entities must belong to the same catalog");
      return metaStore.resolvedPolarisEntities(catalogId, entities);
    }

    @Override
    public Optional<PolarisBaseEntity> principal(String principalName) {
      return Optional.ofNullable(
          metaStore.lookupEntityByName(
              catalog.stableId(), 0L, PolarisEntityType.PRINCIPAL.getCode(), principalName));
    }

    @Override
    public List<PolarisBaseEntity> catalogRoles(long catalogId, long[] catalogRoleIds) {
      return metaStore.lookupEntities(
          catalog.stableId(), PolarisEntityType.CATALOG_ROLE.getCode(), catalogRoleIds);
    }

    @Override
    public List<PolarisBaseEntity> principalRoles(Set<String> roleNames) {
      return metaStore.lookupEntitiesByName(
          catalog.stableId(), 0L, PolarisEntityType.PRINCIPAL_ROLE.getCode(), roleNames);
    }

    @Override
    public List<PolarisBaseEntity> principalRoles(long[] principalRoleIds) {
      return metaStore.lookupEntities(
          catalog.stableId(), PolarisEntityType.PRINCIPAL_ROLE.getCode(), principalRoleIds);
    }

    @Override
    public PolarisBaseEntity rootEntity() {
      return metaStore.lookupEntity(0L, 0L, PolarisEntityType.ROOT.getCode());
    }

    @Override
    public Optional<String> hasOverlappingSiblings(String checkLocation) {
      return metaStore.hasOverlappingSiblings(catalog.stableId(), checkLocation);
    }

    @Override
    public Stream<? extends ContentObj> listChildren(
        Index<ObjRef> nameIndex, ContentIdentifier parent) {
      return metaStore.listChildren(nameIndex, parent);
    }

    @Override
    public PolarisEntity mapToPolarisEntity(ContentObj obj) {
      return new PolarisEntity(EntityObjMappings.mapToEntity(obj, catalog.stableId()));
    }

    @Override
    public List<PolarisEntity> resolvedPathToPolarisEntities(
        ResolvedPath resolvedPath, boolean includeLeafIfPresent) {
      var rawFullPath = new ArrayList<PolarisEntity>();
      rawFullPath.add(catalogEntity());
      resolvedPath.namespaceElements().stream()
          .map(this::mapToPolarisEntity)
          .forEach(rawFullPath::add);
      if (includeLeafIfPresent && resolvedPath.leafObj().isPresent()) {
        rawFullPath.add(mapToPolarisEntity(resolvedPath.leafObj().get()));
      }
      return rawFullPath;
    }

    @Override
    public CatalogEntity catalogEntity() {
      if (catalogEntity == null) {
        catalogEntity = new CatalogEntity(EntityObjMappings.mapToEntity(catalog, 0L));
      }
      return catalogEntity;
    }
  }
}

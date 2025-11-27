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

package org.apache.polaris.service.catalog.common.nosql;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifierFor;
import static org.apache.polaris.persistence.nosql.metastore.ResolvedPath.resolvePathWithOptionalLeaf;
import static org.apache.polaris.service.catalog.common.CatalogUtils.determinePolicyMappingOperation;

import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogContent;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogs;
import org.apache.polaris.service.catalog.common.CatalogAccess;
import org.apache.polaris.service.catalog.common.CatalogAccessFactory;
import org.apache.polaris.service.catalog.common.CatalogAuthZ;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;

@RequestScoped
@Identifier("nosql")
public class NoSqlCatalogAccessFactory implements CatalogAccessFactory {

  private final RealmConfig realmConfig;
  private final NoSqlCatalogs noSqlCatalogs;
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final FileIOFactory fileIOFactory;
  private final MonotonicClock monotonicClock;
  private final CatalogAccessFactory catalogAccessFactory;
  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final EventAttributeMap eventAttributeMap;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public NoSqlCatalogAccessFactory(
      RealmConfig realmConfig,
      NoSqlCatalogs noSqlCatalogs,
      StorageAccessConfigProvider storageAccessConfigProvider,
      FileIOFactory fileIOFactory,
      MonotonicClock monotonicClock,
      @Identifier("default") CatalogAccessFactory catalogAccessFactory,
      PolarisAuthorizer authorizer,
      PolarisPrincipal polarisPrincipal,
      EventAttributeMap eventAttributeMap) {
    this.realmConfig = realmConfig;
    this.noSqlCatalogs = noSqlCatalogs;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.fileIOFactory = fileIOFactory;
    this.monotonicClock = monotonicClock;
    this.catalogAccessFactory = catalogAccessFactory;
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.eventAttributeMap = eventAttributeMap;
  }

  @Override
  public <CATALOG> CatalogAccess<CATALOG> forCatalog(
      String catalogName, Class<CATALOG> catalogClass) {
    if (!Catalog.class.isAssignableFrom(catalogClass)) {
      return catalogAccessFactory.forCatalog(catalogName, catalogClass);
    }
    var catalog = noSqlCatalogs.getCatalog(catalogName);
    if (catalog.isEmpty()) {
      throw new IllegalStateException("Catalog '" + catalogName + "' does not exist.");
    }
    @SuppressWarnings("unchecked")
    var r = (CatalogAccess<CATALOG>) new NoSqlCatalogAccessImpl(catalog.get());
    return r;
  }

  /** Combined {@link CatalogAccess} and {@link CatalogAuthZ}. */
  private final class NoSqlCatalogAccessImpl extends NoSqlCatalogAuthZ
      implements CatalogAccess<Catalog> {

    NoSqlCatalogAccessImpl(NoSqlCatalogContent store) {
      super(store);
    }

    @Override
    public CatalogAuthZ authZ() {
      return this;
    }

    @Override
    public Catalog initializeCatalog(Map<String, String> additionalProperties) {
      var catalog =
          new NoSqlIcebergCatalog(
              realmConfig,
              store,
              storageAccessConfigProvider,
              fileIOFactory,
              monotonicClock,
              eventAttributeMap);

      var properties = new HashMap<>(store.catalog().properties());
      properties.putAll(additionalProperties);

      // Mimics what org.apache.polaris.service.catalog.iceberg.IcebergCatalog.initialize does.
      // This somewhat contradicts with the behavior of
      // org.apache.polaris.service.catalog.iceberg.IcebergCatalog.initialize.
      var defaultBaseLocation =
          store
              .catalog()
              .defaultBaseLocation()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          format(
                              "Catalog '%s' does not have a configured warehouse location. "
                                  + "Please configure a default base location for this catalog.",
                              store.catalog().name())));
      properties.put(WAREHOUSE_LOCATION, defaultBaseLocation);

      catalog.initialize(store.catalog().name(), properties);
      return catalog;
    }

    @Override
    public String catalogName() {
      return store.catalog().name();
    }

    @Override
    public CatalogEntity catalogEntity() {
      return store.catalogEntity();
    }

    @Override
    public boolean isStaticFacade() {
      var isExternal = store.catalog().catalogType() == CatalogType.EXTERNAL;
      var isPassthroughFacade =
          store
              .catalog()
              .internalProperties()
              .containsKey(PolarisEntityConstants.getConnectionConfigInfoPropertyName());
      return isExternal && !isPassthroughFacade;
    }

    @Override
    public org.apache.polaris.core.admin.model.Catalog.TypeEnum catalogType() {
      return org.apache.polaris.core.admin.model.Catalog.TypeEnum.valueOf(
          store.catalog().catalogType().name());
    }

    @Override
    public Map<String, String> catalogProperties() {
      return store.catalog().properties();
    }

    @Override
    public Optional<String> metadataLocation(TableIdentifier tableIdentifier) {
      var ident = identifierFor(tableIdentifier);
      var indexKey = ident.toIndexKey();
      return store
          .catalogContent()
          .nameIndex()
          .map(index -> index.get(indexKey))
          .flatMap(
              objRef -> Optional.ofNullable(store.persistence().fetch(objRef, TableLikeObj.class)))
          .flatMap(TableLikeObj::metadataLocation);
    }

    @Override
    public Optional<List<PolarisEntity>> resolvePath(TableIdentifier tableIdentifier) {
      var ident = identifierFor(tableIdentifier);
      return Optional.ofNullable(resolvePathByIdentifier(ident, TableLikeObj.class));
    }
  }

  /** AuthZ parts. */
  private class NoSqlCatalogAuthZ extends NoSqlCatalogBase implements CatalogAuthZ {
    private ResolvedPolarisEntity resolvedRootEntity;
    private ResolvedPolarisEntity resolvedCatalogEntity;

    private NoSqlCatalogAuthZ(NoSqlCatalogContent store) {
      super(store);
    }

    @Override
    public void authorizeBasicNamespaceOperationOrThrow(
        PolarisAuthorizableOperation op,
        Namespace namespace,
        List<Namespace> extraPassthroughNamespaces,
        List<TableIdentifier> extraPassthroughTableLikes,
        List<PolicyIdentifier> extraPassThroughPolicies) {
      authOrThrow(op, new Resolv(namespace));
    }

    @Override
    public void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation op, TableIdentifier identifier) {
      authOrThrow(op, new Resolv(identifier.namespace()));
    }

    @Override
    public void authorizeBasicTableLikeOperationsOrThrow(
        EnumSet<PolarisAuthorizableOperation> ops,
        PolarisEntitySubType subType,
        TableIdentifier identifier) {
      var resolv = new Resolv(identifier, Optional.of(subType));
      ops.forEach(op -> authOrThrow(op, resolv));
    }

    @Override
    public void authorizeRenameTableLikeOperationOrThrow(
        PolarisAuthorizableOperation op,
        PolarisEntitySubType subType,
        TableIdentifier src,
        TableIdentifier dst) {
      authOrThrow(op, new Resolv(src, Optional.of(subType)).withSecondary(dst, Optional.empty()));
    }

    @Override
    public void authorizeBasicPolicyOperationOrThrow(
        PolarisAuthorizableOperation op, PolicyIdentifier identifier) {
      authOrThrow(op, new Resolv(identifier));
    }

    @Override
    public void authorizeGetApplicablePoliciesOperationOrThrow(
        @Nullable Namespace namespace, @Nullable String targetName) {
      if (namespace == null || namespace.isEmpty()) {
        // catalog
        authOrThrow(
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG, new Resolv(true));
      } else if (Strings.isNullOrEmpty(targetName)) {
        // namespace
        authOrThrow(
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE,
            new Resolv(namespace));
      } else {
        // table
        var tableIdentifier = TableIdentifier.of(namespace, targetName);
        // only Iceberg tables are supported
        authOrThrow(
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE,
            new Resolv(tableIdentifier, Optional.of(PolarisEntitySubType.ICEBERG_TABLE)));
      }
    }

    @Override
    public void authorizeBasicCatalogOperationOrThrow(PolarisAuthorizableOperation op) {
      authOrThrow(op, new Resolv(true));
    }

    @Override
    public void authorizePolicyMappingOperationOrThrow(
        PolicyIdentifier identifier, PolicyAttachmentTarget target, boolean isAttach) {
      var resolv =
          switch (target.getType()) {
            case CATALOG -> new Resolv(true);
            case NAMESPACE -> new Resolv(Namespace.of(target.getPath().toArray(new String[0])));
            case TABLE_LIKE ->
                new Resolv(
                    TableIdentifier.of(target.getPath().toArray(new String[0])), Optional.empty());
          };
      resolv.withSecondary(identifier);
      if (resolv.secondary() == null) {
        throw new NoSuchPolicyException(format("Policy does not exist: %s", identifier));
      }
      var op = determinePolicyMappingOperation(target, resolv.secondary(), isAttach);
      authorizer.authorizeOrThrow(
          polarisPrincipal,
          resolv.allActivatedCatalogRoleAndPrincipalRoles(),
          op,
          // secondary and target are _reversed_ here
          resolv.secondary(),
          resolv.target());
    }

    @Override
    public void authorizeCollectionOfTableLikeOperationOrThrow(
        PolarisAuthorizableOperation op, PolarisEntitySubType subType, List<TableIdentifier> ids) {
      var resolv = new Resolv(ids, Optional.of(subType));
      authorizer.authorizeOrThrow(
          polarisPrincipal,
          resolv.allActivatedCatalogRoleAndPrincipalRoles(),
          op,
          resolv.targets(),
          resolv.secondaries());
    }

    private void authOrThrow(PolarisAuthorizableOperation op, Resolv resolv) {
      authorizer.authorizeOrThrow(
          polarisPrincipal,
          resolv.allActivatedCatalogRoleAndPrincipalRoles(),
          op,
          resolv.target(),
          resolv.secondary());
    }

    private class Resolv {
      private final List<PolarisResolvedPathWrapper> targets;
      private final Set<PolarisBaseEntity> activatedRoles;
      private PolarisResolvedPathWrapper secondary;

      Resolv(boolean forCatalog) {
        checkArgument(forCatalog);
        targets = List.of(resolvePolarisEntities(List.of()));
        activatedRoles = activatedRoles();
      }

      Resolv(Namespace namespace) {
        this(List.of(identifierFor(namespace)), NamespaceObj.class);
      }

      Resolv(TableIdentifier tableIdentifier, Optional<PolarisEntitySubType> subType) {
        this(List.of(identifierFor(tableIdentifier)), expectedTableLike(subType));
      }

      Resolv(List<TableIdentifier> tableIdentifiers, Optional<PolarisEntitySubType> subType) {
        this(
            tableIdentifiers.stream().map(ContentIdentifier::identifierFor).toList(),
            expectedTableLike(subType));
      }

      Resolv(PolicyIdentifier policyIdentifier) {
        this(List.of(identifierFor(policyIdentifier)), PolicyObj.class);
      }

      private static Class<? extends Obj> expectedTableLike(
          Optional<PolarisEntitySubType> subType) {
        if (subType.isEmpty()) {
          return TableLikeObj.class;
        }
        return EntityObjMappings.byEntityType(PolarisEntityType.TABLE_LIKE)
            .objTypeForSubType(subType.get())
            .targetClass();
      }

      private Resolv(List<ContentIdentifier> idents, Class<? extends Obj> expectedType) {
        targets =
            idents.stream()
                .map(
                    i -> {
                      var entities = resolvePathByIdentifier(i, expectedType);
                      if (authorizer.requiresResolvedEntities()) {
                        return resolvePolarisEntities(entities);
                      }
                      // Don't resolve, if authorizer doesn't require resolved entities.
                      return new PolarisResolvedPathWrapper(
                          entities.stream()
                              .map(e -> new ResolvedPolarisEntity(e, List.of(), List.of()))
                              .toList());
                    })
                .toList();
        activatedRoles = activatedRoles();
      }

      private Set<PolarisBaseEntity> activatedRoles() {
        // org.apache.polaris.core.persistence.resolver.Resolver.resolveCallerPrincipalAndPrincipalRoles
        var principal =
            store
                .principal(polarisPrincipal.getName())
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            format("Principal %s not found", polarisPrincipal.getName())));

        var requiresPrincipalRoles =
            authorizer.requiresCatalogRoles() || authorizer.requiresPrincipalRoles();

        var principalRoles = List.<PolarisBaseEntity>of();
        var catalogRoles = List.<PolarisBaseEntity>of();
        if (requiresPrincipalRoles) {
          if (polarisPrincipal.getRoles().isEmpty()) {
            var resolvedPrincipal = store.resolveEntity(principal);
            var principalRoleIds =
                resolvedPrincipal.getGrantRecordsAsGrantee().stream()
                    .filter(
                        gr ->
                            gr.getPrivilegeCode()
                                == PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode())
                    .mapToLong(PolarisGrantRecord::getSecurableId)
                    .toArray();
            principalRoles = store.principalRoles(principalRoleIds);
          } else {
            principalRoles = store.principalRoles(polarisPrincipal.getRoles());
          }
        }

        if (authorizer.requiresCatalogRoles()) {
          var catalogRoleIds =
              store.resolveEntities(principalRoles).stream()
                  .filter(Objects::nonNull)
                  .flatMap(gr -> gr.getGrantRecordsAsGrantee().stream())
                  .filter(
                      gr ->
                          gr.getPrivilegeCode() == PolarisPrivilege.CATALOG_ROLE_USAGE.getCode()
                              && gr.getSecurableCatalogId() == store.catalog().stableId())
                  .mapToLong(PolarisGrantRecord::getSecurableId)
                  .distinct()
                  .toArray();
          catalogRoles = store.catalogRoles(store.catalog().stableId(), catalogRoleIds);
        }

        return Stream.concat(catalogRoles.stream(), principalRoles.stream())
            .collect(Collectors.toSet());
      }

      @CanIgnoreReturnValue
      Resolv withSecondary(
          TableIdentifier tableIdentifier, Optional<PolarisEntitySubType> subType) {
        return withSecondary(identifierFor(tableIdentifier), expectedTableLike(subType));
      }

      @CanIgnoreReturnValue
      Resolv withSecondary(PolicyIdentifier policyIdentifier) {
        return withSecondary(identifierFor(policyIdentifier), PolicyObj.class);
      }

      @CanIgnoreReturnValue
      Resolv withSecondary(ContentIdentifier ident, Class<? extends Obj> expectedType) {
        secondary = resolvePolarisEntities(resolvePathByIdentifier(ident, expectedType));
        return this;
      }

      Set<PolarisBaseEntity> allActivatedCatalogRoleAndPrincipalRoles() {
        return activatedRoles;
      }

      PolarisResolvedPathWrapper target() {
        return targets.getFirst();
      }

      List<PolarisResolvedPathWrapper> targets() {
        return targets;
      }

      PolarisResolvedPathWrapper secondary() {
        return secondary;
      }

      List<PolarisResolvedPathWrapper> secondaries() {
        return secondary != null ? List.of(secondary) : List.of();
      }
    }

    private PolarisResolvedPathWrapper resolvePolarisEntities(List<PolarisEntity> entities) {
      var resolved = new ArrayList<ResolvedPolarisEntity>(entities.size() + 2);
      resolved.add(resolvedRootEntity());
      resolved.add(resolvedCatalogEntity());
      resolved.addAll(store.resolveEntities(entities));
      return new PolarisResolvedPathWrapper(resolved);
    }

    private ResolvedPolarisEntity resolvedRootEntity() {
      if (resolvedRootEntity == null) {
        resolvedRootEntity = store.resolveEntity(store.rootEntity());
      }
      return resolvedRootEntity;
    }

    private ResolvedPolarisEntity resolvedCatalogEntity() {
      if (resolvedCatalogEntity == null) {
        resolvedCatalogEntity = store.resolveEntity(store.catalogEntity());
      }
      return resolvedCatalogEntity;
    }
  }

  /** Base parts. */
  private static class NoSqlCatalogBase {
    final NoSqlCatalogContent store;

    private NoSqlCatalogBase(NoSqlCatalogContent store) {
      this.store = store;
    }

    List<PolarisEntity> resolvePathByIdentifier(
        ContentIdentifier ident, Class<? extends Obj> expectedType) {
      return store
          .catalogContent()
          .nameIndex()
          .flatMap(byName -> resolvePathWithOptionalLeaf(store.persistence(), byName, ident))
          .filter(resolvedPath -> resolvedPath.leafObj().map(expectedType::isInstance).orElse(true))
          .map(resolvedPath -> store.resolvedPathToPolarisEntities(resolvedPath, true))
          .orElse(List.of());
    }
  }
}

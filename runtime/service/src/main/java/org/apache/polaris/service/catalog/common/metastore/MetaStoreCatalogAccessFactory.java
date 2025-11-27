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

package org.apache.polaris.service.catalog.common.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.apache.polaris.service.catalog.common.CatalogUtils.determinePolicyMappingOperation;
import static org.apache.polaris.service.catalog.common.CatalogUtils.throwNotFoundExceptionForTableLikeEntity;

import com.google.common.base.Strings;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.GenericTableCatalog;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.service.catalog.common.CatalogAccess;
import org.apache.polaris.service.catalog.common.CatalogAccessFactory;
import org.apache.polaris.service.catalog.common.CatalogAuthZ;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.generic.PolarisGenericTableCatalog;
import org.apache.polaris.service.catalog.policy.PolicyCatalog;
import org.apache.polaris.service.catalog.policy.PolicyCatalogUtils;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;

@RequestScoped
@Identifier("default")
public class MetaStoreCatalogAccessFactory implements CatalogAccessFactory {
  private final CallContextCatalogFactory catalogFactory;
  private final CallContext callContext;
  private final PolarisMetaStoreManager metaStoreManager;
  private final ResolutionManifestFactory resolutionManifestFactory;
  private final ResolverFactory resolverFactory;
  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;

  public MetaStoreCatalogAccessFactory(
      PolarisMetaStoreManager metaStoreManager,
      CallContext callContext,
      CallContextCatalogFactory catalogFactory,
      ResolutionManifestFactory resolutionManifestFactory,
      ResolverFactory resolverFactory,
      PolarisAuthorizer authorizer,
      PolarisPrincipal polarisPrincipal) {
    this.metaStoreManager = metaStoreManager;
    this.callContext = callContext;
    this.catalogFactory = catalogFactory;
    this.resolutionManifestFactory = resolutionManifestFactory;
    this.resolverFactory = resolverFactory;
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
  }

  @Override
  public <CATALOG> CatalogAccess<CATALOG> forCatalog(
      String catalogName, Class<CATALOG> catalogClass) {
    return new MetaStoreCatalogAccess<>(catalogName, catalogClass);
  }

  /** Combined {@link CatalogAccess} and {@link CatalogAuthZ}. */
  final class MetaStoreCatalogAccess<CATALOG> extends MetaStoreCatalogAuthZ
      implements CatalogAccess<CATALOG> {
    private final String catalogName;
    private final Class<CATALOG> catalogClass;

    MetaStoreCatalogAccess(String catalogName, Class<CATALOG> catalogClass) {
      this.catalogName = catalogName;
      this.catalogClass = catalogClass;
    }

    @Override
    PolarisResolutionManifest newResolutionManifest() {
      return resolutionManifestFactory.createResolutionManifest(polarisPrincipal, catalogName);
    }

    @Override
    public CatalogAuthZ authZ() {
      return this;
    }

    @Override
    public String catalogName() {
      return catalogName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CATALOG initializeCatalog(Map<String, String> additionalProperties) {
      if (Catalog.class.isAssignableFrom(catalogClass)) {
        checkArgument(additionalProperties.isEmpty(), "additionalProperties must be empty");
        return (CATALOG) catalogFactory.createCallContextCatalog(resolutionManifest());
      }
      if (GenericTableCatalog.class.isAssignableFrom(catalogClass)) {
        var genericTableCatalog =
            new PolarisGenericTableCatalog(metaStoreManager, callContext, resolutionManifest());
        genericTableCatalog.initialize(catalogName, additionalProperties);
        return (CATALOG) genericTableCatalog;
      }
      if (PolicyCatalog.class.isAssignableFrom(catalogClass)) {
        checkArgument(additionalProperties.isEmpty(), "additionalProperties must be empty");
        return (CATALOG) new PolicyCatalog(metaStoreManager, callContext, resolutionManifest());
      }
      throw new IllegalArgumentException(
          String.format("Unsupported catalog class: %s", catalogClass.getName()));
    }

    @Override
    public CatalogEntity catalogEntity() {
      if (resolutionManifest != null) {
        CatalogEntity catalogEntity = resolutionManifest.getResolvedCatalogEntity();
        checkNotNull(catalogEntity, "No catalog available");
        return catalogEntity;
      } else {
        Resolver resolver = resolverFactory.createResolver(polarisPrincipal, catalogName);
        ResolverStatus resolverStatus = resolver.resolveAll();
        if (!resolverStatus.getStatus().equals(ResolverStatus.StatusEnum.SUCCESS)) {
          throw new NotFoundException("Unable to find warehouse %s", catalogName);
        }
        ResolvedPolarisEntity resolvedReferenceCatalog = resolver.getResolvedReferenceCatalog();
        return new CatalogEntity(requireNonNull(resolvedReferenceCatalog).getEntity());
      }
    }

    @Override
    public boolean isStaticFacade() {
      return catalogEntity().isStaticFacade();
    }

    @Override
    public org.apache.polaris.core.admin.model.Catalog.TypeEnum catalogType() {
      return catalogEntity().getCatalogType();
    }

    @Override
    public Map<String, String> catalogProperties() {
      return catalogEntity().getPropertiesAsMap();
    }

    @Override
    public Optional<String> metadataLocation(TableIdentifier tableIdentifier) {
      PolarisResolvedPathWrapper target = resolutionManifest().getResolvedPath(tableIdentifier);
      PolarisEntity rawLeafEntity = target.getRawLeafEntity();
      if (rawLeafEntity.getType() == PolarisEntityType.TABLE_LIKE) {
        return Optional.of(IcebergTableLikeEntity.of(rawLeafEntity))
            .map(IcebergTableLikeEntity::getMetadataLocation);
      }
      return Optional.empty(); // could be an external catalog
    }

    @Override
    public Optional<List<PolarisEntity>> resolvePath(TableIdentifier tableIdentifier) {
      PolarisResolvedPathWrapper resolvedStoragePath =
          CatalogUtils.findResolvedStorageEntity(resolutionManifest(), tableIdentifier);
      return Optional.ofNullable(
          resolvedStoragePath != null ? resolvedStoragePath.getRawFullPath() : null);
    }
  }

  /** AuthZ parts incl resolution manifest. */
  abstract class MetaStoreCatalogAuthZ implements CatalogAuthZ {
    PolarisResolutionManifest resolutionManifest;

    @Nonnull
    PolarisResolutionManifest resolutionManifest() {
      return requireNonNull(
          resolutionManifest,
          "resolutionManifest is not initialized, no authorize*() function called?");
    }

    abstract PolarisResolutionManifest newResolutionManifest();

    @Override
    public void authorizeBasicNamespaceOperationOrThrow(
        PolarisAuthorizableOperation op,
        Namespace namespace,
        List<Namespace> extraPassthroughNamespaces,
        List<TableIdentifier> extraPassthroughTableLikes,
        List<PolicyIdentifier> extraPassThroughPolicies) {
      resolutionManifest = newResolutionManifest();
      resolutionManifest.addPath(
          new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
          namespace);

      if (extraPassthroughNamespaces != null) {
        for (Namespace ns : extraPassthroughNamespaces) {
          resolutionManifest.addPassthroughPath(
              new ResolverPath(
                  Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
              ns);
        }
      }
      if (extraPassthroughTableLikes != null) {
        for (TableIdentifier id : extraPassthroughTableLikes) {
          resolutionManifest.addPassthroughPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(id),
                  PolarisEntityType.TABLE_LIKE,
                  true /* optional */),
              id);
        }
      }

      if (extraPassThroughPolicies != null) {
        for (PolicyIdentifier id : extraPassThroughPolicies) {
          resolutionManifest.addPassthroughPath(
              new ResolverPath(
                  PolarisCatalogHelpers.identifierToList(id.getNamespace(), id.getName()),
                  PolarisEntityType.POLICY,
                  true /* optional */),
              id);
        }
      }

      resolutionManifest.resolveAll();
      PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
      if (target == null) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      authOrThrow(op, target, null);
    }

    @Override
    public void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation op, Namespace namespace) {
      resolutionManifest = newResolutionManifest();

      Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);
      resolutionManifest.addPath(
          new ResolverPath(Arrays.asList(parentNamespace.levels()), PolarisEntityType.NAMESPACE),
          parentNamespace);

      // When creating an entity under a namespace, the authz target is the parentNamespace, but we
      // must also add the actual path that will be created as an "optional" passthrough resolution
      // path to indicate that the underlying catalog is "allowed" to check the creation path for
      // a conflicting entity.
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE, true /* optional */),
          namespace);
      resolutionManifest.resolveAll();
      PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(parentNamespace, true);
      if (target == null) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", parentNamespace);
      }
      authOrThrow(op, target, null);
    }

    @Override
    public void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        PolarisAuthorizableOperation op, TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();

      resolutionManifest = newResolutionManifest();
      resolutionManifest.addPath(
          new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE),
          namespace);

      // When creating an entity under a namespace, the authz target is the namespace, but we must
      // also
      // add the actual path that will be created as an "optional" passthrough resolution path to
      // indicate that the underlying catalog is "allowed" to check the creation path for a
      // conflicting
      // entity.
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(identifier),
              PolarisEntityType.TABLE_LIKE,
              true /* optional */),
          identifier);
      resolutionManifest.resolveAll();
      PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(namespace, true);
      if (target == null) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }
      authOrThrow(op, target, null);
    }

    /**
     * Ensures resolution manifest is initialized for a table identifier. This allows checking
     * catalog-level feature flags or other resolved entities before authorization. If already
     * initialized, this is a no-op.
     */
    private void ensureResolutionManifestForTable(TableIdentifier identifier) {
      if (resolutionManifest == null) {
        resolutionManifest = newResolutionManifest();

        // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
        resolutionManifest.addPassthroughPath(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(identifier),
                PolarisEntityType.TABLE_LIKE,
                true /* optional */),
            identifier);
        resolutionManifest.resolveAll();
      }
    }

    @Override
    public void authorizeBasicTableLikeOperationOrThrow(
        PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
      authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
    }

    @Override
    public void authorizeBasicTableLikeOperationsOrThrow(
        EnumSet<PolarisAuthorizableOperation> ops,
        PolarisEntitySubType subType,
        TableIdentifier identifier) {
      ensureResolutionManifestForTable(identifier);
      PolarisResolvedPathWrapper target =
          resolutionManifest.getResolvedPath(
              identifier, PolarisEntityType.TABLE_LIKE, subType, true);
      if (target == null) {
        throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
      }

      for (PolarisAuthorizableOperation op : ops) {
        authOrThrow(op, target, null);
      }
    }

    @Override
    public void authorizeRenameTableLikeOperationOrThrow(
        PolarisAuthorizableOperation op,
        PolarisEntitySubType subType,
        TableIdentifier src,
        TableIdentifier dst) {
      resolutionManifest = newResolutionManifest();
      // Add src, dstParent, and dst(optional)
      resolutionManifest.addPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(src), PolarisEntityType.TABLE_LIKE),
          src);
      resolutionManifest.addPath(
          new ResolverPath(Arrays.asList(dst.namespace().levels()), PolarisEntityType.NAMESPACE),
          dst.namespace());
      resolutionManifest.addPath(
          new ResolverPath(
              PolarisCatalogHelpers.tableIdentifierToList(dst),
              PolarisEntityType.TABLE_LIKE,
              true /* optional */),
          dst);
      ResolverStatus status = resolutionManifest.resolveAll();
      if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED
          && status.getFailedToResolvePath().getLastEntityType() == PolarisEntityType.NAMESPACE) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", dst.namespace());
      } else if (resolutionManifest.getResolvedPath(src, PolarisEntityType.TABLE_LIKE, subType)
          == null) {
        throwNotFoundExceptionForTableLikeEntity(dst, List.of(subType));
      }

      // Normally, since we added the dst as an optional path, we'd expect it to only get resolved
      // up to its parent namespace, and for there to be no TABLE_LIKE already in the dst in which
      // case the leafSubType will be NULL_SUBTYPE.
      // If there is a conflicting TABLE or VIEW, this leafSubType will indicate that conflicting
      // type.
      // TODO: Possibly modify the exception thrown depending on whether the caller has privileges
      // on the parent namespace.
      PolarisEntitySubType dstLeafSubType = resolutionManifest.getLeafSubType(dst);

      switch (dstLeafSubType) {
        case ICEBERG_TABLE:
          throw new AlreadyExistsException(
              "Cannot rename %s to %s. Table already exists", src, dst);

        case PolarisEntitySubType.ICEBERG_VIEW:
          throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", src, dst);

        case PolarisEntitySubType.GENERIC_TABLE:
          throw new AlreadyExistsException(
              "Cannot rename %s to %s. Generic table already exists", src, dst);

        default:
          break;
      }

      PolarisResolvedPathWrapper target =
          resolutionManifest.getResolvedPath(src, PolarisEntityType.TABLE_LIKE, subType, true);
      PolarisResolvedPathWrapper secondary =
          resolutionManifest.getResolvedPath(dst.namespace(), true);
      authOrThrow(op, target, secondary);
    }

    @Override
    public void authorizeBasicPolicyOperationOrThrow(
        PolarisAuthorizableOperation op, PolicyIdentifier identifier) {
      resolutionManifest = newResolutionManifest();
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.identifierToList(
                  identifier.getNamespace(), identifier.getName()),
              PolarisEntityType.POLICY,
              true /* optional */),
          identifier);
      resolutionManifest.resolveAll();

      PolarisResolvedPathWrapper target = resolutionManifest.getResolvedPath(identifier, true);
      if (target == null) {
        throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
      }

      authOrThrow(op, target, null);
    }

    @Override
    public void authorizeGetApplicablePoliciesOperationOrThrow(
        @Nullable Namespace namespace, @Nullable String targetName) {
      if (namespace == null || namespace.isEmpty()) {
        // catalog
        PolarisAuthorizableOperation op =
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG;
        authorizeBasicCatalogOperationOrThrow(op);
      } else if (Strings.isNullOrEmpty(targetName)) {
        // namespace
        PolarisAuthorizableOperation op =
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE;
        authorizeBasicNamespaceOperationOrThrow(op, namespace);
      } else {
        // table
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, targetName);
        PolarisAuthorizableOperation op =
            PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE;
        // only Iceberg tables are supported
        authorizeBasicTableLikeOperationOrThrow(
            op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
      }
    }

    @Override
    public void authorizeBasicCatalogOperationOrThrow(PolarisAuthorizableOperation op) {
      resolutionManifest = newResolutionManifest();
      resolutionManifest.resolveAll();

      PolarisResolvedPathWrapper targetCatalog =
          resolutionManifest.getResolvedReferenceCatalogEntity();
      if (targetCatalog == null) {
        throw new NotFoundException("Catalog not found");
      }
      authOrThrow(op, targetCatalog, null);
    }

    @Override
    public void authorizePolicyMappingOperationOrThrow(
        PolicyIdentifier identifier, PolicyAttachmentTarget target, boolean isAttach) {
      resolutionManifest = newResolutionManifest();
      resolutionManifest.addPassthroughPath(
          new ResolverPath(
              PolarisCatalogHelpers.identifierToList(
                  identifier.getNamespace(), identifier.getName()),
              PolarisEntityType.POLICY,
              true /* optional */),
          identifier);

      switch (target.getType()) {
        case CATALOG -> {}
        case NAMESPACE -> {
          Namespace targetNamespace = Namespace.of(target.getPath().toArray(new String[0]));
          resolutionManifest.addPath(
              new ResolverPath(
                  Arrays.asList(targetNamespace.levels()), PolarisEntityType.NAMESPACE),
              targetNamespace);
        }
        case TABLE_LIKE -> {
          TableIdentifier targetIdentifier =
              TableIdentifier.of(target.getPath().toArray(new String[0]));
          resolutionManifest.addPath(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(targetIdentifier),
                  PolarisEntityType.TABLE_LIKE),
              targetIdentifier);
        }
        default ->
            throw new IllegalArgumentException("Unsupported target type: " + target.getType());
      }

      ResolverStatus status = resolutionManifest.resolveAll();

      throwNotFoundExceptionIfFailToResolve(status, identifier);

      PolarisResolvedPathWrapper policyWrapper =
          resolutionManifest.getPassthroughResolvedPath(
              identifier, PolarisEntityType.POLICY, PolarisEntitySubType.NULL_SUBTYPE);
      if (policyWrapper == null) {
        throw new NoSuchPolicyException(String.format("Policy does not exist: %s", identifier));
      }

      PolarisResolvedPathWrapper targetWrapper =
          PolicyCatalogUtils.getResolvedPathWrapper(resolutionManifest, target);

      PolarisAuthorizableOperation op =
          determinePolicyMappingOperation(target, targetWrapper, isAttach);

      authOrThrow(op, policyWrapper, targetWrapper);
    }

    @Override
    public void authorizeCollectionOfTableLikeOperationOrThrow(
        PolarisAuthorizableOperation op,
        final PolarisEntitySubType subType,
        List<TableIdentifier> ids) {
      resolutionManifest = newResolutionManifest();
      ids.forEach(
          identifier ->
              resolutionManifest.addPassthroughPath(
                  new ResolverPath(
                      PolarisCatalogHelpers.tableIdentifierToList(identifier),
                      PolarisEntityType.TABLE_LIKE),
                  identifier));

      ResolverStatus status = resolutionManifest.resolveAll();

      // If one of the paths failed to resolve, throw exception based on the one that
      // we first failed to resolve.
      if (status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
        TableIdentifier identifier =
            PolarisCatalogHelpers.listToTableIdentifier(
                status.getFailedToResolvePath().getEntityNames());
        throwNotFoundExceptionForTableLikeEntity(identifier, List.of(subType));
      }

      List<PolarisResolvedPathWrapper> targets =
          ids.stream()
              .map(
                  identifier ->
                      Optional.ofNullable(
                              resolutionManifest.getResolvedPath(
                                  identifier, PolarisEntityType.TABLE_LIKE, subType, true))
                          .orElseThrow(
                              () ->
                                  subType == ICEBERG_TABLE
                                      ? new NoSuchTableException(
                                          "Table does not exist: %s", identifier)
                                      : new NoSuchViewException(
                                          "View does not exist: %s", identifier)))
              .toList();
      authorizer.authorizeOrThrow(
          polarisPrincipal,
          resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
          op,
          targets,
          null /* secondaries */);
    }

    private void authOrThrow(
        PolarisAuthorizableOperation op,
        PolarisResolvedPathWrapper target,
        PolarisResolvedPathWrapper secondary) {
      authorizer.authorizeOrThrow(
          polarisPrincipal,
          resolutionManifest.getAllActivatedCatalogRoleAndPrincipalRoles(),
          op,
          target,
          secondary);
    }

    private void throwNotFoundExceptionIfFailToResolve(
        ResolverStatus status, PolicyIdentifier identifier) {
      if ((status.getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED)) {
        switch (status.getFailedToResolvePath().getLastEntityType()) {
          case PolarisEntityType.TABLE_LIKE ->
              throw new NoSuchTableException(
                  "Table or view does not exist: %s",
                  PolarisCatalogHelpers.listToTableIdentifier(
                      status.getFailedToResolvePath().getEntityNames()));
          case PolarisEntityType.NAMESPACE ->
              throw new NoSuchNamespaceException(
                  "Namespace does not exist: %s",
                  Namespace.of(
                      status.getFailedToResolvePath().getEntityNames().toArray(new String[0])));
          case PolarisEntityType.POLICY ->
              throw new NoSuchPolicyException(
                  String.format("Policy does not exist: %s", identifier));
          default -> throw new IllegalStateException("Cannot resolve");
        }
      }
    }
  }
}

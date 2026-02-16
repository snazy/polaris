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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.identifierFor;
import static org.apache.polaris.persistence.nosql.metastore.ResolvedPath.resolvePathWithOptionalLeaf;

import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.PolicyObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.apache.polaris.persistence.nosql.coretypes.mapping.EntityObjMappings;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.NoSqlCatalogContent;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;

public abstract class NoSqlAuthZ implements CatalogHandler {
  private ResolvedPolarisEntity resolvedRootEntity;
  private ResolvedPolarisEntity resolvedCatalogEntity;

  // TODO refactor to work within a commit-retry-loop with custom `Persistence` instance and derived
  //    `IndexContainerAccess`/`MemoizedIndexAccess`
  //  Idea: Add some function NoSqlCatalogContent.withPersistence(), but also needs the indexes
  // passed to the
  //     CatalogChangeCommiter

  protected abstract NoSqlCatalogContent store();

  public void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  public void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      Namespace namespace,
      List<Namespace> extraPassthroughNamespaces,
      List<TableIdentifier> extraPassthroughTableLikes,
      List<PolicyIdentifier> extraPassThroughPolicies) {
    authOrThrow(op, new Resolv(namespace));
  }

  public void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  public void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    authOrThrow(op, new Resolv(identifier.namespace()));
  }

  public void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
  }

  public void authorizeBasicTableLikeOperationsOrThrow(
      EnumSet<PolarisAuthorizableOperation> ops,
      PolarisEntitySubType subType,
      TableIdentifier identifier) {
    var resolv = new Resolv(identifier, Optional.of(subType));
    ops.forEach(op -> authOrThrow(op, resolv));
  }

  public void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst) {
    authOrThrow(op, new Resolv(src, Optional.of(subType)).withSecondary(dst, Optional.empty()));
  }

  public void authorizeBasicPolicyOperationOrThrow(
      PolarisAuthorizableOperation op, PolicyIdentifier identifier) {
    authOrThrow(op, new Resolv(identifier));
  }

  public void authorizeGetApplicablePoliciesOperationOrThrow(
      @Nullable Namespace namespace, @Nullable String targetName) {
    if (namespace == null || namespace.isEmpty()) {
      // catalog
      authOrThrow(
          PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_CATALOG, new Resolv(true));
    } else if (Strings.isNullOrEmpty(targetName)) {
      // namespace
      authOrThrow(
          PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_NAMESPACE, new Resolv(namespace));
    } else {
      // table
      var tableIdentifier = TableIdentifier.of(namespace, targetName);
      // only Iceberg tables are supported
      authOrThrow(
          PolarisAuthorizableOperation.GET_APPLICABLE_POLICIES_ON_TABLE,
          new Resolv(tableIdentifier, Optional.of(PolarisEntitySubType.ICEBERG_TABLE)));
    }
  }

  public void authorizeBasicCatalogOperationOrThrow(PolarisAuthorizableOperation op) {
    authOrThrow(op, new Resolv(true));
  }

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
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolv.allActivatedCatalogRoleAndPrincipalRoles(),
            op,
            // secondary and target are _reversed_ here
            resolv.secondary(),
            resolv.target());
  }

  public void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, List<TableIdentifier> ids) {
    var resolv = new Resolv(ids, Optional.of(subType));
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
            resolv.allActivatedCatalogRoleAndPrincipalRoles(),
            op,
            resolv.targets(),
            resolv.secondaries());
  }

  public Set<PolarisStorageActions> authorizeLoadTable(
      TableIdentifier tableIdentifier, EnumSet<AccessDelegationMode> delegationModes) {
    if (delegationModes.isEmpty()) {
      authorizeBasicTableLikeOperationOrThrow(
          PolarisAuthorizableOperation.LOAD_TABLE,
          PolarisEntitySubType.ICEBERG_TABLE,
          tableIdentifier);
      return Set.of();
    }

    // Here we have a single method that falls through multiple candidate
    // PolarisAuthorizableOperations because instead of identifying the desired operation up-front
    // and
    // failing the authz check if grants aren't found, we find the first most-privileged authz match
    // and respond according to that.
    var read = PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION;
    var write = PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION;

    var actionsRequested = EnumSet.of(PolarisStorageActions.READ, PolarisStorageActions.LIST);
    try {
      // TODO: Refactor to have a boolean-return version of the helpers so we can fallthrough
      // easily.
      authorizeBasicTableLikeOperationOrThrow(
          write, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
      actionsRequested.add(PolarisStorageActions.WRITE);
    } catch (ForbiddenException e) {
      authorizeBasicTableLikeOperationOrThrow(
          read, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    }

    // TODO
    // checkAllowExternalCatalogCredentialVending(delegationModes);

    return actionsRequested;
  }

  // TODO (pretty much) duplicated code in IcebergCatalogHandlerImpl
  public EnumSet<PolarisAuthorizableOperation> getUpdateTableAuthorizableOperations(
      UpdateTableRequest request) {
    boolean useFineGrainedOperations =
        realmConfig()
            .getConfig(
                FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES,
                store().catalog().properties());

    if (useFineGrainedOperations) {
      EnumSet<PolarisAuthorizableOperation> actions =
          request.updates().stream()
              .map(
                  update ->
                      switch (update) {
                        case MetadataUpdate.AssignUUID assignUuid ->
                            PolarisAuthorizableOperation.ASSIGN_TABLE_UUID;
                        case MetadataUpdate.UpgradeFormatVersion upgradeFormat ->
                            PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION;
                        case MetadataUpdate.AddSchema addSchema ->
                            PolarisAuthorizableOperation.ADD_TABLE_SCHEMA;
                        case MetadataUpdate.SetCurrentSchema setCurrentSchema ->
                            PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA;
                        case MetadataUpdate.AddPartitionSpec addPartitionSpec ->
                            PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC;
                        case MetadataUpdate.AddSortOrder addSortOrder ->
                            PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER;
                        case MetadataUpdate.SetDefaultSortOrder setDefaultSortOrder ->
                            PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER;
                        case MetadataUpdate.AddSnapshot addSnapshot ->
                            PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT;
                        case MetadataUpdate.SetSnapshotRef setSnapshotRef ->
                            PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.RemoveSnapshots removeSnapshots ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS;
                        case MetadataUpdate.RemoveSnapshotRef removeSnapshotRef ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.SetLocation setLocation ->
                            PolarisAuthorizableOperation.SET_TABLE_LOCATION;
                        case MetadataUpdate.SetProperties setProperties ->
                            PolarisAuthorizableOperation.SET_TABLE_PROPERTIES;
                        case MetadataUpdate.RemoveProperties removeProperties ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES;
                        case MetadataUpdate.SetStatistics setStatistics ->
                            PolarisAuthorizableOperation.SET_TABLE_STATISTICS;
                        case MetadataUpdate.RemoveStatistics removeStatistics ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS;
                        case MetadataUpdate.RemovePartitionSpecs removePartitionSpecs ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS;
                        default ->
                            PolarisAuthorizableOperation
                                .UPDATE_TABLE; // Fallback for unknown update types
                      })
              .collect(
                  () -> EnumSet.noneOf(PolarisAuthorizableOperation.class),
                  EnumSet::add,
                  EnumSet::addAll);

      // If there are no MetadataUpdates, then default to the UPDATE_TABLE operation.
      if (actions.isEmpty()) {
        actions.add(PolarisAuthorizableOperation.UPDATE_TABLE);
      }

      return actions;
    } else {
      return EnumSet.of(PolarisAuthorizableOperation.UPDATE_TABLE);
    }
  }

  private void authOrThrow(PolarisAuthorizableOperation op, Resolv resolv) {
    authorizer()
        .authorizeOrThrow(
            polarisPrincipal(),
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

    private static Class<? extends Obj> expectedTableLike(Optional<PolarisEntitySubType> subType) {
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
                    if (authorizer().requiresResolvedEntities()) {
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

    // TODO memoize the result to prevented resolution in commit-retry-loops
    private Set<PolarisBaseEntity> activatedRoles() {
      // org.apache.polaris.core.persistence.resolver.Resolver.resolveCallerPrincipalAndPrincipalRoles
      var principal =
          store()
              .principal(polarisPrincipal().getName())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          format("Principal %s not found", polarisPrincipal().getName())));

      var requiresPrincipalRoles =
          authorizer().requiresCatalogRoles() || authorizer().requiresPrincipalRoles();

      var principalRoles = List.<PolarisBaseEntity>of();
      var catalogRoles = List.<PolarisBaseEntity>of();
      if (requiresPrincipalRoles) {
        if (polarisPrincipal().getRoles().isEmpty()) {
          var resolvedPrincipal = store().resolveEntity(principal);
          var principalRoleIds =
              resolvedPrincipal.getGrantRecordsAsGrantee().stream()
                  .filter(
                      gr ->
                          gr.getPrivilegeCode() == PolarisPrivilege.PRINCIPAL_ROLE_USAGE.getCode())
                  .mapToLong(PolarisGrantRecord::getSecurableId)
                  .toArray();
          principalRoles = store().principalRoles(principalRoleIds);
        } else {
          principalRoles = store().principalRoles(polarisPrincipal().getRoles());
        }
      }

      if (authorizer().requiresCatalogRoles()) {
        var catalogRoleIds =
            store().resolveEntities(principalRoles).stream()
                .filter(Objects::nonNull)
                .flatMap(gr -> gr.getGrantRecordsAsGrantee().stream())
                .filter(
                    gr ->
                        gr.getPrivilegeCode() == PolarisPrivilege.CATALOG_ROLE_USAGE.getCode()
                            && gr.getSecurableCatalogId() == store().catalog().stableId())
                .mapToLong(PolarisGrantRecord::getSecurableId)
                .distinct()
                .toArray();
        catalogRoles = store().catalogRoles(store().catalog().stableId(), catalogRoleIds);
      }

      return Stream.concat(catalogRoles.stream(), principalRoles.stream())
          .collect(Collectors.toSet());
    }

    @CanIgnoreReturnValue
    Resolv withSecondary(TableIdentifier tableIdentifier, Optional<PolarisEntitySubType> subType) {
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
    resolved.addAll(store().resolveEntities(entities));
    return new PolarisResolvedPathWrapper(resolved);
  }

  private ResolvedPolarisEntity resolvedRootEntity() {
    if (resolvedRootEntity == null) {
      resolvedRootEntity = store().resolveEntity(store().rootEntity());
    }
    return resolvedRootEntity;
  }

  private ResolvedPolarisEntity resolvedCatalogEntity() {
    if (resolvedCatalogEntity == null) {
      resolvedCatalogEntity = store().resolveEntity(store().catalogEntity());
    }
    return resolvedCatalogEntity;
  }

  private List<PolarisEntity> resolvePathByIdentifier(
      ContentIdentifier ident, Class<? extends Obj> expectedType) {
    return store()
        .catalogContent()
        .nameIndex()
        .flatMap(byName -> resolvePathWithOptionalLeaf(store().persistence(), byName, ident))
        .filter(resolvedPath -> resolvedPath.leafObj().map(expectedType::isInstance).orElse(true))
        .map(resolvedPath -> store().resolvedPathToPolarisEntities(resolvedPath, true))
        .orElse(List.of());
  }

  // Duplicate in PolicyCatalogHandler
  private PolarisAuthorizableOperation determinePolicyMappingOperation(
      PolicyAttachmentTarget target, PolarisResolvedPathWrapper targetWrapper, boolean isAttach) {
    return switch (targetWrapper.getRawLeafEntity().getType()) {
      case CATALOG ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_CATALOG
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_CATALOG;
      case NAMESPACE ->
          isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_NAMESPACE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_NAMESPACE;
      case TABLE_LIKE -> {
        PolarisEntitySubType subType = targetWrapper.getRawLeafEntity().getSubType();
        if (subType == PolarisEntitySubType.ICEBERG_TABLE) {
          yield isAttach
              ? PolarisAuthorizableOperation.ATTACH_POLICY_TO_TABLE
              : PolarisAuthorizableOperation.DETACH_POLICY_FROM_TABLE;
        }
        throw new IllegalArgumentException("Unsupported table-like subtype: " + subType);
      }
      default -> throw new IllegalArgumentException("Unsupported target type: " + target.getType());
    };
  }
}

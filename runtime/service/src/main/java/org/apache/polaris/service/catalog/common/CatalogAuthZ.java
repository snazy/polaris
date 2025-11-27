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
import java.util.EnumSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.service.types.PolicyAttachmentTarget;
import org.apache.polaris.service.types.PolicyIdentifier;

/** Authorization functions for catalog operations. */
public interface CatalogAuthZ {
  default void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      Namespace namespace,
      List<Namespace> extraPassthroughNamespaces,
      List<TableIdentifier> extraPassthroughTableLikes,
      List<PolicyIdentifier> extraPassThroughPolicies);

  default void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier);

  default void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
  }

  void authorizeBasicTableLikeOperationsOrThrow(
      EnumSet<PolarisAuthorizableOperation> ops,
      PolarisEntitySubType subType,
      TableIdentifier identifier);

  void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids);

  void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst);

  void authorizeBasicPolicyOperationOrThrow(
      PolarisAuthorizableOperation op, PolicyIdentifier identifier);

  void authorizeGetApplicablePoliciesOperationOrThrow(
      @Nullable Namespace namespace, @Nullable String targetName);

  void authorizeBasicCatalogOperationOrThrow(PolarisAuthorizableOperation op);

  void authorizePolicyMappingOperationOrThrow(
      PolicyIdentifier identifier, PolicyAttachmentTarget target, boolean isAttach);
}

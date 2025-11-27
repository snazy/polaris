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
package org.apache.polaris.service.catalog.policy;

import jakarta.annotation.Nullable;
import jakarta.enterprise.inject.Instance;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.service.catalog.common.CatalogAccess;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.ListPoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.service.types.UpdatePolicyRequest;

public class PolicyCatalogHandler extends CatalogHandler<PolicyCatalog> {

  private PolicyCatalog policyCatalog;

  public PolicyCatalogHandler(
      RealmConfig realmConfig,
      CatalogAccess<PolicyCatalog> catalogAccess,
      PolarisCredentialManager polarisCredentialManager,
      Instance<ExternalCatalogFactory> externalCatalogFactories) {
    super(realmConfig, catalogAccess, polarisCredentialManager, externalCatalogFactories);
  }

  @Override
  protected void initializeCatalog() {
    this.policyCatalog = catalogAccess().initializeCatalog(Map.of());
  }

  public ListPoliciesResponse listPolicies(Namespace parent, @Nullable PolicyType policyType) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_POLICY;
    catalogAuthZ().authorizeBasicNamespaceOperationOrThrow(op, parent);
    initializeCatalog();

    return ListPoliciesResponse.builder()
        .setIdentifiers(new HashSet<>(policyCatalog.listPolicies(parent, policyType)))
        .build();
  }

  public LoadPolicyResponse createPolicy(Namespace namespace, CreatePolicyRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_POLICY;
    PolicyIdentifier identifier =
        PolicyIdentifier.builder().setNamespace(namespace).setName(request.getName()).build();

    // authorize the creating policy under namespace operation
    catalogAuthZ()
        .authorizeBasicNamespaceOperationOrThrow(
            op, identifier.getNamespace(), null, null, List.of(identifier));
    initializeCatalog();

    return LoadPolicyResponse.builder()
        .setPolicy(
            policyCatalog.createPolicy(
                identifier, request.getType(), request.getDescription(), request.getContent()))
        .build();
  }

  public LoadPolicyResponse loadPolicy(PolicyIdentifier identifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_POLICY;
    catalogAuthZ().authorizeBasicPolicyOperationOrThrow(op, identifier);
    initializeCatalog();

    return LoadPolicyResponse.builder().setPolicy(policyCatalog.loadPolicy(identifier)).build();
  }

  public LoadPolicyResponse updatePolicy(PolicyIdentifier identifier, UpdatePolicyRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_POLICY;
    catalogAuthZ().authorizeBasicPolicyOperationOrThrow(op, identifier);
    initializeCatalog();

    return LoadPolicyResponse.builder()
        .setPolicy(
            policyCatalog.updatePolicy(
                identifier,
                request.getDescription(),
                request.getContent(),
                request.getCurrentPolicyVersion()))
        .build();
  }

  public boolean dropPolicy(PolicyIdentifier identifier, boolean detachAll) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_POLICY;
    catalogAuthZ().authorizeBasicPolicyOperationOrThrow(op, identifier);
    initializeCatalog();

    return policyCatalog.dropPolicy(identifier, detachAll);
  }

  public boolean attachPolicy(PolicyIdentifier identifier, AttachPolicyRequest request) {
    catalogAuthZ().authorizePolicyMappingOperationOrThrow(identifier, request.getTarget(), true);
    initializeCatalog();
    return policyCatalog.attachPolicy(identifier, request.getTarget(), request.getParameters());
  }

  public boolean detachPolicy(PolicyIdentifier identifier, DetachPolicyRequest request) {
    catalogAuthZ().authorizePolicyMappingOperationOrThrow(identifier, request.getTarget(), false);
    initializeCatalog();
    return policyCatalog.detachPolicy(identifier, request.getTarget());
  }

  public GetApplicablePoliciesResponse getApplicablePolicies(
      @Nullable Namespace namespace, @Nullable String targetName, @Nullable PolicyType policyType) {
    catalogAuthZ().authorizeGetApplicablePoliciesOperationOrThrow(namespace, targetName);
    initializeCatalog();

    return GetApplicablePoliciesResponse.builder()
        .setApplicablePolicies(
            new HashSet<>(policyCatalog.getApplicablePolicies(namespace, targetName, policyType)))
        .build();
  }
}

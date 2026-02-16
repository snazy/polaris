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

package org.apache.polaris.service.catalog;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalogAdapter;
import org.apache.polaris.service.types.CommitTableRequest;
import org.apache.polaris.service.types.CommitViewRequest;

/**
 * Provides a fake {@link RESTCatalog} that delegates directly to a {@link IcebergCatalogAdapter},
 * not using HTTP transport.
 */
@PolarisImmutable
public abstract class FakeRestIcebergCatalog {

  protected abstract IcebergCatalogAdapter icebergCatalogAdapter();

  protected abstract RealmContext realmContext();

  protected abstract SecurityContext securityContext();

  protected abstract String name();

  protected abstract Map<String, String> properties();

  protected abstract RequestScopedRunner requestScopedRunner();

  public FakeRestIcebergCatalog() {}

  public RESTCatalog newRestCatalog() {
    var effectiveProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .put(CatalogProperties.URI, "http://0.0.0.0:0/")
            .buildKeepingLast();
    var catalog = new RESTCatalog(this::newRestClient);
    catalog.initialize(name(), effectiveProperties);
    return catalog;
  }

  private RESTClient newRestClient(Map<String, String> properties) {
    return new FakeRESTClient(RESTUtil.configHeaders(properties));
  }

  @SuppressWarnings("resource")
  private final class FakeRESTClient implements RESTClient {
    private static final Pattern NAMESPACES_PATTERN = Pattern.compile("^v1/([^/]+)/namespaces$");
    private static final Pattern TABLES_RENAME_PATTERN =
        Pattern.compile("^v1/([^/]+)/tables/rename");
    private static final Pattern VIEWS_RENAME_PATTERN =
        Pattern.compile("^v1/([^/]+)/views/rename$");
    private static final Pattern TABLE_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)$");
    private static final Pattern TABLE_PATTERN_METRICS =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/metrics$");
    private static final Pattern TABLE_PATTERN_CREDENTIALS =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/tables/([^/]+)/credentials$");
    private static final Pattern VIEW_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/views/([^/]+)$");
    private static final Pattern TABLES_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/tables$");
    private static final Pattern REGISTER_TABLE_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/register$");
    private static final Pattern VIEWS_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/views$");
    private static final Pattern NAMESPACES_PROPERTIES_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)/properties$");
    private static final Pattern NAMESPACES_NAMESPACE_PATTERN =
        Pattern.compile("^v1/([^/]+)/namespaces/([^/]+)$");

    private final Map<String, String> configHeaders;

    FakeRESTClient(Map<String, String> configHeaders) {
      this.configHeaders = configHeaders;
    }

    private static String nameFromPath(Matcher m) {
      return RESTUtil.decodeString(m.group(3));
    }

    private static String namespaceFromPath(Matcher m) {
      return RESTUtil.decodeString(m.group(2));
    }

    private static String prefixFromPath(Matcher m) {
      return RESTUtil.decodeString(m.group(1));
    }

    private String header(Map<String, String> requestHeaders, String name) {
      return Optional.ofNullable(requestHeaders.get(name)).orElseGet(() -> configHeaders.get(name));
    }

    @Override
    public void head(
        String path, Map<String, String> requestHeaders, Consumer<ErrorResponse> errorHandler) {
      requestScopedRunner()
          .runWithRequestContext(
              () -> {
                var m = TABLE_PATTERN.matcher(path);
                if (m.matches()) {
                  icebergCatalogAdapter()
                      .tableExists(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          nameFromPath(m),
                          realmContext(),
                          securityContext());
                  return;
                }

                m = VIEW_PATTERN.matcher(path);
                if (m.matches()) {
                  icebergCatalogAdapter()
                      .viewExists(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          nameFromPath(m),
                          realmContext(),
                          securityContext());
                  return;
                }

                m = NAMESPACES_NAMESPACE_PATTERN.matcher(path);
                if (m.matches()) {
                  icebergCatalogAdapter()
                      .namespaceExists(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          realmContext(),
                          securityContext());
                  return;
                }

                throw new UnsupportedOperationException("HEAD " + path);
              },
              FakeRestIcebergCatalog::postProcessException);
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      return delete(path, Map.of(), responseType, requestHeaders, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      return requestScopedRunner()
          .supplyWithRequestContext(
              () -> {
                var m = TABLE_PATTERN.matcher(path);
                if (m.matches()) {
                  var purgeRequested =
                      Optional.ofNullable(queryParams.get("purgeRequested"))
                          .map(Boolean::parseBoolean)
                          .orElse(null);
                  icebergCatalogAdapter()
                      .dropTable(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          nameFromPath(m),
                          purgeRequested,
                          realmContext(),
                          securityContext());
                  return null;
                }

                m = VIEW_PATTERN.matcher(path);
                if (m.matches()) {
                  icebergCatalogAdapter()
                      .dropView(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          nameFromPath(m),
                          realmContext(),
                          securityContext());
                  return null;
                }

                m = NAMESPACES_NAMESPACE_PATTERN.matcher(path);
                if (m.matches()) {
                  icebergCatalogAdapter()
                      .dropNamespace(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          realmContext(),
                          securityContext());
                  return null;
                }

                throw new UnsupportedOperationException("DELETE " + path);
              },
              FakeRestIcebergCatalog::postProcessException);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {

      return requestScopedRunner()
          .supplyWithRequestContext(
              () -> {
                if ("v1/config".equals(path)) {
                  return (T)
                      icebergCatalogAdapter()
                          .getConfig(
                              requireNonNull(
                                  properties().get("prefix"), "'prefix' property not configured"),
                              realmContext(),
                              securityContext())
                          .readEntity(ConfigResponse.class);
                }
                var m = NAMESPACES_PATTERN.matcher(path);
                if (m.matches()) {
                  var parent = queryParams.get("parent");
                  var pageSize =
                      Optional.ofNullable(queryParams.get("pageSize"))
                          .map(Integer::valueOf)
                          .orElse(null);
                  var pageToken = queryParams.get("pageToken");
                  return (T)
                      icebergCatalogAdapter()
                          .listNamespaces(
                              prefixFromPath(m),
                              pageToken,
                              pageSize,
                              parent,
                              realmContext(),
                              securityContext())
                          .readEntity(ListNamespacesResponse.class);
                }

                m = TABLE_PATTERN.matcher(path);
                if (m.matches()) {
                  var accessDelegationMode = header(requestHeaders, "X-Iceberg-Access-Delegation");
                  var ifNoneMatch = header(requestHeaders, "If-None-Match");
                  var snapshots = queryParams.get("snapshots");
                  return (T)
                      icebergCatalogAdapter()
                          .loadTable(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              nameFromPath(m),
                              accessDelegationMode,
                              ifNoneMatch,
                              snapshots,
                              realmContext(),
                              securityContext())
                          .readEntity(LoadTableResponse.class);
                }

                m = VIEW_PATTERN.matcher(path);
                if (m.matches()) {
                  return (T)
                      icebergCatalogAdapter()
                          .loadView(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              nameFromPath(m),
                              realmContext(),
                              securityContext())
                          .readEntity(LoadViewResponse.class);
                }

                m = NAMESPACES_NAMESPACE_PATTERN.matcher(path);
                if (m.matches()) {
                  return (T)
                      icebergCatalogAdapter()
                          .loadNamespaceMetadata(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              realmContext(),
                              securityContext())
                          .readEntity(GetNamespaceResponse.class);
                }

                m = TABLES_PATTERN.matcher(path);
                if (m.matches()) {
                  var pageSize =
                      Optional.ofNullable(queryParams.get("pageSize"))
                          .map(Integer::valueOf)
                          .orElse(null);
                  var pageToken = queryParams.get("pageToken");
                  return (T)
                      icebergCatalogAdapter()
                          .listTables(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              pageToken,
                              pageSize,
                              realmContext(),
                              securityContext())
                          .readEntity(ListTablesResponse.class);
                }

                m = VIEWS_PATTERN.matcher(path);
                if (m.matches()) {
                  var pageSize =
                      Optional.ofNullable(queryParams.get("pageSize"))
                          .map(Integer::valueOf)
                          .orElse(null);
                  var pageToken = queryParams.get("pageToken");
                  return (T)
                      icebergCatalogAdapter()
                          .listViews(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              pageToken,
                              pageSize,
                              realmContext(),
                              securityContext())
                          .readEntity(ListTablesResponse.class);
                }

                throw new UnsupportedOperationException("GET " + path);
              },
              FakeRestIcebergCatalog::postProcessException);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      return requestScopedRunner()
          .supplyWithRequestContext(
              () -> {
                var m = NAMESPACES_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (CreateNamespaceRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .createNamespace(
                              prefixFromPath(m), req, realmContext(), securityContext())
                          .readEntity(CreateNamespaceResponse.class);
                }

                m = TABLES_RENAME_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (RenameTableRequest) body;
                  icebergCatalogAdapter()
                      .renameTable(prefixFromPath(m), req, realmContext(), securityContext());
                  return null;
                }

                m = VIEWS_RENAME_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (RenameTableRequest) body;
                  icebergCatalogAdapter()
                      .renameView(prefixFromPath(m), req, realmContext(), securityContext());
                  return null;
                }

                m = TABLE_PATTERN_METRICS.matcher(path);
                if (m.matches()) {
                  var req = (ReportMetricsRequest) body;
                  icebergCatalogAdapter()
                      .reportMetrics(
                          prefixFromPath(m),
                          namespaceFromPath(m),
                          nameFromPath(m),
                          req,
                          realmContext(),
                          securityContext());
                  return null;
                }

                m = TABLE_PATTERN_CREDENTIALS.matcher(path);
                if (m.matches()) {
                  return (T)
                      icebergCatalogAdapter()
                          .loadCredentials(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              nameFromPath(m),
                              realmContext(),
                              securityContext())
                          .readEntity(LoadCredentialsResponse.class);
                }

                m = TABLE_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (UpdateTableRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .updateTable(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              nameFromPath(m),
                              IcebergRestSerializationFixup.fixupCommitTableRequest(req),
                              realmContext(),
                              securityContext())
                          .readEntity(LoadTableResponse.class);
                }

                m = VIEW_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (CommitViewRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .replaceView(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              nameFromPath(m),
                              req,
                              realmContext(),
                              securityContext())
                          .readEntity(LoadViewResponse.class);
                }

                m = NAMESPACES_PROPERTIES_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (UpdateNamespacePropertiesRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .updateProperties(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              req,
                              realmContext(),
                              securityContext())
                          .readEntity(UpdateNamespacePropertiesResponse.class);
                }

                // Create table
                m = TABLES_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (CreateTableRequest) body;
                  var accessDelegationMode = header(requestHeaders, "X-Iceberg-Access-Delegation");
                  return (T)
                      icebergCatalogAdapter()
                          .createTable(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              req,
                              accessDelegationMode,
                              realmContext(),
                              securityContext())
                          .readEntity(LoadTableResponse.class);
                }

                // Create view
                m = VIEWS_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (CreateViewRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .createView(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              req,
                              realmContext(),
                              securityContext())
                          .readEntity(LoadViewResponse.class);
                }

                m = REGISTER_TABLE_PATTERN.matcher(path);
                if (m.matches()) {
                  var req = (RegisterTableRequest) body;
                  return (T)
                      icebergCatalogAdapter()
                          .registerTable(
                              prefixFromPath(m),
                              namespaceFromPath(m),
                              req,
                              realmContext(),
                              securityContext())
                          .readEntity(LoadTableResponse.class);
                }

                throw new UnsupportedOperationException("POST " + path);
              },
              FakeRestIcebergCatalog::postProcessException);
    }

    @Override
    public <T extends RESTResponse> T postForm(
        String path,
        Map<String, String> formData,
        Class<T> responseType,
        Map<String, String> requestHeaders,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException("POST-FORM " + path);
    }

    @Override
    public void close() {}
  }

  static RuntimeException postProcessException(RuntimeException e) {
    if (e instanceof CommitFailedException) {
      return new CommitFailedException(e, "Commit failed: %s", e.getMessage());
    }
    return e;
  }

  static class IcebergRestSerializationFixup {

    private IcebergRestSerializationFixup() {}

    static CommitTableRequest fixupCommitTableRequest(UpdateTableRequest request) {
      try {
        var commitTableRequest = new CommitTableRequest();
        var f = UpdateTableRequest.class.getDeclaredField("identifier");
        f.setAccessible(true);
        f.set(commitTableRequest, request.identifier());
        f = UpdateTableRequest.class.getDeclaredField("requirements");
        f.setAccessible(true);
        f.set(commitTableRequest, request.requirements());
        f = UpdateTableRequest.class.getDeclaredField("updates");
        f.setAccessible(true);
        f.set(commitTableRequest, request.updates());
        return commitTableRequest;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

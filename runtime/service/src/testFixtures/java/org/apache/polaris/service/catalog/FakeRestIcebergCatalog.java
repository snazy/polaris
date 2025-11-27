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

import static org.apache.iceberg.rest.RESTUtil.decodeNamespace;
import static org.apache.iceberg.rest.RESTUtil.decodeString;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.service.catalog.iceberg.RESTCompatibleCatalog;

/**
 * Provides a fake {@link RESTCatalog} that delegates directly to a {@link RESTCompatibleCatalog},
 * not using HTTP transport.
 */
public class FakeRestIcebergCatalog extends RESTCatalog {

  public FakeRestIcebergCatalog(RESTCompatibleCatalog restCompatibleCatalog) {
    super(properties -> new FakeRESTClient(restCompatibleCatalog));
    initialize(restCompatibleCatalog.name(), restCompatibleCatalog.properties());
  }

  private record FakeRESTClient(RESTCompatibleCatalog restCompatibleCatalog) implements RESTClient {
    private static final Set<Endpoint> DEFAULT_ENDPOINTS =
        ImmutableSet.<Endpoint>builder()
            .add(Endpoint.V1_LIST_NAMESPACES)
            .add(Endpoint.V1_LOAD_NAMESPACE)
            .add(Endpoint.V1_NAMESPACE_EXISTS)
            .add(Endpoint.V1_CREATE_NAMESPACE)
            .add(Endpoint.V1_UPDATE_NAMESPACE)
            .add(Endpoint.V1_DELETE_NAMESPACE)
            .add(Endpoint.V1_LIST_TABLES)
            .add(Endpoint.V1_LOAD_TABLE)
            .add(Endpoint.V1_TABLE_EXISTS)
            .add(Endpoint.V1_CREATE_TABLE)
            .add(Endpoint.V1_UPDATE_TABLE)
            .add(Endpoint.V1_DELETE_TABLE)
            .add(Endpoint.V1_RENAME_TABLE)
            .add(Endpoint.V1_REGISTER_TABLE)
            .add(Endpoint.V1_REPORT_METRICS)
            .add(Endpoint.V1_COMMIT_TRANSACTION)
            .build();

    private static final Set<Endpoint> VIEW_ENDPOINTS =
        ImmutableSet.<Endpoint>builder()
            .add(Endpoint.V1_LIST_VIEWS)
            .add(Endpoint.V1_LOAD_VIEW)
            .add(Endpoint.V1_VIEW_EXISTS)
            .add(Endpoint.V1_CREATE_VIEW)
            .add(Endpoint.V1_UPDATE_VIEW)
            .add(Endpoint.V1_DELETE_VIEW)
            .add(Endpoint.V1_RENAME_VIEW)
            .build();

    private static final Pattern TABLE_PATTERN =
        Pattern.compile("^v1/namespaces/([^/]+)/tables/(.+)$");
    private static final Pattern VIEW_PATTERN =
        Pattern.compile("^v1/namespaces/([^/]+)/views/(.+)$");
    private static final Pattern TABLES_PATTERN = Pattern.compile("^v1/namespaces/([^/]+)/tables$");
    private static final Pattern REGISTER_TABLE_PATTERN =
        Pattern.compile("^v1/namespaces/([^/]+)/register$");
    private static final Pattern VIEWS_PATTERN = Pattern.compile("^v1/namespaces/([^/]+)/views$");
    private static final Pattern NAMESPACES_PROPERTIES_PATTERN =
        Pattern.compile("^v1/namespaces/([^/]+)/properties$");
    private static final Pattern NAMESPACES_PATTERN = Pattern.compile("^v1/namespaces/([^/]+)$");

    @Override
    public void head(
        String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
      switch (path) {
        default -> {
          var m = TABLE_PATTERN.matcher(path);
          if (m.matches()) {
            var tableIdentifier = tableIdentifierFromPath(m);
            if (!restCompatibleCatalog.tableExists(tableIdentifier)) {
              throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
            }
            return;
          }

          m = VIEW_PATTERN.matcher(path);
          if (m.matches()) {
            var tableIdentifier = tableIdentifierFromPath(m);
            if (!restCompatibleCatalog.viewExists(tableIdentifier)) {
              throw new NoSuchViewException("View does not exist: %s", tableIdentifier);
            }
            return;
          }

          m = NAMESPACES_PATTERN.matcher(path);
          if (m.matches()) {
            var namespace = namespaceFromPath(m);
            if (!restCompatibleCatalog.namespaceExists(namespace)) {
              throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
            }
            return;
          }

          throw new UnsupportedOperationException("HEAD " + path);
        }
      }
    }

    private static TableIdentifier tableIdentifierFromPath(Matcher m) {
      return TableIdentifier.of(namespaceFromPath(m), decodeString(m.group(2)));
    }

    private static Namespace namespaceFromPath(Matcher m) {
      return decodeNamespace(m.group(1));
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      return delete(path, Map.of(), responseType, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      switch (path) {
        default -> {
          var m = TABLE_PATTERN.matcher(path);
          if (m.matches()) {
            restCompatibleCatalog.dropTable(tableIdentifierFromPath(m));
            return null;
          }

          m = VIEW_PATTERN.matcher(path);
          if (m.matches()) {
            restCompatibleCatalog.dropView(tableIdentifierFromPath(m));
            return null;
          }

          m = NAMESPACES_PATTERN.matcher(path);
          if (m.matches()) {
            restCompatibleCatalog.dropNamespace(namespaceFromPath(m));
            return null;
          }

          throw new UnsupportedOperationException("DELETE " + path);
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      return (T)
          switch (path) {
            case "v1/config" -> {
              yield ConfigResponse.builder()
                  // No prefix -> easier pattern matching
                  // .withOverride("prefix", catalog.name())
                  // TODO catalog properties, maybe?
                  .withEndpoints(
                      ImmutableList.<Endpoint>builder()
                          .addAll(DEFAULT_ENDPOINTS)
                          .addAll(VIEW_ENDPOINTS)
                          .build())
                  .build();
            }
            case "v1/namespaces" -> {
              var parent = queryParams.get("parent");
              var namespace =
                  parent == null ? Namespace.empty() : Namespace.of(decodeString(parent));
              var page = restCompatibleCatalog.listNamespaces(namespace, pageToken(queryParams));
              yield ListNamespacesResponse.builder()
                  .addAll(page.items())
                  .nextPageToken(page.encodedResponseToken())
                  .build();
            }
            default -> {
              var m = TABLE_PATTERN.matcher(path);
              if (m.matches()) {
                yield LoadTableResponse.builder()
                    .withTableMetadata(
                        restCompatibleCatalog.loadTableMetadata(tableIdentifierFromPath(m)))
                    .build();
              }

              m = VIEW_PATTERN.matcher(path);
              if (m.matches()) {
                var metadata = restCompatibleCatalog.loadViewMetadata(tableIdentifierFromPath(m));
                yield ImmutableLoadViewResponse.builder()
                    .metadata(metadata)
                    .metadataLocation(metadata.metadataFileLocation())
                    .build();
              }

              m = NAMESPACES_PATTERN.matcher(path);
              if (m.matches()) {
                var namespace = namespaceFromPath(m);
                yield GetNamespaceResponse.builder()
                    .withNamespace(namespace)
                    .setProperties(restCompatibleCatalog.loadNamespaceMetadata(namespace))
                    .build();
              }

              m = TABLES_PATTERN.matcher(path);
              if (m.matches()) {
                var namespace = namespaceFromPath(m);
                var page = restCompatibleCatalog.listTables(namespace, pageToken(queryParams));
                yield ListTablesResponse.builder()
                    .addAll(page.items())
                    .nextPageToken(page.encodedResponseToken())
                    .build();
              }

              m = VIEWS_PATTERN.matcher(path);
              if (m.matches()) {
                var namespace = namespaceFromPath(m);
                var page = restCompatibleCatalog.listViews(namespace, pageToken(queryParams));
                yield ListTablesResponse.builder()
                    .addAll(page.items())
                    .nextPageToken(page.encodedResponseToken())
                    .build();
              }

              throw new UnsupportedOperationException("GET " + path);
            }
          };
    }

    private PageToken pageToken(Map<String, String> queryParams) {
      var pageSize = queryParams.get("pageSize");
      var pageToken = queryParams.get("pageToken");
      if (pageToken != null && !pageToken.isBlank()) {
        var pageSizeInt = pageSize != null ? Integer.valueOf(pageSize) : null;
        return PageToken.build(pageToken, pageSizeInt, () -> true);
      }
      if (pageSize != null) {
        return PageToken.fromLimit(Integer.parseInt(pageSize));
      }
      return PageToken.readEverything();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      return (T)
          switch (path) {
            case "v1/namespaces" -> {
              var req = (CreateNamespaceRequest) body;
              restCompatibleCatalog.createNamespace(req.namespace(), req.properties());
              yield CreateNamespaceResponse.builder().withNamespace(req.namespace()).build();
            }
            case "v1/tables/rename" -> {
              var req = (RenameTableRequest) body;
              restCompatibleCatalog.renameTable(req.source(), req.destination());
              yield null;
            }
            case "v1/views/rename" -> {
              var req = (RenameTableRequest) body;
              restCompatibleCatalog.renameView(req.source(), req.destination());
              yield null;
            }
            default -> {
              // Update table
              var m = TABLE_PATTERN.matcher(path);
              if (m.matches()) {
                var req = (UpdateTableRequest) body;
                req = fixedUpdateTableRequest(tableIdentifierFromPath(m), req);
                try {
                  yield LoadTableResponse.builder()
                      .withTableMetadata(restCompatibleCatalog.updateTable(req, false))
                      .build();
                } catch (CommitFailedException e) {
                  throw new CommitFailedException(e, "Commit failed: %s", e.getMessage());
                }
              }

              m = VIEW_PATTERN.matcher(path);
              if (m.matches()) {
                var req = (UpdateTableRequest) body;
                req = fixedUpdateTableRequest(tableIdentifierFromPath(m), req);
                try {
                  var metadata = restCompatibleCatalog.updateView(req);
                  yield ImmutableLoadViewResponse.builder()
                      .metadata(metadata)
                      .metadataLocation(metadata.metadataFileLocation())
                      .build();
                } catch (CommitFailedException e) {
                  throw new CommitFailedException(e, "Commit failed: %s", e.getMessage());
                }
              }

              m = NAMESPACES_PROPERTIES_PATTERN.matcher(path);
              if (m.matches()) {
                var namespace = namespaceFromPath(m);
                yield restCompatibleCatalog.updateNamespaceProperties(
                    namespace, (UpdateNamespacePropertiesRequest) body);
              }

              // Create table
              m = TABLES_PATTERN.matcher(path);
              if (m.matches()) {
                var req = (CreateTableRequest) body;
                var ident = TableIdentifier.of(namespaceFromPath(m), req.name());
                yield LoadTableResponse.builder()
                    .withTableMetadata(restCompatibleCatalog.createTable(ident, req))
                    .build();
              }

              // Create view
              m = VIEWS_PATTERN.matcher(path);
              if (m.matches()) {
                var req = (CreateViewRequest) body;

                var updateTableRequest =
                    restCompatibleCatalog.updateTableRequestForNewView(namespaceFromPath(m), req);
                var meta = restCompatibleCatalog.updateView(updateTableRequest, true);

                yield ImmutableLoadViewResponse.builder()
                    .metadata(meta)
                    .metadataLocation(meta.metadataFileLocation())
                    .build();
              }

              m = REGISTER_TABLE_PATTERN.matcher(path);
              if (m.matches()) {
                var namespace = namespaceFromPath(m);
                var req = (RegisterTableRequest) body;
                var meta =
                    restCompatibleCatalog.registerTable(
                        namespace, req.name(), req.metadataLocation());
                yield LoadTableResponse.builder().withTableMetadata(meta).build();
              }

              throw new UnsupportedOperationException("POST " + path);
            }
          };
    }

    private UpdateTableRequest fixedUpdateTableRequest(
        TableIdentifier identifier, UpdateTableRequest req) {
      var newUpdates =
          req.updates().stream()
              .map(
                  u -> {
                    if (u instanceof MetadataUpdate.SetLocation setLocation
                        && setLocation.location() == null) {
                      return new MetadataUpdate.SetLocation(
                          restCompatibleCatalog.defaultWarehouseLocation(identifier));
                    }
                    return u;
                  })
              .toList();
      return UpdateTableRequest.create(identifier, req.requirements(), newUpdates);
    }

    @Override
    public <T extends RESTResponse> T postForm(
        String path,
        Map<String, String> formData,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      switch (path) {
        default -> throw new UnsupportedOperationException("POST-FORM " + path);
      }
    }

    @Override
    public void close() {}
  }
}

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

package org.apache.polaris.service.catalog.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;

/**
 * Implemented by catalogs that support the REST API "primitives".
 *
 * <p>Implementations do not need and therefore do not implement {@code
 * newTableOps(TableIdentifier)} and {@code newViewOps(TableIdentifier)} and {@link
 * #buildTable(TableIdentifier, Schema)} and {@link #buildView(TableIdentifier)}.
 */
public interface RESTCompatibleCatalog extends PolarisIcebergCatalog {

  TableMetadata registerTable(Namespace namespace, String name, String metadataLocation);

  default TableMetadata updateTable(UpdateTableRequest request) {
    return updateTable(request, false);
  }

  TableMetadata updateTable(UpdateTableRequest request, boolean directUpdateOrCreate);

  default ViewMetadata updateView(UpdateTableRequest request) {
    return updateView(request, false);
  }

  ViewMetadata updateView(UpdateTableRequest request, boolean directUpdateOrCreate);

  UpdateNamespacePropertiesResponse updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request);

  TableMetadata loadTableMetadata(TableIdentifier identifier);

  ViewMetadata loadViewMetadata(TableIdentifier identifier);

  TableMetadata createTable(TableIdentifier tableIdentifier, CreateTableRequest request);

  String defaultWarehouseLocation(TableIdentifier tableIdentifier);

  UpdateTableRequest updateTableRequestForNewView(Namespace namespace, CreateViewRequest req);

  @Override
  default ViewBuilder buildView(TableIdentifier identifier) {
    throw new UnsupportedOperationException(
        "Builders are not needed and therefore not supported for Iceberg-REST-compatible catalogs");
  }

  @Override
  default TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    throw new UnsupportedOperationException(
        "Builders are not needed and therefore not supported for Iceberg-REST-compatible catalogs");
  }

  @Override
  default Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    throw new UnsupportedOperationException(
        "Base metastore operations are not needed and therefore not supported for Iceberg-REST-compatible catalogs");
  }

  @Override
  default Table loadTable(TableIdentifier identifier) {
    throw new UnsupportedOperationException(
        "Base metastore operations are not needed and therefore not supported for Iceberg-REST-compatible catalogs");
  }

  @Override
  default View loadView(TableIdentifier identifier) {
    throw new UnsupportedOperationException(
        "Base metastore operations are not needed and therefore not supported for Iceberg-REST-compatible catalogs");
  }
}

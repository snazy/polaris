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

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;

/** Helper class for {@link PolarisIcebergCatalog} implementations keeping common state. */
public final class PolarisCatalogHelper {
  private final AtomicReference<PolarisIcebergCatalog.InitializedConfig> initializedConfig;
  private final CloseableGroup closeableGroup;
  private boolean loggedPrefixOverlapWarning;

  // Should ideally live in `InitializedConfig`, but some tests would break if it is moved there.
  private FileIO catalogFileIO;

  public PolarisCatalogHelper() {
    initializedConfig = new AtomicReference<>();
    closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);
  }

  public FileIO catalogFileIO() {
    return catalogFileIO;
  }

  public void setCatalogFileIO(FileIO catalogFileIO) {
    this.catalogFileIO = catalogFileIO;
  }

  public @Nonnull PolarisIcebergCatalog.InitializedConfig initializedConfig() {
    return requireNonNull(initializedConfig.get(), "Catalog not initialized");
  }

  public void setInitializedConfig(PolarisIcebergCatalog.InitializedConfig initializedConfig) {
    this.initializedConfig.set(initializedConfig);
  }

  public void addCloseable(AutoCloseable closeable) {
    closeableGroup.addCloseable(closeable);
  }

  public void closeCloseables() throws IOException {
    closeableGroup.close();
  }

  public boolean logPrefixOverlapWarning() {
    var c = loggedPrefixOverlapWarning;
    if (c) {
      return false;
    }
    loggedPrefixOverlapWarning = true;
    return true;
  }
}

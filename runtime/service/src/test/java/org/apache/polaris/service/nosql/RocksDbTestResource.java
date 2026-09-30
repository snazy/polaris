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
package org.apache.polaris.service.nosql;

import static java.nio.file.FileVisitResult.CONTINUE;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

public class RocksDbTestResource implements QuarkusTestResourceLifecycleManager {
  private Path databaseDirectory;

  @Override
  public Map<String, String> start() {
    try {
      databaseDirectory = Files.createTempDirectory("junit-polaris-rocksdb");
    } catch (IOException e) {
      throw new IllegalStateException("Unable to create a RocksDB test directory", e);
    }
    return Map.of(
        "polaris.persistence.type",
        "nosql",
        "polaris.persistence.auto-bootstrap-types",
        "nosql",
        "polaris.persistence.nosql.backend",
        "RocksDb",
        "polaris.backend.rocksdb.database-directory",
        databaseDirectory.toString());
  }

  @Override
  public void stop() {
    if (databaseDirectory == null) {
      return;
    }
    try {
      Files.walkFileTree(
          databaseDirectory,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                throws IOException {
              Files.delete(file);
              return CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException exception)
                throws IOException {
              Files.delete(directory);
              return CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete the RocksDB test directory", e);
    } finally {
      databaseDirectory = null;
    }
  }
}

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
package org.apache.polaris.persistence.nosql.rocksdb;

import static java.nio.file.FileVisitResult.CONTINUE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.testextension.BackendTestFactory;

public class RocksDbBackendTestFactory implements BackendTestFactory {
  public static final String NAME = RocksDbBackendFactory.NAME;
  private Path rocksDir;
  private Backend backend;

  @Override
  public Backend createNewBackend() {
    return backend;
  }

  @Override
  public void start() throws Exception {
    rocksDir = Files.createTempDirectory("junit-polaris-rocksdb");
    backend = new RocksDbBackendFactory().buildBackend(new RocksDbBackendConfig(rocksDir));
  }

  @Override
  public void stop() throws Exception {
    var b = backend;
    var dir = rocksDir;
    backend = null;
    rocksDir = null;
    try {
      if (b != null) {
        b.close();
      }
    } finally {
      if (dir != null) {
        deleteTempDir(dir);
      }
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  private static void deleteTempDir(Path dir) throws IOException {
    if (Files.notExists(dir)) {
      return;
    }

    var failures = new ArrayList<IOException>();
    Files.walkFileTree(
        dir,
        new SimpleFileVisitor<>() {

          @Nonnull
          @Override
          public FileVisitResult visitFile(
              @Nonnull Path file, @Nonnull BasicFileAttributes attributes) {
            return tryDelete(file);
          }

          @Nonnull
          @Override
          public FileVisitResult postVisitDirectory(@Nonnull Path dir, IOException exc) {
            return tryDelete(dir);
          }

          private FileVisitResult tryDelete(Path path) {
            try {
              Files.delete(path);
            } catch (NoSuchFileException ignore) {
              // pass
            } catch (IOException e) {
              failures.add(e);
            }
            return CONTINUE;
          }
        });

    if (!failures.isEmpty()) {
      var e = new IOException("Could not delete temp-directory " + dir);
      failures.forEach(e::addSuppressed);
      throw e;
    }
  }
}

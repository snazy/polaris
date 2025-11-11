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

package org.apache.polaris.storage.files.api.ms;

import java.util.stream.Stream;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileOperations;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.PurgeSpec;

/**
 * Mark-and-sweep approach to purge all stale/unreferenced files.
 *
 * <p>The overall approach is to first identify all referenced files from all tables and view (mark
 * phase) and then scan the object stores and purge the files that have not been identified as
 * "referenced" (sweep phase).
 *
 * <p>It is possible that not all unreferenced files are purged. This is because the implementation
 * uses a probabilistic data structure (bloom filter) to identify referenced files.
 *
 * <p>A typical caller performs the following steps:
 *
 * <ol>
 *   <li>Set up the {@link MarkAndSweepSpec} and call {@link #startMarkPhase(MarkAndSweepSpec)}.
 *   <li>For each table and view in the catalog, pass the result of {@link
 *       FileOperations#identifyIcebergTableFiles(String) identifyIcebergTableFiles()} (or {@link
 *       FileOperations#identifyIcebergViewFiles(String) identifyIcebergViewFiles()}) to {@link
 *       MarkPhase#mark(Stream)}.
 *   <li>Call {@link MarkPhase#finish()} to obtain the {@link MarkResult}.
 *   <li>If the {@link MarkResult} yields {@link MarkResult#canSweep()}{@code == true}, start the
 *       sweep phase by calling {@link MarkPhase#startSweepPhase(PurgeSpec)}.
 *   <li>For each object store, find all files using {@link FileOperations#findFiles(String,
 *       FileFilter)} and pass the returned stream to {@link SweepPhase#foundFiles(Stream,
 *       FileOperations)}.
 *   <li>Call {@link SweepPhase#finish()} to obtain the {@link SweepResult}.
 * </ol>
 *
 * <p>The {@link MarkAndSweepSpec} defines a bunch of constraints and parameters that influence the
 * mark and sweep phases. The {@link MarkAndSweepSpec#expectedFileCount() expectedFileCount} and the
 * {@link MarkAndSweepSpec#filterInitializedFpp() filterInitializedFpp} + {@link
 * MarkAndSweepSpec#maxAcceptableFilterFpp() maxAcceptableFilterFpp} attributes are used to
 * determine whether a sweep phase can be run or not. The default FPP values are suitable for most
 * use cases. The {@link MarkAndSweepSpec#expectedFileCount() expectedFileCount} value however must
 * be derived from the result of mark phase run by applying {@link
 * MarkAndSweepSpec#applyMarkResult(MarkResult)}, which will adopt the {@link
 * MarkAndSweepSpec#expectedFileCount() expectedFileCount} using the formula {@link
 * MarkResult#identifiedFiles() identifiedFiles}{@code * }{@link
 * MarkAndSweepSpec#countFromLastRunMultiplier() countFromLastRunMultiplier}.
 *
 * <p>Note that the sweep-phase <em>will</em> delete all files that are not marked as referenced
 * during the mark-phase. It is important to understand that this will delete all "additional" data.
 * To prevent the sweep-phase from deleting "additional" data, use the {@link FileFilter} parameter
 * of {@link SweepPhase#foundFiles(Stream, FileOperations)}.
 */
public interface MarkAndSweep {
  MarkPhase startMarkPhase(MarkAndSweepSpec spec);

  interface MarkPhase {
    /**
     * Marks all {@link {@link FileSpec#location() locations} of the elements from the given stream, the {@link Stream} will be fully consumed.
     *
     * <p>It is safe to call this function concurrently.
     * @throws IllegalStateException if the mark phase has already been {@link #finish() finished).
     */
    void mark(Stream<FileSpec> files);

    /**
     * Finishes the mark phase and returns the result.
     *
     * @throws IllegalStateException if this phase has already been finished.
     */
    MarkResult finish();

    /**
     * Returns a {@link SweepPhase} instance using the state of this mark phase.
     *
     * <p>This function should only be called once.
     *
     * @throws MarkPhaseOverflowException if the mark phase has detected overflow, if an overflow of
     *     the constraints for the internal probabilistic data structures has been detected (see
     *     {@link MarkResult#canSweep()}).
     */
    SweepPhase startSweepPhase(PurgeSpec purgeSpec) throws MarkPhaseOverflowException;
  }

  interface SweepPhase {
    /**
     * Inform the {@link SweepPhase} about files that are present in the object store.
     *
     * <p>Only files that have not been marked during the {@link MarkPhase} will be purged. However,
     * the opposite is not guaranteed to be true, which means that unmarked files may not be deleted
     * due to a false-positive hit in the backing probabilistic data structure.
     *
     * @param files files found by the given {@link FileOperations} instance and no other instance,
     *     the {@link Stream} will be fully consumed.
     * @param fromOperations the {@link FileOperations} instance that was used to find the files, it
     *     will be used to purge files.
     * @throws IllegalStateException if the sweep phase has already been {@link #finish() finished).
     */
    void foundFiles(Stream<FileSpec> files, FileOperations fromOperations);

    /**
     * Finishes the sweep phase and returns the result.
     *
     * @throws IllegalStateException if this phase has already been finished.
     */
    SweepResult finish();
  }

  /**
   * Indicates that the mark phase has detected overflow, likely because of too many identified
   * files exceeding the defined {@link MarkAndSweepSpec specification}.
   */
  class MarkPhaseOverflowException extends RuntimeException {
    public MarkPhaseOverflowException(String message) {
      super(message);
    }
  }
}

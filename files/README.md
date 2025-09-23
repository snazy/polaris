<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Polaris object storage operations

API and implementations to perform long-running operations against object storages, mostly to purge files.

Functionalities to scan an object storage and to purge files are separated. Filter mechanisms are used to
select the files to be deleted (purged).

There are implementations to identify the files references by a particular table or view metadata, including
statistics files, manifest lists of all snapshots, the manifest files and the data/delete files.

The implementation performs no effort to identify duplicates during the identification of files referenced by
a table or view metadata.
This means that, for example, a data file referenced in multiple manifest files will be returned twice.

Purge operations are performed in one or more bulk delete operations. The implementation takes care of not including
the same file more than once in a single bulk delete operation.

One alternative implementation purges all files within the base location of a table or view metadata.

All implemented operations are resilient against failures as those are expected to be run as maintenance
operations or as part of such.

The operations are implemented to continue in case of errors and eventually succeed instead of failing eagerly.
Maintenance operations are usually not actively observed, and manually fixing consistency issues in object
stores is not a straightforward task for users.

# Mark+sweep implementation

Identify all table/view metadata, memoize all files referenced by those, likely in a probabilistic data structure
like a Bloom filter.
A second step scans all object storages and purges all files that are not referenced by any table/view metadata,
more precisely: not included in the probabilistic data structure (bloom filter).
A grace-time with a reasonable default value would be used to avoid deleting files that have been created after
the identify-step started.

A mark-sweep implementation would be run regularly as a scheduled maintenance operation.
It has to be resilient against bloom filter false positives, likely to skip the purge step and to yield information
about a better size of the bloom filter to be used in the next run.
This "bloom filter auto-tuning" should be automatic and not require manual intervention.

The number of files to purge and the number of batch deletes can both be rate-limited.

# Potential future enhancements

The operations provided by `FileOperations` and `MarkAndSweep` are meant for maintenance operations, which are not
time- or performance-critical.
It is more important that the operations are resilient against failures, do not add unnecessary CPU or heap pressure
and eventually succeed.
Further, maintenance operations should not eat up too much I/O bandwidth to not interfere with other user-facing
operations.

Depending on the overall load of the system, it might be worth running some operations in parallel.
For examples, call sites of `MarkAndSweep` could mark referenced files for multiple tables or views in parallel.

Contrary, implementations may limit the load against the system when scanning the whole catalog to avoid excessive
system pressure.

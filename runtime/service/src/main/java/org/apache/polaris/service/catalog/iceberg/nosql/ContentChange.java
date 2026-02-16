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

package org.apache.polaris.service.catalog.iceberg.nosql;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;
import org.apache.polaris.persistence.nosql.metastore.ResolvedPath;
import org.immutables.value.Value;

/** Describe an individual change as part of a commit operation. */
@PolarisImmutable
interface ContentChange {
  Target target();

  Operation operation();

  @Value.Derived
  default ContentIdentifier identifier() {
    return ContentIdentifier.identifier(
        tableIdentifier().namespace().levels(), tableIdentifier().name());
  }

  TableIdentifier tableIdentifier();

  List<UpdateRequirement> requirements();

  List<MetadataUpdate> updates();

  static ImmutableContentChange.Builder builder() {
    return ImmutableContentChange.builder();
  }

  @PolarisImmutable
  interface CommittedContentChange {
    @SuppressWarnings("ClassEscapesDefinedScope") // interface-->public
    ContentChange change();

    /** The base ("current") metadata object, empty for creating operations. */
    Optional<Object> base();

    /**
     * The updated metadata object, present if the {@link Target} uses/supports metadata and the
     * change operation produces a new metadata object.
     */
    Optional<Object> metadata();

    default Object requiredMetadata() {
      return metadata()
          .orElseThrow(() -> new IllegalStateException("Metadata is required but not present"));
    }

    ResolvedPath resolvedPath();

    static Builder builder() {
      return ImmutableCommittedContentChange.builder();
    }

    interface Builder {
      @CanIgnoreReturnValue
      @SuppressWarnings("ClassEscapesDefinedScope") // interface-->public
      Builder change(ContentChange change);

      @CanIgnoreReturnValue
      Builder base(Object base);

      @CanIgnoreReturnValue
      Builder base(Optional<?> base);

      @CanIgnoreReturnValue
      Builder metadata(Object metadata);

      @CanIgnoreReturnValue
      Builder resolvedPath(ResolvedPath resolvedPath);

      CommittedContentChange build();
    }
  }

  static ContentChange fromUpdateTableRequest(
      Target target, UpdateTableRequest updateTableRequest) {
    if (updateTableRequest.identifier().namespace().isEmpty()) {
      throw new NoSuchNamespaceException("Empty namespace is not allowed");
    }
    var isCreate =
        updateTableRequest.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
    return ContentChange.builder()
        .target(target)
        .operation(isCreate ? Operation.CREATE : Operation.UPDATE)
        .tableIdentifier(updateTableRequest.identifier())
        .updates(updateTableRequest.updates())
        .requirements(updateTableRequest.requirements())
        .build();
  }

  enum Operation {
    CREATE,
    UPDATE,
    // DROP + RENAME are not handled here yet.
    // Should add DROP later though to make it part of an atomic change.
    // Should add RENAME later though to make it part of an atomic change.
  }

  // TODO make this more generic to handle other types, for example namespaces, as well
  enum Target {
    TABLE(IcebergTableObj.TYPE),
    VIEW(IcebergViewObj.TYPE),
    ;

    final ObjType objType;

    Target(ObjType objType) {
      this.objType = objType;
    }
  }
}

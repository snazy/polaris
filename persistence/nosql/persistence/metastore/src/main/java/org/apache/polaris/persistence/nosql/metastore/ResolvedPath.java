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

package org.apache.polaris.persistence.nosql.metastore;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.commit.CommitterState;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.content.ContentObj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;

/**
 * Represents the path of an object within a catalog including all intermediate {@link NamespaceObj
 * namespace} object instances.
 *
 * @param namespaceElements list of intermediate namespace objects, empty for the "root" level.
 * @param leafObj the leaf object, which can be {@code null}, if the leaf does not exist.
 */
public record ResolvedPath(List<NamespaceObj> namespaceElements, Optional<ContentObj> leafObj) {

  public @Nullable ContentObj leafObjOrNull() {
    return leafObj.orElse(null);
  }

  public <T extends Obj> Optional<T> leafObjAs(Class<T> clazz) {
    return leafObj.filter(clazz::isInstance).map(clazz::cast);
  }

  public boolean leafObjIs(ObjType type) {
    return leafObj.map(ContentObj::type).map(type::equals).orElse(false);
  }

  /**
   * Resolves the path to the entity referenced by the given {@link ContentIdentifier path} using
   * the given index and persistence instance.
   *
   * <p>From with a committing function, call sites must pass the {@link Persistence} instance from
   * {@link CommitterState#persistence()}.
   *
   * <p>When resolving multiple {@link ContentIdentifier}s, use {@link #newPathResolutionContext()}.
   */
  public static Optional<ResolvedPath> resolvePathWithOptionalLeaf(
      Persistence persistence, Index<ObjRef> byName, ContentIdentifier contentIdentifier) {
    return newPathResolutionContext().resolvePath(persistence, byName, contentIdentifier);
  }

  public static Map<ContentIdentifier, Optional<ResolvedPath>> resolvePathsWithOptionalLeaf(
      Persistence persistence, Index<ObjRef> byName, Stream<ContentIdentifier> contentIdentifiers) {
    var result = new HashMap<ContentIdentifier, Optional<ResolvedPath>>();
    // TODO batch persistence.fetchMany()
    contentIdentifiers.forEach(
        contentIdentifier ->
            result.put(
                contentIdentifier,
                resolvePathWithOptionalLeaf(persistence, byName, contentIdentifier)));
    return result;
  }

  public static PathResolutionContext newPathResolutionContext() {
    return new PathResolutionContext();
  }

  public static class PathResolutionContext {
    private final Map<ObjRef, ContentObj> memoized = new HashMap<>();

    PathResolutionContext() {}

    Optional<ResolvedPath> resolvePath(
        Persistence persistence, Index<ObjRef> byName, ContentIdentifier contentIdentifier) {
      if (contentIdentifier.isEmpty()) {
        return Optional.empty();
      }
      var objRefs = identifierToObjRefs(byName, contentIdentifier);
      var fetched = memoizedFetch(persistence, objRefs);

      var namespaceElements = new ArrayList<NamespaceObj>(fetched.length - 1);
      for (int n = fetched.length - 1; n >= 1; n--) {
        var element = fetched[n];
        if (element instanceof NamespaceObj namespaceObj) {
          namespaceElements.add(namespaceObj);
        } else {
          return Optional.empty();
        }
      }
      // leaf first
      var leaf = fetched[0];
      return Optional.of(new ResolvedPath(namespaceElements, Optional.ofNullable(leaf)));
    }

    private ContentObj[] memoizedFetch(Persistence persistence, ObjRef[] objRefs) {
      var toFetch = (ObjRef[]) null;
      var result = new ContentObj[objRefs.length];
      for (int i = 0; i < objRefs.length; i++) {
        var present = memoized.get(objRefs[i]);
        if (present == null) {
          if (toFetch == null) {
            toFetch = new ObjRef[objRefs.length];
          }
          toFetch[i] = objRefs[i];
        } else {
          result[i] = present;
        }
      }
      fetchMissing(persistence, toFetch, result);
      return result;
    }

    private void fetchMissing(Persistence persistence, ObjRef[] toFetch, ContentObj[] result) {
      if (toFetch == null) {
        return;
      }
      var fetched = persistence.fetchMany(ContentObj.class, toFetch);
      for (int i = 0; i < fetched.length; i++) {
        var fetchedObj = fetched[i];
        if (fetchedObj != null) {
          memoized.put(toFetch[i], fetchedObj);
          result[i] = fetchedObj;
        }
      }
    }

    private static ObjRef[] identifierToObjRefs(
        Index<ObjRef> byName, ContentIdentifier contentIdentifier) {
      var objRefs = new ObjRef[contentIdentifier.length()];
      var i = contentIdentifier;
      for (var n = 0; !i.isEmpty(); i = i.parent(), n++) {
        var ref = byName.get(i.toIndexKey());
        // ref==null is safe
        objRefs[n] = ref;
      }
      return objRefs;
    }
  }
}

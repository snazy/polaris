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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION;
import static org.apache.polaris.core.config.FeatureConfiguration.DROP_WITH_PURGE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.azure.core.exception.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.catalog.Profiles;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.apache.polaris.service.exception.FakeAzureHttpResponse;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.test.TestData;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.assertj.core.configuration.PreferredAssumptionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.objectstoragemock.Bucket;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.objectstoragemock.sts.ImmutableAssumeRoleResult;
import org.projectnessie.objectstoragemock.sts.ImmutableCredentials;
import org.projectnessie.objectstoragemock.sts.ImmutableRoleUser;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.RetryableException;

@SuppressWarnings("resource")
public abstract class AbstractIcebergCatalogTestBase extends CatalogTests<RESTCatalog> {

  protected static final String S3_PREFIX = "s3://my-bucket/";
  protected static final String DEFAULT_BASE_STORAGE_LOCATION = S3_PREFIX + "path/to/data";
  protected static final String CLIENT_ID = "clientId";
  protected static final String CLIENT_SECRET = "secret";

  static {
    org.assertj.core.api.Assumptions.setPreferredAssumptionException(
        PreferredAssumptionException.JUNIT5);
  }

  protected DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ALLOW_TABLE_LOCATION_OVERLAP\"", "true")
          .put("polaris.features.\"LIST_PAGINATION_ENABLED\"", "true")
          .put("polaris.behavior-changes.\"ALLOW_NAMESPACE_CUSTOM_LOCATION\"", "true")
          // Give Polaris some arbitrary S3 credentials
          .put("polaris.storage.aws.access-key", "access-key")
          .put("polaris.storage.aws." + CLIENT_SECRET + "-key", CLIENT_SECRET + "-access-key")
          .put("polaris.realm-context.type", "test")
          .build();
    }
  }

  public static final String CATALOG_NAME = "polaris-catalog";

  protected static String NS_ENCODED = RESTUtil.encodeNamespace(NS);

  public static Map<String, String> TABLE_PREFIXES =
      Map.of(
          CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
          "catalog-default-key1",
          CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
          "catalog-default-key2",
          CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
          "catalog-default-key3",
          CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
          "catalog-override-key3",
          CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
          "catalog-override-key4");

  protected @Inject StorageCredentialCache storageCredentialCache;
  protected @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  protected @Inject PolarisMetaStoreManager metaStoreManager;
  protected @Inject PolarisAdminService adminService;
  protected @Inject PolarisEventListener polarisEventListener;
  protected @Inject ServiceIdentityProvider serviceIdentityProvider;
  protected @Inject RealmConfig realmConfig;
  protected @Inject CallContext callContext;
  protected @Inject CatalogPrefixParser catalogPrefixParser;
  protected @Inject RealmContext realmContext;
  protected @Inject RealmContextHolder realmContextHolder;

  private @AutoClose RESTCatalog catalog;
  protected String realmName;
  protected TestPolarisEventListener testPolarisEventListener;
  protected SecurityContext securityContext;
  protected PolarisPrincipal authenticatedRootPrincipal;

  /** Properties for all catalogs created via {@link #initCatalog(String, Map)}. */
  private final Map<String, String> additionalCatalogProperties = new HashMap<>();

  private final Set<String> createdPolarisCatalogs = new HashSet<>();

  protected static ObjectStorageMock objectStorageMock;
  protected static HeapStorageBucket heapStorageBucket;
  protected static ObjectStorageMock.MockServer objectStorageMockServer;

  @BeforeAll
  public static void setUpMocks() {
    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    objectStorageMock =
        ObjectStorageMock.builder()
            .putBuckets("my-bucket", HeapStorageBucket.newHeapStorageBucket().bucket())
            .assumeRoleHandler(
                (action,
                    version,
                    roleArn,
                    roleSessionName,
                    policy,
                    durationSeconds,
                    externalId,
                    serialNumber) ->
                    ImmutableAssumeRoleResult.builder()
                        .credentials(
                            ImmutableCredentials.builder()
                                .accessKeyId("access-key")
                                .secretAccessKey(CLIENT_SECRET + "-access-key")
                                .sessionToken("foo-token")
                                .expiration(Instant.now().plus(15, ChronoUnit.MINUTES))
                                .build())
                        .sourceIdentity("source-identity")
                        .assumedRoleUser(
                            ImmutableRoleUser.builder()
                                .arn("arn")
                                .assumedRoleId("assumedRoleId")
                                .build())
                        .build())
            .build();
    objectStorageMockServer = objectStorageMock.start();
  }

  @AfterAll
  public static void tearDownMocks() throws Exception {
    if (objectStorageMockServer != null) {
      objectStorageMockServer.close();
    }
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    heapStorageBucket.clear();

    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    metaStoreManagerFactory.bootstrapRealms(
        List.of(realmName),
        RootCredentialsSet.fromString(realmName + "," + CLIENT_ID + "," + CLIENT_SECRET));

    realmContextHolder.set(() -> realmName);

    var rootPrincipal =
        metaStoreManager.findRootPrincipal(callContext.getPolarisCallContext()).orElseThrow();
    authenticatedRootPrincipal = PolarisPrincipal.of(rootPrincipal, Set.of());
    QuarkusMock.installMockForType(authenticatedRootPrincipal, PolarisPrincipal.class);

    securityContext =
        new SecurityContext() {
          @Override
          public String getAuthenticationScheme() {
            throw new UnsupportedOperationException(
                AbstractIcebergCatalogTestBase.class.getSimpleName()
                    + ".SecurityContext.getAuthenticationScheme() is not implemented");
          }

          @Override
          public boolean isSecure() {
            throw new UnsupportedOperationException(
                AbstractIcebergCatalogTestBase.class.getSimpleName()
                    + ".SecurityContext.isSecure() is not implemented");
          }

          @Override
          public boolean isUserInRole(String role) {
            throw new UnsupportedOperationException(
                AbstractIcebergCatalogTestBase.class.getSimpleName()
                    + ".SecurityContext.isUserInRole() is not implemented");
          }

          @Override
          public Principal getUserPrincipal() {
            return authenticatedRootPrincipal;
          }
        };

    additionalCatalogProperties.clear();
    additionalCatalogProperties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
    // For the DefaultRealmContextResolver
    additionalCatalogProperties.put("header.Polaris-Realm", realmName);
    // For the TestRealmContextResolver
    additionalCatalogProperties.put("header.realm", realmName);

    testPolarisEventListener = (TestPolarisEventListener) polarisEventListener;
    testPolarisEventListener.clear();
  }

  @AfterEach
  public void after() throws IOException {
    metaStoreManagerFactory.purgeRealms(List.of(realmName));
  }

  @SuppressWarnings("SameParameterValue")
  protected void additionalCatalogProperty(String name, String value) {
    additionalCatalogProperties.put(name, value);
  }

  @Override
  protected RESTCatalog catalog() {
    synchronized (this) {
      if (catalog == null) {
        catalog = initCatalog(CATALOG_NAME, Map.of());
      }
      return catalog;
    }
  }

  @Override
  protected final RESTCatalog initCatalog(String catalogName, Map<String, String> properties) {
    try {
      if (catalog != null) {
        catalog.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      catalog = null;
    }

    createPolarisCatalog(catalogName);

    var effectiveProperties =
        ImmutableMap.<String, String>builder()
            .putAll(TABLE_PREFIXES)
            .putAll(additionalCatalogProperties)
            .putAll(properties)
            .put("prefix", catalogPrefixParser.catalogNameToPrefix(catalogName))
            .buildKeepingLast();
    var catalog = createCatalog(catalogName, effectiveProperties);
    this.catalog = catalog;
    return catalog;
  }

  protected void createPolarisCatalog(String catalogName) {
    if (createdPolarisCatalogs.add(catalogName)) {
      var storageLocation = DEFAULT_BASE_STORAGE_LOCATION;
      var storageConfigModel =
          AwsStorageConfigInfo.builder()
              .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
              .setExternalId("externalId")
              .setUserArn("aws::a:user:arn")
              .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
              .setAllowedLocations(List.of(storageLocation, S3_PREFIX))
              .setEndpoint(objectStorageMockServer.getS3BaseUri().toString())
              .setStsEndpoint(objectStorageMockServer.getStsEndpointURI().toString())
              .setPathStyleAccess(true)
              .build();
      adminService.createCatalog(
          new CreateCatalogRequest(
              new CatalogEntity.Builder()
                  .setName(catalogName)
                  .setDefaultBaseLocation(storageLocation)
                  .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                  .addProperty(ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                  .addProperty(ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
                  .addProperty(DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
                  .setStorageConfigurationInfo(realmConfig, storageConfigModel, storageLocation)
                  .build()
                  .asCatalog(serviceIdentityProvider)));
    }
  }

  protected abstract RESTCatalog createCatalog(String catalogName, Map<String, String> properties);

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }

  protected boolean supportsNotifications() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected String baseTableLocation(TableIdentifier identifier) {
    return DEFAULT_BASE_STORAGE_LOCATION + "/" + identifier.namespace() + "/" + identifier.name();
  }

  @Test
  public void testRenameTableMissingDestinationNamespace() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");

    var catalog = catalog();
    catalog.createNamespace(NS);

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();

    Namespace newNamespace = Namespace.of("nonexistent_namespace");
    TableIdentifier renamedTable = TableIdentifier.of(newNamespace, "table_renamed");

    Assertions.assertThat(catalog.namespaceExists(newNamespace))
        .as("Destination namespace should not exist before rename")
        .isFalse();

    Assertions.assertThat(catalog.tableExists(renamedTable))
        .as("Destination table should not exist before rename")
        .isFalse();

    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, renamedTable))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");

    Assertions.assertThat(catalog.namespaceExists(newNamespace))
        .as("Destination namespace should not exist after failed rename")
        .isFalse();

    Assertions.assertThat(catalog.tableExists(renamedTable))
        .as("Table should not exist after failed rename")
        .isFalse();
  }

  @Test
  public void testCreateNestedNamespaceUnderMissingParent() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");

    var catalog = catalog();

    Namespace child1 = Namespace.of("parent", "child1");

    Assertions.assertThatThrownBy(() -> catalog.createNamespace(child1))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Parent");
  }

  @Test
  public void testConcurrentWritesWithRollbackNonEmptyTable() {
    var catalog = catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    // set the snapshot property in the summary to make this snapshot
    // rollback-able.
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    try {
      // Now call IRC server to commit delete operation.
      CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
      catalogHandlerUtils.commit(((BaseTable) catalog.loadTable(TABLE)).operations(), request);
    } catch (Exception e) {
      fail("Rollback Compaction on conflict feature failed", e);
      // just make the IDE happy (no warning)
      throw e;
    }

    table.refresh();

    // Assert only 2 snapshots and no snapshot of REPLACE left.
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      // no snapshot in the hierarchy for REPLACE operations
      assertThat(currentSnapshot.operation()).isNotEqualTo(DataOperations.REPLACE);
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);

    // Inspect the files 1 DELETE file i.e. FILE_A_DELETES and 1 DATA FILE FILE_A
    try {
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        List<CharSequence> dataFilePaths =
            Streams.stream(tasks)
                .map(ContentScanTask::file)
                .map(ContentFile::location)
                .collect(Collectors.toList());
        List<CharSequence> deleteFilePaths =
            Streams.stream(tasks)
                .flatMap(t -> t.deletes().stream().map(ContentFile::location))
                .collect(Collectors.toList());
        Assertions.assertThat(dataFilePaths)
            .as("Should contain expected number of data files", new Object[0])
            .hasSize(1);
        Assertions.assertThat(deleteFilePaths)
            .as("Should contain expected number of delete files", new Object[0])
            .hasSize(1);

        Assertions.assertThat(CharSequenceSet.of(dataFilePaths))
            .as("Should contain correct file paths", new Object[0])
            .isEqualTo(
                CharSequenceSet.of(Iterables.transform(List.of(FILE_A), ContentFile::location)));
        Assertions.assertThat(CharSequenceSet.of(deleteFilePaths))
            .as("Should contain correct file paths", new Object[0])
            .isEqualTo(
                CharSequenceSet.of(
                    Iterables.transform(
                        Collections.singletonList(FILE_A_DELETES), ContentFile::location)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testConcurrentWritesWithRollbackWithNonReplaceSnapshotInBetween() {
    var catalog = catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    // commit the transaction.
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    // commit FILE_C
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(3);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B, FILE_C);
  }

  @Test
  public void
      testConcurrentWritesWithRollbackEnableWithToRollbackSnapshotReferencedByOtherBranch() {
    var catalog = catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    Table t = catalog.loadTable(TABLE);
    // add another branch B
    t.manageSnapshots()
        .createBranch("non-main")
        .setCurrentSnapshot(t.currentSnapshot().snapshotId())
        .commit();
    // now add more files to non-main branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).toBranch("non-main").commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B);
  }

  @Test
  public void testConcurrentWritesWithRollbackWithConcurrentWritesToDifferentBranches() {
    var catalog = catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A to main branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    Table t = catalog.loadTable(TABLE);
    // add another branch B
    t.manageSnapshots()
        .createBranch("non-main")
        .setCurrentSnapshot(t.currentSnapshot().snapshotId())
        .commit();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B on main branch
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    // now add more files to non-main branch, this will make sequence number non monotonic for main
    // branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).toBranch("non-main").commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B);
  }

  @Test
  public void testValidateNotificationWhenTableAndNamespacesDontExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = S3_PREFIX + "validate_table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    var catalog = catalog();

    Assumptions.assumeTrue(catalog instanceof SupportsNotifications);
    SupportsNotifications supportsNotifications = (SupportsNotifications) catalog;

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    // For a VALIDATE request we can pass in a full metadata JSON filename or just the table's
    // metadata directory; either way the path will be validated to be under the allowed locations,
    // but any actual metadata JSON file will not be accessed.
    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    // We should be able to send the notification without creating the metadata file since it's
    // only validating the ability to send the CREATE/UPDATE notification possibly before actually
    // creating the table at all on the remote catalog.
    Assertions.assertThat(supportsNotifications.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should not be created")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should not be created for a VALIDATE notification")
        .isFalse();

    // Now also check that despite creating the metadata file, the validation call still doesn't
    // create any namespaces or tables.
    heapStorageBucket
        .bucket()
        .updater()
        .update(tableMetadataLocation.substring(S3_PREFIX.length()), Bucket.UpdaterMode.CREATE_NEW)
        .append(
            0L,
            new ByteArrayInputStream(
                TableMetadataParser.toJson(createSampleTableMetadata(tableLocation))
                    .getBytes(UTF_8)))
        .commit();

    Assertions.assertThat(supportsNotifications.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should not be created")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should not be created for a VALIDATE notification")
        .isFalse();
  }

  @Test
  @Override
  public void testDropTableWithPurge() {
    super.testDropTableWithPurge();
    // if (this.requiresNamespaceCreate()) {
    //  catalog.createNamespace(NS);
    // }
    //
    // Assertions.assertThatPredicate(catalog::tableExists)
    //    .as("Table should not exist before create")
    //    .rejects(TABLE);
    //
    // Table table = catalog.buildTable(TABLE, SCHEMA).create();
    // Assertions.assertThatPredicate(catalog::tableExists)
    //    .as("Table should exist after create")
    //    .accepts(TABLE);
    // Assertions.assertThat(table).isInstanceOf(BaseTable.class);
    // TableMetadata tableMetadata = ((BaseTable) table).operations().current();
    //
    // boolean dropped = catalog.dropTable(TABLE, true);
    // Assertions.assertThat(dropped)
    //    .as("Should drop a table that does exist", new Object[0])
    //    .isTrue();
    // Assertions.assertThatPredicate(catalog::tableExists)
    //    .as("Table should not exist after drop")
    //    .rejects(TABLE);
    // List<PolarisBaseEntity> tasks =
    //    metaStoreManager
    //        .loadTasks(callContext, "testExecutor", PageToken.fromLimit(1))
    //        .getEntities();
    // Assertions.assertThat(tasks).hasSize(1);
    // TaskEntity taskEntity = TaskEntity.of(tasks.get(0));
    // Map<String, String> credentials =
    //    metaStoreManager
    //        .getSubscopedCredsForEntity(
    //            callContext,
    //            0,
    //            taskEntity.getId(),
    //            taskEntity.getType(),
    //            true,
    //            Set.of(tableMetadata.location()),
    //            Set.of(tableMetadata.location()),
    //            Optional.empty())
    //        .getStorageAccessConfig()
    //        .credentials();
    // Assertions.assertThat(credentials)
    //    .isNotNull()
    //    .isNotEmpty()
    //    .containsEntry(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), TEST_ACCESS_KEY)
    //    .containsEntry(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), SECRET_ACCESS_KEY)
    //    .containsEntry(StorageAccessProperty.AWS_TOKEN.getPropertyName(), SESSION_TOKEN);
    // FileIO fileIO = taskFileIOSupplier.apply(taskEntity, TABLE);
    // Assertions.assertThat(fileIO).isNotNull().isInstanceOf(ExceptionMappingFileIO.class);
    // Assertions.assertThat(((ExceptionMappingFileIO) fileIO).getInnerIo())
    //    .isInstanceOf(InMemoryFileIO.class);
    // }
    //
    // @Test
    // public void testDropTableWithPurgeDisabled() {
    //// Create a catalog with purge disabled:
    // String noPurgeCatalogName = CATALOG_NAME + "_no_purge";
    // String storageLocation = "s3://testDropTableWithPurgeDisabled/data";
    // AwsStorageConfigInfo noPurgeStorageConfigModel =
    //    AwsStorageConfigInfo.builder()
    //        .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
    //        .setExternalId("externalId")
    //        .setUserArn("aws::a:user:arn")
    //        .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
    //        .build();
    // adminService.createCatalog(
    //    new CreateCatalogRequest(
    //        new CatalogEntity.Builder()
    //            .setName(noPurgeCatalogName)
    //            .setDefaultBaseLocation(storageLocation)
    //            .setReplaceNewLocationPrefixWithCatalogDefault("file:")
    //            .addProperty(
    //                FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
    //            .addProperty(
    //                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
    // "true")
    //            .addProperty(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(),
    // "false")
    //            .setStorageConfigurationInfo(
    //                realmConfig, noPurgeStorageConfigModel, storageLocation)
    //            .build()
    //            .asCatalog(serviceIdentityProvider)));
    // IcebergCatalog noPurgeCatalog =
    //    newIcebergCatalog(noPurgeCatalogName, metaStoreManager, fileIOFactory);
    // noPurgeCatalog.initialize(
    //    noPurgeCatalogName,
    //    ImmutableMap.of(
    //        CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    //
    // if (this.requiresNamespaceCreate()) {
    //  noPurgeCatalog.createNamespace(NS);
    // }
    //
    // Assertions.assertThatPredicate(noPurgeCatalog::tableExists)
    //    .as("Table should not exist before create")
    //    .rejects(TABLE);
    //
    // Table table = noPurgeCatalog.buildTable(TABLE, SCHEMA).create();
    // Assertions.assertThatPredicate(noPurgeCatalog::tableExists)
    //    .as("Table should exist after create")
    //    .accepts(TABLE);
    // Assertions.assertThat(table).isInstanceOf(BaseTable.class);
    //
    //// Attempt to drop the table:
    // Assertions.assertThatThrownBy(() -> noPurgeCatalog.dropTable(TABLE, true))
    //    .isInstanceOf(ForbiddenException.class)
    //    .hasMessageContaining(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.key());
  }

  private TableMetadata createSampleTableMetadata(String tableLocation) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get()));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("intType").withSpecId(1000).build();

    return TableMetadata.newTableMetadata(schema, partitionSpec, tableLocation, ImmutableMap.of());
  }

  @ParameterizedTest
  @MethodSource
  public void testRetriableException(Exception exception, boolean shouldRetry) {
    Assertions.assertThat(IcebergCatalog.SHOULD_RETRY_REFRESH_PREDICATE.test(exception))
        .isEqualTo(shouldRetry);
  }

  static Stream<Arguments> testRetriableException() {
    Set<Integer> NON_RETRYABLE_CODES = Set.of(401, 403, 404);
    Set<Integer> RETRYABLE_CODES = Set.of(408, 504);

    // Create a map of HTTP code returned from a cloud provider to whether it should be retried
    Map<Integer, Boolean> cloudCodeMappings = new HashMap<>();
    NON_RETRYABLE_CODES.forEach(code -> cloudCodeMappings.put(code, false));
    RETRYABLE_CODES.forEach(code -> cloudCodeMappings.put(code, true));

    return Stream.of(
            Stream.of(
                Arguments.of(new RuntimeException(new IOException("Connection reset")), true),
                Arguments.of(RetryableException.builder().build(), true),
                Arguments.of(NonRetryableException.builder().build(), false)),
            IcebergExceptionMapper.getAccessDeniedHints().stream()
                .map(hint -> Arguments.of(new RuntimeException(hint), false)),
            cloudCodeMappings.entrySet().stream()
                .flatMap(
                    entry ->
                        Stream.of(
                            Arguments.of(
                                new HttpResponseException(
                                    "", new FakeAzureHttpResponse(entry.getKey()), ""),
                                entry.getValue()),
                            Arguments.of(
                                new StorageException(entry.getKey(), ""), entry.getValue()))),
            IcebergExceptionMapper.RETRYABLE_AZURE_HTTP_CODES.stream()
                .map(
                    code ->
                        Arguments.of(
                            new HttpResponseException("", new FakeAzureHttpResponse(code), ""),
                            true)))
        .flatMap(Function.identity());
  }

  @Test
  public void testRegisterTableWithSlashlessMetadataLocation() {
    var catalog = catalog();
    Assertions.assertThatThrownBy(
            () -> catalog.registerTable(TABLE, "metadata_location_without_slashes"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid metadata file location");
  }

  @Test
  public void createCatalogWithReservedProperty() {
    Assertions.assertThatCode(
            () -> {
              adminService.createCatalog(
                  new CreateCatalogRequest(
                      new CatalogEntity.Builder()
                          .setDefaultBaseLocation("file://")
                          .setName("createCatalogWithReservedProperty")
                          .setProperties(ImmutableMap.of("polaris.reserved", "true"))
                          .build()
                          .asCatalog(serviceIdentityProvider)));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
  }

  @Test
  public void updateCatalogWithReservedProperty() {
    adminService.createCatalog(
        new CreateCatalogRequest(
            new CatalogEntity.Builder()
                .setDefaultBaseLocation("file://")
                .setName("updateCatalogWithReservedProperty")
                .setProperties(ImmutableMap.of("a", "b"))
                .build()
                .asCatalog(serviceIdentityProvider)));
    Assertions.assertThatCode(
            () -> {
              adminService.updateCatalog(
                  "updateCatalogWithReservedProperty",
                  UpdateCatalogRequest.builder()
                      .setCurrentEntityVersion(1)
                      .setProperties(ImmutableMap.of("polaris.reserved", "true"))
                      .build());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
    adminService.deleteCatalog("updateCatalogWithReservedProperty");
  }

  @Test
  public void testEventsAreEmitted() {
    var catalog = catalog();
    catalog.createNamespace(TestData.NAMESPACE);
    Table table = catalog.buildTable(TestData.TABLE, TestData.SCHEMA).create();

    String key = "foo";
    String valOld = "bar1";
    String valNew = "bar2";
    table.updateProperties().set(key, valOld).commit();
    table.updateProperties().set(key, valNew).commit();

    var beforeRefreshEvent =
        testPolarisEventListener.getLatest(PolarisEventType.BEFORE_REFRESH_TABLE);
    Assertions.assertThat(
            beforeRefreshEvent.attributes().getRequired(EventAttributes.TABLE_IDENTIFIER))
        .isEqualTo(TestData.TABLE);

    var afterRefreshEvent =
        testPolarisEventListener.getLatest(PolarisEventType.AFTER_REFRESH_TABLE);
    Assertions.assertThat(
            afterRefreshEvent.attributes().getRequired(EventAttributes.TABLE_IDENTIFIER))
        .isEqualTo(TestData.TABLE);
  }
}

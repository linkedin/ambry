/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Unit tests for {@link Account}, {@link Container}, {@link AccountBuilder}, and {@link ContainerBuilder}.
 */
@RunWith(Parameterized.class)
public class AccountContainerTest {
  private static final Random random = new Random();
  private static final int CONTAINER_COUNT = 10;
  private static final int DATASET_COUNT = 5;
  private static final short LATEST_CONTAINER_JSON_VERSION = Container.JSON_VERSION_2;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // Reference Account fields
  private final short refAccountId;
  private final String refAccountName;
  private final AccountStatus refAccountStatus;
  private final QuotaResourceType refQuotaResourceType;
  private final int refAccountSnapshotVersion;
  private final Account refAccount;
  private final boolean refAccountAclInheritedByContainer;

  // Reference Container fields
  private List<Container> refContainers;

  private List<Short> refContainerIds;
  private List<String> refContainerNames;
  private List<String> refContainerDescriptions;
  private List<ContainerStatus> refContainerStatuses;
  private List<Boolean> refContainerEncryptionValues;
  private List<Boolean> refContainerPreviousEncryptionValues;
  private List<Boolean> refContainerCachingValues;
  private List<Boolean> refContainerBackupEnabledValues;
  private List<Boolean> refContainerMediaScanDisabledValues;
  private List<String> refContainerReplicationPolicyValues;
  private List<Boolean> refContainerTtlRequiredValues;
  private List<Long> refContainerDeleteTriggerTime;
  private List<Boolean> refContainerSignedPathRequiredValues;
  private List<Boolean> refContainerOverrideAccountAcls;
  private List<NamedBlobMode> refContainerNamedBlobModes;
  private List<Set<String>> refContainerContentTypeAllowListForFilenamesOnDownloadValues;
  private List<Long> refContainerLastModifiedTimes;
  private List<Integer> refContainerSnapshotVersions;
  private List<String> refAccessControlAllowOriginValues;

  //Reference Dataset fields
  private List<Dataset> refDatasets;
  private List<String> refDatasetNames;
  private List<Dataset.VersionSchema> refDatasetVersionSchemas;
  private List<Long> refDatasetRetentionTimeInSeconds;
  private List<Integer> refDatasetRetentionCounts;
  private List<Map<String, String>> refDatasetUserTags;


  // Newly added fields to Container (2022/10/19)
  private List<Long> refContainerCacheTtlInSeconds;
  private List<Set<String>> refContainerUserMetadataKeysToNotPrefixInResponses;

  private List<String> newContainerFieldNames =
      Arrays.asList("cacheTtlInSecond", "userMetadataKeysToNotPrefixInResponse");

  /**
   * Initialize the metadata in JsonObject for account and container.
   * @param containerJsonVersion the container JSON version to use in the test.
   * @param quotaResourceType {@link QuotaResourceType} object.
   */
  public AccountContainerTest(short containerJsonVersion, QuotaResourceType quotaResourceType) {
    Container.setCurrentJsonVersion(containerJsonVersion);
    refAccountId = Utils.getRandomShort(random);
    refAccountName = UUID.randomUUID().toString();
    refAccountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
    refAccountAclInheritedByContainer = random.nextBoolean();
    refAccountSnapshotVersion = random.nextInt();
    refQuotaResourceType = quotaResourceType;
    initializeRefContainers();
    refAccount = new AccountBuilder().id(refAccountId)
        .name(refAccountName)
        .status(refAccountStatus)
        .aclInheritedByContainer(refAccountAclInheritedByContainer)
        .snapshotVersion(refAccountSnapshotVersion)
        .quotaResourceType(refQuotaResourceType)
        .containers(refContainers)
        .lastModifiedTime(0)
        .build();
    initializeRefDatasets();
  }

  /**
   * Run this test for all versions of the container schema and all values of {@link QuotaResourceType}.
   * @return the constructor arguments to use.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{Container.JSON_VERSION_1, QuotaResourceType.CONTAINER},
        {Container.JSON_VERSION_2, QuotaResourceType.CONTAINER}, {Container.JSON_VERSION_1, QuotaResourceType.ACCOUNT},
        {Container.JSON_VERSION_2, QuotaResourceType.ACCOUNT}});
  }

  /**
   * Tests constructing {@link Account} and {@link Container} using individual arguments.
   */
  @Test
  public void testConstructAccountAndContainerFromArguments() {
    Account accountFromArguments =
        new Account(refAccountId, refAccountName, refAccountStatus, refAccountAclInheritedByContainer,
            refAccountSnapshotVersion, refContainers, refQuotaResourceType);
    assertAccountAgainstReference(accountFromArguments, true);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated name.
   */
  @Test
  public void testDuplicateContainerName() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    // second container with (id=1, name="0")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id.
   */
  @Test
  public void testDuplicateContainerId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    // second container with (id=0, name="1")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 0, "1", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id and name.
   */
  @Test
  public void testDuplicateContainerNameAndId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    // second container with (id=1, name="0")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    // third container with (id=10, name="10")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 10, "10", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    // second container with (id=10, name="11")
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder((short) 10, "11", refContainerStatuses.get(0), refContainerDescriptions.get(0),
            refAccountId), refContainers.get(0)));
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests in an {@link AccountBuilder} the account id mismatches with container id.
   */
  @Test
  public void testMismatchForAccountId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // container with parentAccountId = refAccountId + 1
    containers.add(buildContainerWithOtherFields(
        new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), (short) (refAccountId + 1)), refContainers.get(0)));
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  // Tests for builders

  /**
   * Tests {@code toString()} methods.
   */
  @Test
  public void testToString() {
    Account account = refAccount;
    assertEquals("Account[" + account.getId() + "," + account.getSnapshotVersion() + "]", account.toString());
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = new ContainerBuilder(refContainers.get(i)).setParentAccountId(refAccountId).build();
      assertEquals("Container[" + account.getId() + ":" + container.getId() + "]", container.toString());
    }
  }

  /**
   * Tests building an {@link Account} using {@link AccountBuilder}.
   */
  @Test
  public void testAccountBuilder() {
    // build an account with arguments supplied
    AccountBuilder accountBuilder =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, refQuotaResourceType).snapshotVersion(
            refAccountSnapshotVersion).aclInheritedByContainer(refAccountAclInheritedByContainer);
    Account accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, false);

    // set containers
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = new ContainerBuilder(refContainers.get(i)).setParentAccountId(refAccountId).build();
      accountBuilder.addOrUpdateContainer(container);
    }
    accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, true);

    // build an account from existing account
    accountBuilder = new AccountBuilder(accountByBuilder);
    Account account2ByBuilder = accountBuilder.build();
    assertAccountAgainstReference(account2ByBuilder, true);

    // clear containers
    Account account3ByBuilder = new AccountBuilder(account2ByBuilder).containers(null).build();
    assertAccountAgainstReference(account3ByBuilder, false);
    assertTrue("Container list should be empty.", account3ByBuilder.getAllContainers().isEmpty());
  }

  /**
   * Tests building a {@link Container} using {@link ContainerBuilder}.
   */
  @Test
  public void testContainerBuilder() {
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      // build a container with arguments supplied
      Container containerFromBuilder = buildContainerWithOtherFields(
          new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refAccountId), refContainers.get(i));
      assertContainer(containerFromBuilder, i);

      // build a container from existing container
      Container anotherContainerFromBuilder = new ContainerBuilder(containerFromBuilder).build();
      assertContainer(anotherContainerFromBuilder, i);
      assertEquals("Two container should be equal", containerFromBuilder, anotherContainerFromBuilder);

      boolean previouslyEncrypted = containerFromBuilder.wasPreviouslyEncrypted();
      // turn off encryption, check that previouslyEncrypted is the same as the previous value.
      containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(false).build();
      assertEncryptionSettings(containerFromBuilder, false, previouslyEncrypted);
      // turn off encryption, by turning it on and off again.
      containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(true).setEncrypted(false).build();
      assertEncryptionSettings(containerFromBuilder, false, previouslyEncrypted);
      // turn it back on, previouslyEncrypted should be set.
      containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(true).build();
      assertEncryptionSettings(containerFromBuilder, true, true);
      // turn off again, previouslyEncrypted should still be set.
      containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(false).build();
      assertEncryptionSettings(containerFromBuilder, false, true);
    }
  }

  @Test
  public void testDatasetBuilder() {
    for (int i = 0; i < DATASET_COUNT; i++) {
      // build a dataset with arguments supplied
      Dataset datasetFromBuilder = buildDatasetWithOtherFields(
          new DatasetBuilder(refAccountName, refContainers.get(0).getName(), refDatasetNames.get(i))
              .setVersionSchema(refDatasetVersionSchemas.get(i))
              .setRetentionTimeInSeconds(refDatasetRetentionTimeInSeconds.get(i))
              .setRetentionCount(refDatasetRetentionCounts.get(i))
              .setUserTags(refDatasetUserTags.get(i)), refDatasets.get(i));
      assertDataset(datasetFromBuilder, i);
      // build a dataset from existing container
      Dataset anotherDatasetFromBuilder = new DatasetBuilder(datasetFromBuilder).build();
      assertDataset(anotherDatasetFromBuilder, i);
    }
  }

  /**
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testFieldMissingToBuildAccount() throws Exception {
    // test when required fields are null
    buildAccountWithMissingFieldsAndFail(null, refAccountStatus, IllegalStateException.class);
    buildAccountWithMissingFieldsAndFail(refAccountName, null, IllegalStateException.class);
  }

  /**
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testBuildingContainerWithBadFields() throws Exception {
    // test when required fields are null
    String name = refContainerNames.get(0);
    ContainerStatus status = refContainerStatuses.get(0);
    buildContainerWithBadFieldsAndFail(null, status, false, false, IllegalStateException.class);
    buildContainerWithBadFieldsAndFail(name, null, false, false, IllegalStateException.class);
    buildContainerWithBadFieldsAndFail(name, status, true, false, IllegalStateException.class);
  }

  /**
   * Tests update an {@link Account}.
   */
  @Test
  public void testUpdateAccount() {
    // set an account with different field value
    Account origin = refAccount;
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    short updatedAccountId = (short) (refAccountId + 1);
    String updatedAccountName = refAccountName + "-updated";
    Account.AccountStatus updatedAccountStatus = Account.AccountStatus.INACTIVE;
    accountBuilder.id(updatedAccountId);
    accountBuilder.name(updatedAccountName);
    accountBuilder.status(updatedAccountStatus);

    try {
      accountBuilder.build();
      fail("Should have thrown");
    } catch (IllegalStateException e) {
      // expected, as new account id does not match the parentAccountId of the two containers.
    }

    // remove all existing containers.
    for (Container container : origin.getAllContainers()) {
      accountBuilder.removeContainer(container);
    }

    // build the account and assert
    Account updatedAccount = accountBuilder.build();
    assertEquals(updatedAccountId, updatedAccount.getId());
    assertEquals(updatedAccountName, updatedAccount.getName());
    assertEquals(updatedAccountStatus, updatedAccount.getStatus());

    // add back the containers and assert
    for (Container container : origin.getAllContainers()) {
      accountBuilder.addOrUpdateContainer(container);
    }
    accountBuilder.id(refAccountId);
    updatedAccount = accountBuilder.build();
    assertEquals(origin.getAllContainers().toString(), updatedAccount.getAllContainers().toString());
  }

  /**
   * Tests removing containers in AccountBuilder.
   */
  @Test
  public void testRemovingContainers() {
    Account origin = refAccount;
    AccountBuilder accountBuilder = new AccountBuilder(origin);

    // first, remove 10 containers
    ArrayList<Container> containers = new ArrayList<>(origin.getAllContainers());
    Set<Container> removed = new HashSet<>();
    while (removed.size() < 10) {
      Container container = containers.get(random.nextInt(containers.size()));
      removed.add(container);
      accountBuilder.removeContainer(container);
    }

    Account account = accountBuilder.build();
    assertEquals("Wrong number of containers", CONTAINER_COUNT - 10, account.getAllContainers().size());

    for (Container removedContainer : removed) {
      assertNull("Container not removed ", account.getContainerById(removedContainer.getId()));
      assertNull("Container not removed ", account.getContainerByName(removedContainer.getName()));
    }

    // then, remove the rest containers
    for (Container container : origin.getAllContainers()) {
      accountBuilder.removeContainer(container);
    }
    account = accountBuilder.build();
    assertEquals("Wrong container number.", 0, account.getAllContainers().size());
  }

  /**
   * Tests updating containers in an account.
   */
  @Test
  public void testUpdateContainerInAccount() {
    Account account = refAccount;
    AccountBuilder accountBuilder = new AccountBuilder(account);

    // updating with different containers
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = account.getContainerById(refContainerIds.get(i));
      accountBuilder.removeContainer(container);
      ContainerBuilder containerBuilder = new ContainerBuilder(container);
      short updatedContainerId = (short) (-1 * (container.getId()));
      String updatedContainerName = container.getName() + "-updated";
      Container.ContainerStatus updatedContainerStatus = Container.ContainerStatus.INACTIVE;
      String updatedContainerDescription = container.getDescription() + "--updated";
      boolean updatedEncrypted = !container.isEncrypted();
      boolean updatedPreviouslyEncrypted = updatedEncrypted || container.wasPreviouslyEncrypted();
      boolean updatedCacheable = !container.isCacheable();
      boolean updatedMediaScanDisabled = !container.isMediaScanDisabled();
      String updatedReplicationPolicy = container.getReplicationPolicy() + "---updated";
      boolean updatedTtlRequired = !container.isTtlRequired();
      boolean updatedSignedPathRequired = !container.isSecurePathRequired();
      String updatedAccessControlAllowOrigin = container.getAccessControlAllowOrigin() + "---updated";
      Set<String> updatedContentTypeAllowListForFilenamesOnDownloadValues =
          container.getContentTypeWhitelistForFilenamesOnDownload()
              .stream()
              .map(contentType -> contentType + "--updated")
              .collect(Collectors.toSet());
      Long updatedCacheTtlInSecond = System.currentTimeMillis();
      Set<String> updatedUserMetadataKeysToNotPrefixInResponse = container.getUserMetadataKeysToNotPrefixInResponse()
          .stream()
          .map(key -> key + "--updated")
          .collect(Collectors.toSet());

      containerBuilder.setId(updatedContainerId)
          .setName(updatedContainerName)
          .setStatus(updatedContainerStatus)
          .setDescription(updatedContainerDescription)
          .setEncrypted(updatedEncrypted)
          .setCacheable(updatedCacheable)
          .setMediaScanDisabled(updatedMediaScanDisabled)
          .setReplicationPolicy(updatedReplicationPolicy)
          .setTtlRequired(updatedTtlRequired)
          .setSecurePathRequired(updatedSignedPathRequired)
          .setContentTypeWhitelistForFilenamesOnDownload(updatedContentTypeAllowListForFilenamesOnDownloadValues)
          .setAccessControlAllowOrigin(updatedAccessControlAllowOrigin)
          .setCacheTtlInSecond(updatedCacheTtlInSecond)
          .setUserMetadataKeysToNotPrefixInResponse(updatedUserMetadataKeysToNotPrefixInResponse);
      accountBuilder.addOrUpdateContainer(containerBuilder.build());

      // build account and assert
      Account updatedAccount = accountBuilder.build();
      Container updatedContainer = updatedAccount.getContainerById(updatedContainerId);
      assertEquals("container id is not correctly updated", updatedContainerId, updatedContainer.getId());
      assertEquals("container name is not correctly updated", updatedContainerName, updatedContainer.getName());
      assertEquals("container status is not correctly updated", updatedContainerStatus, updatedContainer.getStatus());
      assertEquals("container description is not correctly updated", updatedContainerDescription,
          updatedContainer.getDescription());
      assertEquals("cacheable is not correctly updated", updatedCacheable, updatedContainer.isCacheable());
      switch (Container.getCurrentJsonVersion()) {
        case Container.JSON_VERSION_1:
          assertEquals("Wrong encryption setting", ENCRYPTED_DEFAULT_VALUE, updatedContainer.isEncrypted());
          assertEquals("Wrong previous encryption setting", PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE,
              updatedContainer.wasPreviouslyEncrypted());
          assertEquals("Wrong media scan disabled setting", MEDIA_SCAN_DISABLED_DEFAULT_VALUE,
              updatedContainer.isMediaScanDisabled());
          assertNull("Wrong replication policy", updatedContainer.getReplicationPolicy());
          assertEquals("Wrong ttl required setting", TTL_REQUIRED_DEFAULT_VALUE, updatedContainer.isTtlRequired());
          assertEquals("Wrong secure required setting", SECURE_PATH_REQUIRED_DEFAULT_VALUE,
              updatedContainer.isSecurePathRequired());
          assertEquals("Wrong content type allow list for filenames on download value",
              CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE,
              updatedContainer.getContentTypeWhitelistForFilenamesOnDownload());
          assertEquals("Wrong accessControlAllowOrigin", "", updatedContainer.getAccessControlAllowOrigin());
          break;
        case Container.JSON_VERSION_2:
          assertEquals("Wrong encryption setting", updatedEncrypted, updatedContainer.isEncrypted());
          assertEquals("Wrong previous encryption setting", updatedPreviouslyEncrypted,
              updatedContainer.wasPreviouslyEncrypted());
          assertEquals("Wrong media scan disabled setting", updatedMediaScanDisabled,
              updatedContainer.isMediaScanDisabled());
          assertEquals("Wrong replication policy", updatedReplicationPolicy, updatedContainer.getReplicationPolicy());
          assertEquals("Wrong ttl required setting", updatedTtlRequired, updatedContainer.isTtlRequired());
          assertEquals("Wrong secure path required setting", updatedSignedPathRequired,
              updatedContainer.isSecurePathRequired());
          assertEquals("Wrong content type allow list for filenames on download value",
              updatedContentTypeAllowListForFilenamesOnDownloadValues,
              updatedContainer.getContentTypeWhitelistForFilenamesOnDownload());
          assertEquals("Wrong accessControlAllowOrigin", updatedAccessControlAllowOrigin,
              updatedContainer.getAccessControlAllowOrigin());
          assertEquals("Wrong cacheTtlInSeconds", updatedCacheTtlInSecond, updatedContainer.getCacheTtlInSecond());
          assertEquals("Wrong user metadata keys to not prefix in response",
              updatedUserMetadataKeysToNotPrefixInResponse,
              updatedContainer.getUserMetadataKeysToNotPrefixInResponse());
          break;
        default:
          throw new IllegalStateException("Unsupported version: " + Container.getCurrentJsonVersion());
      }
    }
  }

  /**
   * Tests updating the parent account id for a container.
   */
  @Test
  public void testUpdateContainerParentAccountId() {
    ContainerBuilder containerBuilder = new ContainerBuilder(refContainers.get(0)).setParentAccountId(refAccountId);
    short newParentAccountId = (short) (refAccountId + 1);
    containerBuilder.setParentAccountId(newParentAccountId);
    assertEquals("Container's parent account id is incorrectly updated.", newParentAccountId,
        containerBuilder.build().getParentAccountId());
  }

  /**
   * Tests removing a non-existent container from accountBuilder.
   */
  @Test
  public void testRemoveNonExistContainer() {
    Account origin = refAccount;
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    Container container = buildContainerWithOtherFields(
        new ContainerBuilder((short) -999, refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), refAccountId), refContainers.get(0));
    accountBuilder.removeContainer(container);
    accountBuilder.removeContainer(null);
    Account account = accountBuilder.build();
    assertAccountAgainstReference(account, true);
  }

  /**
   * Tests for {@link InMemAccountService#UNKNOWN_ACCOUNT}, {@link Container#UNKNOWN_CONTAINER},
   * {@link Container#DEFAULT_PUBLIC_CONTAINER}, and {@link Container#DEFAULT_PRIVATE_CONTAINER}.
   */
  @Test
  public void testUnknownAccountAndContainer() {
    Account unknownAccount = InMemAccountService.UNKNOWN_ACCOUNT;
    Container unknownContainer = Container.UNKNOWN_CONTAINER;
    Container unknownPublicContainer = Container.DEFAULT_PUBLIC_CONTAINER;
    Container unknownPrivateContainer = Container.DEFAULT_PRIVATE_CONTAINER;
    // UNKNOWN_CONTAINER
    assertEquals("Wrong id for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_ID, unknownContainer.getId());
    assertEquals("Wrong name for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_NAME, unknownContainer.getName());
    assertEquals("Wrong status for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_STATUS,
        unknownContainer.getStatus());
    assertEquals("Wrong description for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_DESCRIPTION,
        unknownContainer.getDescription());
    assertEquals("Wrong parent account id for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID,
        unknownContainer.getParentAccountId());
    assertEquals("Wrong cacheable setting for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_CACHEABLE_SETTING,
        unknownContainer.isCacheable());
    assertEquals("Wrong encrypted setting for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_ENCRYPTED_SETTING,
        unknownContainer.isEncrypted());
    assertEquals("Wrong previouslyEncrypted setting for UNKNOWN_CONTAINER",
        Container.UNKNOWN_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING, unknownContainer.wasPreviouslyEncrypted());
    assertEquals("Wrong mediaScanDisabled setting for UNKNOWN_CONTAINER",
        Container.UNKNOWN_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, unknownContainer.isMediaScanDisabled());
    // DEFAULT_PUBLIC_CONTAINER
    assertEquals("Wrong id for DEFAULT_PUBLIC_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_ID,
        unknownPublicContainer.getId());
    assertEquals("Wrong name for DEFAULT_PUBLIC_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_NAME,
        unknownPublicContainer.getName());
    assertEquals("Wrong status for DEFAULT_PUBLIC_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_STATUS,
        unknownPublicContainer.getStatus());
    assertEquals("Wrong description for DEFAULT_PUBLIC_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_DESCRIPTION,
        unknownPublicContainer.getDescription());
    assertEquals("Wrong parent account id for DEFAULT_PUBLIC_CONTAINER",
        Container.DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID, unknownPublicContainer.getParentAccountId());
    assertEquals("Wrong cacheable setting for DEFAULT_PUBLIC_CONTAINER",
        Container.DEFAULT_PUBLIC_CONTAINER_CACHEABLE_SETTING, unknownPublicContainer.isCacheable());
    assertEquals("Wrong encrypted setting for DEFAULT_PUBLIC_CONTAINER",
        Container.DEFAULT_PUBLIC_CONTAINER_ENCRYPTED_SETTING, unknownPublicContainer.isEncrypted());
    assertEquals("Wrong previouslyEncrypted setting for DEFAULT_PUBLIC_CONTAINER",
        Container.DEFAULT_PUBLIC_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING,
        unknownPublicContainer.wasPreviouslyEncrypted());
    assertEquals("Wrong mediaScanDisabled setting for DEFAULT_PUBLIC_CONTAINER",
        Container.DEFAULT_PUBLIC_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, unknownPublicContainer.isMediaScanDisabled());
    // DEFAULT_PRIVATE_CONTAINER
    assertEquals("Wrong id for DEFAULT_PRIVATE_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_ID,
        unknownPrivateContainer.getId());
    assertEquals("Wrong name for DEFAULT_PRIVATE_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_NAME,
        unknownPrivateContainer.getName());
    assertEquals("Wrong status for DEFAULT_PRIVATE_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_STATUS,
        unknownPrivateContainer.getStatus());
    assertEquals("Wrong description for DEFAULT_PRIVATE_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_DESCRIPTION,
        unknownPrivateContainer.getDescription());
    assertEquals("Wrong parent account id for DEFAULT_PRIVATE_CONTAINER",
        Container.DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID, unknownPrivateContainer.getParentAccountId());
    assertEquals("Wrong cacheable setting for DEFAULT_PRIVATE_CONTAINER",
        Container.DEFAULT_PRIVATE_CONTAINER_CACHEABLE_SETTING, unknownPrivateContainer.isCacheable());
    assertEquals("Wrong encrypted setting for DEFAULT_PRIVATE_CONTAINER",
        Container.DEFAULT_PRIVATE_CONTAINER_ENCRYPTED_SETTING, unknownPrivateContainer.isEncrypted());
    assertEquals("Wrong previouslyEncrypted setting for DEFAULT_PRIVATE_CONTAINER",
        Container.DEFAULT_PRIVATE_CONTAINER_PREVIOUSLY_ENCRYPTED_SETTING,
        unknownPrivateContainer.wasPreviouslyEncrypted());
    assertEquals("Wrong mediaScanDisabled setting for DEFAULT_PRIVATE_CONTAINER",
        Container.DEFAULT_PRIVATE_CONTAINER_MEDIA_SCAN_DISABLED_SETTING, unknownPrivateContainer.isMediaScanDisabled());
    // UNKNOWN_ACCOUNT
    assertEquals("Wrong id for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_ID, unknownAccount.getId());
    assertEquals("Wrong name for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_NAME, unknownAccount.getName());
    assertEquals("Wrong status for UNKNOWN_ACCOUNT", AccountStatus.ACTIVE, unknownAccount.getStatus());
    assertEquals("Wrong number of containers for UNKNOWN_ACCOUNT", 3, unknownAccount.getAllContainers().size());
    assertEquals("Wrong unknown container get from UNKNOWN_ACCOUNT", Container.UNKNOWN_CONTAINER,
        unknownAccount.getContainerById(Container.UNKNOWN_CONTAINER_ID));
    assertEquals("Wrong unknown public container get from UNKNOWN_ACCOUNT", Container.DEFAULT_PUBLIC_CONTAINER,
        unknownAccount.getContainerById(Container.DEFAULT_PUBLIC_CONTAINER_ID));
    assertEquals("Wrong unknown private container get from UNKNOWN_ACCOUNT", Container.DEFAULT_PRIVATE_CONTAINER,
        unknownAccount.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID));
  }

  /**
   * Tests {@link Account#equals(Object)} that checks equality of {@link Container}s.
   */
  @Test
  public void testAccountEqual() {
    // Check two accounts with same fields but no containers.
    Account accountNoContainer = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    Account accountNoContainerDuplicate = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    assertEquals("Two accounts should be equal.", accountNoContainer, accountNoContainerDuplicate);

    // Check two accounts with same fields and containers.
    Account accountWithContainers = refAccount;
    Account accountWithContainersDuplicate = new AccountBuilder(refAccount).build();
    assertEquals("Two accounts should be equal.", accountWithContainers, accountWithContainersDuplicate);

    // Check two accounts with same fields but one has containers, the other one does not.
    assertFalse("Two accounts should not be equal.", accountNoContainer.equals(accountWithContainers));

    // Check two accounts with the same fields and the same number of containers. One container of one account has one
    // field different from the other one.
    Container updatedContainer = buildContainerWithOtherFields(
        new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
            "A changed container description", refAccountId), refContainers.get(0));
    refContainers.remove(0);
    refContainers.add(updatedContainer);
    Account accountWithModifiedContainers = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    assertFalse("Two accounts should not be equal.", accountWithContainers.equals(accountWithModifiedContainers));
  }

  @Test
  public void testAccountAndContainerSerDe() throws IOException {
    assumeTrue(Container.getCurrentJsonVersion() == JSON_VERSION_2);
    Account origin = refAccount;
    String serializedAccountStr = objectMapper.writeValueAsString(origin);
    Account deserializedAccount = objectMapper.readValue(serializedAccountStr, Account.class);
    assertEquals("Two accounts should be equal", origin, deserializedAccount);

    // Serialize the account without container
    @JsonIgnoreProperties({"containers"})
    abstract class AccountMixIn {
    }
    ObjectMapper newObjectMapper = new ObjectMapper();
    newObjectMapper.addMixIn(Account.class, AccountMixIn.class);
    serializedAccountStr = newObjectMapper.writeValueAsString(origin);
    deserializedAccount = objectMapper.readValue(serializedAccountStr, Account.class);
    assertTrue(deserializedAccount.equalsWithoutContainers(origin));

    Account originWithoutContainers = new AccountBuilder(origin).containers(Collections.EMPTY_LIST).build();
    assertEquals("Two accounts should be equal", originWithoutContainers, deserializedAccount);

    for (Container container : refContainers) {
      String serializedContainerStr = objectMapper.writeValueAsString(container);
      Container deserializedContainer =
          new ContainerBuilder(objectMapper.readValue(serializedContainerStr, Container.class)).setParentAccountId(
              container.getParentAccountId()).build();
      assertEquals("Two containers should be equal", container, deserializedContainer);
    }
  }

  /**
   * Test container serialization and deserialization without the new fields
   * @throws IOException
   */
  @Test
  public void testContainerSerDeWithoutNewFields() throws IOException {
    assumeTrue(Container.getCurrentJsonVersion() == JSON_VERSION_2);
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = refContainers.get(i);

      String serializedContainerStr = objectMapper.writeValueAsString(container);
      ObjectNode newRefContainerJsonNode = (ObjectNode) objectMapper.readTree(serializedContainerStr);
      for (String fieldName : newContainerFieldNames) {
        newRefContainerJsonNode.remove(fieldName);
      }
      Container deseriazlied =
          objectMapper.readValue(objectMapper.writeValueAsString(newRefContainerJsonNode), Container.class);
      assertEquals(Container.CACHE_TTL_IN_SECOND_DEFAULT_VALUE, deseriazlied.getCacheTtlInSecond());
      assertEquals(USER_METADATA_KEYS_TO_NOT_PREFIX_IN_RESPONSE_DEFAULT_VALUE,
          deseriazlied.getUserMetadataKeysToNotPrefixInResponse());

      Container newContainer = new ContainerBuilder(deseriazlied).setCacheTtlInSecond(container.getCacheTtlInSecond())
          .setUserMetadataKeysToNotPrefixInResponse(container.getUserMetadataKeysToNotPrefixInResponse())
          .setParentAccountId(container.getParentAccountId())
          .build();

      assertEquals(container, newContainer);
    }
  }

  @Test
  public void testAccountAndContainerSerDeWithoutIdAndName() throws IOException {
    assumeTrue(Container.getCurrentJsonVersion() == JSON_VERSION_2);
    // remove account id
    String serializedAccountStr = objectMapper.writeValueAsString(refAccount);
    ObjectNode newRefAccountJsonNode = (ObjectNode) objectMapper.readTree(serializedAccountStr);
    newRefAccountJsonNode.remove(ACCOUNT_ID_KEY);

    try {
      objectMapper.readValue(objectMapper.writeValueAsString(newRefAccountJsonNode), Account.class);
      fail("Missing account id should fail");
    } catch (ValueInstantiationException e) {
      assertTrue(e.getCause() instanceof IllegalStateException);
    }

    serializedAccountStr = objectMapper.writeValueAsString(refAccount);
    newRefAccountJsonNode = (ObjectNode) objectMapper.readTree(serializedAccountStr);
    newRefAccountJsonNode.remove(ACCOUNT_NAME_KEY);
    try {
      objectMapper.readValue(objectMapper.writeValueAsString(newRefAccountJsonNode), Account.class);
      fail("Missing account name should fail");
    } catch (ValueInstantiationException e) {
      assertTrue(e.getCause() instanceof IllegalStateException);
    }

    serializedAccountStr = objectMapper.writeValueAsString(refAccount);
    newRefAccountJsonNode = (ObjectNode) objectMapper.readTree(serializedAccountStr);
    ObjectNode containerJsonNode = (ObjectNode) newRefAccountJsonNode.get(CONTAINERS_KEY).get(0);
    containerJsonNode.remove(CONTAINER_ID_KEY);

    try {
      objectMapper.readValue(objectMapper.writeValueAsString(newRefAccountJsonNode), Account.class);
      fail("Missing container id should fail");
    } catch (ValueInstantiationException e) {
      assertTrue(e.getCause() instanceof IllegalStateException);
    }

    serializedAccountStr = objectMapper.writeValueAsString(refAccount);
    newRefAccountJsonNode = (ObjectNode) objectMapper.readTree(serializedAccountStr);
    containerJsonNode = (ObjectNode) newRefAccountJsonNode.get(CONTAINERS_KEY).get(0);
    containerJsonNode.remove(CONTAINER_NAME_KEY);

    try {
      objectMapper.readValue(objectMapper.writeValueAsString(newRefAccountJsonNode), Account.class);
      fail("Missing container name should fail");
    } catch (ValueInstantiationException e) {
      assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }

  /**
   * Asserts an {@link Account} against the reference account.
   * @param account The {@link Account} to assert.
   * @param compareContainer {@code true} to compare each individual {@link Container}. {@code false} to skip this test.
   */
  private void assertAccountAgainstReference(Account account, boolean compareContainer) {
    assertEquals(refAccountId, account.getId());
    assertEquals(refAccountName, account.getName());
    assertEquals(refAccountStatus, account.getStatus());
    assertEquals("Snapshot versions do not match", refAccountSnapshotVersion, account.getSnapshotVersion());
    assertEquals("Acl inherited by container settings don't match", refAccountAclInheritedByContainer,
        account.isAclInheritedByContainer());
    if (compareContainer) {
      Collection<Container> containersFromAccount = account.getAllContainers();
      assertEquals("Wrong number of containers.", CONTAINER_COUNT, containersFromAccount.size());
      assertEquals(CONTAINER_COUNT, containersFromAccount.size());
      for (int i = 0; i < CONTAINER_COUNT; i++) {
        assertContainer(account.getContainerById(refContainerIds.get(i)), i);
        assertContainer(account.getContainerByName(refContainerNames.get(i)), i);
      }
    }
  }

  /**
   * Build a container in the {@link ContainerBuilder} with the values from {@code container}. It expects the
   * {@code builder} to already have name, id, description and status.
   * @param builder The {@link ContainerBuilder}.
   * @param container The {@link Container}.
   * @return
   */
  private Container buildContainerWithOtherFields(ContainerBuilder builder, Container container) {
    return builder.setEncrypted(container.isEncrypted())
        .setPreviouslyEncrypted(container.wasPreviouslyEncrypted())
        .setCacheable(container.isCacheable())
        .setBackupEnabled(container.isBackupEnabled())
        .setMediaScanDisabled(container.isMediaScanDisabled())
        .setReplicationPolicy(container.getReplicationPolicy())
        .setTtlRequired(container.isTtlRequired())
        .setContentTypeWhitelistForFilenamesOnDownload(container.getContentTypeWhitelistForFilenamesOnDownload())
        .setDeleteTriggerTime(container.getDeleteTriggerTime())
        .setOverrideAccountAcl(container.isAccountAclOverridden())
        .setSecurePathRequired(container.isSecurePathRequired())
        .setAccessControlAllowOrigin(container.getAccessControlAllowOrigin())
        .setNamedBlobMode(container.getNamedBlobMode())
        .setLastModifiedTime(container.getLastModifiedTime())
        .setSnapshotVersion(container.getSnapshotVersion())
        .setCacheTtlInSecond(container.getCacheTtlInSecond())
        .setUserMetadataKeysToNotPrefixInResponse(container.getUserMetadataKeysToNotPrefixInResponse())
        .build();
  }

  /**
   * Build a dataset in the {@link DatasetBuilder} with the values from {@code dataset}.
   * @param builder the {@link DatasetBuilder}
   * @param dataset the {@link Dataset}
   * @return a dataset
   */
  private Dataset buildDatasetWithOtherFields(DatasetBuilder builder, Dataset dataset) {
    return builder.setAccountName(dataset.getAccountName())
        .setContainerName(dataset.getContainerName())
        .setVersionSchema(dataset.getVersionSchema())
        .build();
  }

  /**
   * Asserts a {@link Dataset} against the reference account for every internal field.
   * @param dataset the {@link Dataset} to assert.
   * @param index the index in the reference dataset list to assert against.
   */
  private void assertDataset(Dataset dataset, int index) {
    assertEquals("Wrong account name", refAccountName, dataset.getAccountName());
    assertEquals("Wrong container name", refContainers.get(0).getName(), dataset.getContainerName());
    assertEquals("Wrong dataset name", refDatasetNames.get(index), dataset.getDatasetName());
    assertEquals("Wrong version schema", refDatasetVersionSchemas.get(index), dataset.getVersionSchema());
    assertEquals("Wrong retention count", refDatasetRetentionCounts.get(index), dataset.getRetentionCount());
    assertEquals("Wrong retention time", refDatasetRetentionTimeInSeconds.get(index), dataset.getRetentionTimeInSeconds());
    assertEquals("Wrong user tage", refDatasetUserTags.get(index), dataset.getUserTags());
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   */
  private void assertContainer(Container container, int index) {
    assertEquals("Wrong container ID", (short) refContainerIds.get(index), container.getId());
    assertEquals("Wrong name", refContainerNames.get(index), container.getName());
    assertEquals("Wrong status", refContainerStatuses.get(index), container.getStatus());
    assertEquals("Wrong description", refContainerDescriptions.get(index), container.getDescription());
    assertEquals("Wrong caching setting", refContainerCachingValues.get(index), container.isCacheable());
    assertEquals("Wrong account ID", refAccountId, container.getParentAccountId());
    switch (Container.getCurrentJsonVersion()) {
      case Container.JSON_VERSION_1:
        assertEquals("Wrong encryption setting", ENCRYPTED_DEFAULT_VALUE, container.isEncrypted());
        assertEquals("Wrong previous encryption setting", PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE,
            container.wasPreviouslyEncrypted());
        assertEquals("Wrong media scan disabled setting", MEDIA_SCAN_DISABLED_DEFAULT_VALUE,
            container.isMediaScanDisabled());
        assertNull("Wrong replication policy", container.getReplicationPolicy());
        assertEquals("Wrong ttl required setting", TTL_REQUIRED_DEFAULT_VALUE, container.isTtlRequired());
        assertEquals("Wrong secure path required setting", SECURE_PATH_REQUIRED_DEFAULT_VALUE,
            container.isSecurePathRequired());
        assertEquals("Wrong override account acl setting", OVERRIDE_ACCOUNT_ACL_DEFAULT_VALUE,
            container.isAccountAclOverridden());
        assertEquals("Wrong accessControlAllowOrigin", "", container.getAccessControlAllowOrigin());
        break;
      case Container.JSON_VERSION_2:
        assertEquals("Wrong encryption setting", refContainerEncryptionValues.get(index), container.isEncrypted());
        assertEquals("Wrong previous encryption setting", refContainerPreviousEncryptionValues.get(index),
            container.wasPreviouslyEncrypted());
        assertEquals("Wrong media scan disabled setting", refContainerMediaScanDisabledValues.get(index),
            container.isMediaScanDisabled());
        assertEquals("Wrong replication policy", refContainerReplicationPolicyValues.get(index),
            container.getReplicationPolicy());
        assertEquals("Wrong ttl required setting", refContainerTtlRequiredValues.get(index), container.isTtlRequired());
        assertEquals("Wrong secure path required setting", refContainerSignedPathRequiredValues.get(index),
            container.isSecurePathRequired());
        assertEquals("Wrong override account acl setting", refContainerOverrideAccountAcls.get(index),
            container.isAccountAclOverridden());
        assertEquals("Wrong named blob mode setting", refContainerNamedBlobModes.get(index),
            container.getNamedBlobMode());
        Set<String> expectedContentTypeAllowListForFilenamesOnDownloadValue =
            refContainerContentTypeAllowListForFilenamesOnDownloadValues.get(index) == null ? Collections.emptySet()
                : refContainerContentTypeAllowListForFilenamesOnDownloadValues.get(index);
        assertEquals("Wrong content types allow listed for filename on download",
            expectedContentTypeAllowListForFilenamesOnDownloadValue,
            container.getContentTypeWhitelistForFilenamesOnDownload());
        assertEquals("Wrong last modified time setting", (long) refContainerLastModifiedTimes.get(index),
            container.getLastModifiedTime());
        assertEquals("Wrong snapshot version setting", (int) refContainerSnapshotVersions.get(index),
            container.getSnapshotVersion());
        assertEquals("Wrong accessControlAllowOrigin", refAccessControlAllowOriginValues.get(index),
            container.getAccessControlAllowOrigin());
        assertEquals("Wrong cacheTtlInSeconds", refContainerCacheTtlInSeconds.get(index),
            container.getCacheTtlInSecond());
        Set<String> expectedUserMetadataKeysToNotPrefixInResponse =
            refContainerUserMetadataKeysToNotPrefixInResponses.get(index) == null ? Collections.emptySet()
                : refContainerUserMetadataKeysToNotPrefixInResponses.get(index);
        assertEquals("Wrong user metadata keys to not prefix in response",
            expectedUserMetadataKeysToNotPrefixInResponse, container.getUserMetadataKeysToNotPrefixInResponse());
        break;
      default:
        throw new IllegalStateException("Unsupported version: " + Container.getCurrentJsonVersion());
    }
  }

  /**
   * Check the value of the encryption settings and json serde for a container.
   * @param container the {@link Container} to check.
   * @param encrypted the expected encrypted setting.
   * @param previouslyEncrypted the expected previouslyEncrypted setting.
   */
  private void assertEncryptionSettings(Container container, boolean encrypted, boolean previouslyEncrypted) {
    switch (Container.getCurrentJsonVersion()) {
      case Container.JSON_VERSION_1:
        assertEquals("encrypted wrong", ENCRYPTED_DEFAULT_VALUE, container.isEncrypted());
        assertEquals("previouslyEncrypted wrong", PREVIOUSLY_ENCRYPTED_DEFAULT_VALUE,
            container.wasPreviouslyEncrypted());
        break;
      case Container.JSON_VERSION_2:
        assertEquals("encrypted wrong", encrypted, container.isEncrypted());
        assertEquals("previouslyEncrypted wrong", previouslyEncrypted, container.wasPreviouslyEncrypted());
        break;
      default:
        throw new IllegalStateException("Unsupported version: " + Container.getCurrentJsonVersion());
    }
  }

  /**
   * Asserts that create an {@link Account} fails and throw an exception as expected, when supplying an invalid
   * list of {@link Container}s.
   * @param containers A list of invalid {@link Container}s.
   * @param exceptionClass The class of expected exception.
   */
  private void createAccountWithBadContainersAndFail(List<Container> containers,
      Class<? extends Exception> exceptionClass) throws Exception {
    TestUtils.assertException(exceptionClass,
        () -> new Account(refAccountId, refAccountName, refAccountStatus, refAccountAclInheritedByContainer,
            Account.SNAPSHOT_VERSION_DEFAULT_VALUE, containers, refQuotaResourceType), null);
  }

  /**
   * Asserts that build an {@link Account} will fail because of missing field.
   * @param name The name for the {@link Account} to build.
   * @param status The status for the {@link Account} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildAccountWithMissingFieldsAndFail(String name, AccountStatus status,
      Class<? extends Exception> exceptionClass) throws Exception {
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, name, status);
    TestUtils.assertException(exceptionClass, accountBuilder::build, null);
  }

  /**
   * Asserts that build a {@link Container} will fail because of missing field.
   * @param name The name for the {@link Container} to build.
   * @param status The status for the {@link Container} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildContainerWithBadFieldsAndFail(String name, ContainerStatus status, boolean encrypted,
      boolean previouslyEncrypted, Class<? extends Exception> exceptionClass) throws Exception {
    TestUtils.assertException(exceptionClass, () -> {
      new Container((short) 0, name, status, "description", encrypted, previouslyEncrypted, false, false, null, false,
          false, Collections.emptySet(), false, false, NamedBlobMode.DISABLED, (short) 0, System.currentTimeMillis(),
          System.currentTimeMillis(), 0, null, null, null);
    }, null);
  }

  /**
   * Initializes reference containers.
   */
  private void initializeRefContainers() {
    refContainerIds = new ArrayList<>();
    refContainerNames = new ArrayList<>();
    refContainerStatuses = new ArrayList<>();
    refContainerDescriptions = new ArrayList<>();
    refContainerEncryptionValues = new ArrayList<>();
    refContainerPreviousEncryptionValues = new ArrayList<>();
    refContainerCachingValues = new ArrayList<>();
    refContainerBackupEnabledValues = new ArrayList<>();
    refContainerMediaScanDisabledValues = new ArrayList<>();
    refContainerReplicationPolicyValues = new ArrayList<>();
    refContainerTtlRequiredValues = new ArrayList<>();
    refContainerDeleteTriggerTime = new ArrayList<>();
    refContainerSignedPathRequiredValues = new ArrayList<>();
    refContainerOverrideAccountAcls = new ArrayList<>();
    refContainerNamedBlobModes = new ArrayList<>();
    refContainerContentTypeAllowListForFilenamesOnDownloadValues = new ArrayList<>();
    refContainerLastModifiedTimes = new ArrayList<>();
    refContainerSnapshotVersions = new ArrayList<>();
    refContainers = new ArrayList<>();
    refAccessControlAllowOriginValues = new ArrayList<>();
    refContainerCacheTtlInSeconds = new ArrayList<>();
    refContainerUserMetadataKeysToNotPrefixInResponses = new ArrayList<>();
    Set<Short> containerIdSet = new HashSet<>();
    Set<String> containerNameSet = new HashSet<>();
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      short containerId = Utils.getRandomShort(random);
      String containerName = UUID.randomUUID().toString();
      if (!containerIdSet.add(containerId) || !containerNameSet.add(containerName)) {
        i--;
        continue;
      }
      refContainerIds.add(containerId);
      refContainerNames.add(containerName);
      refContainerStatuses.add(random.nextBoolean() ? ContainerStatus.ACTIVE : ContainerStatus.INACTIVE);
      refContainerDescriptions.add(UUID.randomUUID().toString());
      boolean encrypted = (i % 2 == 0);
      boolean previouslyEncrypted = encrypted || (i % 4 < 2);
      refContainerEncryptionValues.add(encrypted);
      refContainerPreviousEncryptionValues.add(previouslyEncrypted);
      refContainerCachingValues.add(random.nextBoolean());
      refContainerBackupEnabledValues.add(random.nextBoolean());
      refContainerMediaScanDisabledValues.add(random.nextBoolean());
      if (refContainerReplicationPolicyValues.contains(null)) {
        refContainerReplicationPolicyValues.add(TestUtils.getRandomString(10));
      } else {
        refContainerReplicationPolicyValues.add(null);
      }
      refContainerTtlRequiredValues.add(random.nextBoolean());
      refContainerSignedPathRequiredValues.add(random.nextBoolean());
      refContainerOverrideAccountAcls.add(random.nextBoolean());
      refContainerNamedBlobModes.add(random.nextBoolean() ? NamedBlobMode.DISABLED : NamedBlobMode.OPTIONAL);
      refContainerDeleteTriggerTime.add((long) 0);
      if (i == 0) {
        refContainerContentTypeAllowListForFilenamesOnDownloadValues.add(null);
        refContainerUserMetadataKeysToNotPrefixInResponses.add(null);
      } else if (i == 1) {
        refContainerContentTypeAllowListForFilenamesOnDownloadValues.add(Collections.emptySet());
        refContainerUserMetadataKeysToNotPrefixInResponses.add(Collections.emptySet());
      } else {
        refContainerContentTypeAllowListForFilenamesOnDownloadValues.add(getRandomStringSet());
        refContainerUserMetadataKeysToNotPrefixInResponses.add(getRandomStringSet());
      }
      refContainerLastModifiedTimes.add(System.currentTimeMillis());
      refContainerSnapshotVersions.add(random.nextInt());
      if (i == 0) {
        refAccessControlAllowOriginValues.add("*");
        refContainerCacheTtlInSeconds.add(null);
      } else if (i == 1) {
        refAccessControlAllowOriginValues.add("");
        refContainerCacheTtlInSeconds.add(null);
      } else {
        refAccessControlAllowOriginValues.add("https://" + TestUtils.getRandomString(10) + ".com");
        refContainerCacheTtlInSeconds.add(System.currentTimeMillis());
      }
      refContainers.add(new Container(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
          refContainerDescriptions.get(i), refContainerEncryptionValues.get(i),
          refContainerPreviousEncryptionValues.get(i), refContainerCachingValues.get(i),
          refContainerMediaScanDisabledValues.get(i), refContainerReplicationPolicyValues.get(i),
          refContainerTtlRequiredValues.get(i), refContainerSignedPathRequiredValues.get(i),
          refContainerContentTypeAllowListForFilenamesOnDownloadValues.get(i), refContainerBackupEnabledValues.get(i),
          refContainerOverrideAccountAcls.get(i), refContainerNamedBlobModes.get(i), refAccountId,
          refContainerDeleteTriggerTime.get(i), refContainerLastModifiedTimes.get(i),
          refContainerSnapshotVersions.get(i), refAccessControlAllowOriginValues.get(i),
          refContainerCacheTtlInSeconds.get(i), refContainerUserMetadataKeysToNotPrefixInResponses.get(i)));
    }
  }

  /**
   * Initializes reference datasets.
   */
  private void initializeRefDatasets() {
    refDatasetNames = new ArrayList<>();
    refDatasetVersionSchemas = new ArrayList<>();
    refDatasetRetentionTimeInSeconds = new ArrayList<>();
    refDatasetRetentionCounts = new ArrayList<>();
    refDatasetUserTags = new ArrayList<>();
    refDatasets = new ArrayList<>();

    for (int i = 0; i < DATASET_COUNT; i++) {
      String datasetName = UUID.randomUUID().toString();
      refDatasetNames.add(datasetName);
      refDatasetVersionSchemas.add(
          random.nextBoolean() ? Dataset.VersionSchema.MONOTONIC : Dataset.VersionSchema.TIMESTAMP);
      long retentionTimeInSeconds = random.nextBoolean() ? -1 : Utils.getRandomLong(random, Long.MAX_VALUE);
      refDatasetRetentionTimeInSeconds.add(retentionTimeInSeconds);
      int retentionCount = Utils.getRandomShort(random);
      refDatasetRetentionCounts.add(retentionCount);
      Map<String, String> userTags = new HashMap<>();
      userTags.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
      refDatasetUserTags.add(userTags);
      refDatasets.add(new Dataset(refAccountName, refContainers.get(0).getName(), refDatasetNames.get(i),
          refDatasetVersionSchemas.get(i), refDatasetRetentionCounts.get(i), refDatasetRetentionTimeInSeconds.get(i),
          refDatasetUserTags.get(i)));
    }
  }

  /**
   * @return a random set of strings
   */
  private Set<String> getRandomStringSet() {
    Set<String> toRet = new HashSet<>();
    IntStream.range(0, random.nextInt(10) + 1).boxed().forEach(i -> toRet.add(TestUtils.getRandomString(10)));
    return toRet;
  }
}

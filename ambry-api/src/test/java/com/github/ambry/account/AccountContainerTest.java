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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link Account}, {@link Container}, {@link AccountBuilder}, and {@link ContainerBuilder}.
 */
@RunWith(Parameterized.class)
public class AccountContainerTest {
  private static final Random random = new Random();
  private static final int CONTAINER_COUNT = 10;
  private static final short LATEST_CONTAINER_JSON_VERSION = Container.JSON_VERSION_2;

  // Reference Account fields
  private short refAccountId;
  private String refAccountName;
  private AccountStatus refAccountStatus;
  private int refAccountSnapshotVersion = SNAPSHOT_VERSION_DEFAULT_VALUE;
  private JSONObject refAccountJson;

  // Reference Container fields
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
  private List<Boolean> refContainerSignedPathRequiredValues;
  private List<Set<String>> refContainerContentTypeWhitelistForFilenamesOnDownloadValues;
  private List<JSONObject> containerJsonList;
  private List<Container> refContainers;

  /**
   * Run this test for all versions of the container schema.
   * @return the constructor arguments to use.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{Container.JSON_VERSION_1}, {Container.JSON_VERSION_2}});
  }

  /**
   * Initialize the metadata in JsonObject for account and container.
   * @param containerJsonVersion the container JSON version to use in the test.
   * @throws JSONException
   */
  public AccountContainerTest(short containerJsonVersion) throws JSONException {
    Container.setCurrentJsonVersion(containerJsonVersion);
    refAccountId = Utils.getRandomShort(random);
    refAccountName = UUID.randomUUID().toString();
    refAccountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
    refAccountSnapshotVersion = random.nextInt();
    initializeRefContainers();
    refAccountJson = new JSONObject();
    refAccountJson.put(Account.JSON_VERSION_KEY, Account.JSON_VERSION_1);
    refAccountJson.put(ACCOUNT_ID_KEY, refAccountId);
    refAccountJson.put(ACCOUNT_NAME_KEY, refAccountName);
    refAccountJson.put(Account.STATUS_KEY, refAccountStatus.name());
    refAccountJson.put(SNAPSHOT_VERSION_KEY, refAccountSnapshotVersion);
    refAccountJson.put(CONTAINERS_KEY, containerJsonList);
  }

  /**
   * Tests constructing an {@link Account} from Json metadata.
   */
  @Test
  public void testConstructAccountFromJson() {
    assertAccountAgainstReference(Account.fromJson(refAccountJson), true, true);
  }

  /**
   * Tests constructing {@link Account} and {@link Container} using individual arguments.
   */
  @Test
  public void testConstructAccountAndContainerFromArguments() throws JSONException {
    Account accountFromArguments =
        new Account(refAccountId, refAccountName, refAccountStatus, refAccountSnapshotVersion, refContainers);
    assertAccountAgainstReference(accountFromArguments, true, true);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated name.
   */
  @Test
  public void testDuplicateContainerName() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    // second container with (id=1, name="0")
    containers.add(
        new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id.
   */
  @Test
  public void testDuplicateContainerId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    // second container with (id=0, name="1")
    containers.add(
        new ContainerBuilder((short) 0, "1", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id and name.
   */
  @Test
  public void testDuplicateContainerNameAndId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(
        new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    // second container with (id=1, name="0")
    containers.add(
        new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0), refAccountId)
            .setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build());
    // third container with (id=10, name="10")
    containers.add(new ContainerBuilder((short) 10, "10", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refAccountId).setEncrypted(refContainerEncryptionValues.get(0))
        .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
        .setCacheable(refContainerCachingValues.get(0))
        .setBackupEnabled(refContainerBackupEnabledValues.get(0))
        .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
        .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
        .setTtlRequired(refContainerTtlRequiredValues.get(0))
        .setContentTypeWhitelistForFilenamesOnDownload(
            refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
        .build());
    // second container with (id=10, name="11")
    containers.add(new ContainerBuilder((short) 10, "11", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refAccountId).setEncrypted(refContainerEncryptionValues.get(0))
        .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
        .setCacheable(refContainerCachingValues.get(0))
        .setBackupEnabled(refContainerBackupEnabledValues.get(0))
        .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
        .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
        .setTtlRequired(refContainerTtlRequiredValues.get(0))
        .setContentTypeWhitelistForFilenamesOnDownload(
            refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
        .build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing a {@link Container} from json object.
   */
  @Test
  public void testConstructContainerFromJson() throws JSONException {
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container containerFromJson = Container.fromJson(containerJsonList.get(i), refAccountId);
      assertContainer(containerFromJson, i);
    }
  }

  /**
   * Tests in an {@link AccountBuilder} the account id mismatches with container id.
   */
  @Test
  public void testMismatchForAccountId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // container with parentAccountId = refAccountId + 1
    containers.add(new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerDescriptions.get(0), (short) (refAccountId + 1)).setEncrypted(refContainerEncryptionValues.get(0))
        .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
        .setCacheable(refContainerCachingValues.get(0))
        .setBackupEnabled(refContainerBackupEnabledValues.get(0))
        .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
        .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
        .setTtlRequired(refContainerTtlRequiredValues.get(0))
        .setContentTypeWhitelistForFilenamesOnDownload(
            refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
        .build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests bad inputs for constructors or methods.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void badInputs() throws Exception {
    // null account metadata
    TestUtils.assertException(IllegalArgumentException.class, () -> Account.fromJson(null), null);

    // account metadata in wrong format
    JSONObject badMetadata1 = new JSONObject().put("badKey", "badValue");
    TestUtils.assertException(JSONException.class, () -> Account.fromJson(badMetadata1), null);

    // required fields are missing in the metadata
    JSONObject badMetadata2 = deepCopy(refAccountJson);
    badMetadata2.remove(ACCOUNT_ID_KEY);
    TestUtils.assertException(JSONException.class, () -> Account.fromJson(badMetadata2), null);

    // unsupported account json version
    JSONObject badMetadata3 = deepCopy(refAccountJson).put(Account.JSON_VERSION_KEY, 2);
    TestUtils.assertException(IllegalStateException.class, () -> Account.fromJson(badMetadata3), null);

    // invalid account status
    JSONObject badMetadata4 = deepCopy(refAccountJson).put(Account.STATUS_KEY, "invalidAccountStatus");
    TestUtils.assertException(IllegalArgumentException.class, () -> Account.fromJson(badMetadata4), null);

    // null container metadata
    TestUtils.assertException(IllegalArgumentException.class, () -> Container.fromJson(null, refAccountId), null);

    // invalid container status
    JSONObject badMetadata5 = deepCopy(containerJsonList.get(0)).put(Container.STATUS_KEY, "invalidContainerStatus");
    TestUtils.assertException(IllegalArgumentException.class, () -> Container.fromJson(badMetadata5, refAccountId),
        null);

    // required fields are missing.
    JSONObject badMetadata6 = deepCopy(containerJsonList.get(0));
    badMetadata6.remove(CONTAINER_ID_KEY);
    TestUtils.assertException(JSONException.class, () -> Container.fromJson(badMetadata6, refAccountId), null);

    // unsupported container json version
    JSONObject badMetadata7 =
        deepCopy(containerJsonList.get(0)).put(Container.JSON_VERSION_KEY, LATEST_CONTAINER_JSON_VERSION + 1);
    TestUtils.assertException(IllegalStateException.class, () -> Container.fromJson(badMetadata7, refAccountId), null);
  }

  /**
   * Tests {@code toString()} methods.
   * @throws JSONException
   */
  @Test
  public void testToString() throws JSONException {
    Account account = Account.fromJson(refAccountJson);
    assertEquals("Account[" + account.getId() + "," + account.getSnapshotVersion() + "]", account.toString());
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = Container.fromJson(containerJsonList.get(i), refAccountId);
      assertEquals("Container[" + account.getId() + ":" + container.getId() + "]", container.toString());
    }
  }

  // Tests for builders

  /**
   * Tests building an {@link Account} using {@link AccountBuilder}.
   * @throws JSONException
   */
  @Test
  public void testAccountBuilder() throws JSONException {
    // build an account with arguments supplied
    AccountBuilder accountBuilder =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus).snapshotVersion(refAccountSnapshotVersion);
    Account accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, false, false);

    // set containers
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = Container.fromJson(containerJsonList.get(i), refAccountId);
      containers.add(container);
      accountBuilder.addOrUpdateContainer(container);
    }
    accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, true, true);

    // build an account from existing account
    accountBuilder = new AccountBuilder(accountByBuilder);
    Account account2ByBuilder = accountBuilder.build();
    assertAccountAgainstReference(account2ByBuilder, true, true);

    // clear containers
    Account account3ByBuilder = new AccountBuilder(account2ByBuilder).containers(null).build();
    assertAccountAgainstReference(account3ByBuilder, false, false);
    assertTrue("Container list should be empty.", account3ByBuilder.getAllContainers().isEmpty());
  }

  /**
   * Tests building a {@link Container} using {@link ContainerBuilder}.
   * @throws JSONException
   */
  @Test
  public void testContainerBuilder() throws JSONException {
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      // build a container with arguments supplied
      ContainerBuilder containerBuilder =
          new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refAccountId).setEncrypted(refContainerEncryptionValues.get(i))
              .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(i))
              .setCacheable(refContainerCachingValues.get(i))
              .setBackupEnabled(refContainerBackupEnabledValues.get(i))
              .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(i))
              .setReplicationPolicy(refContainerReplicationPolicyValues.get(i))
              .setTtlRequired(refContainerTtlRequiredValues.get(i))
              .setSecurePathRequired(refContainerSignedPathRequiredValues.get(i))
              .setContentTypeWhitelistForFilenamesOnDownload(
                  refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(i));
      Container containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i);

      // build a container from existing container
      containerBuilder = new ContainerBuilder(containerFromBuilder);
      containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i);

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
   * @throws JSONException
   */
  @Test
  public void testUpdateAccount() throws JSONException {
    // set an account with different field value
    Account origin = Account.fromJson(refAccountJson);
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
  public void testRemovingContainers() throws JSONException {
    Account origin = Account.fromJson(refAccountJson);
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
   * @throws JSONException
   */
  @Test
  public void testUpdateContainerInAccount() throws JSONException {
    Account account = Account.fromJson(refAccountJson);
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
      Set<String> updatedContentTypeWhitelistForFilenamesOnDownloadValues =
          container.getContentTypeWhitelistForFilenamesOnDownload()
              .stream()
              .map(contentType -> contentType + "--updated")
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
          .setContentTypeWhitelistForFilenamesOnDownload(updatedContentTypeWhitelistForFilenamesOnDownloadValues);
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
          assertEquals("Wrong content type whitelist for filenames on download value",
              CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD_DEFAULT_VALUE,
              updatedContainer.getContentTypeWhitelistForFilenamesOnDownload());
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
          assertEquals("Wrong content type whitelist for filenames on download value",
              updatedContentTypeWhitelistForFilenamesOnDownloadValues,
              updatedContainer.getContentTypeWhitelistForFilenamesOnDownload());
          break;
        default:
          throw new IllegalStateException("Unsupported version: " + Container.getCurrentJsonVersion());
      }
    }
  }

  /**
   * Tests updating the parent account id for a container.
   * @throws JSONException
   */
  @Test
  public void testUpdateContainerParentAccountId() throws JSONException {
    ContainerBuilder containerBuilder =
        new ContainerBuilder(Container.fromJson(containerJsonList.get(0), refAccountId));
    short newParentAccountId = (short) (refAccountId + 1);
    containerBuilder.setParentAccountId(newParentAccountId);
    assertEquals("Container's parent account id is incorrectly updated.", newParentAccountId,
        containerBuilder.build().getParentAccountId());
  }

  /**
   * Tests removing a non-existent container from accountBuilder.
   * @throws JSONException
   */
  @Test
  public void testRemoveNonExistContainer() throws JSONException {
    Account origin = Account.fromJson(refAccountJson);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    ContainerBuilder containerBuilder =
        new ContainerBuilder((short) -999, refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), refAccountId).setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0));
    Container container = containerBuilder.build();
    accountBuilder.removeContainer(container);
    accountBuilder.removeContainer(null);
    Account account = accountBuilder.build();
    assertAccountAgainstReference(account, true, true);
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
    assertTrue("Two accounts should be equal.", accountNoContainer.equals(accountNoContainerDuplicate));

    // Check two accounts with same fields and containers.
    Account accountWithContainers = Account.fromJson(refAccountJson);
    Account accountWithContainersDuplicate = Account.fromJson(refAccountJson);
    assertTrue("Two accounts should be equal.", accountWithContainers.equals(accountWithContainersDuplicate));

    // Check two accounts with same fields but one has containers, the other one does not.
    assertFalse("Two accounts should not be equal.", accountNoContainer.equals(accountWithContainers));

    // Check two accounts with the same fields and the same number of containers. One container of one account has one
    // field different from the other one.
    Container updatedContainer =
        new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
            "A changed container description", refAccountId).setEncrypted(refContainerEncryptionValues.get(0))
            .setPreviouslyEncrypted(refContainerPreviousEncryptionValues.get(0))
            .setCacheable(refContainerCachingValues.get(0))
            .setBackupEnabled(refContainerBackupEnabledValues.get(0))
            .setMediaScanDisabled(refContainerMediaScanDisabledValues.get(0))
            .setReplicationPolicy(refContainerReplicationPolicyValues.get(0))
            .setTtlRequired(refContainerTtlRequiredValues.get(0))
            .setContentTypeWhitelistForFilenamesOnDownload(
                refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(0))
            .build();
    refContainers.remove(0);
    refContainers.add(updatedContainer);
    Account accountWithModifiedContainers = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    assertFalse("Two accounts should not be equal.", accountWithContainers.equals(accountWithModifiedContainers));
  }

  /**
   * Asserts an {@link Account} against the reference account.
   * @param account The {@link Account} to assert.
   * @param compareMetadata {@code true} to compare account metadata generated from {@link Account#toJson(boolean)}, and
   *                                    also serialize then deserialize to get an identical account. {@code false} to
   *                                    skip these tests.
   * @param compareContainer {@code true} to compare each individual {@link Container}. {@code false} to skip this test.
   * @throws JSONException
   */
  private void assertAccountAgainstReference(Account account, boolean compareMetadata, boolean compareContainer)
      throws JSONException {
    assertEquals(refAccountId, account.getId());
    assertEquals(refAccountName, account.getName());
    assertEquals(refAccountStatus, account.getStatus());
    assertEquals("Snapshot versions do not match", refAccountSnapshotVersion, account.getSnapshotVersion());
    if (compareMetadata) {
      assertAccountJsonSerDe(false, account);
      assertAccountJsonSerDe(true, account);
    }
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
   * Assert that JSON ser/de is working correctly by comparing against the reference account JSON
   * @param incrementSnapshotVersion {@code true} to increment the snapshot version when serializing.
   * @param account the {@link Account} to test.
   * @throws JSONException
   */
  private void assertAccountJsonSerDe(boolean incrementSnapshotVersion, Account account) throws JSONException {
    assertEquals("Failed to compare account to a reference account", Account.fromJson(refAccountJson), account);
    JSONObject expectedAccountJson = deepCopy(refAccountJson);
    if (incrementSnapshotVersion) {
      expectedAccountJson.put(SNAPSHOT_VERSION_KEY, refAccountSnapshotVersion + 1);
    }
    JSONObject accountJson = account.toJson(incrementSnapshotVersion);
    // extra check for snapshot version since the lengths would likely not differ even if the snapshot version was not
    // correct
    assertEquals("Snapshot versions in JSON do not match", expectedAccountJson.get(SNAPSHOT_VERSION_KEY),
        accountJson.get(SNAPSHOT_VERSION_KEY));
    // The order of containers in json string may be different, so we cannot compare the exact string.
    assertEquals("Wrong metadata JsonObject from toJson()", expectedAccountJson.toString().length(),
        accountJson.toString().length());

    AccountBuilder expectedAccountBuilder = new AccountBuilder(account);
    if (incrementSnapshotVersion) {
      expectedAccountBuilder.snapshotVersion(refAccountSnapshotVersion + 1);
    }
    assertEquals("Wrong behavior in serialize and then deserialize", expectedAccountBuilder.build(),
        Account.fromJson(account.toJson(incrementSnapshotVersion)));
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field, {@link Container#toJson()}
   * method, and also asserts the same object after serialize and then deserialize.
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   * @throws JSONException
   */
  private void assertContainer(Container container, int index) throws JSONException {
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
        Set<String> expectedContentTypeWhitelistForFilenamesOnDownloadValue =
            refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(index) == null ? Collections.emptySet()
                : refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(index);
        assertEquals("Wrong content types whitelisted for filename on download",
            expectedContentTypeWhitelistForFilenamesOnDownloadValue,
            container.getContentTypeWhitelistForFilenamesOnDownload());
        break;
      default:
        throw new IllegalStateException("Unsupported version: " + Container.getCurrentJsonVersion());
    }
    assertEquals("Serialization error", containerJsonList.get(index).toString(), container.toJson().toString());
    assertEquals("Serde chain error", Container.fromJson(container.toJson(), refAccountId), container);
  }

  /**
   * Check the value of the encryption settings and json serde for a container.
   * @param container the {@link Container} to check.
   * @param encrypted the expected encrypted setting.
   * @param previouslyEncrypted the expected previouslyEncrypted setting.
   */
  private void assertEncryptionSettings(Container container, boolean encrypted, boolean previouslyEncrypted)
      throws JSONException {
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
    assertEquals("Deserialization failed", container, Container.fromJson(buildContainerJson(container), refAccountId));
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
        () -> new Account(refAccountId, refAccountName, refAccountStatus, SNAPSHOT_VERSION_DEFAULT_VALUE, containers),
        null);
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
          false, Collections.emptySet(), false, (short) 0);
    }, null);
  }

  /**
   * Initializes reference containers.
   * @throws JSONException
   */
  private void initializeRefContainers() throws JSONException {
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
    refContainerSignedPathRequiredValues = new ArrayList<>();
    refContainerContentTypeWhitelistForFilenamesOnDownloadValues = new ArrayList<>();
    containerJsonList = new ArrayList<>();
    refContainers = new ArrayList<>();
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
      if (i == 0) {
        refContainerContentTypeWhitelistForFilenamesOnDownloadValues.add(null);
      } else if (i == 1) {
        refContainerContentTypeWhitelistForFilenamesOnDownloadValues.add(Collections.emptySet());
      } else {
        refContainerContentTypeWhitelistForFilenamesOnDownloadValues.add(
            getRandomContentTypeWhitelistForFilenamesOnDownload());
      }
      refContainers.add(new Container(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
          refContainerDescriptions.get(i), refContainerEncryptionValues.get(i),
          refContainerPreviousEncryptionValues.get(i), refContainerCachingValues.get(i),
          refContainerMediaScanDisabledValues.get(i), refContainerReplicationPolicyValues.get(i),
          refContainerTtlRequiredValues.get(i), refContainerSignedPathRequiredValues.get(i),
          refContainerContentTypeWhitelistForFilenamesOnDownloadValues.get(i), refContainerBackupEnabledValues.get(i),
          refAccountId));
      containerJsonList.add(buildContainerJson(refContainers.get(i)));
    }
  }

  /**
   * @return a random set of strings
   */
  private Set<String> getRandomContentTypeWhitelistForFilenamesOnDownload() {
    Set<String> toRet = new HashSet<>();
    IntStream.range(0, random.nextInt(10) + 1).boxed().forEach(i -> toRet.add(TestUtils.getRandomString(10)));
    return toRet;
  }

  /**
   * Construct a container JSON object in the version specified by {@link Container#getCurrentJsonVersion()}.
   * @param container The {@link Container} to serialize.
   * @return the {@link JSONObject}
   */
  private JSONObject buildContainerJson(Container container) throws JSONException {
    JSONObject containerJson = new JSONObject();
    switch (Container.getCurrentJsonVersion()) {
      case Container.JSON_VERSION_1:
        containerJson.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_1);
        containerJson.put(CONTAINER_ID_KEY, container.getId());
        containerJson.put(CONTAINER_NAME_KEY, container.getName());
        containerJson.put(Container.STATUS_KEY, container.getStatus().name());
        containerJson.put(DESCRIPTION_KEY, container.getDescription());
        containerJson.put(IS_PRIVATE_KEY, !container.isCacheable());
        containerJson.put(PARENT_ACCOUNT_ID_KEY, container.getParentAccountId());
        break;
      case Container.JSON_VERSION_2:
        containerJson.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_2);
        containerJson.put(CONTAINER_ID_KEY, container.getId());
        containerJson.put(CONTAINER_NAME_KEY, container.getName());
        containerJson.put(Container.STATUS_KEY, container.getStatus().name());
        containerJson.put(DESCRIPTION_KEY, container.getDescription());
        containerJson.put(ENCRYPTED_KEY, container.isEncrypted());
        containerJson.put(PREVIOUSLY_ENCRYPTED_KEY, container.wasPreviouslyEncrypted());
        containerJson.put(CACHEABLE_KEY, container.isCacheable());
        containerJson.put(BACKUP_ENABLED_KEY, container.isBackupEnabled());
        containerJson.put(MEDIA_SCAN_DISABLED_KEY, container.isMediaScanDisabled());
        containerJson.putOpt(REPLICATION_POLICY_KEY, container.getReplicationPolicy());
        containerJson.put(TTL_REQUIRED_KEY, container.isTtlRequired());
        containerJson.put(SECURE_PATH_REQUIRED_KEY, container.isSecurePathRequired());
        if (container.getContentTypeWhitelistForFilenamesOnDownload() != null
            && !container.getContentTypeWhitelistForFilenamesOnDownload().isEmpty()) {
          containerJson.put(CONTENT_TYPE_WHITELIST_FOR_FILENAMES_ON_DOWNLOAD,
              container.getContentTypeWhitelistForFilenamesOnDownload());
        }
        break;
      default:
        throw new IllegalStateException("Unsupported container json version=" + Container.getCurrentJsonVersion());
    }
    return containerJson;
  }

  /**
   * @param original the {@link JSONObject} to deep copy.
   * @return a deep copy of {@code original}.
   * @throws JSONException
   */
  private static JSONObject deepCopy(JSONObject original) throws JSONException {
    return new JSONObject(original.toString());
  }
}

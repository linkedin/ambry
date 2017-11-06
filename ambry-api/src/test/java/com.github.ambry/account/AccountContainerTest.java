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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link Account}, {@link Container}, {@link AccountBuilder}, and {@link ContainerBuilder}.
 */
public class AccountContainerTest {
  private static final Random random = new Random();
  private static final int CONTAINER_COUNT = 10;

  // Reference Account fields
  private short refAccountId;
  private String refAccountName;
  private AccountStatus refAccountStatus;
  private JSONObject accountJsonLike;

  // Reference Container fields
  private List<Short> refContainerIds;
  private List<String> refContainerNames;
  private List<String> refContainerDescriptions;
  private List<ContainerStatus> refContainerStatuses;
  private List<Boolean> refContainerEncryptionValues;
  private List<Boolean> refContainerPreviousEncryptionValues;
  private List<Boolean> refContainerCachingValues;
  private List<Boolean> refContainerMediaScanDisabledValues;
  private List<JSONObject> containerJsonLikeList;
  private List<Container> refContainers;

  /**
   * Initialize the metadata in JsonObject for account and container.
   * @throws JSONException
   */
  @Before
  public void init() throws JSONException {
    refAccountId = Utils.getRandomShort(random);
    refAccountName = UUID.randomUUID().toString();
    refAccountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
    accountJsonLike = new JSONObject();
    accountJsonLike.put(Account.JSON_VERSION_KEY, Account.JSON_VERSION_1);
    accountJsonLike.put(ACCOUNT_ID_KEY, refAccountId);
    accountJsonLike.put(ACCOUNT_NAME_KEY, refAccountName);
    accountJsonLike.put(Account.STATUS_KEY, refAccountStatus.name());
    accountJsonLike.put(CONTAINERS_KEY, getContainerArray());
  }

  /**
   * Tests constructing an {@link Account} from Json metadata.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testConstructAccountFromJson() throws Exception {
    assertAccountAgainstReference(Account.fromJson(accountJsonLike), true, true);
  }

  /**
   * Tests constructing {@link Account} and {@link Container} using individual arguments.
   */
  @Test
  public void testConstructAccountAndContainerFromArguments() throws JSONException {
    Account accountFromArguments =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, refContainers).build();
    assertAccountAgainstReference(accountFromArguments, true, true);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated name.
   */
  @Test
  public void testDuplicateContainerName() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    // second container with (id=1, name="0")
    containers.add(new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id.
   */
  @Test
  public void testDuplicateContainerId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    // second container with (id=0, name="1")
    containers.add(new ContainerBuilder((short) 0, "1", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id and name.
   */
  @Test
  public void testDuplicateContainerNameAndId() throws Exception {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    // second container with (id=1, name="0")
    containers.add(new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    // third container with (id=10, name="10")
    containers.add(new ContainerBuilder((short) 10, "10", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    // second container with (id=10, name="11")
    containers.add(new ContainerBuilder((short) 10, "11", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerEncryptionValues.get(0), refContainerPreviousEncryptionValues.get(0),
        refContainerCachingValues.get(0), refContainerMediaScanDisabledValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing a {@link Container} from json object.
   */
  @Test
  public void testConstructContainerFromJson() throws JSONException {
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      // container generated from amended v1 JSON
      Container containerFromJson = Container.fromJson(containerJsonLikeList.get(i));
      assertContainer(containerFromJson, i, false);
      // container generated from original v1 JSON
      containerFromJson = Container.fromJson(buildV1ContainerJson(i));
      assertContainer(containerFromJson, i, true);
      // container generated from v2 JSON
      containerFromJson = Container.fromJson(buildV2ContainerJson(i));
      assertContainer(containerFromJson, i, false);
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
        refContainerDescriptions.get(0), refContainerEncryptionValues.get(0),
        refContainerPreviousEncryptionValues.get(0), refContainerCachingValues.get(0),
        refContainerMediaScanDisabledValues.get(0), (short) (refAccountId + 1)).build());
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
    JSONObject badMetadata2 = deepCopy(accountJsonLike);
    badMetadata2.remove(ACCOUNT_ID_KEY);
    TestUtils.assertException(JSONException.class, () -> Account.fromJson(badMetadata2), null);

    // unsupported account json version
    JSONObject badMetadata3 = deepCopy(accountJsonLike).put(Account.JSON_VERSION_KEY, 2);
    TestUtils.assertException(IllegalStateException.class, () -> Account.fromJson(badMetadata3), null);

    // invalid account status
    JSONObject badMetadata4 = deepCopy(accountJsonLike).put(Account.STATUS_KEY, "invalidAccountStatus");
    TestUtils.assertException(IllegalArgumentException.class, () -> Account.fromJson(badMetadata4), null);

    // null container metadata
    TestUtils.assertException(IllegalArgumentException.class, () -> Container.fromJson(null), null);

    // invalid container status
    JSONObject badMetadata5 =
        deepCopy(containerJsonLikeList.get(0)).put(Container.STATUS_KEY, "invalidContainerStatus");
    TestUtils.assertException(IllegalArgumentException.class, () -> Container.fromJson(badMetadata5), null);

    // required fields are missing.
    JSONObject badMetadata6 = deepCopy(containerJsonLikeList.get(0));
    badMetadata6.remove(CONTAINER_ID_KEY);
    TestUtils.assertException(JSONException.class, () -> Container.fromJson(badMetadata6), null);

    JSONObject badMetadata7 = buildV2ContainerJson(0);
    badMetadata7.remove(ENCRYPTED_KEY);
    TestUtils.assertException(JSONException.class, () -> Container.fromJson(badMetadata7), null);

    // unsupported container json version
    JSONObject badMetadata8 = deepCopy(containerJsonLikeList.get(0)).put(Container.JSON_VERSION_KEY, 3);
    TestUtils.assertException(IllegalStateException.class, () -> Container.fromJson(badMetadata8), null);
  }

  /**
   * Tests {@code toString()} methods.
   * @throws JSONException
   */
  @Test
  public void testToString() throws JSONException {
    Account account = Account.fromJson(accountJsonLike);
    assertEquals("Account[" + account.getId() + "]", account.toString());
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = Container.fromJson(containerJsonLikeList.get(i));
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
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, refAccountName, refAccountStatus, null);
    Account accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, false, false);

    // set containers
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container container = Container.fromJson(containerJsonLikeList.get(i));
      containers.add(container);
      accountBuilder.addOrUpdateContainer(container);
    }
    accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, true, true);

    // build an account from existing account
    accountBuilder = new AccountBuilder(accountByBuilder);
    Account account2ByBuilder = accountBuilder.build();
    assertAccountAgainstReference(account2ByBuilder, true, true);
  }

  /**
   * Tests building a {@link Container} using {@link ContainerBuilder}.
   * @throws JSONException
   */
  @Test
  public void testContainerBuilder() throws JSONException {
    ContainerBuilder containerBuilder;
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      // build a container with arguments supplied
      containerBuilder =
          new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refContainerEncryptionValues.get(i),
              refContainerPreviousEncryptionValues.get(i), refContainerCachingValues.get(i),
              refContainerMediaScanDisabledValues.get(i), refAccountId);
      Container containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i, false);

      // build a container from existing container
      containerBuilder = new ContainerBuilder(containerFromBuilder);
      containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i, false);

      // test changing encryption setting through builder.
      if (containerFromBuilder.isEncrypted()) {
        // turn off and on, check that previouslyEncrypted is not set,
        // as the builder should allow multiple sets before building.
        containerFromBuilder =
            new ContainerBuilder(containerFromBuilder).setEncrypted(false).setEncrypted(true).build();
        assertEncryptionSettings(containerFromBuilder, true, true);
        // turn off encryption, check that previouslyEncrypted is set.
        containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(false).build();
        assertEncryptionSettings(containerFromBuilder, false, true);
        // turn it back on, previouslyEncrypted should still be set.
        containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(true).build();
        assertEncryptionSettings(containerFromBuilder, true, true);
        // turn off again, previouslyEncrypted should still be set.
        containerFromBuilder = new ContainerBuilder(containerFromBuilder).setEncrypted(false).build();
        assertEncryptionSettings(containerFromBuilder, false, true);
      }
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
  public void testFieldMissingToBuildContainer() throws Exception {
    // test when required fields are null
    buildContainerWithMissingFieldsAndFail(null, refContainerStatuses.get(0), IllegalStateException.class);
    buildContainerWithMissingFieldsAndFail(refContainerNames.get(0), null, IllegalStateException.class);
  }

  /**
   * Tests update an {@link Account}.
   * @throws JSONException
   */
  @Test
  public void testUpdateAccount() throws JSONException {
    // set an account with different field value
    Account origin = Account.fromJson(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    short updatedAccountId = (short) (refAccountId + 1);
    String updatedAccountName = refAccountName + "-updated";
    Account.AccountStatus updatedAccountStatus = Account.AccountStatus.INACTIVE;
    accountBuilder.setId(updatedAccountId);
    accountBuilder.setName(updatedAccountName);
    accountBuilder.setStatus(updatedAccountStatus);

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
    accountBuilder.setId(refAccountId);
    updatedAccount = accountBuilder.build();
    assertEquals(origin.getAllContainers().toString(), updatedAccount.getAllContainers().toString());
  }

  /**
   * Tests removing containers in AccountBuilder.
   */
  @Test
  public void testRemovingContainers() throws JSONException {
    Account origin = Account.fromJson(accountJsonLike);
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
    Account account = Account.fromJson(accountJsonLike);
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
      containerBuilder.setId(updatedContainerId)
          .setName(updatedContainerName)
          .setStatus(updatedContainerStatus)
          .setDescription(updatedContainerDescription);
      Container updatedContainer = containerBuilder.build();
      accountBuilder.addOrUpdateContainer(updatedContainer);

      // build account and assert
      Account updatedAccount = accountBuilder.build();
      assertEquals("container id is not correctly updated", updatedContainerId,
          updatedAccount.getContainerById(updatedContainerId).getId());
      assertEquals("container name is not correctly updated", updatedContainerName,
          updatedAccount.getContainerById(updatedContainerId).getName());
      assertEquals("container status is not correctly updated", updatedContainerStatus,
          updatedAccount.getContainerById(updatedContainerId).getStatus());
      assertEquals("container description is not correctly updated", updatedContainerDescription,
          updatedAccount.getContainerById(updatedContainerId).getDescription());
    }
  }

  /**
   * Tests updating the parent account id for a container.
   * @throws JSONException
   */
  @Test
  public void testUpdateContainerParentAccountId() throws JSONException {
    ContainerBuilder containerBuilder = new ContainerBuilder(Container.fromJson(containerJsonLikeList.get(0)));
    short newParentAccountId = (short) (refContainerIds.get(0) + 1);
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
    Account origin = Account.fromJson(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    ContainerBuilder containerBuilder =
        new ContainerBuilder((short) -999, refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), refContainerEncryptionValues.get(0),
            refContainerPreviousEncryptionValues.get(0), refContainerCachingValues.get(0),
            refContainerMediaScanDisabledValues.get(0), refAccountId);
    Container container = containerBuilder.build();
    accountBuilder.removeContainer(container);
    accountBuilder.removeContainer(null);
    Account account = accountBuilder.build();
    assertAccountAgainstReference(account, true, true);
  }

  /**
   * Tests for {@link Account#UNKNOWN_ACCOUNT}, {@link Container#UNKNOWN_CONTAINER},
   * {@link Container#DEFAULT_PUBLIC_CONTAINER}, and {@link Container#DEFAULT_PRIVATE_CONTAINER}.
   */
  @Test
  public void testUnknownAccountAndContainer() {
    Account unknownAccount = Account.UNKNOWN_ACCOUNT;
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
    // DEFAULT_PUBLIC_CONTAINER
    assertEquals("Wrong id for UNKNOWN_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_ID,
        unknownPublicContainer.getId());
    assertEquals("Wrong name for UNKNOWN_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_NAME,
        unknownPublicContainer.getName());
    assertEquals("Wrong status for UNKNOWN_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_STATUS,
        unknownPublicContainer.getStatus());
    assertEquals("Wrong description for UNKNOWN_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_DESCRIPTION,
        unknownPublicContainer.getDescription());
    assertEquals("Wrong parent account id for UNKNOWN_CONTAINER", Container.DEFAULT_PUBLIC_CONTAINER_PARENT_ACCOUNT_ID,
        unknownPublicContainer.getParentAccountId());
    // DEFAULT_PRIVATE_CONTAINER
    assertEquals("Wrong id for UNKNOWN_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_ID,
        unknownPrivateContainer.getId());
    assertEquals("Wrong name for UNKNOWN_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_NAME,
        unknownPrivateContainer.getName());
    assertEquals("Wrong status for UNKNOWN_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_STATUS,
        unknownPrivateContainer.getStatus());
    assertEquals("Wrong description for UNKNOWN_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_DESCRIPTION,
        unknownPrivateContainer.getDescription());
    assertEquals("Wrong parent account id for UNKNOWN_CONTAINER", Container.DEFAULT_PRIVATE_CONTAINER_PARENT_ACCOUNT_ID,
        unknownPrivateContainer.getParentAccountId());
    // UNKNOWN_ACCOUNT
    assertEquals("Wrong id for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_ID, unknownAccount.getId());
    assertEquals("Wrong name for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_NAME, unknownAccount.getName());
    assertEquals("Wrong status for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_STATUS, unknownAccount.getStatus());
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
   * @throws Exception
   */
  @Test
  public void testAccountEqual() throws Exception {
    // Check two accounts with same fields but no containers.
    Account accountNoContainer = new AccountBuilder(refAccountId, refAccountName, refAccountStatus, null).build();
    Account accountNoContainerDuplicate =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, null).build();
    assertTrue("Two accounts should be equal.", accountNoContainer.equals(accountNoContainerDuplicate));

    // Check two accounts with same fields and containers.
    Account accountWithContainers = Account.fromJson(accountJsonLike);
    Account accountWithContainersDuplicate = Account.fromJson(accountJsonLike);
    assertTrue("Two accounts should be equal.", accountWithContainers.equals(accountWithContainersDuplicate));

    // Check two accounts with same fields but one has containers, the other one does not.
    assertFalse("Two accounts should not be equal.", accountNoContainer.equals(accountWithContainers));

    // Check two accounts with the same fields and the same number of containers. One container of one account has one
    // field different from the other one.
    Container updatedContainer =
        new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
            "A changed container description", refContainerEncryptionValues.get(0),
            refContainerPreviousEncryptionValues.get(0), refContainerCachingValues.get(0),
            refContainerMediaScanDisabledValues.get(0), refAccountId).build();
    refContainers.remove(0);
    refContainers.add(updatedContainer);
    Account accountWithModifiedContainers =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, refContainers).build();
    assertFalse("Two accounts should not be equal.", accountWithContainers.equals(accountWithModifiedContainers));
  }

  /**
   * Asserts an {@link Account} against the reference account.
   * @param account The {@link Account} to assert.
   * @param compareMetadata {@code true} to compare account metadata generated from {@link Account#toJson()}, and also
   *                                    serialize then deserialize to get an identical account. {@code false} to skip
   *                                    these tests.
   * @param compareContainer {@code true} to compare each individual {@link Container}. {@code false} to skip this test.
   * @throws JSONException
   */
  private void assertAccountAgainstReference(Account account, boolean compareMetadata, boolean compareContainer)
      throws JSONException {
    assertEquals(refAccountId, account.getId());
    assertEquals(refAccountName, account.getName());
    assertEquals(refAccountStatus, account.getStatus());
    if (compareMetadata) {
      // The order of containers in json string may be different, so we cannot compare the exact string.
      assertEquals("Wrong metadata JsonObject from toJson()", accountJsonLike.toString().length(),
          account.toJson().toString().length());
      assertEquals("Wrong behavior in serialize and then deserialize", account, Account.fromJson(account.toJson()));
      assertEquals("Failed to compare account to a reference account", Account.fromJson(accountJsonLike), account);
    }
    if (compareContainer) {
      Collection<Container> containersFromAccount = account.getAllContainers();
      assertEquals("Wrong number of containers.", CONTAINER_COUNT, containersFromAccount.size());
      assertEquals(CONTAINER_COUNT, containersFromAccount.size());
      for (int i = 0; i < CONTAINER_COUNT; i++) {
        assertContainer(account.getContainerById(refContainerIds.get(i)), i, false);
        assertContainer(account.getContainerByName(refContainerNames.get(i)), i, false);
      }
    }
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field, {@link Container#toJson()}
   * method, and also asserts the same object after serialize and then deserialize.
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   * @param fromUnamendedV1 If the {@link Container} was deserialized from an unamended V1 record (no extra fields).
   * @throws JSONException
   */
  private void assertContainer(Container container, int index, boolean fromUnamendedV1) throws JSONException {
    assertEquals("Wrong container ID", (short) refContainerIds.get(index), container.getId());
    assertEquals("Wrong name", refContainerNames.get(index), container.getName());
    assertEquals("Wrong status", refContainerStatuses.get(index), container.getStatus());
    assertEquals("Wrong description", refContainerDescriptions.get(index), container.getDescription());
    assertEquals("Wrong caching setting", refContainerCachingValues.get(index), container.isCacheable());
    assertEquals("Wrong account ID", refAccountId, container.getParentAccountId());
    if (fromUnamendedV1) {
      assertEquals("Wrong encryption setting", false, container.isEncrypted());
      assertEquals("Wrong previous encryption setting", false, container.wasPreviouslyEncrypted());
      assertEquals("Wrong media scan disabled setting", false, container.isMediaScanDisabled());
    } else {
      assertEquals("Wrong encryption setting", refContainerEncryptionValues.get(index), container.isEncrypted());
      assertEquals("Wrong previous encryption setting", refContainerPreviousEncryptionValues.get(index),
          container.wasPreviouslyEncrypted());
      assertEquals("Wrong media scan disabled setting", refContainerMediaScanDisabledValues.get(index),
          container.isMediaScanDisabled());
      assertEquals("Serialization error", containerJsonLikeList.get(index).toString(), container.toJson().toString());
      assertEquals("Deserialization error", Container.fromJson(containerJsonLikeList.get(index)), container);
    }
  }

  /**
   * Check the value of the encryption settings and json serde for a container.
   * @param container the {@link Container} to check.
   * @param encrypted the expected encrypted setting.
   * @param previouslyEncrypted the expected previouslyEncrypted setting.
   */
  private void assertEncryptionSettings(Container container, boolean encrypted, boolean previouslyEncrypted)
      throws JSONException {
    assertEquals("encrypted wrong", encrypted, container.isEncrypted());
    assertEquals("previouslyEncrypted wrong", previouslyEncrypted, container.wasPreviouslyEncrypted());
    assertEquals("Serde failed", container, Container.fromJson(container.toJson()));
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
        () -> new Account(refAccountId, refAccountName, refAccountStatus, containers), null);
  }

  /**
   * Asserts that build an {@link Account} will fail because of missing field.
   * @param name The name for the {@link Account} to build.
   * @param status The status for the {@link Account} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildAccountWithMissingFieldsAndFail(String name, AccountStatus status,
      Class<? extends Exception> exceptionClass) throws Exception {
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, name, status, null);
    TestUtils.assertException(exceptionClass, accountBuilder::build, null);
  }

  /**
   * Asserts that build a {@link Container} will fail because of missing field.
   * @param name The name for the {@link Container} to build.
   * @param status The status for the {@link Container} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildContainerWithMissingFieldsAndFail(String name, ContainerStatus status,
      Class<? extends Exception> exceptionClass) throws Exception {
    ContainerBuilder containerBuilder =
        new ContainerBuilder((short) 0, name, status, "description", false, false, false, false, (short) 0);
    TestUtils.assertException(exceptionClass, containerBuilder::build, null);
  }

  /**
   * Initializes reference containers and get a {@link JSONArray} of containers.
   * @return A {@link JSONArray} of containers.
   * @throws JSONException
   */
  private JSONArray getContainerArray() throws JSONException {
    refContainerIds = new ArrayList<>();
    refContainerNames = new ArrayList<>();
    refContainerStatuses = new ArrayList<>();
    refContainerDescriptions = new ArrayList<>();
    refContainerEncryptionValues = new ArrayList<>();
    refContainerPreviousEncryptionValues = new ArrayList<>();
    refContainerCachingValues = new ArrayList<>();
    refContainerMediaScanDisabledValues = new ArrayList<>();
    containerJsonLikeList = new ArrayList<>();
    refContainers = new ArrayList<>();
    JSONArray containerArray = new JSONArray();
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
      boolean encrypted = random.nextBoolean();
      refContainerEncryptionValues.add(encrypted);
      refContainerPreviousEncryptionValues.add(encrypted || random.nextBoolean());
      refContainerCachingValues.add(random.nextBoolean());
      refContainerMediaScanDisabledValues.add(random.nextBoolean());
      JSONObject containerJsonLike = buildAmendedV1ContainerJson(i);
      containerJsonLikeList.add(containerJsonLike);
      containerArray.put(containerJsonLike);
      refContainers.add(
          new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refContainerEncryptionValues.get(i),
              refContainerPreviousEncryptionValues.get(i), refContainerCachingValues.get(i),
              refContainerMediaScanDisabledValues.get(i), refAccountId).build());
    }
    return containerArray;
  }

  /**
   * Construct a V1 JSON object without the new optional fields (encrypted, cacheable, previouslyEncrypted).
   * @param index The index in the reference container list to assert against.
   * @return the {@link JSONObject}
   */
  private JSONObject buildV1ContainerJson(int index) throws JSONException {
    JSONObject containerJson = new JSONObject();
    containerJson.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_1);
    containerJson.put(CONTAINER_ID_KEY, refContainerIds.get(index));
    containerJson.put(CONTAINER_NAME_KEY, refContainerNames.get(index));
    containerJson.put(Container.STATUS_KEY, refContainerStatuses.get(index).name());
    containerJson.put(DESCRIPTION_KEY, refContainerDescriptions.get(index));
    containerJson.put(IS_PRIVATE_KEY, !refContainerCachingValues.get(index));
    containerJson.put(PARENT_ACCOUNT_ID_KEY, refAccountId);
    return containerJson;
  }

  /**
   * Construct a V1 JSON object with the new optional fields (encrypted, cacheable, previouslyEncrypted).
   * @param index The index in the reference container list to assert against.
   * @return the {@link JSONObject}
   */
  private JSONObject buildAmendedV1ContainerJson(int index) throws JSONException {
    JSONObject containerJson = new JSONObject();
    containerJson.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_1);
    containerJson.put(CONTAINER_ID_KEY, refContainerIds.get(index));
    containerJson.put(CONTAINER_NAME_KEY, refContainerNames.get(index));
    containerJson.put(Container.STATUS_KEY, refContainerStatuses.get(index).name());
    containerJson.put(DESCRIPTION_KEY, refContainerDescriptions.get(index));
    containerJson.put(IS_PRIVATE_KEY, !refContainerCachingValues.get(index));
    containerJson.put(ENCRYPTED_KEY, refContainerEncryptionValues.get(index));
    containerJson.put(PREVIOUSLY_ENCRYPTED_KEY, refContainerPreviousEncryptionValues.get(index));
    containerJson.put(CACHEABLE_KEY, refContainerCachingValues.get(index));
    containerJson.put(MEDIA_SCAN_DISABLED, refContainerMediaScanDisabledValues.get(index));
    containerJson.put(PARENT_ACCOUNT_ID_KEY, refAccountId);
    return containerJson;
  }

  /**
   * Construct a V2 JSON object.
   * @param index The index in the reference container list to assert against.
   * @return the {@link JSONObject}
   */
  private JSONObject buildV2ContainerJson(int index) throws JSONException {
    JSONObject containerJson = new JSONObject();
    containerJson.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_2);
    containerJson.put(CONTAINER_ID_KEY, refContainerIds.get(index));
    containerJson.put(CONTAINER_NAME_KEY, refContainerNames.get(index));
    containerJson.put(Container.STATUS_KEY, refContainerStatuses.get(index).name());
    containerJson.put(DESCRIPTION_KEY, refContainerDescriptions.get(index));
    containerJson.put(ENCRYPTED_KEY, refContainerEncryptionValues.get(index));
    containerJson.put(PREVIOUSLY_ENCRYPTED_KEY, refContainerPreviousEncryptionValues.get(index));
    containerJson.put(CACHEABLE_KEY, refContainerCachingValues.get(index));
    containerJson.put(MEDIA_SCAN_DISABLED, refContainerMediaScanDisabledValues.get(index));
    containerJson.put(PARENT_ACCOUNT_ID_KEY, refAccountId);
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

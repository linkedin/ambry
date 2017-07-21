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
import static com.github.ambry.account.Account.JSON_VERSION_1;
import static com.github.ambry.account.Account.JSON_VERSION_KEY;
import static com.github.ambry.account.Account.STATUS_KEY;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link Account}, {@link Container}, {@link AccountBuilder}, and {@link ContainerBuilder}.
 */
public class AccountContainerTest {
  private static final Random random = new Random();
  private static final int CONTAINER_COUNT = 100;

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
  private List<Boolean> refContainerPrivacyValues;
  private List<JSONObject> containerJsonLikeList;

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
    accountJsonLike.put(JSON_VERSION_KEY, JSON_VERSION_1);
    accountJsonLike.put(ACCOUNT_ID_KEY, refAccountId);
    accountJsonLike.put(ACCOUNT_NAME_KEY, refAccountName);
    accountJsonLike.put(STATUS_KEY, refAccountStatus);
    accountJsonLike.put(CONTAINERS_KEY, new JSONArray());
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
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      containers.add(new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
          refContainerDescriptions.get(i), refContainerPrivacyValues.get(i), refAccountId).build());
    }
    Account accountFromArguments =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, containers).build();
    assertAccountAgainstReference(accountFromArguments, true, true);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated name.
   */
  @Test
  public void testDuplicateContainerName() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    // second container with (id=1, name="0")
    containers.add(new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id.
   */
  @Test
  public void testDuplicateContainerId() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    // second container with (id=0, name="1")
    containers.add(new ContainerBuilder((short) 0, "1", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id and name.
   */
  @Test
  public void testDuplicateContainerNameAndId() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new ContainerBuilder((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    // second container with (id=1, name="0")
    containers.add(new ContainerBuilder((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    // third container with (id=10, name="10")
    containers.add(new ContainerBuilder((short) 10, "10", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    // second container with (id=10, name="11")
    containers.add(new ContainerBuilder((short) 10, "11", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacyValues.get(0), refAccountId).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing a {@link Container} from json object.
   */
  @Test
  public void testConstructContainerFromJson() throws JSONException {
    List<Container> containersFromJson = new ArrayList<>();
    for (int i = 0; i < CONTAINER_COUNT; i++) {
      Container containerFromJson = Container.fromJson(containerJsonLikeList.get(i));
      containersFromJson.add(containerFromJson);
      assertContainer(containerFromJson, i);
    }
  }

  /**
   * Tests in an {@link AccountBuilder} the account id mismatches with container id.
   */
  @Test
  public void testMismatchForAccountId() {
    ArrayList<Container> containers = new ArrayList<>();
    // container with parentAccountId = refAccountId + 1
    containers.add(new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerDescriptions.get(0), refContainerPrivacyValues.get(0), (short) (refAccountId + 1)).build());
    createAccountWithBadContainersAndFail(containers, IllegalStateException.class);
  }

  /**
   * Tests bad inputs for constructors or methods.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void badInputs() throws Exception {
    JSONObject badMetadata;

    // null account metadata
    creatAccountWithBadJsonMetadataAndFail(null, IllegalArgumentException.class);

    // account metadata in wrong format
    badMetadata = new JSONObject();
    badMetadata.put("badKey", "badValue");
    creatAccountWithBadJsonMetadataAndFail(badMetadata, JSONException.class);

    // required fields are missing in the metadata
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.remove(ACCOUNT_ID_KEY);
    creatAccountWithBadJsonMetadataAndFail(badMetadata, JSONException.class);

    // unsupported account json version
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.put(JSON_VERSION_KEY, 2);
    creatAccountWithBadJsonMetadataAndFail(badMetadata, IllegalStateException.class);

    // invalid account status
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.put(STATUS_KEY, "invalidAccountStatus");
    creatAccountWithBadJsonMetadataAndFail(badMetadata, IllegalArgumentException.class);

    // null container metadata
    creatContainerWithBadJsonMetadataAndFail(null, IllegalArgumentException.class);

    // invalid container status
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.put(Container.STATUS_KEY, "invalidContainerStatus");
    creatContainerWithBadJsonMetadataAndFail(badMetadata, IllegalArgumentException.class);

    // required fields are missing.
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.remove(CONTAINER_ID_KEY);
    creatContainerWithBadJsonMetadataAndFail(badMetadata, JSONException.class);

    // unsupported container json version
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.put(Container.JSON_VERSION_KEY, 2);
    creatContainerWithBadJsonMetadataAndFail(badMetadata, IllegalStateException.class);
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
              refContainerDescriptions.get(i), refContainerPrivacyValues.get(i), refAccountId);
      Container containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i);

      // build a container from existing container
      containerBuilder = new ContainerBuilder(containerFromBuilder);
      Container container2ByBuilder = containerBuilder.build();
      assertContainer(container2ByBuilder, i);
    }
  }

  /**
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testFieldMissingToBuildAccount() {
    // test when required fields are null
    buildAccountWithMissingFieldsAndFail(null, refAccountName, refAccountStatus, IllegalStateException.class);
    buildAccountWithMissingFieldsAndFail(refAccountId, null, refAccountStatus, IllegalStateException.class);
    buildAccountWithMissingFieldsAndFail(refAccountId, refAccountName, null, IllegalStateException.class);
  }

  /**
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testFieldMissingToBuildContainer() {
    // test when required fields are null
    buildContainerWithMissingFieldsAndFail(null, refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerPrivacyValues.get(0), refAccountId, IllegalStateException.class);
    buildContainerWithMissingFieldsAndFail(refContainerIds.get(0), null, refContainerStatuses.get(0),
        refContainerPrivacyValues.get(0), refAccountId, IllegalStateException.class);
    buildContainerWithMissingFieldsAndFail(refContainerIds.get(0), refContainerNames.get(0), null,
        refContainerPrivacyValues.get(0), refAccountId, IllegalStateException.class);
    buildContainerWithMissingFieldsAndFail(refContainerIds.get(0), refContainerNames.get(0),
        refContainerStatuses.get(0), null, refAccountId, IllegalStateException.class);
    buildContainerWithMissingFieldsAndFail(refContainerIds.get(0), refContainerNames.get(0),
        refContainerStatuses.get(0), refContainerPrivacyValues.get(0), null, IllegalStateException.class);
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
      boolean updatedIsPrivate = !container.isPrivate();
      containerBuilder.setId(updatedContainerId)
          .setName(updatedContainerName)
          .setStatus(updatedContainerStatus)
          .setDescription(updatedContainerDescription)
          .setIsPrivate(updatedIsPrivate);
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
      assertEquals("container isPrivate attribute is not correctly updated", updatedIsPrivate,
          updatedAccount.getContainerById(updatedContainerId).isPrivate());
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
            refContainerDescriptions.get(0), refContainerPrivacyValues.get(0), refAccountId);
    Container container = containerBuilder.build();
    accountBuilder.removeContainer(container);
    accountBuilder.removeContainer(null);
    Account account = accountBuilder.build();
    assertAccountAgainstReference(account, true, true);
  }

  /**
   * Tests for {@link Account#UNKNOWN_ACCOUNT} and {@link Container#UNKNOWN_CONTAINER}.
   */
  @Test
  public void testUnknownAccountAndContainer() {
    Account unknownAccount = Account.UNKNOWN_ACCOUNT;
    Container unknownContainer = Container.UNKNOWN_CONTAINER;
    assertEquals("Wrong id for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_ID, unknownContainer.getId());
    assertEquals("Wrong name for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_NAME, unknownContainer.getName());
    assertEquals("Wrong status for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_STATUS,
        unknownContainer.getStatus());
    assertEquals("Wrong description for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_DESCRIPTION,
        unknownContainer.getDescription());
    assertEquals("Wrong privacy setting for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_IS_PRIVATE_SETTING,
        unknownContainer.isPrivate());
    assertEquals("Wrong parent account id for UNKNOWN_CONTAINER", Container.UNKNOWN_CONTAINER_PARENT_ACCOUNT_ID,
        unknownContainer.getParentAccountId());
    assertEquals("Wrong id for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_ID, unknownAccount.getId());
    assertEquals("Wrong name for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_NAME, unknownAccount.getName());
    assertEquals("Wrong status for UNKNOWN_ACCOUNT", Account.UNKNOWN_ACCOUNT_STATUS, unknownAccount.getStatus());
    assertEquals("Wrong number of containers for UNKNOWN_ACCOUNT", 1, unknownAccount.getAllContainers().size());
    assertEquals("Wrong container get from UNKNOWN_ACCOUNT", Container.UNKNOWN_CONTAINER,
        unknownAccount.getContainerById(Container.UNKNOWN_CONTAINER_ID));
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
        assertContainer(account.getContainerById(refContainerIds.get(i)), i);
        assertContainer(account.getContainerByName(refContainerNames.get(i)), i);
      }
    }
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field, {@link Container#toJson()}
   * method, and also asserts the same object after serialize and then deserialize.
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   * @throws JSONException
   */
  private void assertContainer(Container container, int index) throws JSONException {
    assertEquals((short) refContainerIds.get(index), container.getId());
    assertEquals(refContainerNames.get(index), container.getName());
    assertEquals(refContainerStatuses.get(index), container.getStatus());
    assertEquals(refContainerDescriptions.get(index), container.getDescription());
    assertEquals(refContainerPrivacyValues.get(index), container.isPrivate());
    assertEquals(refAccountId, container.getParentAccountId());
    assertEquals(containerJsonLikeList.get(index).toString(), container.toJson().toString());
    assertEquals(Container.fromJson(containerJsonLikeList.get(index)), container);
  }

  /**
   * Asserts that create an {@link Account} fails and throw an exception as expected, when supplying an invalid
   * list of {@link Container}s.
   * @param containers A list of invalid {@link Container}s.
   * @param exceptionClass The class of expected exception.
   */
  private void createAccountWithBadContainersAndFail(List<Container> containers, Class<?> exceptionClass) {
    try {
      new Account(refAccountId, refAccountName, refAccountStatus, containers);
      fail("should have thrown");
    } catch (Exception e) {
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that create an {@link Account} fails and throw an exception as expected, when supplying an invalid
   * metadata in Json.
   * @param metadata An invalid metadata in Json.
   * @param exceptionClass The class of expected exception.
   */
  private void creatAccountWithBadJsonMetadataAndFail(JSONObject metadata, Class<?> exceptionClass) {
    try {
      Account.fromJson(metadata);
      fail("should have thrown");
    } catch (Exception e) {
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that create an {@link Container} fails and throw an exception as expected, when supplying an invalid
   * metadata in Json.
   * @param metadata An invalid metadata in Json.
   * @param exceptionClass The class of expected exception.
   */
  private void creatContainerWithBadJsonMetadataAndFail(JSONObject metadata, Class<?> exceptionClass) {
    try {
      Container.fromJson(metadata);
      fail("should have thrown");
    } catch (Exception e) {
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that build an {@link Account} will fail because of missing field.
   * @param id The id for the {@link Account} to build.
   * @param name The name for the {@link Account} to build.
   * @param status The status for the {@link Account} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildAccountWithMissingFieldsAndFail(Short id, String name, AccountStatus status,
      Class<?> exceptionClass) {
    try {
      new AccountBuilder(id, name, status, null).build();
      fail("Should have thrown");
    } catch (Exception e) {
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that build a {@link Container} will fail because of missing field.
   * @param id The id for the {@link Container} to build.
   * @param name The name for the {@link Container} to build.
   * @param status The status for the {@link Container} to build.
   * @param isPrivate The isPrivate setting for the {@link Container} to build.
   * @param parentAccountId The id of the parent {@link Account} for the {@link Container} to build.
   * @param exceptionClass The class of expected exception.
   */
  private void buildContainerWithMissingFieldsAndFail(Short id, String name, ContainerStatus status, Boolean isPrivate,
      Short parentAccountId, Class<?> exceptionClass) {
    try {
      ContainerBuilder containerBuilder =
          new ContainerBuilder(id, name, status, "description", isPrivate, parentAccountId);
      containerBuilder.build();
      fail("Should have thrown");
    } catch (Exception e) {
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
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
    refContainerPrivacyValues = new ArrayList<>();
    containerJsonLikeList = new ArrayList<>();
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
      refContainerPrivacyValues.add(random.nextBoolean());
      JSONObject containerJsonLike = new JSONObject();
      containerJsonLike.put(Container.JSON_VERSION_KEY, Container.JSON_VERSION_1);
      containerJsonLike.put(CONTAINER_ID_KEY, refContainerIds.get(i));
      containerJsonLike.put(CONTAINER_NAME_KEY, refContainerNames.get(i));
      containerJsonLike.put(Container.STATUS_KEY, refContainerStatuses.get(i));
      containerJsonLike.put(DESCRIPTION_KEY, refContainerDescriptions.get(i));
      containerJsonLike.put(IS_PRIVATE_KEY, refContainerPrivacyValues.get(i));
      containerJsonLike.put(PARENT_ACCOUNT_ID_KEY, refAccountId);
      containerJsonLikeList.add(containerJsonLike);
      containerArray.put(containerJsonLike);
    }
    return containerArray;
  }
}

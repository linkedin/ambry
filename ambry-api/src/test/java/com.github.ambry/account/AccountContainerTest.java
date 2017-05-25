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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
  // Reference Account fields
  private final short refAccountId = 1234;
  private final String refAccountName = "testAccount1";
  private final AccountStatus refAccountStatus = AccountStatus.ACTIVE;
  private final JSONObject accountJsonLike = new JSONObject();
  // Reference Container fields
  private final List<Short> refContainerIds = Arrays.asList(new Short[]{0, 1});
  private final List<String> refContainerNames = Arrays.asList(new String[]{"Container_0_0", "Container_0_1"});
  private final List<String> refContainerDescriptions =
      Arrays.asList(new String[]{"Public container", "Private container"});
  private final List<ContainerStatus> refContainerStatuses =
      Arrays.asList(new Container.ContainerStatus[]{ContainerStatus.ACTIVE, ContainerStatus.INACTIVE});
  private final List<Boolean> refContainerPrivacies = Arrays.asList(new Boolean[]{false, true});
  private final List<JSONObject> containerJsonLikeList = new ArrayList<>();

  /**
   * Initialize the metadata in JsonObject for account and container.
   * @throws JSONException
   */
  @Before
  public void init() throws JSONException {
    accountJsonLike.put(ACCOUNT_METADATA_VERSION_KEY, ACCOUNT_METADATA_VERSION_1);
    accountJsonLike.put(ACCOUNT_ID_KEY, refAccountId);
    accountJsonLike.put(ACCOUNT_NAME_KEY, refAccountName);
    accountJsonLike.put(ACCOUNT_STATUS_KEY, refAccountStatus);
    accountJsonLike.put(CONTAINERS_KEY, new JSONArray());
    JSONArray containerArray = new JSONArray();
    for (int i = 0; i < 2; i++) {
      JSONObject containerJsonLike = new JSONObject();
      containerJsonLike.put(CONTAINER_METADATA_VERSION_KEY, CONTAINER_METADATA_VERSION_1);
      containerJsonLike.put(CONTAINER_ID_KEY, refContainerIds.get(i));
      containerJsonLike.put(CONTAINER_NAME_KEY, refContainerNames.get(i));
      containerJsonLike.put(CONTAINER_STATUS_KEY, refContainerStatuses.get(i));
      containerJsonLike.put(CONTAINER_DESCRIPTION_KEY, refContainerDescriptions.get(i));
      containerJsonLike.put(CONTAINER_IS_PRIVATE_KEY, refContainerPrivacies.get(i));
      containerJsonLike.put(CONTAINER_PARENT_ACCOUNT_ID_KEY, refAccountId);
      containerJsonLikeList.add(containerJsonLike);
      containerArray.put(containerJsonLike);
    }
    accountJsonLike.put(CONTAINERS_KEY, containerArray);
  }

  /**
   * Tests operations on {@link Account}. Deserializes json metadata to an {@link Account}, and then serializes it
   * to json metadata.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testConstructAccountFromJson() throws Exception {
    Account accountFromJson = new Account(accountJsonLike);
    assertAccount(accountFromJson, true);
    assertEquals("Wrong account", accountFromJson, new Account(accountFromJson.toJson()));
    List<Container> containersFromAccount = accountFromJson.getAllContainers();
    assertEquals(2, containersFromAccount.size());
    for (int i = 0; i < 2; i++) {
      assertContainer(accountFromJson.getContainerById(refContainerIds.get(i)), i);
      assertContainer(accountFromJson.getContainerByName(refContainerNames.get(i)), i);
    }
  }

  /**
   * Tests constructing a {@link Container} from json object.
   * @throws JSONException
   */
  @Test
  public void testConstructContainerFromJson() throws JSONException {
    List<Container> containersFromJson = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Container containerFromJson = new Container(containerJsonLikeList.get(i));
      containersFromJson.add(containerFromJson);
      assertContainer(containerFromJson, i);
    }
  }

  /**
   * Tests building an {@link Account} using {@link AccountBuilder}.
   * @throws JSONException
   */
  @Test
  public void testAccountBuilder() throws JSONException {
    // build an account with arguments supplied
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, refAccountName, refAccountStatus);
    Account accountByBuilder = accountBuilder.build();
    assertAccount(accountByBuilder, false);

    // set containers
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Container container = new Container(containerJsonLikeList.get(i));
      containers.add(container);
      accountBuilder.setContainer(container);
    }
    accountByBuilder = accountBuilder.build();
    assertAccount(accountByBuilder, true);

    // build an account from existing account
    accountBuilder = new AccountBuilder(accountByBuilder);
    Account account2ByBuilder = accountBuilder.build();
    assertAccount(account2ByBuilder, true);
  }

  /**
   * Tests removing containers from an account, and then update the account.
   * @throws JSONException
   */
  @Test
  public void testRemoveContainerAndUpdateAccount() throws JSONException {
    // set an account with different field value
    Account origin = new Account(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    short updatedAccountId = refAccountId + 1;
    String updatedAccountName = refAccountName + "-updated";
    AccountStatus updatedAccountStatus = AccountStatus.INACTIVE;
    accountBuilder.setId(updatedAccountId);
    accountBuilder.setName(updatedAccountName);
    accountBuilder.setStatus(updatedAccountStatus);

    // remove two existing containers.
    for (Container container : origin.getAllContainers()) {
      accountBuilder.removeContainer(container);
    }

    // build the account and assert
    Account updatedAccount = accountBuilder.build();
    assertEquals(updatedAccountId, updatedAccount.getId());
    assertEquals(updatedAccountName, updatedAccount.getName());
    assertEquals(updatedAccountStatus, updatedAccount.getStatus());

    // add back the containers and assert
    accountBuilder.setContainers(origin.getAllContainers());
    accountBuilder.setId(refAccountId);
    updatedAccount = accountBuilder.build();
    assertEquals(origin.getAllContainers(), updatedAccount.getAllContainers());
  }

  /**
   * Tests updating containers in an account.
   * @throws JSONException
   */
  @Test
  public void testUpdateContainerInAccount() throws JSONException {
    Account account = new Account(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(account);

    // updating with different containers
    for (int i = 0; i < 2; i++) {
      Container container = account.getContainerById(refContainerIds.get(i));
      ContainerBuilder containerBuilder = new ContainerBuilder(container);
      String updatedContainerName = container.getName() + "-updated";
      ContainerStatus updatedContainerStatus = ContainerStatus.INACTIVE;
      String updatedContainerDescription = container.getDescription() + "--updated";
      boolean updatedIsPrivate = !container.getIsPrivate();
      containerBuilder.setName(updatedContainerName)
          .setStatus(updatedContainerStatus)
          .setDescription(updatedContainerDescription)
          .setIsPrivate(updatedIsPrivate);
      Container updatedContainer = containerBuilder.build();
      accountBuilder.setContainer(updatedContainer);

      // build account and assert
      Account updatedAccount = accountBuilder.build();
      assertEquals("container name is not correctly updated", updatedContainerName,
          updatedAccount.getContainerById(refContainerIds.get(i)).getName());
      assertEquals("container status is not correctly updated", updatedContainerStatus,
          updatedAccount.getContainerById(refContainerIds.get(i)).getStatus());
      assertEquals("container description is not correctly updated", updatedContainerDescription,
          updatedAccount.getContainerById(refContainerIds.get(i)).getDescription());
      assertEquals("container isPrivate attribute is not correctly updated", updatedIsPrivate,
          updatedAccount.getContainerById(refContainerIds.get(i)).getIsPrivate());
    }
  }

  /**
   * Tests updating account id.
   * @throws JSONException
   */
  @Test
  public void testUpdateAccountId() throws JSONException {
    Account origin = new Account(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    short updatedAccountId = refAccountId + 1;
    accountBuilder.setId(updatedAccountId);

    // in order to build the account, the container id needs to be updated.
    for (int i = 0; i < 2; i++) {
      Container container = origin.getContainerById(refContainerIds.get(i));
      ContainerBuilder containerBuilder = new ContainerBuilder(container);
      containerBuilder.setParentAccountId(updatedAccountId);
      Container updatedContainer = containerBuilder.build();
      accountBuilder.setContainer(updatedContainer);
    }
    Account updatedAccount = accountBuilder.build();
    assertEquals("Account id is not correctly updated", updatedAccountId, updatedAccount.getId());
    for (Container container : updatedAccount.getAllContainers()) {
      assertEquals("Parent account id in container is not correctly updated", updatedAccountId,
          container.getParentAccountId());
    }
  }

  /**
   * Tests in an {@link AccountBuilder} the account id mismatches with container id.
   * @throws JSONException
   */
  @Test
  public void testMismatchForAccountId() throws JSONException {
    short differentAccountId = refAccountId + 1;
    ContainerBuilder containerBuilder =
        new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), refContainerPrivacies.get(0), differentAccountId);
    Container nonChildContainer = containerBuilder.build();
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, refAccountName, refAccountStatus);
    accountBuilder.setContainer(nonChildContainer);
    try {
      accountBuilder.build();
      fail("should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests invalid state to build an account.
   * @throws JSONException
   */
  @Test
  public void testInvalidStateToBuildAccount() throws JSONException {
    AccountBuilder accountBuilder;

    // test when required fields are null
    try {
      accountBuilder = new AccountBuilder(refAccountId, null, refAccountStatus);
      accountBuilder.build();
      fail("Should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      accountBuilder = new AccountBuilder(refAccountId, refAccountName, null);
      accountBuilder.build();
      fail("Should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests building a {@link Container} using {@link ContainerBuilder}.
   * @throws JSONException
   */
  @Test
  public void testContainerBuilder() throws JSONException {
    ContainerBuilder containerBuilder;
    for (int i = 0; i < 2; i++) {
      // build a container with arguments supplied
      containerBuilder =
          new ContainerBuilder(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refContainerPrivacies.get(i), refAccountId);
      Container containerFromBuilder = containerBuilder.build();
      assertContainer(containerFromBuilder, i);

      // build a container from existing container
      containerBuilder = new ContainerBuilder(containerFromBuilder);
      Container container2ByBuilder = containerBuilder.build();
      assertContainer(container2ByBuilder, i);
    }
  }

  /**
   * Tests invalid state to build container.
   * @throws JSONException
   */
  @Test
  public void testInvalidStateToBuildContainer() throws JSONException {
    ContainerBuilder containerBuilder;
    // test when required fields are null
    try {
      containerBuilder = new ContainerBuilder(refContainerIds.get(0), null, refContainerStatuses.get(0),
          refContainerDescriptions.get(0), refContainerPrivacies.get(0), refAccountId);
      containerBuilder.build();
      fail("Should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }

    try {
      containerBuilder =
          new ContainerBuilder(refContainerIds.get(0), refContainerNames.get(0), null, refContainerDescriptions.get(0),
              refContainerPrivacies.get(0), refAccountId);
      containerBuilder.build();
      fail("Should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests bad inputs for constructors or methods.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void badInputs() throws Exception {
    // metadata is null
    try {
      new Account(null);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // bad account metadata
    JSONObject badJson = new JSONObject();
    badJson.put("badKey", "badValue");
    try {
      new Account(badJson);
      fail("Should have thrown.");
    } catch (JSONException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // good account metadata version but required fields are missing in the metadata
    JSONObject badJsonWithCorrectVersion = new JSONObject();
    badJsonWithCorrectVersion.put(ACCOUNT_METADATA_VERSION_KEY, ACCOUNT_METADATA_VERSION_1);
    try {
      new Account(badJsonWithCorrectVersion);
      fail("Should have thrown.");
    } catch (JSONException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // bad account metadata version
    JSONObject badJsonWithUnsupportedAccountMetadataVersion = new JSONObject();
    badJsonWithUnsupportedAccountMetadataVersion.put(ACCOUNT_METADATA_VERSION_KEY, 2);
    try {
      new Account(badJsonWithUnsupportedAccountMetadataVersion);
      fail("Should have thrown.");
    } catch (IllegalStateException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // null container metadata
    try {
      new Container(null);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // valid container metadata version but required fields are missing.
    JSONObject badJsonWithCorrectContainerMetadataVersion = new JSONObject();
    badJsonWithCorrectContainerMetadataVersion.put(CONTAINER_METADATA_VERSION_KEY, CONTAINER_METADATA_VERSION_1);
    try {
      new Container(badJsonWithCorrectContainerMetadataVersion);
      fail("Should have thrown.");
    } catch (JSONException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // unsupported container metadata version
    JSONObject badJsonWithUnsupportedContainerMetadataVersion = new JSONObject();
    badJsonWithUnsupportedContainerMetadataVersion.put(CONTAINER_METADATA_VERSION_KEY, 2);
    try {
      new Container(badJsonWithUnsupportedContainerMetadataVersion);
      fail("Should have thrown.");
    } catch (IllegalStateException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }
  }

  /**
   * Tests when invalid account or container status are in the corresponding json metadata.
   * @throws JSONException
   */
  @Test
  public void testInvalidStatus() throws JSONException {
    JSONObject badAccountJson = accountJsonLike.put(ACCOUNT_STATUS_KEY, "invalidAccountStatus");
    try {
      new Account(badAccountJson);
    } catch (IllegalArgumentException e) {
      // expected
    }
    JSONObject badContainerJson = containerJsonLikeList.get(0);
    badContainerJson.put(CONTAINER_STATUS_KEY, "invalidContainerStatus");
    try {
      new Container(badContainerJson);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Tests {@code toString()} methods.
   * @throws JSONException
   */
  @Test
  public void testToString() throws JSONException {
    Account account = new Account(accountJsonLike);
    assertEquals("Account[" + account.getId() + "]", account.toString());
    Container container = new Container(containerJsonLikeList.get(0));
    assertEquals("Container[" + account.getId() + ":" + container.getId() + "]", container.toString());
  }

  /**
   * Asserts an {@link Account} against the reference account for every internal field except metadata.
   * @param account The {@link Account} to assert.
   * @throws JSONException
   */
  private void assertAccount(Account account, boolean compareMetadata) throws JSONException {
    assertEquals(refAccountId, account.getId());
    assertEquals(refAccountName, account.getName());
    assertEquals(refAccountStatus, account.getStatus());
    if (compareMetadata) {
      assertEquals(accountJsonLike.toString(), account.toJson().toString());
      assertEquals(new Account(accountJsonLike), account);
    }
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field.
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   * @throws JSONException
   */
  private void assertContainer(Container container, int index) throws JSONException {
    assertEquals((short) refContainerIds.get(index), container.getId());
    assertEquals(refContainerNames.get(index), container.getName());
    assertEquals(refContainerStatuses.get(index), container.getStatus());
    assertEquals(refContainerDescriptions.get(index), container.getDescription());
    assertEquals(refContainerPrivacies.get(index), container.getIsPrivate());
    assertEquals(refAccountId, container.getParentAccountId());
    assertEquals(containerJsonLikeList.get(index).toString(), container.toJson().toString());
    assertEquals(new Container(containerJsonLikeList.get(index)), container);
  }
}
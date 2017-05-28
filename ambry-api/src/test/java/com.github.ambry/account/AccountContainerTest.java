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
import java.util.Collection;
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
   * Tests constructing an {@link Account} from Json metadata.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testConstructAccountFromJson() throws Exception {
    assertAccountAgainstReference(new Account(accountJsonLike), true, true);
  }

  /**
   * Tests constructing {@link Account} and {@link Container} using individual arguments.
   */
  @Test
  public void testConstructAccountAndContainerFromArguments() throws JSONException {
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      containers.add(new Container(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
          refContainerDescriptions.get(i), refContainerPrivacies.get(i), refAccountId));
    }
    Account accountFromArguments = new Account(refAccountId, refAccountName, refAccountStatus, containers);
    assertAccountAgainstReference(accountFromArguments, true, true);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated name.
   */
  @Test
  public void testDuplicateContainerName() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new Container((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    // second container with (id=1, name="0")
    containers.add(new Container((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    assertCreateAccountWithBadContainers(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id.
   */
  @Test
  public void testDuplicateContainerId() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new Container((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    // second container with (id=0, name="1")
    containers.add(new Container((short) 0, "1", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    assertCreateAccountWithBadContainers(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing {@link Account} when supplying a list of {@link Container}s with duplicated id and name.
   */
  @Test
  public void testDuplicateContainerNameAndId() {
    ArrayList<Container> containers = new ArrayList<>();
    // first container with (id=0, name="0")
    containers.add(new Container((short) 0, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    // second container with (id=1, name="0")
    containers.add(new Container((short) 1, "0", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    // third container with (id=10, name="10")
    containers.add(new Container((short) 10, "10", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    // second container with (id=10, name="11")
    containers.add(new Container((short) 10, "11", refContainerStatuses.get(0), refContainerDescriptions.get(0),
        refContainerPrivacies.get(0), refAccountId));
    assertCreateAccountWithBadContainers(containers, IllegalStateException.class);
  }

  /**
   * Tests constructing a {@link Container} from json object.
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
   * Tests in an {@link AccountBuilder} the account id mismatches with container id.
   */
  @Test
  public void testMismatchForAccountId() {
    ArrayList<Container> containers = new ArrayList<>();
    // container with parentAccountId = refAccountId + 1
    containers.add(new Container(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerDescriptions.get(0), refContainerPrivacies.get(0), (short) (refAccountId + 1)));
    assertCreateAccountWithBadContainers(containers, IllegalStateException.class);
  }

  /**
   * Tests bad inputs for constructors or methods.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void badInputs() throws Exception {
    JSONObject badMetadata;

    // null account metadata
    assertCreatAccountWithBadJsonMetadata(null, IllegalArgumentException.class);

    // account metadata in wrong format
    badMetadata = new JSONObject();
    badMetadata.put("badKey", "badValue");
    assertCreatAccountWithBadJsonMetadata(badMetadata, JSONException.class);

    // required fields are missing in the metadata
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.remove(ACCOUNT_ID_KEY);
    assertCreatAccountWithBadJsonMetadata(badMetadata, JSONException.class);

    // unsupported account metadata version
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.put(ACCOUNT_METADATA_VERSION_KEY, 2);
    assertCreatAccountWithBadJsonMetadata(badMetadata, IllegalStateException.class);

    // invalid account status
    badMetadata = new JSONObject(accountJsonLike, JSONObject.getNames(accountJsonLike));
    badMetadata.put(ACCOUNT_STATUS_KEY, "invalidAccountStatus");
    assertCreatAccountWithBadJsonMetadata(badMetadata, IllegalArgumentException.class);

    // null container metadata
    assertCreatContainerWithBadJsonMetadata(null, IllegalArgumentException.class);

    // invalid container status
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.put(CONTAINER_STATUS_KEY, "invalidContainerStatus");
    assertCreatContainerWithBadJsonMetadata(badMetadata, IllegalArgumentException.class);

    // required fields are missing.
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.remove(CONTAINER_ID_KEY);
    assertCreatContainerWithBadJsonMetadata(badMetadata, JSONException.class);

    // unsupported container metadata version
    badMetadata = new JSONObject(containerJsonLikeList.get(0), JSONObject.getNames(containerJsonLikeList.get(0)));
    badMetadata.put(CONTAINER_METADATA_VERSION_KEY, 2);
    assertCreatContainerWithBadJsonMetadata(badMetadata, IllegalStateException.class);
  }

  /**
   * Tests {@code toString()} methods.
   * @throws JSONException
   */
  @Test
  public void testToString() throws JSONException {
    Account account = new Account(accountJsonLike);
    assertEquals("Account[" + account.getId() + "]", account.toString());
    for (int i = 0; i < 2; i++) {
      Container container = new Container(containerJsonLikeList.get(i));
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
    AccountBuilder accountBuilder = new AccountBuilder(refAccountId, refAccountName, refAccountStatus);
    Account accountByBuilder = accountBuilder.build();
    assertAccountAgainstReference(accountByBuilder, false, false);

    // set containers
    List<Container> containers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Container container = new Container(containerJsonLikeList.get(i));
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
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testFieldMissingToBuildAccount() {
    // test when required fields are null
    assertBuildAccountWithMissingFields(null, refAccountName, refAccountStatus, IllegalStateException.class);
    assertBuildAccountWithMissingFields(refAccountId, null, refAccountStatus, IllegalStateException.class);
    assertBuildAccountWithMissingFields(refAccountId, refAccountName, null, IllegalStateException.class);
  }

  /**
   * Tests required fields are missing to build an account.
   */
  @Test
  public void testFieldMissingToBuildContainer() {
    // test when required fields are null
    assertBuildContainerWithMissingFields(null, refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerPrivacies.get(0), refAccountId, IllegalStateException.class);
    assertBuildContainerWithMissingFields(refContainerIds.get(0), null, refContainerStatuses.get(0),
        refContainerPrivacies.get(0), refAccountId, IllegalStateException.class);
    assertBuildContainerWithMissingFields(refContainerIds.get(0), refContainerNames.get(0), null,
        refContainerPrivacies.get(0), refAccountId, IllegalStateException.class);
    assertBuildContainerWithMissingFields(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        null, refAccountId, IllegalStateException.class);
    assertBuildContainerWithMissingFields(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerPrivacies.get(0), null, IllegalStateException.class);
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
    Account.AccountStatus updatedAccountStatus = Account.AccountStatus.INACTIVE;
    accountBuilder.setId(updatedAccountId);
    accountBuilder.setName(updatedAccountName);
    accountBuilder.setStatus(updatedAccountStatus);

    try {
      accountBuilder.build();
    } catch (IllegalStateException e) {
      // expected, as new account id does not match the parentAccountId of the two containers.
      System.out.println(e.getMessage());
    }

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
    for (Container container : origin.getAllContainers()) {
      accountBuilder.addOrUpdateContainer(container);
    }
    accountBuilder.setId(refAccountId);
    updatedAccount = accountBuilder.build();
    assertEquals(origin.getAllContainers().toString(), updatedAccount.getAllContainers().toString());
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
      Container.ContainerStatus updatedContainerStatus = Container.ContainerStatus.INACTIVE;
      String updatedContainerDescription = container.getDescription() + "--updated";
      boolean updatedIsPrivate = !container.getIsPrivate();
      containerBuilder.setName(updatedContainerName)
          .setStatus(updatedContainerStatus)
          .setDescription(updatedContainerDescription)
          .setIsPrivate(updatedIsPrivate);
      Container updatedContainer = containerBuilder.build();
      accountBuilder.addOrUpdateContainer(updatedContainer);

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
   * Tests removing a non-existent container from accountBuilder.
   * @throws JSONException
   */
  @Test
  public void testRemoveNonExistContainer() throws JSONException {
    Account origin = new Account(accountJsonLike);
    AccountBuilder accountBuilder = new AccountBuilder(origin);
    ContainerBuilder containerBuilder =
        new ContainerBuilder((short) 999, refContainerNames.get(0), refContainerStatuses.get(0),
            refContainerDescriptions.get(0), refContainerPrivacies.get(0), refAccountId);
    Container container = containerBuilder.build();
    accountBuilder.removeContainer(container);
    accountBuilder.removeContainer(null);
    Account account = accountBuilder.build();
    assertAccountAgainstReference(account, true, true);
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
      assertEquals("Wrong metadata JsonObject from toJson()", accountJsonLike.toString(), account.toJson().toString());
      assertEquals("Wrong behavior in serialize and then deserialize", account, new Account(account.toJson()));
      assertEquals("Failed to compare account to a reference account", new Account(accountJsonLike), account);
    }
    if (compareContainer) {
      Collection<Container> containersFromAccount = account.getAllContainers();
      assertEquals(2, containersFromAccount.size());
      for (int i = 0; i < 2; i++) {
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
    assertEquals(refContainerPrivacies.get(index), container.getIsPrivate());
    assertEquals(refAccountId, container.getParentAccountId());
    assertEquals(containerJsonLikeList.get(index).toString(), container.toJson().toString());
    assertEquals(new Container(containerJsonLikeList.get(index)), container);
  }

  /**
   * Asserts that create an {@link Account} fails and throw an exception as expected, when supplying an invalid
   * list of {@link Container}s.
   * @param containers A list of invalid {@link Container}s.
   * @param exceptionClass The class of expected exception.
   */
  private void assertCreateAccountWithBadContainers(List<Container> containers, Class<?> exceptionClass) {
    try {
      new Account(refAccountId, refAccountName, refAccountStatus, containers);
      fail("should have thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that create an {@link Account} fails and throw an exception as expected, when supplying an invalid
   * metadata in Json.
   * @param metadata An invalid metadata in Json.
   * @param exceptionClass The class of expected exception.
   */
  private void assertCreatAccountWithBadJsonMetadata(JSONObject metadata, Class<?> exceptionClass) {
    try {
      new Account(metadata);
      fail("should have thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Asserts that create an {@link Container} fails and throw an exception as expected, when supplying an invalid
   * metadata in Json.
   * @param metadata An invalid metadata in Json.
   * @param exceptionClass The class of expected exception.
   */
  private void assertCreatContainerWithBadJsonMetadata(JSONObject metadata, Class<?> exceptionClass) {
    try {
      new Container(metadata);
      fail("should have thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
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
  private void assertBuildAccountWithMissingFields(Short id, String name, AccountStatus status,
      Class<?> exceptionClass) {
    try {
      AccountBuilder accountBuilder = new AccountBuilder(id, name, status);
      accountBuilder.build();
      fail("Should have thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
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
  private void assertBuildContainerWithMissingFields(Short id, String name, ContainerStatus status, Boolean isPrivate,
      Short parentAccountId, Class<?> exceptionClass) {
    try {
      ContainerBuilder containerBuilder =
          new ContainerBuilder(id, name, status, "description", isPrivate, parentAccountId);
      containerBuilder.build();
      fail("Should have thrown");
    } catch (Exception e) {
      System.out.println(e.getMessage());
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }
}

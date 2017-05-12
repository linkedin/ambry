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
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Unit tess for {@link Account} and {@link Container}.
 */
public class AccountTest {
  // Reference Account fields
  private static final short refAccountId = 1234;
  private static final String refAccountName = "testAccount1";
  private static final String refAccountStatus = ACCOUNT_STATUS_ACTIVE;
  private static final JSONObject refAccountJson = new JSONObject();
  // Reference Container fields
  private static final List<Short> refContainerIds = Arrays.asList(new Short[]{0, 1});
  private static final List<String> refContainerNames = Arrays.asList(new String[]{"Container_0_0", "Container_0_1"});
  private static final List<String> refContainerDescriptions =
      Arrays.asList(new String[]{"Public container", "Private container"});
  private static final List<String> refContainerStatuses =
      Arrays.asList(new String[]{CONTAINER_STATUS_ACTIVE, CONTAINER_STATUS_INACTIVE});
  private static final List<Boolean> refContainerPrivacies = Arrays.asList(new Boolean[]{false, true});
  private static final List<JSONObject> refContainerJsons = new ArrayList<>();

  @BeforeClass
  public static void init() throws JSONException {
    refAccountJson.put(ACCOUNT_METADATA_VERSION_KEY, ACCOUNT_METADATA_VERSION_1);
    refAccountJson.put(ACCOUNT_ID_KEY, refAccountId);
    refAccountJson.put(ACCOUNT_NAME_KEY, refAccountName);
    refAccountJson.put(ACCOUNT_STATUS_KEY, refAccountStatus);
    refAccountJson.put(CONTAINERS_KEY, new JSONArray());
    JSONArray containerArray = new JSONArray();
    for (int i = 0; i < 2; i++) {
      JSONObject containerObject = new JSONObject();
      containerObject.put(CONTAINER_METADATA_VERSION_KEY, CONTAINER_METADATA_VERSION_1);
      containerObject.put(CONTAINER_ID_KEY, refContainerIds.get(i));
      containerObject.put(CONTAINER_NAME_KEY, refContainerNames.get(i));
      containerObject.put(CONTAINER_STATUS_KEY, refContainerStatuses.get(i));
      containerObject.put(CONTAINER_DESCRIPTION_KEY, refContainerDescriptions.get(i));
      containerObject.put(CONTAINER_IS_PRIVATE_KEY, refContainerPrivacies.get(i));
      refContainerJsons.add(containerObject);
      containerArray.put(containerObject);
    }
    refAccountJson.put(CONTAINERS_KEY, containerArray);
  }

  /**
   * Tests operations on {@link Account}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testAccount() throws Exception {
    // construct account using JsonObject
    Account accountFromJson = new Account(refAccountJson);
    assertFullAccount(accountFromJson);

    // construct account using arguments
    Account accountFromArguments = new Account(refAccountId, refAccountName, refAccountStatus);
    assertAccountWithoutMetadata(accountFromArguments);
    for (int i = 0; i < 2; i++) {
      accountFromArguments.addContainerAndMetadata(
          new Container(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refContainerPrivacies.get(i), accountFromArguments));
    }
    assertFullAccount(accountFromArguments);
    assertEquals(accountFromJson, accountFromArguments);

    // tests container
    List<Container> containers = accountFromJson.getAllContainers();
    assertEquals(2, containers.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(refContainerJsons.get(i).toString(),
          accountFromArguments.getContainerByContainerId(refContainerIds.get(i)).getMetadata().toString());
      assertEquals(refContainerJsons.get(i).toString(),
          accountFromArguments.getContainerByContainerName(refContainerNames.get(i)).getMetadata().toString());
    }
  }

  /**
   * Tests operations on {@link Container}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testContainer() throws Exception {
    Account account = new Account(refAccountId, refAccountName, refAccountStatus);
    for (int i = 0; i < 2; i++) {
      Container containerFromJson = new Container(refContainerJsons.get(i), account);
      assertContainer(containerFromJson, i);
      Container containerFromArguments =
          new Container(refContainerIds.get(i), refContainerNames.get(i), refContainerStatuses.get(i),
              refContainerDescriptions.get(i), refContainerPrivacies.get(i), account);
      assertContainer(containerFromArguments, i);
      assertEquals(containerFromJson, containerFromArguments);
      assertEquals(account, containerFromJson.getParentAccount());
    }
  }

  /**
   * Tests {@link Account#addContainerAndMetadata(Container)}, for adding or updating a {@link Container} in an
   * {@link Account}.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testUpdateContainer() throws Exception {
    // adding a container
    Account account = new Account(refAccountId, refAccountName, refAccountStatus);
    Container container = new Container(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
        refContainerDescriptions.get(0), refContainerPrivacies.get(0), account);
    account.addContainerAndMetadata(container);
    assertEquals(1, account.getAllContainers().size());
    assertEquals(container.getMetadata(), account.getMetadata().getJSONArray(CONTAINERS_KEY).getJSONObject(0));

    // updating an existing account
    account = new Account(refAccountJson);
    String newNameForContainer0 = "newNameForContainer0";
    String newStatusForContainer0 = "newStatusForContainer0";
    String newDescriptionForContainer0 = "newDescriptionForContainer0";
    boolean newIsPrivateForContainer0 = !refContainerPrivacies.get(0);
    Container updatedContainer_0 =
        new Container(refContainerIds.get(0), newNameForContainer0, newStatusForContainer0, newDescriptionForContainer0,
            newIsPrivateForContainer0, account);
    account.addContainerAndMetadata(updatedContainer_0);
    container = account.getContainerByContainerId((short) 0);
    assertEquals(updatedContainer_0, container);
    JSONObject accountJson = account.getMetadata();
    assertEquals(accountJson.getJSONArray(CONTAINERS_KEY).get(0).toString(), container.getMetadata().toString());
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

    // add null container
    try {
      Account account = new Account(refAccountId, refAccountName, refAccountStatus);
      account.addContainerAndMetadata(null);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // account name is null
    try {
      new Account((short) 1, null, refAccountStatus);
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
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    Account parentAccount = new Account(refAccountJson);
    // null container name
    try {
      new Container(refContainerIds.get(0), null, refContainerStatuses.get(0), refContainerDescriptions.get(0),
          refContainerPrivacies.get(0), parentAccount);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // null parent account
    try {
      new Container(refContainerIds.get(0), refContainerNames.get(0), refContainerStatuses.get(0),
          refContainerDescriptions.get(0), refContainerPrivacies.get(0), null);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // null container metadata
    try {
      new Container(null, parentAccount);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // null parent account
    try {
      new Container(refContainerJsons.get(0), null);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // valid container metadata version but required fields are missing.
    JSONObject badJsonWithCorrectContainerMetadataVersion = new JSONObject();
    badJsonWithCorrectContainerMetadataVersion.put(CONTAINER_METADATA_VERSION_KEY, CONTAINER_METADATA_VERSION_1);
    try {
      new Container(badJsonWithCorrectContainerMetadataVersion, parentAccount);
      fail("Should have thrown.");
    } catch (JSONException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }

    // unsupported container metadata version
    JSONObject badJsonWithUnsupportedContainerMetadataVersion = new JSONObject();
    badJsonWithUnsupportedContainerMetadataVersion.put(CONTAINER_METADATA_VERSION_KEY, 2);
    try {
      Account account = new Account(refAccountJson);
      new Container(badJsonWithUnsupportedContainerMetadataVersion, account);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
      System.err.println("Expected " + e.getClass() + " with message: " + e.getMessage());
    }
  }

  /**
   * Tests {@code toString()} methods.
   * @throws Exception Any unexpected exceptions.
   */
  @Test
  public void testToString() throws Exception {
    Account account = new Account(refAccountJson);
    assertEquals("Account[" + account.getId() + ":" + account.getName() + "]", account.toString());
    Container container = new Container(refContainerJsons.get(0), account);
    assertEquals(
        "Container[" + account.getId() + ":" + account.getName() + ":" + container.getId() + ":" + container.getName()
            + "]", container.toString());
  }

  /**
   * Asserts an {@link Account} against the reference account for every internal field.
   * @param account The {@link Account} to assert.
   */
  private void assertFullAccount(Account account) {
    assertAccountWithoutMetadata(account);
    assertEquals(refAccountJson.toString(), account.getMetadata().toString());
  }

  /**
   * Asserts an {@link Account} against the reference account for every internal field except metadata.
   * @param account The {@link Account} to assert.
   */
  private void assertAccountWithoutMetadata(Account account) {
    assertEquals(refAccountId, account.getId());
    assertEquals(refAccountName, account.getName());
    assertEquals(refAccountStatus, account.getStatus());
  }

  /**
   * Asserts a {@link Container} against the reference account for every internal field.
   * @param container The {@link Container} to assert.
   * @param index The index in the reference container list to assert against.
   */
  private void assertContainer(Container container, int index) {
    assertEquals((short) refContainerIds.get(index), container.getId());
    assertEquals(refContainerNames.get(index), container.getName());
    assertEquals(refContainerStatuses.get(index), container.getStatus());
    assertEquals(refContainerDescriptions.get(index), container.getDescription());
    assertEquals(refContainerPrivacies.get(index), container.getPrivate());
    assertEquals(refContainerJsons.get(index).toString(), container.getMetadata().toString());
  }
}
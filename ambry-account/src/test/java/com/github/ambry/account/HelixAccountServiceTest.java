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

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.account.HelixAccountService.*;
import static junit.framework.Assert.*;


/**
 * Unit tests for {@link HelixAccountService}.
 */
public class HelixAccountServiceTest {
  // Store config
  private static int zkClientConnectTimeoutMs = 20000;
  private static int zkClientSessionTimeoutMs = 20000;
  private static String zkClientConnectString = "localhost:2182";
  private static String storeRootPath = "/ambryTest";
  private static String completeAccountMetadataPath = "/accountMetadata/completeAccountMetadata";
  private static String topicPath = "/topics";
  // Accounts
  private static Account refAccount_1;
  private static Account refAccount_2;
  private static Container refContainer_1_1;
  private static Container refContainer_1_2;
  private static Container refContainer_2_1;
  // Account 1
  private static short refAccountId_1 = 1234;
  private static String refAccountName_1 = "testAccount1";
  private static String refAccountStatus_1 = ACCOUNT_STATUS_ACTIVE;
  // Container 1_1
  private static short refContainerId_1_1 = 0;
  private static String refContainerName_1_1 = "Container_1_1";
  private static String refContainerDescription_1_1 = "Public container";
  private static String refContainerStatus_1_1 = CONTAINER_STATUS_ACTIVE;
  private static boolean refIsPrivate_1_1 = false;
  // Container 1_2
  private static short refContainerId_1_2 = 1;
  private static String refContainerName_1_2 = "Container_1_2";
  private static String refContainerDescription_1_2 = "Private container";
  private static String refContainerStatus_1_2 = CONTAINER_STATUS_INACTIVE;
  private static boolean refIsPrivate_1_2 = true;
  // Account 2
  private static short refAccountId_2 = -1;
  private static String refAccountName_2 = "TestAccount2";
  private static String refAccountStatus_2 = ACCOUNT_STATUS_INACTIVE;
  // Container 2_1
  private static short refContainerId_2_1 = -1;
  private static String refContainerName_2_1 = "Container 1 of Account 2";
  private static String refContainerDescription_2_1 = "Private container";
  private static String refContainerStatus_2_1 = CONTAINER_STATUS_ACTIVE;
  private static boolean refIsPrivate_2_1 = true;
  private static Map<Short, Account> idToRefAccountMap = new HashMap<>();
  private static HelixPropertyStoreConfig storeConfig;
  private static HelixPropertyStoreFactory<ZNRecord> storeFactory = new MockHelixPropertyStoreFactory();

  @BeforeClass
  public static void initialize() throws Exception {
    // Account 1
    refAccount_1 = new Account(refAccountId_1, refAccountName_1, refAccountStatus_1);
    refContainer_1_1 =
        new Container(refContainerId_1_1, refContainerName_1_1, refContainerDescription_1_1, refContainerStatus_1_1,
            refIsPrivate_1_1, refAccount_1);
    refContainer_1_2 =
        new Container(refContainerId_1_2, refContainerName_1_2, refContainerDescription_1_2, refContainerStatus_1_2,
            refIsPrivate_1_2, refAccount_1);
    refAccount_1.addContainerAndMetadata(refContainer_1_1);
    refAccount_1.addContainerAndMetadata(refContainer_1_2);
    idToRefAccountMap.put(refAccountId_1, refAccount_1);
    // Account 2
    refAccount_2 = new Account(refAccountId_2, refAccountName_2, refAccountStatus_2);
    refContainer_2_1 =
        new Container(refContainerId_2_1, refContainerName_2_1, refContainerDescription_2_1, refContainerStatus_2_1,
            refIsPrivate_2_1, refAccount_2);
    refAccount_2.addContainerAndMetadata(refContainer_2_1);
    idToRefAccountMap.put(refAccountId_2, refAccount_2);
    storeConfig = HelixPropertyStoreUtils.getHelixStoreConfig(zkClientConnectString, zkClientSessionTimeoutMs,
        zkClientConnectTimeoutMs, storeRootPath, completeAccountMetadataPath, topicPath);
  }

  /**
   * Cleans up if the store already exists on {@code ZooKeeper}.
   * @throws Exception
   */
  @Before
  public void clean() throws Exception {
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
  }

  /**
   * Cleans up if the store exists on {@code ZooKeeper}.
   * @throws Exception
   */
  @AfterClass
  public static void cleanUp() throws Exception {
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
  }

  /**
   * Tests a clean start up of {@link HelixAccountService}, when the corresponding {@code ZooKeeper} does not
   * have any record for it.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testCleanInitialization() throws Exception {
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // At time zero, no account metadata exists.
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests start up of {@link HelixAccountService}, when  the corresponding {@code ZooKeeper} has account metadata
   * already written into it. The tears down the service.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testInitializationWithExistingDataAndTearDownService() throws Exception {
    writeReferenceAccountMetadataToZk();
    // When start, the helixAccountService should get the account metadata.
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertAllAccounts(helixAccountService);
    helixAccountService.close();
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests update account metadata through {@link HelixAccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateAccount() throws Exception {
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
    // write accounts to ZK.
    if (helixAccountService.updateAccounts(new ArrayList<>(idToRefAccountMap.values()))) {
      helixAccountService.processMessage(HelixAccountService.ACCOUNT_METADATA_CHANGE_TOPIC,
          HelixAccountService.COMPLETE_ACCOUNT_METADATA_CHANGE_MESSAGE);
      assertAllAccounts(helixAccountService);
    } else {
      fail("Unexpected failure when updating accounts.");
    }
    String newName = "ThisIsNewNameForRefAccount1";
    String newStatus = "ThisIsNewStatusForRefAccount1";
    ArrayList<Account> accountsToUpdate = new ArrayList<>();
    accountsToUpdate.add(new Account(refAccountId_1, newName, newStatus));
    // write updated metadata for Account1
    if (helixAccountService.updateAccounts(accountsToUpdate)) {
      helixAccountService.processMessage(HelixAccountService.ACCOUNT_METADATA_CHANGE_TOPIC,
          HelixAccountService.COMPLETE_ACCOUNT_METADATA_CHANGE_MESSAGE);
      assertEquals("The name of account is not correctly updated.", newName,
          helixAccountService.getAccountById(refAccountId_1).getName());
      assertEquals("The status of account is not correctly updated.", newStatus,
          helixAccountService.getAccountById(refAccountId_1).getStatus());
      assertEquals("The name of account is not correctly updated.", refAccountName_2,
          helixAccountService.getAccountById(refAccountId_2).getName());
      assertEquals("The status of account is not correctly updated.", refAccountStatus_2,
          helixAccountService.getAccountById(refAccountId_2).getStatus());
    } else {
      fail("unexpected failure when updating accounts.");
    }
  }

  /**
   * Tests reading bad data from ZooKeeper. The corresponding {@link ZNRecord} is empty.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadDataFromZkCase1() throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    storeOperator.write(storeConfig.completeAccountMetadataPath, zNRecord);
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests reading bad data from ZooKeeper. The corresponding {@link ZNRecord} contains a single key-value pair
   * of irrelevant data.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase2() throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    zNRecord.setSimpleField("key", "value");
    storeOperator.write(storeConfig.completeAccountMetadataPath, zNRecord);
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests reading bad data from ZooKeeper. The corresponding {@link ZNRecord} contains a simple map field of
   * irrelevant data.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase3() throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> badMap = new HashMap<>();
    badMap.put("aaa", "bbb");
    zNRecord.setMapField(ACCOUNT_METADATA_KEY, badMap);
    storeOperator.write(storeConfig.completeAccountMetadataPath, zNRecord);
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests reading bad data from ZooKeeper. The corresponding {@link ZNRecord} is generally correctly formed,
   * but the metadata fields are wrong.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase4() throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> badMap = new HashMap<>();
    badMap.put(String.valueOf(Integer.MAX_VALUE), refAccount_1.getMetadata().toString());
    zNRecord.setMapField(ACCOUNT_METADATA_KEY, badMap);
    storeOperator.write(storeConfig.completeAccountMetadataPath, zNRecord);
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests sending a bad message, it will not be recognized by {@link HelixAccountService}, but will also not
   * crash the service.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void sendBadMessage() throws Exception {
    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
    writeReferenceAccountMetadataToZk();
    helixAccountService.processMessage(ACCOUNT_METADATA_CHANGE_TOPIC, "badMessage");
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests a number of bad inputs.
   */
  @Test
  public void testBadInputs() {
    try {
      new HelixAccountService(null, storeFactory);
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new HelixAccountService(storeConfig, null);
    } catch (IllegalArgumentException e) {
      // expected
    }

    HelixAccountService helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    try {
      helixAccountService.updateAccounts(null);
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  /**
   * Asserts all the accounts in {@link AccountService} against the reference accounts.
   * @param accountService The {@link AccountService} to get all {@link Account}s.
   */
  private void assertAllAccounts(AccountService accountService) {
    assertEquals(idToRefAccountMap.size(), accountService.getAllAccounts().size());
    for (Account account : accountService.getAllAccounts()) {
      assertEquals(idToRefAccountMap.get(account.getId()), account);
      assertEquals(accountService.getAccountById(account.getId()), account);
      assertEquals(accountService.getAccountByName(account.getName()), account);
    }
  }

  /**
   * Writes the metadata of two reference accounts to the underlying ZK.
   * @throws Exception Any unexpected exception.
   */
  private void writeReferenceAccountMetadataToZk() throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    for (Account account : idToRefAccountMap.values()) {
      accountMap.put(String.valueOf(account.getId()), account.getMetadata().toString());
    }
    zNRecord.setMapField(ACCOUNT_METADATA_KEY, accountMap);
    // Write account metadata into ZK.
    storeOperator.write(storeConfig.completeAccountMetadataPath, zNRecord);
  }
}

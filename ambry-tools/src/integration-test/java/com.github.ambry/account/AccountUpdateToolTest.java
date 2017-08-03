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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.HelixNotifier;
import com.github.ambry.commons.HelixStoreOperator;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Test;

import static com.github.ambry.account.AccountTestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static com.github.ambry.utils.Utils.*;
import static org.junit.Assert.*;


/**
 * Integration tests for {@link AccountUpdateTool}. The tests are running against a locally-deployed {@code ZooKeeper}.
 *
 */
public class AccountUpdateToolTest {
  private static final int NUM_REF_ACCOUNT = 10 + (int) (Math.random() * 50);
  private static final int NUM_CONTAINER_PER_ACCOUNT = 4;
  private static final int ZK_SERVER_PORT = 2200;
  private static final String DC_NAME = "testDc";
  private static final String ZK_SERVER_ADDRESS = "localhost:" + ZK_SERVER_PORT;
  private static final String HELIX_STORE_ROOT_PATH = "/ambry/defaultCluster/helixPropertyStore";
  private static final Properties helixConfigProps = new Properties();
  private static final VerifiableProperties vHelixConfigProps;
  private static final HelixPropertyStoreConfig storeConfig;
  private static ZkInfo zkInfo;
  private static String tempDirPath;
  private final Map<Short, Account> idToRefAccountMap;
  private final Map<Short, Map<Short, Container>> idToRefContainerMap;
  private final HelixNotifier notifier;
  private final AccountService accountService;
  private final HelixStoreOperator storeOperator;

  /**
   * Initialization for all the tests.
   */
  static {
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connect.string",
        ZK_SERVER_ADDRESS);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path",
        HELIX_STORE_ROOT_PATH);
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    try {
      zkInfo = new ZkInfo(tempDirPath, DC_NAME, ZK_SERVER_PORT, true);
    } catch (IOException e) {
      fail("Failed to instantiate a ZooKeeper server.");
    }
  }

  /**
   * Constructor that does initialization for every test.
   * @throws Exception Any unexpected exception.
   */
  public AccountUpdateToolTest() throws Exception {
    idToRefAccountMap = new HashMap<>();
    idToRefContainerMap = new HashMap<>();
    generateRefAccounts(idToRefAccountMap, idToRefContainerMap, new HashSet<>(), NUM_REF_ACCOUNT,
        NUM_CONTAINER_PER_ACCOUNT);
    tempDirPath = getTempDir("accountUpdateTool-");

    // clean up data on ZooKeeper
    storeOperator = new HelixStoreOperator(storeConfig);
    if (storeOperator.exist("/")) {
      storeOperator.delete("/");
    }

    // instantiate a HelixAccountService and listen to the change for account metadata
    notifier = new HelixNotifier(storeConfig);
    accountService =
        new HelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier).getAccountService();
  }

  @AfterClass
  public static void destroy() throws Exception {
    zkInfo.shutdown();
  }

  /**
   * Tests creating {@link Account}s to {@code ZooKeeper}, where no {@link Account} exists.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testCreateAccount() throws Exception {
    assertEquals("Wrong number of accounts", 0, accountService.getAllAccounts().size());
    assertNotNull(createOrUpdateAccountsAndWait(idToRefAccountMap.values()));
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);
  }

  /**
   * Tests updating {@link Account}s to {@code ZooKeeper}, where the {@link Account}s to update already exist.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateAccount() throws Exception {
    // first, create NUM_REF_ACCOUNT accounts through the tool
    createOrUpdateAccountsAndWait(idToRefAccountMap.values());
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);

    // then, update the name of all the accounts again through the tool
    String accountNameAppendix = "-accountNameAppendix";
    String containerNameAppendix = "-containerNameAppendix";
    Collection<Account> updatedAccounts = new ArrayList<>();
    for (Account account : accountService.getAllAccounts()) {
      AccountBuilder accountBuilder = new AccountBuilder(account).setName(account.getName() + accountNameAppendix);
      for (Container container : account.getAllContainers()) {
        accountBuilder.addOrUpdateContainer(
            new ContainerBuilder(container).setName(container.getName() + containerNameAppendix).build());
      }
      updatedAccounts.add(accountBuilder.build());
    }
    assertNotNull(createOrUpdateAccountsAndWait(updatedAccounts));
    assertAccountsInAccountService(updatedAccounts, NUM_REF_ACCOUNT, accountService);
  }

  /**
   * Tests updating {@link Account}s that are conflicting.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateConflictAccounts() throws Exception {
    Collection<Account> idConflictAccounts = new ArrayList<>();
    // id conflict
    idConflictAccounts.add(new AccountBuilder((short) 1, "account1", Account.AccountStatus.INACTIVE, null).build());
    idConflictAccounts.add(new AccountBuilder((short) 1, "account2", Account.AccountStatus.INACTIVE, null).build());
    try {
      createOrUpdateAccountsAndWait(idConflictAccounts);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    Thread.sleep(100);
    assertEquals("Wrong number of accounts in accountService", 0, accountService.getAllAccounts().size());

    // name conflict
    Collection<Account> nameConflictAccounts = new ArrayList<>();
    nameConflictAccounts.add(new AccountBuilder((short) 1, "account1", Account.AccountStatus.INACTIVE, null).build());
    nameConflictAccounts.add(new AccountBuilder((short) 2, "account1", Account.AccountStatus.INACTIVE, null).build());
    try {
      createOrUpdateAccountsAndWait(nameConflictAccounts);
      fail("Should have thrown.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    Thread.sleep(100);
    assertEquals("Wrong number of accounts in accountService", 0, accountService.getAllAccounts().size());
  }

  /**
   * Tests creating/updating {@link Account}s using bad file containing no valid json array.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testBadJsonFile() throws Exception {
    String badJsonFile = tempDirPath + File.separator + "badJsonFile.json";
    writeStringToFile("Invalid json string", badJsonFile);
    try {
      AccountUpdater.createOrUpdate(badJsonFile, ZK_SERVER_ADDRESS, HELIX_STORE_ROOT_PATH, null, null);
      fail("Should have thrown.");
    } catch (JSONException e) {
      // expected
    }
    Thread.sleep(100);
    assertEquals("Wrong number of accounts in accountService", 0, accountService.getAllAccounts().size());
  }

  /**
   * Writes a collection of {@link Account}s into a file. The file contains and only contains a {@link JSONArray}, where
   * each element is the json string of the {@link Account}.
   * @param accounts The collection of {@link Account}s to write to the file.
   * @param filePath The path of the file to write.
   * @throws Exception Any unexpected exception.
   */
  private static void writeAccountsToFile(Collection<Account> accounts, String filePath) throws Exception {
    JSONArray accountArray = new JSONArray();
    for (Account account : accounts) {
      accountArray.put(account.toJson());
    }
    writeJsonArrayToFile(accountArray, filePath);
  }

  /**
   * Creates or updates a collection of {@link Account}s to {@code ZooKeeper} server.
   * @param accounts The collection of {@link Account}s to create or update.
   * @return The file name if the operation is successful; {@code null} otherwise.
   * @throws Exception Any unexpected exception.
   */
  private static String createOrUpdateAccountsAndWait(Collection<Account> accounts) throws Exception {
    String jsonFilePath = tempDirPath + File.separator + UUID.randomUUID().toString() + ".json";
    writeAccountsToFile(accounts, jsonFilePath);
    int numOfAccounts =
        AccountUpdater.createOrUpdate(jsonFilePath, ZK_SERVER_ADDRESS, HELIX_STORE_ROOT_PATH, null, null);
    // @todo for now it is hard to know when the accounts in an accountService gets updated, so we just blindly
    // @todo sleep for some time until the update happens. There is a planned work to add support into AccountService,
    // @todo so that callback can be registered for account updates.
    Thread.sleep(100);
    return numOfAccounts == -1 ? null : jsonFilePath;
  }
}

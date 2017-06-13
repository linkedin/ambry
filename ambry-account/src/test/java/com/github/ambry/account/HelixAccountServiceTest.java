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

import com.github.ambry.commons.HelixPropertyStoreFactory;
import com.github.ambry.commons.HelixPropertyStoreUtils;
import com.github.ambry.commons.MockHelixPropertyStoreFactory;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.helix.ZNRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.account.HelixAccountService.*;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


/**
 * Unit tests for {@link HelixAccountService}.
 */
public class HelixAccountServiceTest {
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 20000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private static final String ZK_CONNECT_STRING = "dummyHost:dummyPort";
  private static final String STORE_ROOT_PATH = "/ambry_test/helix_account_service";
  private static Random random = new Random();
  private static final String BAD_ACCOUNT_METADATA_STRING = "badAccountMetadataString";
  private static final int NUM_REF_ACCOUNT = 10;
  private static final int NUM_CONTAINER_PER_ACCOUNT = 4;
  private static final HelixPropertyStoreConfig storeConfig =
      HelixPropertyStoreUtils.getHelixStoreConfig(ZK_CONNECT_STRING, ZK_CLIENT_SESSION_TIMEOUT_MS,
          ZK_CLIENT_CONNECTION_TIMEOUT_MS, STORE_ROOT_PATH);
  private static final HelixPropertyStoreFactory<ZNRecord> storeFactory =
      new MockHelixPropertyStoreFactory(false, false);
  private static final Map<Short, Account> idToRefAccountMap = new HashMap<>();
  private static final Map<Short, Map<Short, Container>> idToRefContainerMap = new HashMap<>();

  private Account refAccount;
  private short refAccountId;
  private String refAccountName;
  private AccountStatus refAccountStatus;
  private Container refContainer;
  private short refContainerId;
  private String refContainerName;
  private ContainerStatus refContainerStatus;
  private String refContainerDescription;
  private boolean refContainerPrivacy;
  private short refParentAccountId;
  private HelixAccountService helixAccountService;

  /**
   * Resets variables and settings, and cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  @Before
  public void init() throws Exception {
    generateReferenceAccountsAndContainers();
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
  }

  /**
   * Cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  @After
  public void cleanUp() throws Exception {
    if (helixAccountService != null) {
      helixAccountService.close();
    }
    HelixPropertyStoreUtils.deleteStoreIfExists(storeConfig, storeFactory);
  }

  /**
   * Tests a clean startup of {@link HelixAccountService}, when the corresponding {@code ZooKeeper} does not
   * have any record for it.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testStartUpWithoutMetadataExists() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // At time zero, no account metadata exists.
    assertEquals("The number of account in HelixAccountService is not zero", 0,
        helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests starting up a {@link HelixAccountService}, when the corresponding {@code ZooKeeper} has account metadata
   * already stored on it.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testStartUpWithMetadataExists() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountHelixPropertyStore(idToRefAccountMap.values());
    // When start, the helixAccountService should get the account metadata.
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertAccountsInHelixAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT);
  }

  /**
   * Tests updating account metadata through {@link HelixAccountService}.
   */
  @Test
  public void testCreateAccount() {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
    boolean res = helixAccountService.updateAccounts(new ArrayList<>(idToRefAccountMap.values()));
    assertTrue("Failed to update accounts", res);
    helixAccountService.onMessage(HelixAccountService.ACCOUNT_METADATA_CHANGE_TOPIC,
        HelixAccountService.FULL_ACCOUNT_METADATA_CHANGE_MESSAGE);
    assertAccountsInHelixAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT);
  }

  /**
   * Tests updating accounts through {@link HelixAccountService} in various situations:
   * 1. add a new {@link Account};
   * 2. update existing {@link Account};
   * 3. add a new {@link Container} to an existing {@link Account};
   * 4. update existing {@link Container}s of existing {@link Account}s.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateAccount() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountHelixPropertyStore(idToRefAccountMap.values());
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);

    // add a new account
    Account newAccountWithoutContainer =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, null).build();
    List<Account> accountsToUpdate = Collections.singletonList(newAccountWithoutContainer);
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT);

    // update all existing reference accounts (not the new account)
    accountsToUpdate = new ArrayList<>();
    for (Account account : idToRefAccountMap.values()) {
      AccountBuilder accountBuilder = new AccountBuilder(account);
      accountBuilder.setName(account.getName() + "-extra");
      accountBuilder.setStatus(
          account.getStatus().equals(AccountStatus.ACTIVE) ? AccountStatus.INACTIVE : AccountStatus.ACTIVE);
      accountsToUpdate.add(accountBuilder.build());
    }
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT);

    // add one container to the new account
    AccountBuilder accountBuilder = new AccountBuilder(newAccountWithoutContainer);
    accountsToUpdate = Collections.singletonList(accountBuilder.addOrUpdateContainer(refContainer).build());
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT);

    // update existing containers for all the reference accounts (not for the new account)
    accountsToUpdate = new ArrayList<>();
    for (Account account : idToRefAccountMap.values()) {
      accountBuilder = new AccountBuilder(account);
      for (Container container : account.getAllContainers()) {
        ContainerBuilder containerBuilder = new ContainerBuilder(container);
        containerBuilder.setId((short) (-1 * (container.getId())));
        containerBuilder.setName(container.getName() + "-extra");
        containerBuilder.setStatus(
            container.getStatus().equals(ContainerStatus.ACTIVE) ? ContainerStatus.INACTIVE : ContainerStatus.ACTIVE);
        containerBuilder.setDescription(container.getDescription() + "--extra");
        containerBuilder.setIsPrivate(!container.isPrivate());
        accountBuilder.addOrUpdateContainer(containerBuilder.build());
      }
      accountsToUpdate.add(accountBuilder.build());
    }
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * is empty.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase1() throws Exception {
    writeZNRecordToHelixPropertyStore(makeZNRecordWithSimpleField(null, null, null));
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 1);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * contains a simple field of irrelevant data.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase2() throws Exception {
    writeZNRecordToHelixPropertyStore(makeZNRecordWithSimpleField(null, "key", "value"));
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 1);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * has a valid value for the map field, but the key field is not {@link HelixAccountService#ACCOUNT_METADATA_MAP_KEY}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase3() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put(String.valueOf(refAccount.getId()), refAccount.toJson().toString());
    writeZNRecordToHelixPropertyStore(makeZNRecordWithMapField(null, "key", mapValue));
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 1);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * has a valid key and value for the map field, but the record does not match the key.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase4() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put("-1", refAccount.toJson().toString());
    writeZNRecordToHelixPropertyStore(makeZNRecordWithMapField(null, ACCOUNT_METADATA_MAP_KEY, mapValue));
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 1);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * has a valid key for the map field, the record string is not a json string.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase5() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put(String.valueOf(refAccount.getId()), BAD_ACCOUNT_METADATA_STRING);
    writeZNRecordToHelixPropertyStore(makeZNRecordWithMapField(null, ACCOUNT_METADATA_MAP_KEY, mapValue));
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals(0, helixAccountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 1);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link org.apache.helix.store.HelixPropertyStore}, where the {@link ZNRecord}
   * has a random number of invalid records. The {@link HelixAccountService} should still be able to get the valid
   * {@link Account}s without taking the invalid ones.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase6() throws Exception {
    int badRecordCount = 0;
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    List<Account> goodAccounts = new ArrayList<>();
    List<Account> badAccounts = new ArrayList<>();
    Map<String, String> accountMap = new HashMap<>();
    for (Account account : idToRefAccountMap.values()) {
      if (random.nextDouble() < 0.3) {
        accountMap.put(String.valueOf(account.getId()), BAD_ACCOUNT_METADATA_STRING);
        badRecordCount++;
        badAccounts.add(account);
      } else {
        accountMap.put(String.valueOf(account.getId()), account.toJson().toString());
        goodAccounts.add(account);
      }
    }
    zNRecord.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
    writeZNRecordToHelixPropertyStore(zNRecord);
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertAccountsInHelixAccountService(goodAccounts, NUM_REF_ACCOUNT - badRecordCount);
    // asserts the bad accounts do not exist in the helixAccountService
    for (Account account : badAccounts) {
      assertAccountNotExistInHelixAccountService(account);
    }
  }

  /**
   * Tests receiving a bad message, it will not be recognized by {@link HelixAccountService}, but will also not
   * crash the service.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void receiveBadMessage() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
    writeAccountHelixPropertyStore(idToRefAccountMap.values());
    helixAccountService.onMessage(ACCOUNT_METADATA_CHANGE_TOPIC, "badMessage");
    assertEquals("The number of account in HelixAccountService is different from expected", 0,
        helixAccountService.getAllAccounts().size());
  }

  /**
   * Tests a number of bad inputs.
   */
  @Test
  public void testNullInputs() {
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
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateIdConflictingAccounts() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build());
    conflictAccounts.add(new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE, null).build());
    updateConflictAccountsAndFail(conflictAccounts, IllegalArgumentException.class);
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in id.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNameConflictingAccounts() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE, null).build());
    updateConflictAccountsAndFail(conflictAccounts, IllegalArgumentException.class);
  }

  /**
   * Tests updating a collection of {@link Account}s, where there are duplicate {@link Account}s in id and name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateDuplicateAccounts() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build());
    updateConflictAccountsAndFail(conflictAccounts, IllegalArgumentException.class);
  }

  /**
   * Tests updating a {@link Account}, which has the same id and name as an existing record, and will replace the
   * existing record. This test corresponds to case A specified in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNonConflictAccountCaseA() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> nonConflictAccounts =
        Collections.singleton((new AccountBuilder((short) 1, "a", AccountStatus.ACTIVE, null).build()));
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 2);
  }

  /**
   * Tests updating a {@link Account}, which has the same id as an existing record and a non-conflicting name with any
   * of the existing record. The new record will replace the existing record. This test corresponds to case B specified
   * in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNonConflictAccountCaseB() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> nonConflictAccounts =
        Collections.singleton((new AccountBuilder((short) 1, "c", AccountStatus.ACTIVE, null).build()));
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 2);
  }

  /**
   * Tests updating a {@link Account}, which has a new id and name different from any of the existing record. The
   * new record will replace the existing record. This test corresponds to case C specified in the JavaDoc of
   * {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNonConflictAccountCaseC() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> nonConflictAccounts =
        Collections.singleton((new AccountBuilder((short) 3, "c", AccountStatus.ACTIVE, null).build()));
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 3);
  }

  /**
   * Tests updating a {@link Account}, which has a new id but a name conflicting with an existing record. The update
   * operation will fail. This test corresponds to case D specified in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateWithAccountConflictCaseD() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 3, "a", AccountStatus.INACTIVE, null).build()));
    updateConflictAccountsAndFail(conflictAccounts, IllegalArgumentException.class);
  }

  /**
   * Tests updating a {@link Account}, which has the same id as an existing record, but the name conflicting with
   * another existing record. The update operation will fail. This test corresponds to case E specified in the JavaDoc
   * of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateWithAccountConflictCaseE() throws Exception {
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE, null).build()));
    updateConflictAccountsAndFail(conflictAccounts, IllegalArgumentException.class);
  }

  /**
   * Tests reading conflicting {@link Account} metadata from {@link org.apache.helix.store.HelixPropertyStore}. Duplicate
   * record was written to the store.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase1() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build();
    conflictAccounts.add(account);
    conflictAccounts.add(account);

    writeAccountHelixPropertyStore(conflictAccounts);
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("Wrong number of accounts in helixAccountService", 1, helixAccountService.getAllAccounts().size());
    assertAccountInHelixAccountService(account);
  }

  /**
   * Tests reading conflicting {@link Account} metadata from {@link org.apache.helix.store.HelixPropertyStore}. Two
   * records have different accountIds but the same accountNames.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase2() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build();
    Account account2 = new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE, null).build();
    conflictAccounts.add(account1);
    conflictAccounts.add(account2);

    writeAccountHelixPropertyStore(conflictAccounts);
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("Wrong number of accounts in helixAccountService", 1, helixAccountService.getAllAccounts().size());
    assertAccountInHelixAccountService(account2);
    assertAccountNotExistInHelixAccountService(account1);
  }

  /**
   * Tests reading conflicting {@link Account} metadata from {@link org.apache.helix.store.HelixPropertyStore}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase3() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build();
    Account account2 = new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE, null).build();
    Account account3 = new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE, null).build();
    Account account4 = new AccountBuilder((short) 3, "b", AccountStatus.INACTIVE, null).build();
    conflictAccounts.add(account1);
    conflictAccounts.add(account2);
    conflictAccounts.add(account3);
    conflictAccounts.add(account4);

    writeAccountHelixPropertyStore(conflictAccounts);
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("Wrong number of accounts in helixAccountService", 2, helixAccountService.getAllAccounts().size());
    assertAccountInHelixAccountService(account1);
    assertAccountInHelixAccountService(account4);
    assertAccountNotExistInHelixAccountService(account2);
    assertAccountNotExistInHelixAccountService(account3);
  }

  /**
   * Tests reading conflicting {@link Account} metadata from {@link org.apache.helix.store.HelixPropertyStore}. Two
   * records with the same accountIds but different accountNames were written into the store.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase4() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build();
    Account account2 = new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE, null).build();
    conflictAccounts.add(account1);
    conflictAccounts.add(account2);

    writeAccountHelixPropertyStore(conflictAccounts);
    helixAccountService = new HelixAccountService(storeConfig, storeFactory);
    assertEquals("Wrong number of accounts in helixAccountService", 1, helixAccountService.getAllAccounts().size());
    assertAccountInHelixAccountService(account2);
    assertAccountNotExistInHelixAccountService(account1);
  }

  /**
   * Asserts that updating conflicting {@link Account} will fail and throw an exception as expected.
   * @param accounts A collection of {@link Account}s that either conflict among themselves or with the existing
   *                 {@link Account}s.
   * @param exceptionClass The class of expected exception.
   */
  private void updateConflictAccountsAndFail(Collection<Account> accounts, Class<?> exceptionClass) {
    try {
      helixAccountService.updateAccounts(accounts);
      fail("should have thrown");
    } catch (Exception e) {
      // expected
      System.out.println(e.getMessage());
      assertEquals("Wrong exception", exceptionClass, e.getClass());
    }
  }

  /**
   * Pre-populates two {@link Account}s: (id=1, name="a") and (id=2, name="b") in
   * {@link org.apache.helix.store.HelixPropertyStore}.
   * @throws Exception Any unexpected exception.
   */
  private void writeAccountsForConflictTest() throws Exception {
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE, null).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE, null).build());
    updateAccountsAndAssertAccountExistence(existingAccounts, 2);
  }

  /**
   * Updates a collection of {@link Account}s through {@link HelixAccountService}, and verifies that the
   * {@link Account}s have been successfully updated, so they can be queried through {@link HelixAccountService}.
   * @param accounts A collection of {@link Account}s to update through {@link HelixAccountService}.
   */
  private void updateAccountsAndAssertAccountExistence(Collection<Account> accounts, int expectedAccountCount) {
    boolean hasUpdateAccountSucceed = helixAccountService.updateAccounts(accounts);
    assertTrue("Updating account failed.", hasUpdateAccountSucceed);
    helixAccountService.onMessage(HelixAccountService.ACCOUNT_METADATA_CHANGE_TOPIC,
        HelixAccountService.FULL_ACCOUNT_METADATA_CHANGE_MESSAGE);
    assertAccountsInHelixAccountService(accounts, expectedAccountCount);
  }

  /**
   * Assert a collection of {@link Account}s exist in the {@link HelixAccountService}.
   * @param accounts The collection of {@link Account}s to assert their existence.
   * @param expectedAccountCount The expected number of {@link Account}s in the {@link HelixAccountService}.
   */
  private void assertAccountsInHelixAccountService(Collection<Account> accounts, int expectedAccountCount) {
    assertEquals("Wrong number of accounts in HelixAccountService", expectedAccountCount,
        helixAccountService.getAllAccounts().size());
    for (Account account : accounts) {
      assertAccountInHelixAccountService(account);
    }
  }

  /**
   * Asserts that an {@link Account} does not exist in the {@link HelixAccountService}.
   * @param account The {@link Account} to assert.
   */
  private void assertAccountNotExistInHelixAccountService(Account account) {
    Account found = helixAccountService.getAccountById(account.getId());
    if (found != null) {
      assertFalse("Account incorrectly exists in HelixAccountService.", account.equals(found));
    }
  }

  /**
   * Asserts that an {@link Account} exists in the {@link HelixAccountService}.
   * @param accountToAssert The {@link Account} to assert existence.
   */
  private void assertAccountInHelixAccountService(Account accountToAssert) {
    Account accountFoundById = helixAccountService.getAccountById(accountToAssert.getId());
    Account accountFoundByName = helixAccountService.getAccountByName(accountToAssert.getName());
    assertEquals("Account got by id from helixAccountService does not match account got by name.", accountFoundById,
        accountFoundByName);
    assertEquals("Account got by id from helixAccountService does not match the assert to assert", accountFoundById,
        accountToAssert);
    assertEquals("The number of containers in the account is wrong.", accountFoundById.getAllContainers().size(),
        accountToAssert.getAllContainers().size());
    for (Container container : accountToAssert.getAllContainers()) {
      assertContainerInHelixAccountService(container);
    }
  }

  /**
   * Assert that a {@link Container} exists in the {@link HelixAccountService}.
   * @param containerToAssert The {@link Container} to assert.
   */
  private void assertContainerInHelixAccountService(Container containerToAssert) {
    Container containerFoundById = helixAccountService.getAccountById(containerToAssert.getParentAccountId())
        .getContainerById(containerToAssert.getId());
    Container containerFoundByName = helixAccountService.getAccountById(containerToAssert.getParentAccountId())
        .getContainerByName(containerToAssert.getName());
    assertEquals("Container got by id from helixAccountService/account does not match container got by name.",
        containerFoundById, containerFoundByName);
    assertEquals("Container got by id from helixAccountService/account does not match the container to assert",
        containerFoundById, containerToAssert);
  }

  /**
   * Pre-populates a collection of {@link Account}s to the underlying {@link org.apache.helix.store.HelixPropertyStore}
   * using {@link com.github.ambry.commons.HelixPropertyStoreUtils.HelixStoreOperator} (not through the
   * {@link HelixAccountService}). This method does not check any conflict among the {@link Account}s to write.
   * @throws Exception Any unexpected exception.
   */
  private void writeAccountHelixPropertyStore(Collection<Account> accounts) throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    for (Account account : accounts) {
      accountMap.put(String.valueOf(account.getId()), account.toJson().toString());
    }
    zNRecord.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
    // Write account metadata into HelixPropertyStore.
    storeOperator.write(HelixAccountService.FULL_ACCOUNT_METADATA_PATH, zNRecord);
  }

  /**
   * Writes a {@link ZNRecord} to {@link org.apache.helix.store.HelixPropertyStore}.
   * @param zNRecord The {@link ZNRecord} to write.
   * @throws Exception Any unexpected exception.
   */
  private void writeZNRecordToHelixPropertyStore(ZNRecord zNRecord) throws Exception {
    HelixPropertyStoreUtils.HelixStoreOperator storeOperator =
        HelixPropertyStoreUtils.getStoreOperator(storeConfig, storeFactory);
    storeOperator.write(HelixAccountService.FULL_ACCOUNT_METADATA_PATH, zNRecord);
  }

  /**
   * Add or update a {@link ZNRecord} to include a map field with the given key and value. If the supplied
   * {@link ZNRecord} is null, a new {@link ZNRecord} will be created.
   * @param oldRecord A {@link ZNRecord} to add or update its map field. {@code null} indicates to create a
   *                  new {@link ZNRecord}.
   * @param mapKey The key for the map field.
   * @param mapValue The value for the map field.
   * @return A {@link ZNRecord} including a map field with the given key and value.
   */
  private ZNRecord makeZNRecordWithMapField(ZNRecord oldRecord, String mapKey, Map<String, String> mapValue) {
    ZNRecord zNRecord = oldRecord == null ? new ZNRecord(String.valueOf(System.currentTimeMillis())) : oldRecord;
    if (mapKey != null && mapValue != null) {
      zNRecord.setMapField(mapKey, mapValue);
    }
    return zNRecord;
  }

  /**
   * Add or update a {@link ZNRecord} to include a simple field with the given key and value. If the supplied
   * {@link ZNRecord} is null, a new {@link ZNRecord} will be created.
   * @param oldRecord A {@link ZNRecord} to add or update its simple field. {@code null} indicates to create a
   *                  new {@link ZNRecord}.
   * @param key The key for the simple field.
   * @param value The value for the simple field.
   * @return A {@link ZNRecord} including a simple field with the given key and value.
   */
  private ZNRecord makeZNRecordWithSimpleField(ZNRecord oldRecord, String key, String value) {
    ZNRecord zNRecord = oldRecord == null ? new ZNRecord(String.valueOf(System.currentTimeMillis())) : oldRecord;
    if (key != null && value != null) {
      zNRecord.setSimpleField(key, value);
    }
    return zNRecord;
  }

  /**
   * Randomly generates a collection of reference {@link Account}s and {@link Container}s that can be referred from
   * from {@link #idToRefAccountMap} and {@link #idToRefContainerMap}. It also generates a single {@link Account}
   * and {@link Container}.
   * @throws Exception Any unexpected exception.
   */
  private void generateReferenceAccountsAndContainers() throws Exception {
    Set accountIdSet = new HashSet<>();

    // generate a single reference account and container that can be referenced by refAccount and refContainer respectively.
    refAccountId = Utils.getRandomShort(random);
    accountIdSet.add(refAccountId);
    refAccountName = UUID.randomUUID().toString();
    refAccountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
    refContainerId = Utils.getRandomShort(random);
    refContainerName = UUID.randomUUID().toString();
    refContainerStatus = random.nextBoolean() ? ContainerStatus.ACTIVE : ContainerStatus.INACTIVE;
    refContainerDescription = UUID.randomUUID().toString();
    refContainerPrivacy = random.nextBoolean();
    refParentAccountId = refAccountId;
    refContainer = new ContainerBuilder(refContainerId, refContainerName, refContainerStatus, refContainerDescription,
        refContainerPrivacy, refParentAccountId).build();
    refAccount =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus, Collections.singleton(refContainer)).build();

    // generates NUM_REF_ACCOUNT reference account and store them in idToRefAccountMap and idToRefContainerMap.
    idToRefAccountMap.clear();
    idToRefContainerMap.clear();
    for (int i = 0; i < NUM_REF_ACCOUNT; i++) {
      short accountId = Utils.getRandomShort(random);
      if (!accountIdSet.add(accountId)) {
        i--;
        continue;
      }
      String accountName = UUID.randomUUID().toString();
      AccountStatus accountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
      Map<Short, Container> idToContainers = new HashMap<>();
      List<Container> containers = new ArrayList<>();
      Set<Short> containerIdSet = new HashSet<>();
      for (int j = 0; j < NUM_CONTAINER_PER_ACCOUNT; j++) {
        short containerId = Utils.getRandomShort(random);
        if (!containerIdSet.add(containerId)) {
          j--;
          continue;
        }
        String containerName = UUID.randomUUID().toString();
        ContainerStatus containerStatus = random.nextBoolean() ? ContainerStatus.ACTIVE : ContainerStatus.INACTIVE;
        String containerDescription = UUID.randomUUID().toString();
        boolean containerPrivacy = random.nextBoolean();
        Container container =
            new ContainerBuilder(containerId, containerName, containerStatus, containerDescription, containerPrivacy,
                accountId).build();
        containers.add(container);
        idToContainers.put(containerId, container);
      }
      Account account = new AccountBuilder(accountId, accountName, accountStatus, containers).build();
      assertEquals("Wrong number of generated containers for the account", NUM_CONTAINER_PER_ACCOUNT,
          account.getAllContainers().size());
      idToRefAccountMap.put(accountId, account);
      idToRefContainerMap.put(accountId, idToContainers);
    }
    assertEquals("Wrong number of generated accounts", NUM_REF_ACCOUNT, idToRefAccountMap.size());
  }
}
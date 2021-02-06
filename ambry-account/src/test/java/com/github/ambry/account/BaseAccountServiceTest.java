/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.utils.AccountTestUtils.*;
import static org.junit.Assert.*;


/**
 * Common unit tests for {@link AccountService} implementations.
 */
public abstract class BaseAccountServiceTest {
  private static final Random random = new Random();
  private static final int NUM_REF_ACCOUNT = 10;
  private static final int NUM_CONTAINER_PER_ACCOUNT = 4;
  private static final Map<Short, Account> idToRefAccountMap = new HashMap<>();
  private static final Map<Short, Map<Short, Container>> idToRefContainerMap = new HashMap<>();
  private final Properties configProps = new Properties();
  private final Path accountBackupDir;
  private VerifiableProperties vConfigProps;
  private Account refAccount;
  private short refAccountId;
  private String refAccountName;
  private AccountStatus refAccountStatus;
  private Container refContainer;
  private short refContainerId;
  private String refContainerName;
  private ContainerStatus refContainerStatus;
  private String refContainerDescription;
  private boolean refContainerCaching;
  private boolean refContainerEncryption;
  private boolean refContainerPreviousEncryption;
  private boolean refContainerMediaScanDisabled;
  private boolean refContainerTtlRequired;
  private String refReplicationPolicy;
  private short refParentAccountId;
  private AccountService accountService;
  private AccountServiceFactory accountServiceFactory;

  /**
   * Resets variables and settings, and cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  public BaseAccountServiceTest(AccountServiceFactory accountServiceFactory) throws Exception {
    this.accountServiceFactory = accountServiceFactory;
    this.accountService = accountServiceFactory.getAccountService();
    accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    setup();
    generateReferenceAccountsAndContainers();
  }

  @Before
  public void setup() {
    configProps.clear();
    // TODO: configProps.setProperty(AccountServiceConfig.BACKUP_DIRECTORY_KEY, accountBackupDir.toString());
    vConfigProps = new VerifiableProperties(configProps);
  }

  /**
   * Cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  @After
  public void cleanUp() throws Exception {
    if (accountService != null) {
      accountService.close();
    }
    deleteBackupDirectoryIfExist();
  }

  /**
   * Tests creating a number of new {@link Account}s.
   */
  @Test
  public void testCreateAccount() throws Exception {
    assertEquals("The number of accounts is incorrect", 0, accountService.getAllAccounts().size());
    accountService.updateAccounts(idToRefAccountMap.values());
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);
  }

  /**
   * Tests {@link AccountService#getContainersByStatus(ContainerStatus)} with generated {@links Container}s
   */
  @Test
  public void testGetContainerByStatus() throws Exception {
    // a set that records the account ids that have already been taken.
    Set<Short> accountIdSet = new HashSet<>();
    // generate a single reference account and container that can be referenced by refAccount and refContainer respectively.
    refAccountId = Utils.getRandomShort(random);
    accountIdSet.add(refAccountId);
    generateRefAccounts(idToRefAccountMap, idToRefContainerMap, accountIdSet, 5, 2);

    accountService.updateAccounts(idToRefAccountMap.values());
    assertAccountsInAccountService(idToRefAccountMap.values(), 5, accountService);

    List<Account> accountsToUpdate = new ArrayList<>();
    int cnt = 0;
    for (Account account : accountService.getAllAccounts()) {
      AccountBuilder accountBuilder = new AccountBuilder(account);
      for (Container container : account.getAllContainers()) {
        if (cnt % 2 == 0) {
          ContainerBuilder containerBuilder = new ContainerBuilder(container);
          containerBuilder.setId((short) (-1 * (container.getId())));
          containerBuilder.setName(container.getName() + "-extra");
          containerBuilder.setStatus(ContainerStatus.DELETE_IN_PROGRESS);
          containerBuilder.setDescription(container.getDescription() + "--extra");
          containerBuilder.setReplicationPolicy(container.getReplicationPolicy() + "---extra");
          containerBuilder.setTtlRequired(container.isTtlRequired());
          accountBuilder.addOrUpdateContainer(containerBuilder.build());
        }
        cnt++;
      }
      accountsToUpdate.add(accountBuilder.build());
    }

    updateAccountsAndAssertAccountExistence(accountsToUpdate, 5, true);
    Set<Container> containerList = accountService.getContainersByStatus(ContainerStatus.DELETE_IN_PROGRESS);
    assertEquals("Wrong number of containers in containerList", 5, containerList.size());
    for (Container container : containerList) {
      assertSame("Container status mismatch", container.getStatus(), ContainerStatus.DELETE_IN_PROGRESS);
    }
  }

  /**
   * Tests creating and updating accounts in various situations:
   * 0. {@link Account}s already exists.
   * 1. add a new {@link Account};
   * 2. update existing {@link Account};
   * 3. add a new {@link Container} to an existing {@link Account};
   * 4. update existing {@link Container}s of existing {@link Account}s.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateAccount() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountsToStore(idToRefAccountMap.values(), false);
    accountService = accountServiceFactory.getAccountService();
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);

    // add a new account
    Account newAccountWithoutContainer = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    List<Account> accountsToUpdate = Collections.singletonList(newAccountWithoutContainer);
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);

    // update all existing reference accounts (not the new account)
    accountsToUpdate = new ArrayList<>();
    for (Account account : accountService.getAllAccounts()) {
      AccountBuilder accountBuilder = new AccountBuilder(account);
      accountBuilder.name(account.getName() + "-extra");
      accountBuilder.status(
          account.getStatus().equals(AccountStatus.ACTIVE) ? AccountStatus.INACTIVE : AccountStatus.ACTIVE);
      accountsToUpdate.add(accountBuilder.build());
    }
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);

    // add one container to the new account
    AccountBuilder accountBuilder = new AccountBuilder(accountService.getAccountById(refAccountId));
    accountsToUpdate = Collections.singletonList(accountBuilder.addOrUpdateContainer(refContainer).build());
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);

    // update existing containers for all the reference accounts (not for the new account)
    accountsToUpdate = new ArrayList<>();
    for (Account account : accountService.getAllAccounts()) {
      accountBuilder = new AccountBuilder(account);
      for (Container container : account.getAllContainers()) {
        ContainerBuilder containerBuilder = new ContainerBuilder(container);
        containerBuilder.setId((short) (-1 * (container.getId())));
        containerBuilder.setName(container.getName() + "-extra");
        containerBuilder.setStatus(
            container.getStatus().equals(ContainerStatus.ACTIVE) ? ContainerStatus.INACTIVE : ContainerStatus.ACTIVE);
        containerBuilder.setDescription(container.getDescription() + "--extra");
        containerBuilder.setReplicationPolicy(container.getReplicationPolicy() + "---extra");
        containerBuilder.setTtlRequired(!container.isTtlRequired());
        accountBuilder.addOrUpdateContainer(containerBuilder.build());
      }
      accountsToUpdate.add(accountBuilder.build());
    }
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);
  }

  /**
   * Test adding container to an existing account.
   */
  @Test
  public void testAddContainer() throws Exception {
    assertEquals("The number of account in HelixAccountService is incorrect", 0,
        accountService.getAllAccounts().size());
    // add an account with single container
    accountService.updateAccounts(Collections.singletonList(refAccount));

    Container brandNewContainer = new ContainerBuilder(refContainer).setId(UNKNOWN_CONTAINER_ID).build();

    // 1. test invalid input
    try {
      accountService.updateContainers("", null);
      fail("should fail because input is invalid");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.BadRequest, e.getErrorCode());
    }

    // 2. test account is not found
    String fakeAccountName = refAccountName + "fake";
    Container containerToAdd1 =
        new ContainerBuilder(UNKNOWN_CONTAINER_ID, "newContainer", ContainerStatus.ACTIVE, "description",
            refParentAccountId).build();
    try {
      accountService.updateContainers(fakeAccountName, Collections.singleton(containerToAdd1));
      fail("should fail because account is not found");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    // 3. test conflict container (new container with existing name but different attributes)
    Container conflictContainer = new ContainerBuilder(brandNewContainer).setBackupEnabled(true).build();
    try {
      accountService.updateContainers(refAccountName, Collections.singleton(conflictContainer));
      fail("should fail because there is a conflicting container");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.ResourceConflict, e.getErrorCode());
    }

    // 4. test adding same container twice, should be no-op and return result should include container with id
    Collection<Container> addedContainers =
        accountService.updateContainers(refAccountName, Collections.singleton(brandNewContainer));
    assertEquals("Mismatch in return count", 1, addedContainers.size());
    for (Container container : addedContainers) {
      assertEquals("Mismatch in account id", refAccountId, container.getParentAccountId());
      assertEquals("Mismatch in container id", refContainerId, container.getId());
      assertEquals("Mismatch in container name", refContainerName, container.getName());
    }

    // 5. (Helix only): test adding a different container (failure case due to ZK update failure)

    // 6. test adding 2 different containers (success case)
    Container containerToAdd2 =
        new ContainerBuilder(UNKNOWN_CONTAINER_ID, "newContainer2", ContainerStatus.ACTIVE, "description",
            refParentAccountId).build();
    addedContainers = accountService.updateContainers(refAccountName, Arrays.asList(containerToAdd1, containerToAdd2));
    for (Container container : addedContainers) {
      assertEquals("Mismatch in account id", refAccountId, container.getParentAccountId());
    }
  }

  /**
   * Test updating an existing container (id specified).
   */
  @Test
  public void testUpdateContainer() throws Exception {
    assertEquals("The number of account in HelixAccountService is incorrect", 0,
        accountService.getAllAccounts().size());
    // add an account with single container
    accountService.updateAccounts(Collections.singletonList(refAccount));

    // 1. Update existing container (success case)
    Container modifiedContainer = new ContainerBuilder(refContainer).setDescription("Different than before").build();
    Collection<Container> updatedContainers =
        accountService.updateContainers(refAccountName, Collections.singleton(modifiedContainer));
    assertEquals("Mismatch in return count", 1, updatedContainers.size());
    assertEquals("Mismatch in container data", modifiedContainer, updatedContainers.iterator().next());

    // 2. Update container with invalid name
    Container badContainer =
        new ContainerBuilder(refContainerId, "Unknown container", ContainerStatus.ACTIVE, "foo", refAccountId).build();
    try {
      accountService.updateContainers(refAccountName, Collections.singleton(badContainer));
      fail("should fail because container is not found");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    // 2. Update container with valid name and wrong id
    badContainer = new ContainerBuilder(refContainer).setId((short) (refContainerId + 7)).build();
    try {
      accountService.updateContainers(refAccountName, Collections.singleton(badContainer));
      fail("should fail due to wrong id");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.ResourceConflict, e.getErrorCode());
    }
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNameConflictingAccounts() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE).build());
    updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in id.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateIdConflictingAccounts() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE).build());
    updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
  }

  /**
   * Tests updating a collection of {@link Account}s, where there are duplicate {@link Account}s in id and name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateDuplicateAccounts() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
  }

  /**
   * Tests updating a {@link Account}, which has the same id and name as an existing record, and will replace the
   * existing record. This test corresponds to case A specified in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testNonConflictingUpdateCaseA() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Account accountToUpdate = accountService.getAccountById((short) 1);
    Collection<Account> nonConflictAccounts =
        Collections.singleton(new AccountBuilder(accountToUpdate).status(AccountStatus.ACTIVE).build());
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 2, true);
  }

  /**
   * Tests updating a {@link Account}, which has the same id as an existing record and a non-conflicting name with any
   * of the existing record. The new record will replace the existing record. This test corresponds to case B specified
   * in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testNonConflictingUpdateCaseB() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Account accountToUpdate = accountService.getAccountById((short) 1);
    Collection<Account> nonConflictAccounts =
        Collections.singleton((new AccountBuilder(accountToUpdate).status(AccountStatus.ACTIVE).build()));
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 2, true);
  }

  /**
   * Tests updating a {@link Account}, which has a new id and name different from any of the existing record. The
   * new record will replace the existing record. This test corresponds to case C specified in the JavaDoc of
   * {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testNonConflictingUpdateCaseC() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> nonConflictAccounts =
        Collections.singleton((new AccountBuilder((short) 3, "c", AccountStatus.ACTIVE).build()));
    updateAccountsAndAssertAccountExistence(nonConflictAccounts, 3, true);
  }

  /**
   * Tests updating a {@link Account}, which has a new id but a name conflicting with an existing record. The update
   * operation will fail. This test corresponds to case D specified in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testConflictingUpdateCaseD() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 3, "a", AccountStatus.INACTIVE).build()));
    updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
    assertEquals("Wrong account number in HelixAccountService", 2, accountService.getAllAccounts().size());
    assertNull("Wrong account got from HelixAccountService", accountService.getAccountById((short) 3));
  }

  /**
   * Tests updating a {@link Account}, which has the same id as an existing record, but the name conflicting with
   * another existing record. The update operation will fail. This test corresponds to case E specified in the JavaDoc
   * of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testConflictingUpdateCaseE() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE).build()));
    updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
    assertEquals("Wrong account number in HelixAccountService", 2, accountService.getAllAccounts().size());
    assertEquals("Wrong account name got from HelixAccountService", "a",
        accountService.getAccountById((short) 1).getName());
  }

  /**
   * Test updating an account with a conflicting expected snapshot version.
   */
  @Test
  public void testConflictingSnapshotVersionUpdate() throws Exception {
    accountService = accountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Account expectedAccount = accountService.getAccountById((short) 1);
    int currentSnapshotVersion = expectedAccount.getSnapshotVersion();
    for (int snapshotVersionOffset : new int[]{-2, -1, 1}) {
      int snapshotVersionToUse = currentSnapshotVersion + snapshotVersionOffset;
      Collection<Account> conflictAccounts = Collections.singleton(
          new AccountBuilder((short) 1, "c", AccountStatus.INACTIVE).snapshotVersion(snapshotVersionToUse).build());
      updateAccountsExpectException(conflictAccounts, AccountServiceErrorCode.ResourceConflict);
      assertEquals("Wrong account number in HelixAccountService", 2, accountService.getAllAccounts().size());
      Account account = accountService.getAccountById((short) 1);
      assertEquals("Account should not have been updated", expectedAccount, account);
      assertEquals("Snapshot version should not have been updated", currentSnapshotVersion,
          account.getSnapshotVersion());
    }
    Collection<Account> validAccounts = Collections.singleton(
        new AccountBuilder((short) 1, "c", AccountStatus.INACTIVE).snapshotVersion(currentSnapshotVersion).build());
    updateAccountsAndAssertAccountExistence(validAccounts, 2, true);
  }

  /**
   * Tests adding/removing {@link Consumer}.
   * @throws Exception
   */
  @Test
  public void testAccountUpdateConsumer() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountsToStore(idToRefAccountMap.values(), false);
    accountService = accountServiceFactory.getAccountService();
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);

    // add consumer
    int numOfConsumers = 10;
    List<Collection<Account>> updatedAccountsReceivedByConsumers = new ArrayList<>();
    List<Consumer<Collection<Account>>> accountUpdateConsumers = IntStream.range(0, numOfConsumers)
        .mapToObj(i -> (Consumer<Collection<Account>>) updatedAccountsReceivedByConsumers::add)
        .peek(accountService::addAccountUpdateConsumer)
        .collect(Collectors.toList());

    // listen to adding a new account
    Account newAccount = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    Set<Account> accountsToUpdate = Collections.singleton(newAccount);
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);
    assertAccountUpdateConsumers(accountsToUpdate, numOfConsumers, updatedAccountsReceivedByConsumers);

    // listen to modification of existing accounts. Only updated accounts will be received by consumers.
    updatedAccountsReceivedByConsumers.clear();
    accountsToUpdate = new HashSet<>();
    for (Account account : accountService.getAllAccounts()) {
      AccountBuilder accountBuilder = new AccountBuilder(account);
      accountBuilder.name(account.getName() + "-extra");
      accountsToUpdate.add(accountBuilder.build());
    }
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);
    assertAccountUpdateConsumers(accountsToUpdate, numOfConsumers, updatedAccountsReceivedByConsumers);

    // removes the consumers so the consumers will not be informed.
    updatedAccountsReceivedByConsumers.clear();
    for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
      accountService.removeAccountUpdateConsumer(accountUpdateConsumer);
    }
    Account account = accountService.getAccountById(refAccountId);
    Account updatedAccount = new AccountBuilder(account).name(account.getName() + "-extra").build();
    accountsToUpdate = new HashSet<>(Collections.singleton(updatedAccount));
    updateAccountsAndAssertAccountExistence(accountsToUpdate, 1 + NUM_REF_ACCOUNT, true);
    assertAccountUpdateConsumers(Collections.emptySet(), 0, updatedAccountsReceivedByConsumers);
  }

  // TODO: tests for disabled backups, disabled updates (needs moving properties to AccountServiceConfig)

  /**
   * Asserts the {@link Account}s received by the {@link Consumer} are as expected.
   * @param expectedAccounts The expected collection of {@link Account}s that should be received by the {@link Consumer}s.
   * @param expectedNumberOfConsumers The expected number of {@link Consumer}s.
   * @param accountsInConsumers A list of collection of {@link Account}s, where each collection of {@link Account}s are
   *                            received by one {@link Consumer}.
   */
  private void assertAccountUpdateConsumers(Set<Account> expectedAccounts, int expectedNumberOfConsumers,
      List<Collection<Account>> accountsInConsumers) throws Exception {
    assertEquals("Wrong number of consumers", expectedNumberOfConsumers, accountsInConsumers.size());
    for (Collection<Account> accounts : accountsInConsumers) {
      assertEquals("Wrong number of updated accounts received by consumers", expectedAccounts.size(), accounts.size());
      for (Account account : accounts) {
        assertTrue("Account update not received by consumers", expectedAccounts.contains(account));
      }
      TestUtils.assertException(UnsupportedOperationException.class,
          () -> accounts.add(InMemoryUnknownAccountService.UNKNOWN_ACCOUNT), null);
    }
  }

  /**
   * Pre-populates two {@link Account}s: (id=1, name="a") and (id=2, name="b") in
   * {@link HelixPropertyStore}.
   * @throws Exception Any unexpected exception.
   */
  private void writeAccountsForConflictTest() throws Exception {
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE).build());
    updateAccountsAndAssertAccountExistence(existingAccounts, 2, true);
  }

  /**
   * Updates a collection of {@link Account}s through {@link HelixAccountService}, and verifies that the
   * {@link Account}s have been successfully updated, so they can be queried through {@link HelixAccountService}.
   * @param accounts A collection of {@link Account}s to update through {@link HelixAccountService}.
   */
  private void updateAccountsAndAssertAccountExistence(Collection<Account> accounts, int expectedAccountCount,
      boolean shouldUpdateSucceed) throws Exception {
    try {
      accountService.updateAccounts(accounts);
    } catch (AccountServiceException ase) {
      if (shouldUpdateSucceed) {
        // Wasn't expecting this, rethrow.
        throw ase;
      }
    }
    if (shouldUpdateSucceed) {
      assertAccountsInAccountService(accounts, expectedAccountCount, accountService);
      if (configProps.containsKey(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY)) {
        Path newBackupFilePath = Files.list(accountBackupDir)
            .filter(path -> BackupFileManager.versionFilenamePattern.matcher(path.getFileName().toString()).find())
            .max(new Comparator<Path>() {
              @Override
              public int compare(Path o1, Path o2) {
                Matcher m1 = BackupFileManager.versionFilenamePattern.matcher(o1.getFileName().toString());
                Matcher m2 = BackupFileManager.versionFilenamePattern.matcher(o2.getFileName().toString());
                m1.find();
                m2.find();
                int v1 = Integer.parseInt(m1.group(1));
                int v2 = Integer.parseInt(m2.group(1));
                return v1 - v2;
              }
            })
            .get();
        checkBackupFileWithVersion(accountService.getAllAccounts(), newBackupFilePath);
      }
    } else {
      assertEquals("Wrong number of accounts in accountService", expectedAccountCount,
          accountService.getAllAccounts().size());
    }
  }

  /**
   * Updates a collection of {@link Account}s and verifies that an {@link AccountServiceException} with the expected
   * error code is thrown.
   * @param accounts the accounts to update.
   * @param expectedErrorCode the {@link AccountServiceErrorCode} expected.
   */
  private void updateAccountsExpectException(Collection<Account> accounts, AccountServiceErrorCode expectedErrorCode) {
    try {
      accountService.updateAccounts(accounts);
      fail("Expected update to fail");
    } catch (AccountServiceException ase) {
      assertEquals("Unexpected error code", expectedErrorCode, ase.getErrorCode());
    }
  }

  /**
   * Check that the provided backup file matches the data in the corresponding serialized accounts.
   * @param expectedAccounts the expected {@link Account}s.
   * @param backupPath the {@link Path} to the backup file.
   * @throws JSONException
   */
  private void checkBackupFileWithVersion(Collection<Account> expectedAccounts, Path backupPath)
      throws JSONException, IOException {
    try (BufferedReader reader = Files.newBufferedReader(backupPath)) {
      JSONArray accountArray = new JSONArray(new JSONTokener(reader));
      int arrayLength = accountArray.length();
      assertEquals("unexpected array size", expectedAccounts.size(), arrayLength);
      Set<Account> expectedAccountSet = new HashSet<>(expectedAccounts);
      for (int i = 0; i < arrayLength; i++) {
        JSONObject accountJson = accountArray.getJSONObject(i);
        Account account = Account.fromJson(accountJson);
        assertTrue("unexpected account in array: " + accountJson.toString(), expectedAccountSet.contains(account));
      }
    }
  }

  /**
   * Assert that the account map has the same accounts with the expected account set.
   * @param expectedAccounts the expected {@link Account}s.
   * @param accountMap the account map.
   * @throws Exception
   */
  private void assertAccountMapEquals(Collection<Account> expectedAccounts, Map<String, String> accountMap)
      throws Exception {
    Set<Account> expectedAccountSet = new HashSet<>(expectedAccounts);
    assertEquals("Number of accounts mismatch", expectedAccountSet.size(), accountMap.size());
    for (String accountJsonString : accountMap.values()) {
      JSONObject accountJson = new JSONObject(accountJsonString);
      Account account = Account.fromJson(accountJson);
      assertTrue("unexpected account in map: " + accountJson.toString(), expectedAccountSet.contains(account));
    }
  }

  /**
   * Pre-populates a collection of {@link Account}s to the underlying store.
   * This method does not check any conflict among the {@link Account}s to write.
   * @throws Exception Any unexpected exception.
   */
  protected abstract void writeAccountsToStore(Collection<Account> accounts, boolean shouldNotify) throws Exception;

  protected abstract void cleanupStore() throws Exception;

  /**
   * Delete backup directory if exist.
   * @throws Exception Any unexpected exception.
   */
  private void deleteBackupDirectoryIfExist() throws Exception {
    if (Files.exists(accountBackupDir)) {
      Files.walk(accountBackupDir).map(Path::toFile).forEach(File::delete);
      Files.deleteIfExists(accountBackupDir);
    }
  }

  /**
   /**
   * Randomly generates a single {@link Account} and {@link Container}. It also generates a collection of reference
   * {@link Account}s and {@link Container}s that can be referred from {@link #idToRefAccountMap} and
   * {@link #idToRefContainerMap}.
   */
  protected void generateReferenceAccountsAndContainers() {
    // a set that records the account ids that have already been taken.
    Set<Short> accountIdSet = new HashSet<>();
    // generate a single reference account and container that can be referenced by refAccount and refContainer respectively.
    refAccountId = Utils.getRandomShort(random);
    accountIdSet.add(refAccountId);
    refAccountName = UUID.randomUUID().toString();
    refAccountStatus = random.nextBoolean() ? AccountStatus.ACTIVE : AccountStatus.INACTIVE;
    refContainerId = Utils.getRandomShort(random);
    refContainerName = UUID.randomUUID().toString();
    refContainerStatus = random.nextBoolean() ? ContainerStatus.ACTIVE : ContainerStatus.INACTIVE;
    refContainerDescription = UUID.randomUUID().toString();
    refContainerCaching = random.nextBoolean();
    refParentAccountId = refAccountId;
    refContainerEncryption = random.nextBoolean();
    refContainerPreviousEncryption = refContainerEncryption || random.nextBoolean();
    refContainerMediaScanDisabled = random.nextBoolean();
    refReplicationPolicy = TestUtils.getRandomString(10);
    refContainerTtlRequired = random.nextBoolean();
    refContainer = new ContainerBuilder(refContainerId, refContainerName, refContainerStatus, refContainerDescription,
        refParentAccountId).setEncrypted(refContainerEncryption)
        .setPreviouslyEncrypted(refContainerPreviousEncryption)
        .setCacheable(refContainerCaching)
        .setMediaScanDisabled(refContainerMediaScanDisabled)
        .setReplicationPolicy(refReplicationPolicy)
        .setTtlRequired(refContainerTtlRequired)
        .build();
    refAccount =
        new AccountBuilder(refAccountId, refAccountName, refAccountStatus).addOrUpdateContainer(refContainer).build();
    generateRefAccounts(idToRefAccountMap, idToRefContainerMap, accountIdSet, NUM_REF_ACCOUNT,
        NUM_CONTAINER_PER_ACCOUNT);
  }
}

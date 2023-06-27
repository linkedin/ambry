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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Test;

import static com.github.ambry.config.MySqlAccountServiceConfig.*;
import static com.github.ambry.utils.AccountTestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link MySqlAccountService}.
 */
public class MySqlAccountServiceTest {

  MySqlAccountService mySqlAccountService;
  MySqlAccountStoreFactory mockMySqlAccountStoreFactory;
  MySqlAccountStore mockMySqlAccountStore;
  Notifier<String> mockNotifier = new MockNotifier<>();
  Properties mySqlConfigProps = new Properties();

  public MySqlAccountServiceTest() throws Exception {
    mySqlConfigProps.setProperty(DB_INFO, "");
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlConfigProps.setProperty(UPDATE_DISABLED, "false");
    mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    mockMySqlAccountStore = mock(MySqlAccountStore.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(mockMySqlAccountStore);
    mySqlAccountService = getAccountService();
  }

  // TODO: parametrize to use mock or real store (maybe blank url = test)
  private MySqlAccountService getAccountService() throws IOException, SQLException {
    AccountServiceMetricsWrapper accountServiceMetrics = new AccountServiceMetricsWrapper(new MetricRegistry());
    return new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStoreFactory,
        mockNotifier);
  }

  /**
   * Tests in-memory cache is initialized with metadata from local file on start up
   * @throws IOException
   */
  @Test
  public void testInitCacheFromDisk() throws IOException, SQLException {
    Path accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    mySqlConfigProps.setProperty(BACKUP_DIRECTORY_KEY, accountBackupDir.toString());

    // write test account to backup file
    long lastModifiedTime = 100;
    Account testAccount =
        new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).lastModifiedTime(lastModifiedTime)
            .build();
    Collection<Account> accounts = new ArrayList<>();
    accounts.add(testAccount);
    String filename = BackupFileManager.getBackupFilename(1, SystemTime.getInstance().seconds());
    Path filePath = accountBackupDir.resolve(filename);
    BackupFileManager.writeAccountsToFile(filePath, accounts);

    mySqlAccountService = getAccountService();

    // verify cache is initialized on startup with test account from backup file
    assertEquals("Mismatch in number of accounts in cache", 1, mySqlAccountService.getAllAccounts().size());
    assertEquals("Mismatch in account info in cache", testAccount,
        mySqlAccountService.getAllAccounts().iterator().next());

    // verify that mySqlAccountStore.getNewAccounts() is called with input argument "lastModifiedTime" value as 100
    verify(mockMySqlAccountStore, atLeastOnce()).getNewAccounts(lastModifiedTime);
    verify(mockMySqlAccountStore, atLeastOnce()).getNewContainers(lastModifiedTime);
  }

  /**
   * Tests in-memory cache is updated with accounts from mysql db store on start up
   */
  @Test
  public void testInitCacheOnStartUp() throws SQLException, IOException {
    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();
    when(mockMySqlAccountStore.getNewAccounts(0)).thenReturn(new ArrayList<>(Collections.singletonList(testAccount)));
    mySqlAccountService = getAccountService();

    // Test in-memory cache is updated with accounts from mysql store on start up.
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account information", testAccount, accounts.get(0));
    assertTrue("Sync time not updated",
        mySqlAccountService.accountServiceMetrics.timeInSecondsSinceLastSyncGauge.getValue() < 10);
    assertEquals("Unexpected container count", 1,
        mySqlAccountService.accountServiceMetrics.containerCountGauge.getValue().intValue());
  }

  /**
   * Tests creating and updating accounts through {@link MySqlAccountService}:
   * 1. add a new {@link Account};
   * 2. update existing {@link Account} by adding new {@link Container} to an existing {@link Account};
   */
  @Test
  public void testUpdateAccounts() throws Exception {

    when(mockMySqlAccountStore.getNewAccounts(0)).thenReturn(new ArrayList<>());
    mySqlAccountService = getAccountService();
    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();

    // 1. Addition of new account. Verify account is added to cache and written to mysql store.
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    Account finalTestAccount = testAccount;
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(argThat(accountsInfo -> {
      Account account = accountsInfo.get(0).getAccount();
      return account.equals(finalTestAccount);
    }));
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount,
        mySqlAccountService.getAccountByName(testAccount.getName()));

    // 2. Update existing account by changing aclInheritedByContainer to true. Verify account is updated in cache and written to mysql store.
    testAccount = new AccountBuilder(testAccount).aclInheritedByContainer(true).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    Account finalTestAccount0 = testAccount;
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(argThat(accountsInfo -> {
      AccountUtils.AccountUpdateInfo accountUpdateInfo = accountsInfo.get(0);
      return accountUpdateInfo.getAccount().equals(finalTestAccount0) && !accountUpdateInfo.isAdded()
          && accountUpdateInfo.isUpdated() && accountUpdateInfo.getAddedContainers().isEmpty()
          && accountUpdateInfo.getUpdatedContainers().isEmpty();
    }));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // 3. Update existing account by adding new container. Verify account is updated in cache and written to mysql store.
    Container testContainer2 =
        new ContainerBuilder((short) 2, "testContainer2", Container.ContainerStatus.ACTIVE, "testContainer2", (short) 1)
            .build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer2).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    Account finalTestAccount1 = testAccount;
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(argThat(accountsInfo -> {
      AccountUtils.AccountUpdateInfo accountUpdateInfo = accountsInfo.get(0);
      return accountUpdateInfo.getAccount().equals(finalTestAccount1) && !accountUpdateInfo.isAdded()
          && !accountUpdateInfo.isUpdated() && accountUpdateInfo.getAddedContainers()
          .equals(Collections.singletonList(testContainer2)) && accountUpdateInfo.getUpdatedContainers().isEmpty();
    }));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // 4. Update existing container. Verify container is updated in cache and written to mysql store.
    testContainer = new ContainerBuilder(testContainer).setMediaScanDisabled(true).setCacheable(true).build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    Container finalTestContainer =
        new ContainerBuilder(testContainer).setSnapshotVersion(testContainer.getSnapshotVersion() + 1).build();
    Account finalTestAccount2 = testAccount;
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(argThat(accountsInfo -> {
      AccountUtils.AccountUpdateInfo accountUpdateInfo = accountsInfo.get(0);
      return accountUpdateInfo.getAccount().equals(finalTestAccount2) && !accountUpdateInfo.isAdded()
          && !accountUpdateInfo.isUpdated() && accountUpdateInfo.getAddedContainers().isEmpty()
          && accountUpdateInfo.getUpdatedContainers().equals(Collections.singletonList(finalTestContainer));
    }));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
    assertTrue("Sync time not updated",
        mySqlAccountService.accountServiceMetrics.timeInSecondsSinceLastSyncGauge.getValue() < 10);
    assertEquals("Unexpected container count", 2,
        mySqlAccountService.accountServiceMetrics.containerCountGauge.getValue().intValue());
  }

  /**
   * Tests background updater for updating cache from mysql store periodically.
   */
  @Test
  public void testBackgroundUpdater() throws InterruptedException, SQLException, IOException {

    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "1");
    Account testAccount = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).build();
    when(mockMySqlAccountStore.getNewAccounts(anyLong())).thenReturn(new ArrayList<>())
        .thenReturn(new ArrayList<>(Collections.singletonList(testAccount)));
    mySqlAccountService = getAccountService();

    assertEquals("Background account updater thread should have been started", 1,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));

    // Verify cache is empty.
    assertNull("Cache should be empty", mySqlAccountService.getAccountById(testAccount.getId()));
    // sleep for polling interval time
    Thread.sleep(Long.parseLong(mySqlConfigProps.getProperty(UPDATER_POLLING_INTERVAL_SECONDS)) * 1000 + 100);
    // Verify account is added to cache by background updater.
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // verify that close() stops the background updater thread
    mySqlAccountService.close();

    // force shutdown background updater thread. As the default timeout value is 1 minute, it is possible that thread is
    // present after close() due to actively executing task.
    mySqlAccountService.getScheduler().shutdownNow();

    assertTrue("Background account updater thread should be stopped",
        TestUtils.checkAndSleep(0, () -> numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX), 1000));
  }

  /**
   * Tests disabling of background updater by setting {@link MySqlAccountServiceConfig#UPDATER_POLLING_INTERVAL_SECONDS} to 0.
   */
  @Test
  public void testDisableBackgroundUpdater() throws IOException, SQLException {

    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlAccountService = getAccountService();
    assertEquals("Background account updater thread should not be started", 0,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));
  }

  /**
   * Tests disabling account updates by setting {@link MySqlAccountServiceConfig#UPDATE_DISABLED} to true.
   */
  @Test
  public void testUpdateDisabled() throws Exception {

    mySqlConfigProps.setProperty(UPDATE_DISABLED, "true");
    mySqlAccountService = getAccountService();

    // Verify account update is disabled
    Account testAccount = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).build();
    try {
      mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
      fail("Update accounts should be disabled");
    } catch (AccountServiceException e) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.UpdateDisabled, e.getErrorCode());
    }
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in name.
   */
  @Test
  public void testUpdateNameConflictingAccounts() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 2, "a", Account.AccountStatus.INACTIVE).build());
    assertUpdateAccountsFails(conflictAccounts, AccountServiceErrorCode.ResourceConflict, mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        mySqlAccountService.accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in id.
   */
  @Test
  public void testUpdateIdConflictingAccounts() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "b", Account.AccountStatus.INACTIVE).build());
    assertUpdateAccountsFails(conflictAccounts, AccountServiceErrorCode.ResourceConflict, mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        mySqlAccountService.accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests updating a collection of {@link Account}s, where there are duplicate {@link Account}s in id and name.
   */
  @Test
  public void testUpdateDuplicateAccounts() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    assertUpdateAccountsFails(conflictAccounts, AccountServiceErrorCode.ResourceConflict, mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        mySqlAccountService.accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests cases for name/id conflicts as specified in the JavaDoc of {@link AccountService#updateAccounts(Collection)}.
   */
  @Test
  public void testConflictingUpdatesWithAccounts() throws Exception {
    AccountServiceMetrics accountServiceMetrics = mySqlAccountService.accountServiceMetrics;
    // write two accounts (1, "a") and (2, "b")
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", Account.AccountStatus.ACTIVE).build());
    mySqlAccountService.updateAccounts(existingAccounts);

    // case A: verify updating status of account (1, "a") replaces existing record (1, "a")
    Account accountToUpdate =
        new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).status(Account.AccountStatus.INACTIVE)
            .build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 1));

    // case B: verify updating name of account (1, "a") to (1, "c") replaces existing record (1, "a")
    accountToUpdate = new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).name("c").build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 1));

    // case C: verify adding new account (3, "d") adds new record (3, "d")
    accountToUpdate = new AccountBuilder((short) 3, "d", Account.AccountStatus.ACTIVE).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 3));

    // case D: verify adding new account (4, "c") conflicts in name with (1, "c")
    accountToUpdate = new AccountBuilder((short) 4, "c", Account.AccountStatus.ACTIVE).build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case E: verify updating name of account  (1, "c") to (1, "b") conflicts in name with (2, "b")
    accountToUpdate = new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).name("b").build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 2", 2,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case F: verify updating account (1, "c") with incorrect version throws resource conflict
    accountToUpdate =
        new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).status(Account.AccountStatus.INACTIVE)
            .snapshotVersion(mySqlAccountService.getAccountById((short) 1).getSnapshotVersion() + 1)
            .build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 3", 3,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // verify there should be 3 accounts (1, "c), (2, "b) and (3, "d) at the end of operation
    assertEquals("Mismatch in number of accounts", 3, mySqlAccountService.getAllAccounts().size());
    assertEquals("Mismatch in name of account", "c", mySqlAccountService.getAccountById((short) 1).getName());
    assertEquals("Mismatch in name of account", "b", mySqlAccountService.getAccountById((short) 2).getName());
    assertEquals("Mismatch in name of account", "d", mySqlAccountService.getAccountById((short) 3).getName());
  }

  /**
   * Tests name/id conflicts in Containers
   */
  @Test
  public void testConflictingUpdatesWithContainers() throws Exception {
    AccountServiceMetrics accountServiceMetrics = mySqlAccountService.accountServiceMetrics;
    List<Container> containersList = new ArrayList<>();
    containersList.add(
        new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build());
    containersList.add(
        new ContainerBuilder((short) 2, "c2", Container.ContainerStatus.ACTIVE, "c2", (short) 1).build());
    Account accountToUpdate =
        new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).containers(containersList).build();

    // write account (1,a) with containers (1,c1), (2,c2)
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in number of containers", 2,
        mySqlAccountService.getAccountById(accountToUpdate.getId()).getAllContainers().size());

    // case A: Verify that changing name of container (1,c1) to (1,c3) replaces existing record
    Container containerToUpdate =
        new ContainerBuilder((short) 1, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getAccountById((short) 1).getContainerById((short) 1));

    // case B: Verify addition of new container (3,c3) conflicts in name with existing container (1,c3)
    containerToUpdate =
        new ContainerBuilder((short) 3, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate =
        new AccountBuilder(accountToUpdate).containers(Collections.singletonList(containerToUpdate)).build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    accountToUpdate = new AccountBuilder(accountToUpdate).removeContainer(containerToUpdate).build();
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case C: Verify addition of new container (3,c4) is successful
    containerToUpdate =
        new ContainerBuilder((short) 3, "c4", Container.ContainerStatus.ACTIVE, "c4", (short) 1).build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getAccountById((short) 1).getContainerById((short) 3));

    // case D: Verify updating name of container (3,c4) to c2 conflicts in name with existing container (2,c2)
    containerToUpdate =
        new ContainerBuilder(mySqlAccountService.getAccountById((short) 1).getContainerById((short) 3)).setName("c2")
            .build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 2", 2,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case E: Verify updating container (3,c4) with incorrect version throws resource conflict
    containerToUpdate = mySqlAccountService.getAccountById((short) 1).getContainerById((short) 3);
    containerToUpdate = new ContainerBuilder(containerToUpdate).setStatus(Container.ContainerStatus.INACTIVE)
        .setSnapshotVersion(containerToUpdate.getSnapshotVersion() + 1)
        .build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    assertEquals("UpdateAccountErrorCount in metrics should be 3", 3,
        accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests adding/removing {@link Consumer}.
   * @throws Exception
   */
  @Test
  public void testAccountUpdateConsumer() throws Exception {

    List<Collection<Account>> updatedAccountsReceivedByConsumers = new ArrayList<>();
    Consumer<Collection<Account>> accountUpdateConsumer = updatedAccountsReceivedByConsumers::add;

    // add consumers
    mySqlAccountService.addAccountUpdateConsumer(accountUpdateConsumer);

    // accounts and containers to be updated
    List<Container> containersList = new ArrayList<>();
    containersList.add(
        new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build());
    containersList.add(
        new ContainerBuilder((short) 2, "c2", Container.ContainerStatus.ACTIVE, "c2", (short) 1).build());
    List<Account> accountsList = new ArrayList<>();
    accountsList.add(new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).build());
    accountsList.add(new AccountBuilder((short) 2, "a2", Account.AccountStatus.ACTIVE).build());

    when(mockMySqlAccountStore.getNewAccounts(anyLong())).thenReturn(accountsList);
    when(mockMySqlAccountStore.getNewContainers(anyLong())).thenReturn(containersList);

    // fetch updated accounts and containers from db
    mySqlAccountService.fetchAndUpdateCache();

    Set<Account> accountsInMap = new HashSet<>(mySqlAccountService.getAllAccounts());

    // verify consumers are notified of updated accounts
    assertEquals("Number of updates by consumer received should be 1", 1, updatedAccountsReceivedByConsumers.size());
    assertEquals("Mismatch in number of accounts received by consumer", accountsInMap.size(),
        updatedAccountsReceivedByConsumers.get(0).size());
    assertTrue("Mismatch in accounts received by consumer",
        accountsInMap.containsAll(updatedAccountsReceivedByConsumers.get(0)));

    //Remove consumer
    mySqlAccountService.removeAccountUpdateConsumer(accountUpdateConsumer);
    updatedAccountsReceivedByConsumers.clear();

    Container newContainer =
        new ContainerBuilder((short) 3, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();
    when(mockMySqlAccountStore.getNewContainers(anyLong())).thenReturn(Collections.singletonList(newContainer));

    // fetch updated accounts and containers from db
    mySqlAccountService.fetchAndUpdateCache();

    // verify consumers are not notified
    assertEquals("No updates should be received", 0, updatedAccountsReceivedByConsumers.size());
  }

  /**
   * Tests ignoring version conflicts in Accounts and Containers
   */
  @Test
  public void testUpdateAccountsWithVersionMismatchIgnored() throws Exception {

    mySqlConfigProps.setProperty(IGNORE_VERSION_MISMATCH, "true");
    AccountService mySqlAccountService = getAccountService();

    // write account (1, "a")
    Account accountToUpdate = new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).addOrUpdateContainer(
        new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build()).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));

    // verify updating account (1, "a") with different version is successful
    accountToUpdate = new AccountBuilder(accountToUpdate).status(Account.AccountStatus.INACTIVE)
        .snapshotVersion(mySqlAccountService.getAccountById((short) 1).getSnapshotVersion() + 5)
        .build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 1));

    // verify updating container (1,"c1") in account (1,"a") with different version is successful
    Container containerToUpdate = accountToUpdate.getContainerById((short) 1);
    containerToUpdate = new ContainerBuilder(containerToUpdate).setStatus(Container.ContainerStatus.INACTIVE)
        .setSnapshotVersion(containerToUpdate.getSnapshotVersion() + 5)
        .build();
    mySqlAccountService.updateContainers(accountToUpdate.getName(), Collections.singletonList(containerToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getContainerByName(accountToUpdate.getName(), containerToUpdate.getName()));
  }

  /**
   * Tests creating existing containers with different states. This is to mock edge case where user attempts to create
   * same container which has been deprecated already.
   * @throws Exception
   */
  @Test
  public void testCreateExistingContainerInDifferentStates() throws Exception {
    AccountService mySqlAccountService = getAccountService();
    String accountName = "test-account";
    String inactiveContainer = "inactive-container";
    String deleteInProgressContainer1 = "delete-in-progress-container1";
    String deleteInProgressContainer2 = "delete-in-progress-container2";
    // create a testing account with inactive and delete-in-progress container.
    Account accountToUpdate =
        new AccountBuilder((short) 1, accountName, Account.AccountStatus.ACTIVE).addOrUpdateContainer(
            new ContainerBuilder((short) 1, inactiveContainer, Container.ContainerStatus.INACTIVE, "",
                (short) 1).build())
            .addOrUpdateContainer(new ContainerBuilder((short) 2, deleteInProgressContainer1,
                Container.ContainerStatus.DELETE_IN_PROGRESS, "", (short) 1).setDeleteTriggerTime(
                System.currentTimeMillis()).build())
            .addOrUpdateContainer(new ContainerBuilder((short) 3, deleteInProgressContainer2,
                Container.ContainerStatus.DELETE_IN_PROGRESS, "", (short) 1).setDeleteTriggerTime(
                System.currentTimeMillis() - TimeUnit.DAYS.toMillis(15)).build())
            .build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));

    // Attempting to create an existing container with INACTIVE state should fail
    Container containerToCreate =
        new ContainerBuilder(Container.UNKNOWN_CONTAINER_ID, inactiveContainer, Container.ContainerStatus.ACTIVE, "",
            (short) 1).build();
    try {
      mySqlAccountService.updateContainers(accountName, Collections.singletonList(containerToCreate));
      fail("should fail because container to create is already marked as inactive");
    } catch (AccountServiceException ase) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.ResourceHasGone, ase.getErrorCode());
    }

    // Attempting to create an existing container with DELETE_IN_PROGRESS state (within retention time) should fail
    containerToCreate = new ContainerBuilder(Container.UNKNOWN_CONTAINER_ID, deleteInProgressContainer1,
        Container.ContainerStatus.ACTIVE, "", (short) 1).build();
    try {
      mySqlAccountService.updateContainers(accountName, Collections.singletonList(containerToCreate));
      fail("should fail because container to create is in DELETE_IN_PROGRESS state");
    } catch (AccountServiceException ase) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.MethodNotAllowed, ase.getErrorCode());
    }

    // Attempting to create an existing container with DELETE_IN_PROGRESS state (past retention time) should fail
    containerToCreate = new ContainerBuilder(Container.UNKNOWN_CONTAINER_ID, deleteInProgressContainer2,
        Container.ContainerStatus.ACTIVE, "", (short) 1).build();
    try {
      mySqlAccountService.updateContainers(accountName, Collections.singletonList(containerToCreate));
      fail("should fail because container to create is in DELETE_IN_PROGRESS state and past retention time");
    } catch (AccountServiceException ase) {
      assertEquals("Mismatch in error code", AccountServiceErrorCode.ResourceHasGone, ase.getErrorCode());
    }
  }

  /**
   * Asserts that sync time was updated and container count is expected.
   * @param accountServiceMetrics the metrics to check
   * @param expectedContainerCount expected value of containerCountGauge
   */
  private static void checkGauges(AccountServiceMetrics accountServiceMetrics, int expectedContainerCount) {
    assertTrue("Sync time not updated", accountServiceMetrics.timeInSecondsSinceLastSyncGauge.getValue() < 10);
    assertEquals("Unexpected container count", expectedContainerCount,
        accountServiceMetrics.containerCountGauge.getValue().intValue());
  }
}

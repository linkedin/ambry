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
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.mysql.AccountDao;
import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import static com.github.ambry.account.Container.*;
import static com.github.ambry.config.MySqlAccountServiceConfig.*;
import static com.github.ambry.mysql.MySqlUtils.*;
import static com.github.ambry.utils.AccountTestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Integration tests for {@link MySqlAccountService}.
 */
public class MySqlAccountServiceIntegrationTest {

  private static final String DESCRIPTION = "Indescribable";
  private final MySqlAccountStoreFactory mockMySqlAccountStoreFactory;
  private final Properties mySqlConfigProps;
  private final MockNotifier<String> mockNotifier = new MockNotifier();
  private MySqlAccountStore mySqlAccountStore;
  private MySqlAccountServiceConfig accountServiceConfig;
  private MySqlAccountService mySqlAccountService;
  private static final String DATASET_NAME = "testDataset";
  private static final String DATASET_NAME_BASIC = "testDatasetBasic";
  private static final String DATASET_NAME_NOT_EXIST = "testDatasetNotExist";
  private static final String DATASET_NAME_WITH_TTL = "testDatasetWithTtl";
  private static final String DATASET_NAME_WITH_SEMANTIC = "testDatasetWithSemantic";
  private static final String DATASET_NAME_EXPIRED = "testDatasetExpired";


  public MySqlAccountServiceIntegrationTest() throws Exception {
    mySqlConfigProps = Utils.loadPropsFromResource("mysql.properties");
    mySqlConfigProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlConfigProps.setProperty(UPDATE_DISABLED, "false");
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    mySqlAccountStore = spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
        new MetricRegistry()).getMySqlAccountStore());
    mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(mySqlAccountStore);
    // Start with empty database
    cleanup();
    mySqlAccountService = getAccountService();
  }

  private MySqlAccountService getAccountService() throws Exception {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    // Don't initialize account store here as it may have preinitialized data
    return new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory,
        mockNotifier);
  }

  @Test
  public void testBadCredentials() throws Exception {
    DbEndpoint endpoint =
        new DbEndpoint("jdbc:mysql://localhost/AccountMetadata", "dc1", true, "baduser", "badpassword");
    try {
      new MySqlAccountStore(Collections.singletonList(endpoint), endpoint.getDatacenter(),
          new MySqlMetrics(MySqlAccountStore.class, new MetricRegistry()), accountServiceConfig);
      fail("Store creation should fail with bad credentials");
    } catch (SQLException e) {
      assertTrue(MySqlDataAccessor.isCredentialError(e));
    }
  }

  /**
   * Tests in-memory cache is initialized with metadata from local file on start up
   * @throws IOException
   */
  @Test
  public void testInitCacheFromDisk() throws Exception {
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
    verify(mySqlAccountStore, atLeastOnce()).getNewAccounts(lastModifiedTime);
    verify(mySqlAccountStore, atLeastOnce()).getNewContainers(lastModifiedTime);
  }

  /**
   * Tests in-memory cache is updated with accounts from mysql db store on start up
   */
  @Test
  public void testInitCacheOnStartUp() throws Exception {
    Account testAccount = makeTestAccountWithContainer();
    AccountUpdateInfo accountUpdateInfo =
        new AccountUpdateInfo(testAccount, true, false, new ArrayList<>(testAccount.getAllContainers()),
            new ArrayList<>());
    mySqlAccountStore.updateAccounts(Collections.singletonList(accountUpdateInfo));
    mySqlAccountService = getAccountService();
    // Test in-memory cache is updated with accounts from mysql store on start up.
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account information", testAccount, accounts.get(0));
  }

  /**
   * Tests creating and updating accounts through {@link MySqlAccountService}:
   * 1. add a new {@link Account};
   * 2. update existing {@link Account} by adding new {@link Container} to an existing {@link Account};
   */
  @Test
  public void testUpdateAccounts() throws Exception {

    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();

    // 1. Addition of new account. Verify account is added to cache.
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount,
        mySqlAccountService.getAccountByName(testAccount.getName()));

    // 2. Update existing account by adding new container. Verify account is updated in cache.
    Container testContainer2 =
        new ContainerBuilder((short) 2, "testContainer2", Container.ContainerStatus.ACTIVE, "testContainer2", (short) 1)
            .build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer2).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // 3. Update existing container. Verify container is updated in cache.
    testContainer = new ContainerBuilder(testContainer).setMediaScanDisabled(true).setCacheable(true).build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
  }

  @Test
  public void testFailover() throws Exception {
    String dbInfoJsonString = mySqlConfigProps.getProperty(DB_INFO);
    JSONArray dbInfo = new JSONArray(dbInfoJsonString);
    JSONObject entry = dbInfo.getJSONObject(0);
    DbEndpoint origEndpoint = DbEndpoint.fromJson(entry);
    // Make two endpoints, one with bad url, writeable, remote; second with good url, writeable, local
    String localDc = "local", remoteDc = "remote";
    String badUrl = "jdbc:mysql://badhost/AccountMetadata";
    DbEndpoint localGoodEndpoint =
        new DbEndpoint(origEndpoint.getUrl(), localDc, false, origEndpoint.getUsername(), origEndpoint.getPassword());
    DbEndpoint remoteBadEndpoint =
        new DbEndpoint(badUrl, remoteDc, true, origEndpoint.getUsername(), origEndpoint.getPassword());
    JSONArray endpointsJson = new JSONArray().put(localGoodEndpoint.toJson()).put(remoteBadEndpoint.toJson());
    mySqlConfigProps.setProperty(DB_INFO, endpointsJson.toString());
    mySqlConfigProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, localDc);
    mySqlAccountStore = spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
        new MetricRegistry()).getMySqlAccountStore());
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(mySqlAccountStore);
    // constructor does initial fetch which will fail on first endpoint and succeed on second
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService =
        new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory,
            mockNotifier);
    MySqlMetrics storeMetrics = mySqlAccountStore.getMySqlDataAccessor().getMetrics();
    // At this point, should have at least one connection failure (bad endpoint) and one success
    long expectedConnectionFail = storeMetrics.connectionFailureCount.getCount();
    long expectedConnectionSuccess = storeMetrics.connectionSuccessCount.getCount();
    assertTrue(expectedConnectionFail > 0);
    assertTrue(expectedConnectionSuccess > 0);
    // Try to update, should fail to get connection
    Account account = makeTestAccountWithContainer();
    try {
      mySqlAccountService.updateAccounts(Collections.singletonList(account));
      fail("Expected failure due to no writeable accounts");
    } catch (AccountServiceException ase) {
      assertEquals(AccountServiceErrorCode.InternalError, ase.getErrorCode());
    }
    expectedConnectionFail++;
    assertEquals(1, accountServiceMetrics.updateAccountErrorCount.getCount());
    assertEquals(expectedConnectionFail, storeMetrics.connectionFailureCount.getCount());
    assertEquals(expectedConnectionSuccess, storeMetrics.connectionSuccessCount.getCount());
    mySqlAccountService.fetchAndUpdateCache();
  }

  /**
   * Tests background updater for updating cache from mysql store periodically.
   */
  @Test
  public void testBackgroundUpdater() throws Exception {

    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "1");
    Account testAccount = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).build();
    mySqlAccountService = getAccountService();

    assertEquals("Background account updater thread should have been started", 1,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));

    // Verify cache is empty.
    assertNull("Cache should be empty", mySqlAccountService.getAccountById(testAccount.getId()));

    // Add account to DB (could use second AS for this)
    mySqlAccountStore.updateAccounts(Collections.singletonList(
        new AccountUpdateInfo(testAccount, true, false, new ArrayList<>(testAccount.getAllContainers()),
            new ArrayList<>())));

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

  /** Producer-consumer test for multiple account services. */
  @Test
  public void testAccountRefresh() throws Exception {
    MySqlAccountService producerAccountService = mySqlAccountService;
    MySqlAccountStore producerAccountStore = mySqlAccountStore;
    // Create second account service with scheduled polling disabled
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    MySqlAccountStore consumerAccountStore =
        spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
            new MetricRegistry()).getMySqlAccountStore());
    MySqlAccountStoreFactory mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(consumerAccountStore);
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    MySqlAccountService consumerAccountService =
        new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory,
            mockNotifier);

    // Add account with 3 containers
    short accountId = 101;
    String accountName = "a1";
    // Number of calls expected in producer account store
    List<Container> containers = new ArrayList<>();
    containers.add(new ContainerBuilder((short) 1, "c1", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    containers.add(new ContainerBuilder((short) 2, "c2", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    containers.add(new ContainerBuilder((short) 3, "c3", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    Account a1 =
        new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(containers).build();
    producerAccountService.updateAccounts(Collections.singletonList(a1));
    Account finalA = a1;
    verify(producerAccountStore).updateAccounts(argThat(accountInfos -> {
      AccountUpdateInfo accountInfo = accountInfos.get(0);
      return accountInfo.getAccount().equals(finalA) && accountInfo.getAddedContainers().equals(containers)
          && accountInfo.isAdded() && !accountInfo.isUpdated() && accountInfo.getUpdatedContainers().isEmpty();
    }));
    // Note: because consumer is notified of changes, its cache should already be in sync with DB
    long lmt = consumerAccountService.accountInfoMapRef.get().getLastModifiedTime();
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));

    // Update account only
    String newAccountName = "a1-updated";
    a1 = new AccountBuilder(a1).name(newAccountName).build();
    producerAccountService.updateAccounts(Collections.singletonList(a1));
    Account finalA1 = a1;
    verify(producerAccountStore).updateAccounts(argThat(accountInfos -> {
      AccountUpdateInfo accountInfo = accountInfos.get(0);
      return accountInfo.getAccount().equals(finalA1) && accountInfo.getAddedContainers().isEmpty()
          && !accountInfo.isAdded() && accountInfo.isUpdated() && accountInfo.getUpdatedContainers().isEmpty();
    }));
    verify(consumerAccountStore).getNewAccounts(eq(lmt));
    verify(consumerAccountStore).getNewContainers(eq(lmt));
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(newAccountName));
    assertNull("Expected no account with old name", consumerAccountService.getAccountByName(accountName));
    accountName = newAccountName;

    // Update container only
    Container c1Mod = new ContainerBuilder(containers.get(0)).setStatus(ContainerStatus.DELETE_IN_PROGRESS).build();
    containers.set(0, c1Mod);
    a1 = new AccountBuilder(a1).containers(containers).build();
    Collection<Container> result =
        producerAccountService.updateContainers(accountName, Collections.singletonList(c1Mod));
    // Account should not have been touched
    verify(producerAccountStore).updateAccounts(
        argThat(accountsInfo -> accountsInfo.get(0).getUpdatedContainers().size() == 1));
    assertEquals("Expected one result", 1, result.size());
    assertEquals("Container mismatch", c1Mod, result.iterator().next());
    verify(consumerAccountStore).getNewAccounts(eq(lmt));
    verify(consumerAccountStore).getNewContainers(eq(lmt));
    assertEquals("Container mismatch", c1Mod, consumerAccountService.getContainerByName(accountName, "c1"));
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));
    lmt = consumerAccountService.accountInfoMapRef.get().getLastModifiedTime();

    // Add container only
    Container cNew = makeNewContainer("c4", accountId, ContainerStatus.ACTIVE);
    result = producerAccountService.updateContainers(accountName, Collections.singletonList(cNew));
    verify(producerAccountStore).updateAccounts(
        argThat(accountsInfo -> accountsInfo.get(0).getAddedContainers().size() == 1));
    assertEquals("Expected one result", 1, result.size());
    cNew = result.iterator().next();
    containers.add(cNew);
    a1 = new AccountBuilder(a1).containers(containers).build();
    verify(consumerAccountStore).getNewAccounts(eq(lmt));
    verify(consumerAccountStore).getNewContainers(eq(lmt));
    assertEquals("Container mismatch", cNew, consumerAccountService.getContainerByName(accountName, "c4"));
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));

    // For this section, consumer must get out of date so need to unsubscribe its notifier
    mockNotifier.unsubscribeAll();
    // TODO:
    // Add container in AS1, call AS2.getContainer() to force fetch
    // Add C1 in AS1, add C1 in AS2 (should succeed and return existing id)
    Container cNewProd = makeNewContainer("c5", accountId, ContainerStatus.ACTIVE);
    result = producerAccountService.updateContainers(accountName, Collections.singletonList(cNewProd));
    short newId = result.iterator().next().getId();
    // Add the same container to second AS with stale cache
    // Expect it to fail first time (conflict), refresh cache and succeed on retry
    result = consumerAccountService.updateContainers(accountName, Collections.singletonList(cNewProd));
    assertEquals(newId, result.iterator().next().getId());
    assertEquals(1, accountServiceMetrics.updateAccountErrorCount.getCount());
    assertEquals(1, accountServiceMetrics.conflictRetryCount.getCount());

    // Add C1 in AS1, add C2 in AS2
    cNewProd = makeNewContainer("c6", accountId, ContainerStatus.ACTIVE);
    result = producerAccountService.updateContainers(accountName, Collections.singletonList(cNewProd));
    newId = result.iterator().next().getId();
    Container cNewCons = makeNewContainer("c7", accountId, ContainerStatus.ACTIVE);
    result = consumerAccountService.updateContainers(accountName, Collections.singletonList(cNewCons));
    assertNotSame(newId, result.iterator().next().getId());
    assertEquals(2, accountServiceMetrics.updateAccountErrorCount.getCount());
    assertEquals(2, accountServiceMetrics.conflictRetryCount.getCount());

    // Check gauge values
    assertTrue("Sync time not updated", accountServiceMetrics.timeInSecondsSinceLastSyncGauge.getValue() < 10);
    assertEquals("Unexpected container count", 7, accountServiceMetrics.containerCountGauge.getValue().intValue());
  }

  /** Container on-demand fetch for multiple account services. */
  @Test
  public void testContainerFetchOnDemand() throws Exception {
    MySqlAccountService producerAccountService = mySqlAccountService;
    // Create second account service with scheduled polling disabled
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    MySqlAccountStore consumerAccountStore =
        spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
            new MetricRegistry()).getMySqlAccountStore());
    MySqlAccountStoreFactory mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(consumerAccountStore);
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    // Note: for these tests, consumer must NOT be notified of changes
    MySqlAccountService consumerAccountService =
        new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory, null);

    // Add new account "a1" on producer account service
    short accountId = 101;
    String accountName = "a1";
    int onDemandContainerFetchCount = 0;
    Container container = new ContainerBuilder((short) 1, "c1", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build();
    Account account = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(
        Collections.singletonList(container)).build();
    producerAccountService.updateAccounts(Collections.singletonList(account));

    // Test getContainer on consumer account service should fail since account doesn't exist
    assertNull("Expected null since Account doesn't exist in consumerAccountService cache",
        consumerAccountService.getContainerByName(accountName, "c1"));
    assertNull("Expected null since Account doesn't exist in consumerAccountService cache",
        consumerAccountService.getContainerById(accountId, (short) 1));

    // Fetch and update cache in consumer account service
    consumerAccountService.fetchAndUpdateCache();
    assertEquals("Account mismatch", account, consumerAccountService.getAccountByName(accountName));
    assertEquals("Container mismatch", container, consumerAccountService.getContainerByName(accountName, "c1"));
    assertEquals("Container mismatch", container, consumerAccountService.getContainerById(accountId, (short) 1));

    // Add new container in producer account service
    producerAccountService.updateContainers(accountName, Collections.singletonList(
        new ContainerBuilder((short) -1, "c2", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build()));
    Container newContainer = producerAccountService.getContainerByName(accountName, "c2");

    // Test getContainerByName() on consumer account service is successful (by fetching from mysql on demand)
    assertEquals("getContainerByName() expected to fetch container from MySql db", newContainer,
        consumerAccountService.getContainerByName(accountName, "c2"));
    verify(consumerAccountStore).getContainerByName(eq((int) accountId), eq(newContainer.getName()));
    assertEquals("Number of on-demand container requests should be 1", ++onDemandContainerFetchCount,
        accountServiceMetrics.onDemandContainerFetchCount.getCount());

    // verify in-memory cache is updated with the fetched container "c2"
    assertEquals("Container c2 should be present in consumer account service", newContainer,
        consumerAccountService.getAccountByName(accountName).getContainerByName(newContainer.getName()));

    // Add new container in producer account service
    producerAccountService.updateContainers(accountName, Collections.singletonList(
        new ContainerBuilder((short) -1, "c3", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build()));
    newContainer = producerAccountService.getContainerById(accountId, (short) 3);

    // Test getContainerById() on consumer account service is successful (by fetching from mysql on demand)
    assertEquals("getContainerById() expected to fetch container from MySql db", newContainer,
        consumerAccountService.getContainerById(accountId, (short) 3));
    verify(consumerAccountStore).getContainerById(eq((int) accountId), eq((int) newContainer.getId()));
    assertEquals("Number of on-demand container requests should be 2", ++onDemandContainerFetchCount,
        accountServiceMetrics.onDemandContainerFetchCount.getCount());

    // verify in-memory cache is updated with the fetched container "c3"
    assertEquals("Container c3 should be present in consumer account service", newContainer,
        consumerAccountService.getAccountByName(accountName).getContainerById(newContainer.getId()));
  }

  /** Test LRU cache for containers not found recently. */
  @Test
  public void testLRUCacheForNotFoundContainers() throws Exception {

    // Add new account on account service
    short accountId = 101;
    String accountName = "a1";
    Container container = new ContainerBuilder((short) 1, "c1", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build();
    Account account = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(
        Collections.singletonList(container)).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(account));

    // Look up container "c2" in account service
    assertNull("Container must not be present in account service",
        mySqlAccountService.getContainerByName(accountName, "c2"));
    // verify call to query container from mysql db
    verify(mySqlAccountStore).getContainerByName(eq((int) accountId), eq("c2"));
    // verify container name "a1:c2" is added to LRU cache
    assertTrue("container a1:c2 must be present in LRU cache",
        mySqlAccountService.getRecentNotFoundContainersCache().contains("a1" + MySqlAccountService.SEPARATOR + "c2"));

    // Look up container "c2" again in account service
    assertNull("Container must not be present in account service",
        mySqlAccountService.getContainerByName(accountName, "c2"));
    // verify mysql is not queried this time
    verify(mySqlAccountStore, times(1)).getContainerByName(anyInt(), anyString());

    // Add container "c2" to account service
    mySqlAccountService.updateContainers(accountName, Collections.singletonList(
        new ContainerBuilder((short) -1, "c2", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build()));

    // verify container "c2" is removed from not-found LRU cache
    assertFalse("Added container a1:c2 must no longer be present in LRU cache",
        mySqlAccountService.getRecentNotFoundContainersCache().contains("a1" + MySqlAccountService.SEPARATOR + "c2"));
  }

  /**
   * Tests following cases for name/id conflicts as specified in the JavaDoc of {@link AccountService#updateAccounts(Collection)}.
   *
   * Existing accounts
   * AccountId     AccountName
   *    1             "a"
   *    2             "b"
   *
   * Accounts will be updated in following order
   * Steps   AccountId   AccountName   If Conflict    Treatment                    Conflict reason
   *  A      1           "a"           no             replace existing record      N/A
   *  B      1           "c"           no             replace existing record      N/A
   *  C      3           "d"           no             add a new record             N/A
   *  D      4           "c"           yes            fail update                  conflicts with existing name.
   *  E      1           "b"           yes            fail update                  conflicts with existing name.
   *
   *
   */
  @Test
  public void testConflictingUpdatesWithAccounts() throws Exception {

    // write two accounts (1, "a") and (2, "b")
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", Account.AccountStatus.ACTIVE).build());
    mySqlAccountService.updateAccounts(existingAccounts);
    AccountServiceMetrics accountServiceMetrics = mySqlAccountService.accountServiceMetrics;

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
    List<Container> containersList = new ArrayList<>();
    containersList.add(new ContainerBuilder((short) 1, "c1", ContainerStatus.ACTIVE, "c1", (short) 1).build());
    containersList.add(new ContainerBuilder((short) 2, "c2", ContainerStatus.ACTIVE, "c2", (short) 1).build());
    Account accountToUpdate =
        new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).containers(containersList).build();
    AccountServiceMetrics accountServiceMetrics = mySqlAccountService.accountServiceMetrics;

    // write account (1,a) with containers (1,c1), (2,c2)
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in number of containers", 2,
        mySqlAccountService.getAccountById(accountToUpdate.getId()).getAllContainers().size());

    // case A: Verify that changing name of container (1,c1) to (1,c3) replaces existing record
    Container containerToUpdate =
        new ContainerBuilder((short) 1, "c3", ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getAccountById((short) 1).getContainerById((short) 1));

    // case B: Verify addition of new container (3,c3) conflicts in name with existing container (1,c3)
    containerToUpdate = new ContainerBuilder((short) 3, "c3", ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate =
        new AccountBuilder(accountToUpdate).containers(Collections.singletonList(containerToUpdate)).build();
    assertUpdateAccountsFails(Collections.singletonList(accountToUpdate), AccountServiceErrorCode.ResourceConflict,
        mySqlAccountService);
    accountToUpdate = new AccountBuilder(accountToUpdate).removeContainer(containerToUpdate).build();
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case C: Verify addition of new container (3,c4) is successful
    containerToUpdate = new ContainerBuilder((short) 3, "c4", ContainerStatus.ACTIVE, "c4", (short) 1).build();
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
  }

  /**
   * Tests addition of multiple accounts in single transaction
   */
  @Test
  public void testAccountUpdatesInTransaction() throws Exception {

    // New account (1,a1) with 5 containers
    List<Container> containerList1 = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      containerList1.add(
          new ContainerBuilder((short) i, "c" + i, Container.ContainerStatus.ACTIVE, "c" + i, (short) 1).build());
    }
    Account a1 = new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).containers(containerList1).build();

    // New account (2,a2) with 5 containers
    List<Container> containersList2 = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      containersList2.add(
          new ContainerBuilder((short) i, "c" + i, Container.ContainerStatus.ACTIVE, "c" + i, (short) 2).build());
    }
    Account a2 = new AccountBuilder((short) 2, "a2", Account.AccountStatus.ACTIVE).containers(containersList2).build();

    mySqlAccountService.updateAccountsWithMySqlStore(Arrays.asList(a1, a2));
    mySqlAccountService.fetchAndUpdateCache();

    // Verify all accounts and containers are added.
    assertEquals("Mismatch in number of accounts", 2, mySqlAccountService.getAllAccounts().size());
    assertEquals("Mismatch in account information of a1", a1, mySqlAccountService.getAccountById(a1.getId()));
    assertEquals("Mismatch in account information of a2", a2, mySqlAccountService.getAccountById(a2.getId()));
  }

  /**
   * Tests update account with container conflicts fails atomically
   */
  @Test
  public void testAccountUpdatesFailureInTransaction() throws Exception {

    mySqlConfigProps.setProperty(DB_EXECUTE_BATCH_SIZE, "5");
    mySqlAccountService = getAccountService();

    // Add account "a1"with few containers
    List<Container> containers = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      containers.add(
          new ContainerBuilder((short) i, "c" + i, Container.ContainerStatus.ACTIVE, "c" + i, (short) 1).build());
    }
    Account a1 = new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).containers(containers).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(a1));

    // Add few more containers with no conflicts.
    containers.clear();
    for (int i = 4; i <= 6; i++) {
      containers.add(
          new ContainerBuilder((short) i, "c" + i, Container.ContainerStatus.ACTIVE, "c" + i, (short) 1).build());
    }
    a1 = new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).containers(containers).build();
    mySqlAccountService.updateAccountsWithMySqlStore(Collections.singletonList(a1));

    // Add few more containers with one of them (container ID: 6) conflicting.
    containers.clear();
    for (int i = 6; i <= 9; i++) {
      containers.add(
          new ContainerBuilder((short) i, "c" + i, Container.ContainerStatus.ACTIVE, "c" + i, (short) 1).build());
    }
    Account a1Updated =
        new AccountBuilder((short) 1, "a1", Account.AccountStatus.ACTIVE).containers(containers).build();

    // Verify updateAccountsWithMySqlStore() fails with Integrity constraint exception
    TestUtils.assertException(SQLException.class,
        () -> mySqlAccountService.updateAccountsWithMySqlStore(Collections.singletonList(a1Updated)), null);

    // Verify account update failed atomically with none of the containers (7 to 9) added.
    mySqlAccountService.fetchAndUpdateCache();
    assertEquals("Mismatch in number of containers of a1", 6,
        mySqlAccountService.getAccountById(a1.getId()).getAllContainers().size());
  }

  /**
   * Tests connection refresh for all non transient sql exceptions
   */
  @Test
  public void testConnectionRefreshOnException() throws Exception {

    //1. Add account to db
    Account a1 = new AccountBuilder((short) 101, "a1", Account.AccountStatus.ACTIVE).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(a1));

    //2. query db - should be successful
    assertEquals("Mismatch in account read from db", a1, mySqlAccountStore.getNewAccounts(0).iterator().next());

    //3. close db connection manually
    mySqlAccountStore.getMySqlDataAccessor().getDatabaseConnection(true).close();

    //4. query db - should establish new connection and get results.
    assertEquals("Mismatch in account read from db", a1, mySqlAccountStore.getNewAccounts(0).iterator().next());
  }

  /**
   * Test add, get and update dataset.
   * @throws Exception
   */
  @Test
  public void testAddGetAndUpdateDatasets() throws Exception {
    Account testAccount = makeTestAccountWithContainer();
    Container testContainer = new ArrayList<>(testAccount.getAllContainers()).get(0);
    //add dataset with must provide info
    Dataset dataset =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_BASIC).setVersionSchema(
            Dataset.VersionSchema.MONOTONIC).build();
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);
    Dataset datasetFromMysql =
        mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_BASIC);
    assertEquals("Mistmatch in dataset read from db", dataset, datasetFromMysql);

    Map<String, String> userTags = new HashMap<>();
    userTags.put("userTag", "tagValue");
    int retentionCount = 5;
    dataset = new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setVersionSchema(
            Dataset.VersionSchema.TIMESTAMP)
        .setRetentionTimeInSeconds(-1)
        .setUserTags(userTags)
        .setRetentionCount(retentionCount)
        .build();
    // Add dataset to db
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    // query db and check
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME);

    assertEquals("Mistmatch in dataset read from db", dataset, datasetFromMysql);

    // Add dataset again, should fail due to already exist.
    try {
      mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);
      fail("Should fail due to dataset already exist");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected ErrorCode", AccountServiceErrorCode.ResourceConflict, e.getErrorCode());
    }

    //update the retention count only.
    int newRetentionCount = 10;
    Dataset datasetForUpdate =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setRetentionCount(10).build();
    mySqlAccountStore.updateDataset(testAccount.getId(), testContainer.getId(), datasetForUpdate);
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME);
    assertEquals("Mismatch in updated retention count", (Integer) newRetentionCount,
        datasetFromMysql.getRetentionCount());

    //update the expiration time.
    long datasetTtl = 30;
    datasetForUpdate =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setRetentionTimeInSeconds(datasetTtl)
            .build();
    mySqlAccountStore.updateDataset(testAccount.getId(), testContainer.getId(), datasetForUpdate);
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME);
    assertEquals("Mismatch in updated dataset ttl", (Long) datasetTtl, datasetFromMysql.getRetentionTimeInSeconds());

    //update the user tags
    userTags.clear();
    userTags.put("userTagNew", "tagValueNew");
    datasetForUpdate =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setUserTags(userTags).build();
    mySqlAccountStore.updateDataset(testAccount.getId(), testContainer.getId(), datasetForUpdate);
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME);
    assertEquals("Mismatch in user tags", userTags, datasetFromMysql.getUserTags());

    //update dataset if the dataset does not exist (should fail)
    datasetForUpdate =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_NOT_EXIST).setUserTags(userTags)
            .build();
    try {
      mySqlAccountStore.updateDataset(testAccount.getId(), testContainer.getId(), datasetForUpdate);
      fail("should fail due to this dataset does not exist");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected ErrorCode", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    //Add a dataset with limited ttl
    dataset =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_WITH_TTL).setVersionSchema(
                Dataset.VersionSchema.TIMESTAMP)
            .setRetentionTimeInSeconds(datasetTtl)
            .setUserTags(userTags)
            .setRetentionCount(retentionCount)
            .build();
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    //update expiration to permanent.
    datasetTtl = -1;
    datasetForUpdate =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_WITH_TTL).setRetentionTimeInSeconds(
            datasetTtl).build();
    mySqlAccountStore.updateDataset(testAccount.getId(), testContainer.getId(), datasetForUpdate);
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME_WITH_TTL);
    assertEquals("Mismatch in updated dataset ttl", (Long) datasetTtl, datasetFromMysql.getRetentionTimeInSeconds());

    //delete a dataset
    mySqlAccountStore.deleteDataset(testAccount.getId(), testContainer.getId(), DATASET_NAME);
    try {
      mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME);
      fail("Should fail due to the dataset already expired");
    } catch (AccountServiceException e) {
      assertEquals("Mistmatch on error code", AccountServiceErrorCode.Deleted, e.getErrorCode());
    }

    //add new dataset with same primary key after it has been deleted.
    dataset = new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setVersionSchema(
        Dataset.VersionSchema.TIMESTAMP).setRetentionTimeInSeconds(-1).setRetentionCount(retentionCount).build();
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);
    datasetFromMysql = mySqlAccountStore.getDataset(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME);
    assertEquals("Mismatch on new added dataset", dataset, datasetFromMysql);
  }

  /**
   * Test add and get latest version.
   * @throws Exception
   */
  @Test
  public void testLatestVersionSupport() throws Exception {
    Account testAccount = makeTestAccountWithContainer();
    Container testContainer = new ArrayList<>(testAccount.getAllContainers()).get(0);
    Dataset dataset = new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setVersionSchema(
        Dataset.VersionSchema.MONOTONIC).build();

    // Add a dataset to db
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    // get a latest dataset version when no version exist, should fail.
    String version = "LATEST";
    DatasetVersionRecord datasetVersionRecordFromMysql;
    try {
      mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME, version);
      fail();
    } catch (AccountServiceException e) {
      assertEquals("Mismatch on error code", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    // Add a LATEST dataset version
    DatasetVersionRecord expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, "1", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // Add a new LATEST dataset version
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, "2", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // Get the LATEST dataset version
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, "2", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // Deleted version 2 and add new latest version
    mySqlAccountStore.deleteDatasetVersion(testAccount.getId(), testContainer.getId(), DATASET_NAME, "2");
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, "3", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // Add a dataset with semantic version
    dataset =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_WITH_SEMANTIC).setVersionSchema(
            Dataset.VersionSchema.SEMANTIC).build();
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    // get a latest dataset version when no version exist, should fail.
    version = "LATEST";
    try {
      mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version);
      fail();
    } catch (AccountServiceException e) {
      assertEquals("Mismatch on error code", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    // add a major version for timestamp schema, should fail.

    // add a major version.
    version = "MAJOR";
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "1.0.0", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);
    // add second major version
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.0.0", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // add a patch version.
    version = "PATCH";
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.0.1", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);
    // add second path version
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.0.2", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // add a minor version.
    version = "MINOR";
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.1.0", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);
    // add second minor version
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.2.0", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    //get the latest version.
    version = "LATEST";
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC, "2.2.0", -1);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);
  }

  /**
   * Test add and get dataset version with different input.
   */
  @Test
  public void testAddAndGetDatasetVersion() throws Exception {
    Account testAccount = makeTestAccountWithContainer();
    Container testContainer = new ArrayList<>(testAccount.getAllContainers()).get(0);
    Map<String, String> userTags = new HashMap<>();
    List<Long> versions = new ArrayList<>();
    userTags.put("userTag", "tagValue");
    long datasetTtl = 3600L;
    Dataset dataset = new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setVersionSchema(
        Dataset.VersionSchema.MONOTONIC).setRetentionTimeInSeconds(datasetTtl).setUserTags(userTags).build();

    // Add a dataset to db
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    // Add a valid dataset version
    long versionNumber = 1;
    String version = String.valueOf(versionNumber);
    versions.add(versionNumber);
    long creationTimeInMs = System.currentTimeMillis();
    DatasetVersionRecord expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, version,
            Utils.addSecondsToEpochTime(creationTimeInMs, datasetTtl));
    DatasetVersionRecord datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version, -1, creationTimeInMs, false);
    assertEquals("Mismatch in dataset", expectedDatasetVersionRecord, datasetVersionRecordFromMysql);

    // Get the dataset version
    datasetVersionRecordFromMysql =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version);
    assertEquals("Mismatch in dataset version read from db", expectedDatasetVersionRecord,
        datasetVersionRecordFromMysql);

    // Enable datasetVersionTtlEnable and add a valid dataset version with ttl and get from DB, should use the dataset
    // version level ttl.
    versionNumber = 2;
    version = String.valueOf(versionNumber);
    versions.add(versionNumber);
    creationTimeInMs = System.currentTimeMillis();
    long datasetVersionTtl = 360L;
    boolean datasetVersionTtlEnable = true;
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, version,
            Utils.addSecondsToEpochTime(creationTimeInMs, datasetVersionTtl));
    mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME, version, datasetVersionTtl, creationTimeInMs, datasetVersionTtlEnable);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version);
    assertEquals("Mismatch in dataset version read from db", expectedDatasetVersionRecord,
        datasetVersionRecordFromMysql);

    // Disable datasetVersionTtlEnable and add a valid dataset version with ttl and get from DB, should use the dataset
    // level ttl.
    versionNumber = 3;
    datasetVersionTtlEnable = false;
    version = String.valueOf(versionNumber);
    versions.add(versionNumber);
    expectedDatasetVersionRecord =
        new DatasetVersionRecord(testAccount.getId(), testContainer.getId(), DATASET_NAME, version,
            Utils.addSecondsToEpochTime(creationTimeInMs, datasetTtl));
    mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME, version, datasetVersionTtl, creationTimeInMs, datasetVersionTtlEnable);
    datasetVersionRecordFromMysql =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version);
    assertEquals("Mismatch in dataset version read from db", expectedDatasetVersionRecord,
        datasetVersionRecordFromMysql);

    // Add a permanent dataset version, it should inherit from dataset level ttl.
    versionNumber = 4;
    version = String.valueOf(versionNumber);
    versions.add(versionNumber);
    creationTimeInMs = System.currentTimeMillis();
    mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME, version, -1, creationTimeInMs, false);
    DatasetVersionRecord datasetVersionRecordWithTtl =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version);
    assertEquals("Mismatch in dataset expirationTimeMs", (long) dataset.getRetentionTimeInSeconds(),
        TimeUnit.MILLISECONDS.toSeconds(datasetVersionRecordWithTtl.getExpirationTimeMs() - creationTimeInMs));

    //Test semantic version.
    dataset =
        new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME_WITH_SEMANTIC).setVersionSchema(
            Dataset.VersionSchema.SEMANTIC).setRetentionTimeInSeconds(-1).setUserTags(userTags).build();
    // Add a dataset to db
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);
    version = "1.2.4";
    mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
    DatasetVersionRecord datasetVersionRecordWithSemanticVersion =
        mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version);
    assertEquals("Mismatch in dataset version", version, datasetVersionRecordWithSemanticVersion.getVersion());

    // Add dataset version for non-existing dataset.
    try {
      mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), "NonExistingDataset", version, -1, System.currentTimeMillis(), false);
      fail("Add dataset version should fail without dataset");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected ErrorCode", AccountServiceErrorCode.NotFound, e.getErrorCode());
    }

    //add same dataset version, should fail
    try {
      mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, 0, false);
      fail("Should fail due to dataset version already exist");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected error code", AccountServiceErrorCode.ResourceConflict, e.getErrorCode());
    }

    //Delete dataset version
    mySqlAccountStore.deleteDatasetVersion(testAccount.getId(), testContainer.getId(), DATASET_NAME_WITH_SEMANTIC,
        version);

    //add dataset version again, should succeed.
    mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);

    //get the new added dataset version, should not fail.
    mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
        testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version);

    // Add dataset version which didn't follow the semantic format.
    version = "1.2";
    try {
      mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME_WITH_SEMANTIC, version, -1, System.currentTimeMillis(), false);
      fail("Add dataset version should fail with wrong format of semantic version");
    } catch (IllegalArgumentException e) {
      // do nothing
    }

    //delete the dataset, and can't add or get any dataset version.
    mySqlAccountStore.deleteDataset(testAccount.getId(), testContainer.getId(), DATASET_NAME);
    version = "1";
    try {
      mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME, version, -1, System.currentTimeMillis(), false);
      fail("Should fail due to the dataset already gone");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected error code", AccountServiceErrorCode.Deleted, e.getErrorCode());
    }

    try {
      mySqlAccountStore.getDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
          testContainer.getName(), DATASET_NAME, version);
      fail("Should fail due to the dataset already gone");
    } catch (AccountServiceException e) {
      assertEquals("Unexpected error code", AccountServiceErrorCode.Deleted, e.getErrorCode());
    }

    List<DatasetVersionRecord> datasetVersionRecords =
        mySqlAccountStore.getAllValidVersion(testAccount.getId(), testContainer.getId(), DATASET_NAME);
    assertEquals("Mismatch on number of valid dataset versions", 4, datasetVersionRecords.size());

    mySqlAccountStore.deleteDatasetVersion(testAccount.getId(), testContainer.getId(), DATASET_NAME,
        "1");

    datasetVersionRecords = mySqlAccountStore.getAllValidVersion(testAccount.getId(), testContainer.getId(), DATASET_NAME);
    assertEquals("Mismatch on number of valid dataset versions", 3, datasetVersionRecords.size());
  }

  /**
   * Test get the version out of retention count.
   */
  @Test
  public void testGetDatasetVersionOutOfRetentionCount() throws AccountServiceException, SQLException {
    Account testAccount = makeTestAccountWithContainer();
    Container testContainer = new ArrayList<>(testAccount.getAllContainers()).get(0);
    Dataset dataset = new DatasetBuilder(testAccount.getName(), testContainer.getName(), DATASET_NAME).setVersionSchema(
        Dataset.VersionSchema.SEMANTIC).setRetentionCount(2).build();

    // Add a dataset to db
    mySqlAccountStore.addDataset(testAccount.getId(), testContainer.getId(), dataset);

    // Add 1st dataset version
    String version1 = "1.2.3";
    long creationTimeInMs = System.currentTimeMillis();
    DatasetVersionRecord datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version1, -1, creationTimeInMs, false);

    // Add 2nd dataset version
    String version2 = "1.2.4";
    creationTimeInMs = System.currentTimeMillis();
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version2, -1, creationTimeInMs, false);

    // Add 3rd dataset version
    String version3 = "1.2.5";
    creationTimeInMs = System.currentTimeMillis();
    datasetVersionRecordFromMysql =
        mySqlAccountStore.addDatasetVersion(testAccount.getId(), testContainer.getId(), testAccount.getName(),
            testContainer.getName(), DATASET_NAME, version3, -1, creationTimeInMs, false);

    List<DatasetVersionRecord> datasetVersionRecordList =
        mySqlAccountStore.getAllValidVersionsOutOfRetentionCount(testAccount.getId(), testContainer.getId(),
            testAccount.getName(), testContainer.getName(), DATASET_NAME);
    assertEquals("Mismatch on size of the datasetVersionRecordList", 1, datasetVersionRecordList.size());
    assertEquals("Mismatch on the version", version1, datasetVersionRecordList.get(0).getVersion());
  }

  private Account makeTestAccountWithContainer() {
    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();
    return testAccount;
  }

  /**
   * Create a new container to add to an account.
   * @param name container name
   * @param parentAccountId id of parent account
   * @param status container status
   * @return the generated container
   */
  private Container makeNewContainer(String name, short parentAccountId, ContainerStatus status) {
    return new ContainerBuilder(Container.UNKNOWN_CONTAINER_ID, name, status, DESCRIPTION, parentAccountId).build();
  }

  /**
   * Empties the accounts and containers tables.
   * @throws SQLException
   */
  private void cleanup() throws SQLException {
    Connection dbConnection = mySqlAccountStore.getMySqlDataAccessor().getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    statement.executeUpdate("DELETE FROM " + AccountDao.ACCOUNT_TABLE);
    statement.executeUpdate("DELETE FROM " + AccountDao.CONTAINER_TABLE);
    statement.executeUpdate("DELETE FROM " + AccountDao.DATASET_TABLE);
    statement.executeUpdate("DELETE FROM " + AccountDao.DATASET_VERSION_TABLE);
  }
}

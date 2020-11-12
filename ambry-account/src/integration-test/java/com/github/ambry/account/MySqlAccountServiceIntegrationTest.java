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
import com.github.ambry.account.mysql.AccountDao;
import com.github.ambry.account.mysql.ContainerDao;
import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.Container.*;
import static com.github.ambry.mysql.MySqlUtils.*;
import static com.github.ambry.config.MySqlAccountServiceConfig.*;
import static com.github.ambry.utils.AccountTestUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Integration tests for {@link MySqlAccountService}.
 */
public class MySqlAccountServiceIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountServiceIntegrationTest.class);
  private static final String DESCRIPTION = "Indescribable";
  private final MySqlAccountStoreFactory mockMySqlAccountStoreFactory;
  private MySqlAccountStore mySqlAccountStore;
  private final AccountServiceMetrics accountServiceMetrics;
  private final Properties mySqlConfigProps;
  private MySqlAccountServiceConfig accountServiceConfig;
  private MySqlAccountService mySqlAccountService;

  public MySqlAccountServiceIntegrationTest() throws Exception {
    mySqlConfigProps = Utils.loadPropsFromResource("mysql.properties");
    mySqlConfigProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlConfigProps.setProperty(UPDATE_DISABLED, "false");
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountStore = spy(new MySqlAccountStoreFactory(new VerifiableProperties(mySqlConfigProps),
        new MetricRegistry()).getMySqlAccountStore());
    mockMySqlAccountStoreFactory = mock(MySqlAccountStoreFactory.class);
    when(mockMySqlAccountStoreFactory.getMySqlAccountStore()).thenReturn(mySqlAccountStore);
    // Start with empty database
    cleanup();
    mySqlAccountService = getAccountService();
  }

  private MySqlAccountService getAccountService() throws Exception {
    accountServiceConfig = new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps));
    // Don't initialize account store here as it may have preinitialized data
    return new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory);
  }

  @Test
  public void testBadCredentials() throws Exception {
    DbEndpoint endpoint =
        new DbEndpoint("jdbc:mysql://localhost/AccountMetadata", "dc1", true, "baduser", "badpassword");
    try {
      new MySqlAccountStore(Collections.singletonList(endpoint), endpoint.getDatacenter(),
          new MySqlMetrics(MySqlAccountStore.class, new MetricRegistry()));
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
    Map<String, String> accountMap = new HashMap<>();
    accountMap.put(Short.toString(testAccount.getId()), testAccount.toJson(false).toString());
    String filename = BackupFileManager.getBackupFilename(1, SystemTime.getInstance().seconds());
    Path filePath = accountBackupDir.resolve(filename);
    BackupFileManager.writeAccountMapToFile(filePath, accountMap);

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
    Container testContainer = testAccount.getContainerByName("testContainer");
    mySqlAccountStore.addAccounts(Collections.singletonList(testAccount));
    mySqlAccountStore.addContainer(testContainer);
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
    mySqlAccountService =
        new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory);
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
    // TODO: after getPreparedStatement tries to upgrade, should fail then succeed
    assertEquals(expectedConnectionFail, storeMetrics.connectionFailureCount.getCount());
    assertEquals(expectedConnectionSuccess, storeMetrics.connectionSuccessCount.getCount());
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
    mySqlAccountStore.addAccounts(Collections.singletonList(testAccount));

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
    MySqlAccountService consumerAccountService =
        new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mockMySqlAccountStoreFactory);

    // Add account with 3 containers
    short accountId = 101;
    String accountName = "a1";
    // Number of calls expected in producer account store
    int expectedAddAccounts = 0, expectedUpdateAccounts = 0;
    int expectedAddContainers = 0, expectedUpdateContainers = 0;
    List<Container> containers = new ArrayList<>();
    containers.add(new ContainerBuilder((short) 1, "c1", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    containers.add(new ContainerBuilder((short) 2, "c2", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    containers.add(new ContainerBuilder((short) 3, "c3", ContainerStatus.ACTIVE, DESCRIPTION, accountId).build());
    Account a1 =
        new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).containers(containers).build();
    producerAccountService.updateAccounts(Collections.singletonList(a1));
    expectedAddAccounts++;
    expectedAddContainers += 3;
    verifyStoreInteractions(producerAccountStore, expectedAddAccounts, expectedUpdateAccounts, expectedAddContainers,
        expectedUpdateContainers);
    consumerAccountService.fetchAndUpdateCache();
    long lmt = consumerAccountService.accountInfoMapRef.get().getLastModifiedTime();
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));

    // Update account only
    String newAccountName = "a1-updated";
    a1 = new AccountBuilder(a1).name(newAccountName).build();
    producerAccountService.updateAccounts(Collections.singletonList(a1));
    expectedUpdateAccounts++;
    verifyStoreInteractions(producerAccountStore, expectedAddAccounts, expectedUpdateAccounts, expectedAddContainers,
        expectedUpdateContainers);
    consumerAccountService.fetchAndUpdateCache();
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
    expectedUpdateContainers++;
    verifyStoreInteractions(producerAccountStore, expectedAddAccounts, expectedUpdateAccounts, expectedAddContainers,
        expectedUpdateContainers);
    assertEquals("Expected one result", 1, result.size());
    assertEquals("Container mismatch", c1Mod, result.iterator().next());
    consumerAccountService.fetchAndUpdateCache();
    verify(consumerAccountStore).getNewAccounts(eq(lmt));
    verify(consumerAccountStore).getNewContainers(eq(lmt));
    assertEquals("Container mismatch", c1Mod, consumerAccountService.getContainer(accountName, "c1"));
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));
    lmt = consumerAccountService.accountInfoMapRef.get().getLastModifiedTime();

    // Add container only
    Container cNew = makeNewContainer("c4", accountId, ContainerStatus.ACTIVE);
    result = producerAccountService.updateContainers(accountName, Collections.singletonList(cNew));
    expectedAddContainers++;
    verifyStoreInteractions(producerAccountStore, expectedAddAccounts, expectedUpdateAccounts, expectedAddContainers,
        expectedUpdateContainers);
    assertEquals("Expected one result", 1, result.size());
    cNew = result.iterator().next();
    containers.add(cNew);
    a1 = new AccountBuilder(a1).containers(containers).build();
    consumerAccountService.fetchAndUpdateCache();
    verify(consumerAccountStore).getNewAccounts(eq(lmt));
    verify(consumerAccountStore).getNewContainers(eq(lmt));
    assertEquals("Container mismatch", cNew, consumerAccountService.getContainer(accountName, "c4"));
    assertEquals("Account mismatch", a1, consumerAccountService.getAccountByName(accountName));

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
   * Checks that {@link MySqlAccountStore} methods were called the expected number of times.
   * @param accountStore
   * @param expectedAddAccounts
   * @param expectedUpdateAccounts
   * @param expectedAddContainers
   * @param expectedUpdateContainers
   * @throws Exception
   */
  private void verifyStoreInteractions(MySqlAccountStore accountStore, int expectedAddAccounts,
      int expectedUpdateAccounts, int expectedAddContainers, int expectedUpdateContainers) throws Exception {
    verify(accountStore, times(expectedAddAccounts)).addAccount(any());
    verify(accountStore, times(expectedUpdateAccounts)).updateAccount(any());
    verify(accountStore, times(expectedAddContainers)).addContainer(any());
    verify(accountStore, times(expectedUpdateContainers)).updateContainer(any());
  }

  /**
   * Empties the accounts and containers tables.
   * @throws SQLException
   */
  private void cleanup() throws SQLException {
    Connection dbConnection = mySqlAccountStore.getMySqlDataAccessor().getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    statement.executeUpdate("DELETE FROM " + AccountDao.ACCOUNT_TABLE);
    statement.executeUpdate("DELETE FROM " + ContainerDao.CONTAINER_TABLE);
  }
}

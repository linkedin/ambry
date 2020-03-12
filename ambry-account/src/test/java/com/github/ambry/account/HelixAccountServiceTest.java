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
import com.github.ambry.clustermap.HelixStoreOperator;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.AccountTestUtils.*;
import static com.github.ambry.account.Container.*;
import static com.github.ambry.account.HelixAccountService.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Unit tests for {@link HelixAccountService}.
 */
@RunWith(Parameterized.class)
public class HelixAccountServiceTest {
  private static final int ZK_CLIENT_CONNECTION_TIMEOUT_MS = 20000;
  private static final int ZK_CLIENT_SESSION_TIMEOUT_MS = 20000;
  private static final String ZK_CONNECT_STRING = "dummyHost:dummyPort";
  private static final String STORE_ROOT_PATH = "/ambry_test/helix_account_service";
  private static final Random random = new Random();
  private static final String BAD_ACCOUNT_METADATA_STRING = "badAccountMetadataString";
  private static final int NUM_REF_ACCOUNT = 10;
  private static final int NUM_CONTAINER_PER_ACCOUNT = 4;
  private static final int TOTAL_NUMBER_OF_VERSION_TO_KEEP = 100;
  private static final Map<Short, Account> idToRefAccountMap = new HashMap<>();
  private static final Map<Short, Map<Short, Container>> idToRefContainerMap = new HashMap<>();
  private final Properties helixConfigProps = new Properties();
  private final Path accountBackupDir;
  private final MockNotifier<String> notifier;
  private VerifiableProperties vHelixConfigProps;
  private HelixPropertyStoreConfig storeConfig;
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
  private MockHelixAccountServiceFactory mockHelixAccountServiceFactory;
  private Router mockRouter;
  private boolean useNewZNodePath;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Resets variables and settings, and cleans up if the store already exists.
   * @throws Exception Any unexpected exception.
   */
  public HelixAccountServiceTest(boolean useNewZNodePath) throws Exception {
    this.useNewZNodePath = useNewZNodePath;
    accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    notifier = new MockNotifier<>();
    mockRouter = new MockRouter();
    setup();
    deleteStoreIfExists();
    generateReferenceAccountsAndContainers();
  }

  @Before
  public void setup() {
    helixConfigProps.clear();
    helixConfigProps.setProperty(
        HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.connection.timeout.ms",
        String.valueOf(ZK_CLIENT_CONNECTION_TIMEOUT_MS));
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "zk.client.session.timeout.ms",
        String.valueOf(ZK_CLIENT_SESSION_TIMEOUT_MS));
    helixConfigProps.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, ZK_CONNECT_STRING);
    helixConfigProps.setProperty(HelixPropertyStoreConfig.HELIX_PROPERTY_STORE_PREFIX + "root.path", STORE_ROOT_PATH);
    helixConfigProps.setProperty(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY, accountBackupDir.toString());
    helixConfigProps.setProperty(HelixAccountServiceConfig.USE_NEW_ZNODE_PATH, String.valueOf(useNewZNodePath));
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, null, mockRouter);
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
    assertEquals("No TopicListeners should still be attached to Notifier", 0,
        notifier.topicToListenersMap.getOrDefault(ACCOUNT_METADATA_CHANGE_TOPIC, Collections.emptySet()).size());
    deleteStoreIfExists();
    deleteBackupDirectoryIfExist();
  }

  /**
   * Tests a clean startup of {@link HelixAccountService}, when the corresponding {@code ZooKeeper} does not
   * have any {@link ZNRecord} on it.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testStartUpWithoutMetadataExists() {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    // At time zero, no account metadata exists.
    assertEquals("The number of account in HelixAccountService is incorrect after clean startup", 0,
        accountService.getAllAccounts().size());
  }

  /**
   * Tests starting up a {@link HelixAccountService}, when the corresponding {@code ZooKeeper} has account metadata
   * already stored on it.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testStartUpWithMetadataExists() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountsToHelixPropertyStore(idToRefAccountMap.values(), false);
    // When start, the helixAccountService should get the account metadata.
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);
  }

  /**
   * Tests creating a number of new {@link Account} through {@link HelixAccountService}, where there is no {@link ZNRecord}
   * exists on the {@code ZooKeeper}.
   */
  @Test
  public void testCreateAccount() {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertEquals("The number of account in HelixAccountService is incorrect", 0,
        accountService.getAllAccounts().size());
    boolean res = accountService.updateAccounts(idToRefAccountMap.values());
    assertTrue("Failed to update accounts", res);
    assertAccountsInAccountService(idToRefAccountMap.values(), NUM_REF_ACCOUNT, accountService);
  }

  /**
   * Tests creating and updating accounts through {@link HelixAccountService} in various situations:
   * 0. {@link Account}s already exists on ZooKeeper.
   * 1. add a new {@link Account};
   * 2. update existing {@link Account};
   * 3. add a new {@link Container} to an existing {@link Account};
   * 4. update existing {@link Container}s of existing {@link Account}s.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateAccount() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountsToHelixPropertyStore(idToRefAccountMap.values(), false);
    accountService = mockHelixAccountServiceFactory.getAccountService();
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
   * Tests starting up a {@link HelixAccountService}, when the corresponding {@code ZooKeeper} does not have any
   * {@link ZNRecord} on it but local backup files exists.
   * @throws Exception Any unexpected exception
   */
  @Test
  public void testStartWithBackupFiles() throws Exception {
    // use testUpdateAccount function to create backups and then delete helixStore data.
    testUpdateAccount();
    if (accountService != null) {
      accountService.close();
    }
    deleteStoreIfExists();

    // should have some backup files.
    if (helixConfigProps.containsKey(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY)) {
      File[] files = accountBackupDir.toFile()
          .listFiles(path -> BackupFileManager.versionFilenamePattern.matcher(path.getName()).find());
      assertTrue("UpdateAccount should create backup files", files.length > 0);

      helixConfigProps.put(HelixAccountServiceConfig.ENABLE_SERVE_FROM_BACKUP, "true");
      vHelixConfigProps = new VerifiableProperties(helixConfigProps);
      storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
      String updaterThreadPrefix = UUID.randomUUID().toString();
      MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
          new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
              mockRouter);
      accountService = mockHelixAccountServiceFactory.getAccountService();
      assertNotNull("Backup files should have data", accountService.getAllAccounts());
      assertEquals("Number of accounts from backup mismatch", accountService.getAllAccounts().size(),
          1 + NUM_REF_ACCOUNT);
    }
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} is empty. This is a
   * good {@link ZNRecord} format that should NOT fail fetch or update.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase1() throws Exception {
    ZNRecord zNRecord = makeZNRecordWithSimpleField(null, null, null);
    updateAndWriteZNRecord(zNRecord, true);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} has an irrelevant
   * simple field ("key": "value"), but ("accountMetadata": someValidMap) is missing. This is a good {@link ZNRecord}
   * format that should NOT fail fetch or update.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase2() throws Exception {
    ZNRecord zNRecord = makeZNRecordWithSimpleField(null, "key", "value");
    updateAndWriteZNRecord(zNRecord, true);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} has a map field
   * ("key": someValidAccountMap), but ({@link LegacyMetadataStore#ACCOUNT_METADATA_MAP_KEY}: someValidMap)
   * is missing. This is a good {@link ZNRecord} format that should NOT fail fetch or update.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase3() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());
    ZNRecord zNRecord = makeZNRecordWithMapField(null, "key", mapValue);
    updateAndWriteZNRecord(zNRecord, true);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} has a map field
   * ({@link LegacyMetadataStore#ACCOUNT_METADATA_MAP_KEY}: accountMap), and accountMap contains
   * ("accountId": accountJsonStr) that does not match. This is a NOT good {@link ZNRecord} format that should
   * fail fetch or update.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase4() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put("-1", refAccount.toJson(true).toString());
    ZNRecord zNRecord = null;
    if (useNewZNodePath) {
      String blobID = RouterStore.writeAccountMapToRouter(mapValue, mockRouter);
      List<String> list = Collections.singletonList(new RouterStore.BlobIDAndVersion(blobID, 1).toJson());
      zNRecord = makeZNRecordWithListField(null, RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
    } else {
      zNRecord = makeZNRecordWithMapField(null, LegacyMetadataStore.ACCOUNT_METADATA_MAP_KEY, mapValue);
    }
    updateAndWriteZNRecord(zNRecord, false);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} has a map field
   * ({@link LegacyMetadataStore#ACCOUNT_METADATA_MAP_KEY}: accountMap), and accountMap contains
   * ("accountId": badAccountJsonString). This is a NOT good {@link ZNRecord} format that should fail fetch or update.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase5() throws Exception {
    Map<String, String> mapValue = new HashMap<>();
    mapValue.put(String.valueOf(refAccount.getId()), BAD_ACCOUNT_METADATA_STRING);
    ZNRecord zNRecord = null;
    if (useNewZNodePath) {
      String blobID = RouterStore.writeAccountMapToRouter(mapValue, mockRouter);
      List<String> list = Collections.singletonList(new RouterStore.BlobIDAndVersion(blobID, 1).toJson());
      zNRecord = makeZNRecordWithListField(null, RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
    } else {
      zNRecord = makeZNRecordWithMapField(null, LegacyMetadataStore.ACCOUNT_METADATA_MAP_KEY, mapValue);
    }
    updateAndWriteZNRecord(zNRecord, false);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore}, where the {@link ZNRecord} has an invalid account
   * record and a valid account record. This is a NOT good {@link ZNRecord} format and it should fail fetch or update
   * operations, with none of the record should be read.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase6() throws Exception {
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    accountMap.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());
    accountMap.put(String.valueOf(refAccount.getId() + 1), BAD_ACCOUNT_METADATA_STRING);
    if (useNewZNodePath) {
      String blobID = RouterStore.writeAccountMapToRouter(accountMap, mockRouter);
      List<String> list = Collections.singletonList(new RouterStore.BlobIDAndVersion(blobID, 1).toJson());
      zNRecord.setListField(RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
    } else {
      zNRecord.setMapField(LegacyMetadataStore.ACCOUNT_METADATA_MAP_KEY, accountMap);
    }
    updateAndWriteZNRecord(zNRecord, false);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore} and ambry, where the ambry has valid accounts
   * record but {@link ZNRecord} has invalid list. This is a NOT good {@link ZNRecord} format and it should fail fetch or update
   * operations, with none of the record should be read.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase7() throws Exception {
    if (!useNewZNodePath) {
      return;
    }
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    accountMap.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());
    String blobID = RouterStore.writeAccountMapToRouter(accountMap, mockRouter);
    List<String> list = Collections.singletonList("badliststring");
    zNRecord.setListField(RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
    updateAndWriteZNRecord(zNRecord, false);
  }

  /**
   * Tests reading {@link ZNRecord} from {@link HelixPropertyStore} and ambry, where the ambry has valid accounts
   * record but {@link ZNRecord} has an invalid list item and a valid list item. This is a NOT good {@link ZNRecord}
   * format and it should fail fetch or update operations, with none of the record should be read.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadBadZNRecordCase8() throws Exception {
    if (!useNewZNodePath) {
      return;
    }
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    accountMap.put(String.valueOf(refAccount.getId()), refAccount.toJson(true).toString());
    String blobID = RouterStore.writeAccountMapToRouter(accountMap, mockRouter);
    List<String> list = new ArrayList<>();
    list.add("badliststring");
    list.add(new RouterStore.BlobIDAndVersion(blobID, 1).toJson());
    zNRecord.setListField(RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
    updateAndWriteZNRecord(zNRecord, false);
  }

  /**
   * Tests receiving a bad message, it will not be recognized by {@link HelixAccountService}, but will also not
   * crash the service.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void receiveBadMessage() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    updateAccountsAndAssertAccountExistence(idToRefAccountMap.values(), NUM_REF_ACCOUNT, true);
    notifier.publish(ACCOUNT_METADATA_CHANGE_TOPIC, "badMessage");
    assertEquals("The number of account in HelixAccountService is different from expected", NUM_REF_ACCOUNT,
        accountService.getAllAccounts().size());
  }

  /**
   * Tests receiving a bad topic, it will not be recognized by {@link HelixAccountService}, but will also not
   * crash the service.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void receiveBadTopic() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    updateAccountsAndAssertAccountExistence(idToRefAccountMap.values(), NUM_REF_ACCOUNT, true);
    notifier.publish("badTopic", FULL_ACCOUNT_METADATA_CHANGE_MESSAGE);
    assertEquals("The number of account in HelixAccountService is different from expected", NUM_REF_ACCOUNT,
        accountService.getAllAccounts().size());
  }

  /**
   * Tests a number of bad inputs.
   */
  @Test
  public void testNullInputs() throws IOException {
    try {
      new MockHelixAccountServiceFactory(null, new MetricRegistry(), notifier, null, mockRouter).getAccountService();
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }

    try {
      new MockHelixAccountServiceFactory(vHelixConfigProps, null, notifier, null, mockRouter).getAccountService();
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
    accountService = new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), null, null, mockRouter)
        .getAccountService();
    accountService.close();
    accountService = mockHelixAccountServiceFactory.getAccountService();
    try {
      accountService.updateAccounts(null);
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
    try {
      accountService.getAccountByName(null);
      fail("should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateNameConflictingAccounts() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in id.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateIdConflictingAccounts() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
  }

  /**
   * Tests updating a collection of {@link Account}s, where there are duplicate {@link Account}s in id and name.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testUpdateDuplicateAccounts() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
  }

  /**
   * Tests updating a {@link Account}, which has the same id and name as an existing record, and will replace the
   * existing record. This test corresponds to case A specified in the JavaDoc of {@link AccountService}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testNonConflictingUpdateCaseA() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
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
    accountService = mockHelixAccountServiceFactory.getAccountService();
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
    accountService = mockHelixAccountServiceFactory.getAccountService();
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
    accountService = mockHelixAccountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 3, "a", AccountStatus.INACTIVE).build()));
    assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
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
    accountService = mockHelixAccountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Collection<Account> conflictAccounts =
        Collections.singleton((new AccountBuilder((short) 1, "b", AccountStatus.INACTIVE).build()));
    assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
    assertEquals("Wrong account number in HelixAccountService", 2, accountService.getAllAccounts().size());
    assertEquals("Wrong account name got from HelixAccountService", "a",
        accountService.getAccountById((short) 1).getName());
  }

  /**
   * Test updating an account with a conflicting expected snapshot version.
   */
  @Test
  public void testConflictingSnapshotVersionUpdate() throws Exception {
    accountService = mockHelixAccountServiceFactory.getAccountService();
    // write two accounts (1, "a") and (2, "b")
    writeAccountsForConflictTest();
    Account expectedAccount = accountService.getAccountById((short) 1);
    int currentSnapshotVersion = expectedAccount.getSnapshotVersion();
    for (int snapshotVersionOffset : new int[]{-2, -1, 1}) {
      int snapshotVersionToUse = currentSnapshotVersion + snapshotVersionOffset;
      Collection<Account> conflictAccounts = Collections.singleton(
          new AccountBuilder((short) 1, "c", AccountStatus.INACTIVE).snapshotVersion(snapshotVersionToUse).build());
      assertFalse("Wrong return value from update operation.", accountService.updateAccounts(conflictAccounts));
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
   * Tests reading conflicting {@link Account} metadata from {@link HelixPropertyStore}. Two {@link Account}s have
   * different accountIds but the same accountNames. This is a BAD record that should impact fetching or updating
   * {@link Account}s.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase1() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build();
    Account account2 = new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE).build();
    conflictAccounts.add(account1);
    conflictAccounts.add(account2);
    readAndUpdateBadRecord(conflictAccounts);
  }

  /**
   * Tests reading conflicting {@link Account} metadata from {@link org.apache.helix.store.HelixPropertyStore}.
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase2() throws Exception {
    List<Account> conflictAccounts = new ArrayList<>();
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build();
    Account account2 = new AccountBuilder((short) 2, "a", AccountStatus.INACTIVE).build();
    Account account3 = new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE).build();
    Account account4 = new AccountBuilder((short) 3, "b", AccountStatus.INACTIVE).build();
    conflictAccounts.add(account1);
    conflictAccounts.add(account2);
    conflictAccounts.add(account3);
    conflictAccounts.add(account4);
    readAndUpdateBadRecord(conflictAccounts);
  }

  /**
   * Tests a series of operations.
   * 1. PrePopulates account (1, "a");
   * 2. Starts up a {@link HelixAccountService};
   * 3. Remote copy adds a new account (2, "b"), and the update has not been propagated to the {@link HelixAccountService};
   * 4. The {@link HelixAccountService} attempts to update an account (3, "b"), which should fail because it will eventually
   *    conflict with the remote copy;
   * @throws Exception Any unexpected exception.
   */
  @Test
  public void testReadConflictAccountDataFromHelixPropertyStoreCase3() throws Exception {
    Account account1 = new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build();
    List<Account> accounts = Collections.singletonList(account1);
    writeAccountsToHelixPropertyStore(accounts, false);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertAccountInAccountService(account1, accountService);

    Account account2 = new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE).build();
    accounts = Collections.singletonList(account2);
    writeAccountsToHelixPropertyStore(accounts, false);

    Account conflictingAccount = new AccountBuilder((short) 3, "b", AccountStatus.INACTIVE).build();
    accounts = Collections.singletonList(conflictingAccount);
    assertFalse(accountService.updateAccounts(accounts));
    assertEquals("Number of account is wrong.", 1, accountService.getAllAccounts().size());
    assertAccountInAccountService(account1, accountService);
  }

  /**
   * Tests adding/removing {@link Consumer}.
   * @throws Exception
   */
  @Test
  public void testAccountUpdateConsumer() throws Exception {
    // pre-populate account metadata in ZK.
    writeAccountsToHelixPropertyStore(idToRefAccountMap.values(), false);
    accountService = mockHelixAccountServiceFactory.getAccountService();
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

  /**
   * Tests the background updater for updating accounts from remote. During the initialization of
   * {@link HelixAccountService}, its internal {@link HelixPropertyStore} will be read to first time get account data.
   * Because of the background account updater, it should continuously make get calls to the {@link HelixPropertyStore},
   * even no notification for account updates is received. Therefore, there will be more than 1 get calls to the
   * {@link HelixPropertyStore}.
   * @throws Exception
   */
  @Test
  public void testBackgroundUpdater() throws Exception {
    helixConfigProps.setProperty(HelixAccountServiceConfig.UPDATER_POLLING_INTERVAL_MS_KEY, "1");
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    String updaterThreadPrefix = UUID.randomUUID().toString();
    MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
            mockRouter);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    CountDownLatch latch = new CountDownLatch(1);
    mockHelixAccountServiceFactory.getHelixStore(ZK_CONNECT_STRING, storeConfig).setReadLatch(latch);
    assertEquals("Wrong number of thread for account updater.", 1, numThreadsByThisName(updaterThreadPrefix));
    awaitLatchOrTimeout(latch, 100);
  }

  /**
   * Tests disabling the background thread. By setting the polling interval to 0ms, the accounts should not be fetched.
   * Therefore, after the {@link HelixAccountService} starts, there should be a single get call to the
   * {@link HelixPropertyStore}.
   */
  @Test
  public void testDisableBackgroundUpdater() {
    helixConfigProps.setProperty(HelixAccountServiceConfig.UPDATER_POLLING_INTERVAL_MS_KEY, "0");
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    String updaterThreadPrefix = UUID.randomUUID().toString();
    MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
            mockRouter);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertEquals("Wrong number of thread for account updater.", 0, numThreadsByThisName(updaterThreadPrefix));
  }

  /**
   * Tests disabling the background thread. By setting the polling interval to 0ms, the accounts should not be fetched.
   * Therefore, after the {@link HelixAccountService} starts, there should be a single get call to the
   * {@link HelixPropertyStore}.
   * @throws Exception
   */
  @Test
  public void testDisabledBackups() throws Exception {
    helixConfigProps.remove(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY);
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    String updaterThreadPrefix = UUID.randomUUID().toString();
    MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
            mockRouter);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    updateAccountsAndAssertAccountExistence(Collections.singleton(refAccount), 1, true);
  }

  /**
   * Tests disabling account updates. By setting the {@link HelixAccountServiceConfig#UPDATE_DISABLED} to be true, all the
   * account update request should be rejected.
   */
  @Test
  public void testUpdateDisabled() {
    helixConfigProps.setProperty(HelixAccountServiceConfig.UPDATE_DISABLED, "true");
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    String updaterThreadPrefix = UUID.randomUUID().toString();
    MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
            mockRouter);
    accountService = mockHelixAccountServiceFactory.getAccountService();

    // add a new account
    Account newAccountWithoutContainer = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    List<Account> accountsToUpdate = Collections.singletonList(newAccountWithoutContainer);
    assertFalse("Update accounts should be disabled", accountService.updateAccounts(accountsToUpdate));
  }

  /**
   * Tests enabling backfilling for new znode path. While {@link HelixAccountService} is still using old znode path, it should
   * forward the new {@link Account} metadata to new znode path.
   */
  @Test
  public void testFillAccountsToNewZNode() throws Exception {
    assumeTrue(!useNewZNodePath);
    helixConfigProps.put(HelixAccountServiceConfig.BACKFILL_ACCOUNTS_TO_NEW_ZNODE, "true");
    vHelixConfigProps = new VerifiableProperties(helixConfigProps);
    storeConfig = new HelixPropertyStoreConfig(vHelixConfigProps);
    String updaterThreadPrefix = UUID.randomUUID().toString();
    MockHelixAccountServiceFactory mockHelixAccountServiceFactory =
        new MockHelixAccountServiceFactory(vHelixConfigProps, new MetricRegistry(), notifier, updaterThreadPrefix,
            mockRouter);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    ((HelixAccountService) accountService).setupRouter(mockRouter);

    // update accounts and then make sure the blob ids are stored in the helixStore
    Account newAccountWithoutContainer = new AccountBuilder(refAccountId, refAccountName, refAccountStatus).build();
    List<Account> accountsToUpdate = Collections.singletonList(newAccountWithoutContainer);
    boolean hasUpdateAccountSucceed = accountService.updateAccounts(accountsToUpdate);
    assertTrue("Wrong update return status", hasUpdateAccountSucceed);
    assertAccountsInAccountService(accountsToUpdate, 1, accountService);

    // verify the RouterStore can read the account map out.
    HelixPropertyStore<ZNRecord> helixStore =
        mockHelixAccountServiceFactory.getHelixStore(ZK_CONNECT_STRING, storeConfig);
    HelixAccountService helixAccountService = (HelixAccountService) accountService;
    RouterStore routerStore =
        new RouterStore(helixAccountService.getAccountServiceMetrics(), helixAccountService.getBackupFileManager(),
            helixStore, new AtomicReference<>(mockRouter), false, TOTAL_NUMBER_OF_VERSION_TO_KEEP);
    Map<String, String> accountMap = routerStore.fetchAccountMetadata();
    assertNotNull("Accounts should be backfilled to new znode", accountMap);
    assertAccountMapEquals(accountService.getAllAccounts(), accountMap);
  }

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
   * {@link org.apache.helix.store.HelixPropertyStore}.
   * @throws Exception Any unexpected exception.
   */
  private void writeAccountsForConflictTest() throws Exception {
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", AccountStatus.INACTIVE).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", AccountStatus.INACTIVE).build());
    updateAccountsAndAssertAccountExistence(existingAccounts, 2, true);
  }

  /**
   * PrePopulates a collection of self-conflicting {@link Account}s, which will impact {@link HelixAccountService}
   * startup and update.
   * @param accounts The self-conflicting {@link Account}s.
   */
  private void readAndUpdateBadRecord(Collection<Account> accounts) throws Exception {
    writeAccountsToHelixPropertyStore(accounts, false);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertEquals("Wrong number of accounts in helixAccountService", 0, accountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), 0, false);
    writeAccountsToHelixPropertyStore(accounts, true);
    assertEquals("Number of account is wrong.", 0, accountService.getAllAccounts().size());
  }

  /**
   * Updates a collection of {@link Account}s through {@link HelixAccountService}, and verifies that the
   * {@link Account}s have been successfully updated, so they can be queried through {@link HelixAccountService}.
   * @param accounts A collection of {@link Account}s to update through {@link HelixAccountService}.
   */
  private void updateAccountsAndAssertAccountExistence(Collection<Account> accounts, int expectedAccountCount,
      boolean shouldUpdateSucceed) throws Exception {
    boolean hasUpdateAccountSucceed = accountService.updateAccounts(accounts);
    assertEquals("Wrong update return status", shouldUpdateSucceed, hasUpdateAccountSucceed);
    if (shouldUpdateSucceed) {
      assertAccountsInAccountService(accounts, expectedAccountCount, accountService);
      if (helixConfigProps.containsKey(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY)) {
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
   * Pre-populates a collection of {@link Account}s to the underlying {@link org.apache.helix.store.HelixPropertyStore}
   * using {@link com.github.ambry.clustermap.HelixStoreOperator} (not through the {@link HelixAccountService}). This method
   * does not check any conflict among the {@link Account}s to write.
   * @throws Exception Any unexpected exception.
   */
  private void writeAccountsToHelixPropertyStore(Collection<Account> accounts, boolean shouldNotify) throws Exception {
    HelixStoreOperator storeOperator =
        new HelixStoreOperator(mockHelixAccountServiceFactory.getHelixStore(ZK_CONNECT_STRING, storeConfig));
    ZNRecord zNRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    Map<String, String> accountMap = new HashMap<>();
    for (Account account : accounts) {
      accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
    }
    if (useNewZNodePath) {
      String blobID = RouterStore.writeAccountMapToRouter(accountMap, mockRouter);
      List<String> list = Collections.singletonList(new RouterStore.BlobIDAndVersion(blobID, 1).toJson());
      zNRecord.setListField(RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, list);
      storeOperator.write(RouterStore.ACCOUNT_METADATA_BLOB_IDS_PATH, zNRecord);
    } else {
      zNRecord.setMapField(LegacyMetadataStore.ACCOUNT_METADATA_MAP_KEY, accountMap);
      // Write account metadata into HelixPropertyStore.
      storeOperator.write(LegacyMetadataStore.FULL_ACCOUNT_METADATA_PATH, zNRecord);
    }
    if (shouldNotify) {
      notifier.publish(ACCOUNT_METADATA_CHANGE_TOPIC, FULL_ACCOUNT_METADATA_CHANGE_MESSAGE);
    }
  }

  /**
   * Writes a {@link ZNRecord} to {@link org.apache.helix.store.HelixPropertyStore}.
   * @param zNRecord The {@link ZNRecord} to write.
   * @throws Exception Any unexpected exception.
   */
  private void writeZNRecordToHelixPropertyStore(ZNRecord zNRecord, boolean shouldNotify) throws Exception {
    HelixStoreOperator storeOperator =
        new HelixStoreOperator(mockHelixAccountServiceFactory.getHelixStore(ZK_CONNECT_STRING, storeConfig));
    if (useNewZNodePath) {
      storeOperator.write(RouterStore.ACCOUNT_METADATA_BLOB_IDS_PATH, zNRecord);
    } else {
      storeOperator.write(LegacyMetadataStore.FULL_ACCOUNT_METADATA_PATH, zNRecord);
    }
    if (shouldNotify) {
      notifier.publish(ACCOUNT_METADATA_CHANGE_TOPIC, FULL_ACCOUNT_METADATA_CHANGE_MESSAGE);
    }
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
   * Add or update a {@link ZNRecord} to include a list field with the given key and value. If the supplied
   * {@link ZNRecord} is null, a new {@link ZNRecord} will be created.
   * @param oldRecord A {@link ZNRecord} to add or update its list field. {@code null} indicates to create a
   *                  new {@link ZNRecord}.
   * @param listKey The key for the list field.
   * @param listValue The value for the list field.
   * @return A {@link ZNRecord} including a list field with the given key and value.
   */
  private ZNRecord makeZNRecordWithListField(ZNRecord oldRecord, String listKey, List<String> listValue) {
    ZNRecord zNRecord = oldRecord == null ? new ZNRecord(String.valueOf(System.currentTimeMillis())) : oldRecord;
    if (listKey != null && listValue != null) {
      zNRecord.setListField(listKey, listValue);
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
   * Delete corresponding {@code ZooKeeper} nodes of a {@link HelixPropertyStore} if exist.
   * @throws Exception Any unexpected exception.
   */
  private void deleteStoreIfExists() throws Exception {
    HelixStoreOperator storeOperator =
        new HelixStoreOperator(mockHelixAccountServiceFactory.getHelixStore(ZK_CONNECT_STRING, storeConfig));
    // check if the store exists by checking if root path (e.g., "/") exists in the store.
    if (storeOperator.exist("/")) {
      storeOperator.delete("/");
    }
  }

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
   * Does several operations:
   * 1. Writes a {@link ZNRecord} to {@link HelixPropertyStore} without notifying listeners;
   * 2. Starts up a {@link HelixAccountService} that should fetch the {@link ZNRecord};
   * 3. Updates (creates) one more {@link Account} through the {@link HelixAccountService};
   * 4. Writes the same {@link ZNRecord} to {@link HelixPropertyStore} and publishes a message for {@link Account} update;
   *
   * If the {@link ZNRecord} is good, it should not affect updating operation.
   * @param zNRecord A {@link ZNRecord} to write to {@link HelixPropertyStore}.
   * @param isGoodZNRecord {code true} to indicate the {@link ZNRecord} is good, which should not affect updating
   *                       operation; {@code false} otherwise.
   * @throws Exception Any unexpected exception.
   */
  private void updateAndWriteZNRecord(ZNRecord zNRecord, boolean isGoodZNRecord) throws Exception {
    writeZNRecordToHelixPropertyStore(zNRecord, false);
    accountService = mockHelixAccountServiceFactory.getAccountService();
    assertEquals("Number of account is wrong", 0, accountService.getAllAccounts().size());
    updateAccountsAndAssertAccountExistence(Collections.singletonList(refAccount), isGoodZNRecord ? 1 : 0,
        isGoodZNRecord);
    writeZNRecordToHelixPropertyStore(zNRecord, true);
    if (isGoodZNRecord) {
      assertAccountInAccountService(refAccount, accountService);
    } else {
      assertEquals("Number of accounts is wrong.", 0, accountService.getAllAccounts().size());
    }
  }

  /**
   * Randomly generates a single {@link Account} and {@link Container}. It also generates a collection of reference
   * {@link Account}s and {@link Container}s that can be referred from {@link #idToRefAccountMap} and
   * {@link #idToRefContainerMap}.
   * @throws Exception Any unexpected exception.
   */
  private void generateReferenceAccountsAndContainers() throws Exception {
    // a set that records the account ids that have already been taken.
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

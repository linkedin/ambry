/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.MockHelixPropertyStore;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Unit test for {@link RouterStore}
 */
@RunWith(Parameterized.class)
public class RouterStoreTest {
  private final AccountServiceMetrics accountServiceMetrics;
  private final BackupFileManager backup;
  private final Path accountBackupDir;
  private final HelixAccountServiceConfig config;
  private final MockHelixPropertyStore<ZNRecord> helixStore;
  private final MockRouter router;
  private final boolean forBackfill;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Construct a unit test for {@link RouterStore}.
   * @param forBackfill True if RouterStore should be created for backfilling.
   * @throws IOException Any I/O error.
   */
  public RouterStoreTest(boolean forBackfill) throws IOException {
    this.forBackfill = forBackfill;
    accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    Properties properties = new Properties();
    properties.setProperty(HelixAccountServiceConfig.BACKUP_DIRECTORY_KEY, accountBackupDir.toString());
    properties.setProperty(HelixAccountServiceConfig.ZK_CLIENT_CONNECT_STRING_KEY, "1000");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    config = new HelixAccountServiceConfig(verifiableProperties);
    backup = new BackupFileManager(accountServiceMetrics, config);
    helixStore = new MockHelixPropertyStore<>();
    router = new MockRouter();
  }

  /**
   * cleanup after each test case.
   * @throws Exception Any unexpected exception.
   */
  @After
  public void cleanUp() throws Exception {
    HelixStoreOperator operator = new HelixStoreOperator(helixStore);
    if (operator.exist("/")) {
      operator.delete("/");
    }
    if (Files.exists(accountBackupDir)) {
      Utils.deleteFileOrDirectory(accountBackupDir.toFile());
    }
  }

  /**
   * Test basic operations of the {@link RouterStore}, update and fetch {@link Account} metadata.
   * @throws Exception Any unexpected Exception
   */
  @Test
  public void testUpdateAndFetch() throws Exception {
    RouterStore store =
        new RouterStore(accountServiceMetrics, backup, helixStore, new AtomicReference<>(router), forBackfill);
    Map<Short, Account> idToRefAccountMap = new HashMap<>();
    Map<Short, Map<Short, Container>> idtoRefContainerMap = new HashMap<>();
    Set<Short> accountIDSet = new HashSet<>();
    // generate an new account and test update and fetch on this account
    AccountTestUtils.generateRefAccounts(idToRefAccountMap, idtoRefContainerMap, accountIDSet, 1, 1);
    assertUpdateAndFetch(store, idToRefAccountMap, idToRefAccountMap, 1);

    // generate another new account and test update and fetch on this account
    Map<Short, Account> anotherIdToRefAccountMap = new HashMap<>();
    AccountTestUtils.generateRefAccounts(anotherIdToRefAccountMap, idtoRefContainerMap, accountIDSet, 1, 1);
    if (!forBackfill) {
      for (Map.Entry<Short, Account> entry : anotherIdToRefAccountMap.entrySet()) {
        idToRefAccountMap.put(entry.getKey(), entry.getValue());
      }
    } else {
      idToRefAccountMap = anotherIdToRefAccountMap;
    }
    // the version should be 2 now
    assertUpdateAndFetch(store, idToRefAccountMap, anotherIdToRefAccountMap, 2);
  }

  /**
   * call {@link RouterStore#updateAccounts(Collection)} to update {@link Account} metadata then call {@link RouterStore#fetchAccountMetadata()}
   * to fetch the {@link Account} metadata back and compare them. Also it fetches the {@link Account} metadata directly from ambry-server
   * and compare them.
   * @param store The {@link RouterStore}.
   * @param allAccounts The whole set of {@link Account} metadata after update.
   * @param accountsToUpdate The {@link Account} to update
   * @param version The expected version of blob id to fetch {@link Account} metadata from ambry-server.
   */
  private void assertUpdateAndFetch(RouterStore store, Map<Short, Account> allAccounts,
      Map<Short, Account> accountsToUpdate, int version) {
    // verify that updateAccount works again
    boolean succeeded = store.updateAccounts(accountsToUpdate.values());
    assertTrue("Update accounts failed at router store", succeeded);

    // verify that fetchAccountMetadata can fetch the accounts we just updated.
    Map<String, String> accountMap = store.fetchAccountMetadata();
    assertAccountsEqual(accountMap, allAccounts);

    // Verify that ZNRecord contains the right data.
    ZNRecord record = helixStore.get(RouterStore.ACCOUNT_METADATA_BLOB_IDS_PATH, null, AccessOption.PERSISTENT);
    assertNotNull("ZNRecord missing after update", record);
    List<String> blobIDAndVersions = record.getListField(RouterStore.ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
    assertNotNull("Blob ids are missing from ZNRecord", blobIDAndVersions);
    // version also equals to the number of blobs
    assertEquals("Number of blobs mismatch", version, blobIDAndVersions.size());

    RouterStore.BlobIDAndVersion blobIDAndVersion = null;
    for (String json : blobIDAndVersions) {
      RouterStore.BlobIDAndVersion current = RouterStore.BlobIDAndVersion.fromJson(json);
      if (current.getVersion() == version) {
        blobIDAndVersion = current;
        break;
      }
    }
    assertNotNull("Version " + version + " expected", blobIDAndVersion);
    accountMap = store.readAccountMetadataFromBlobID(blobIDAndVersion.getBlobID());
    assertAccountsEqual(accountMap, allAccounts);
  }

  /**
   * Compare the account map in json string with the account map from id to account.
   * @param accountMap The account map ini json string.
   * @param accounts The account map from id to account.
   */
  private void assertAccountsEqual(Map<String, String> accountMap, Map<Short, Account> accounts) {
    AccountInfoMap accountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
    Collection<Account> obtainedAccounts = accountInfoMap.getAccounts();

    assertEquals("Account size doesn't match", obtainedAccounts.size(), accounts.size());
    for (Account obtainedAccount : obtainedAccounts) {
      Account expectedAccount = accounts.get(obtainedAccount.getId());
      assertEquals("Account mismatched", expectedAccount, obtainedAccount);
    }
  }
}

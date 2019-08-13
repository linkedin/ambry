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

import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class LegacyMetadataStore implements AccountMetadataStore {
  static final String ACCOUNT_METADATA_MAP_KEY = "accountMetadata";
  static final String FULL_ACCOUNT_METADATA_PATH = "/account_metadata/full_data";
  private static final String ZN_RECORD_ID = "full_account_metadata";
  private static final Logger logger = LoggerFactory.getLogger(LegacyMetadataStore.class);

  private final HelixAccountService accountService;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final ReentrantLock lock = new ReentrantLock();
  private final HelixAccountServiceConfig config;

  LegacyMetadataStore(HelixAccountService accountService, HelixPropertyStore<ZNRecord> helixStore,
      HelixAccountServiceConfig config) throws IOException {
    this.helixStore = helixStore;
    this.accountService = accountService;
    this.config = config;
  }

  @Override
  public void fetchAndUpdateCache(boolean isCalledFromListener) {
    lock.lock();
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.trace("Start reading full account metadata set from path={}", FULL_ACCOUNT_METADATA_PATH);
      ZNRecord zNRecord = helixStore.get(FULL_ACCOUNT_METADATA_PATH, null, AccessOption.PERSISTENT);
      logger.trace("Fetched ZNRecord from path={}, took time={} ms", FULL_ACCOUNT_METADATA_PATH,
          System.currentTimeMillis() - startTimeMs);
      if (zNRecord == null) {
        logger.debug("The ZNRecord to read does not exist on path={}", FULL_ACCOUNT_METADATA_PATH);
      } else {
        Map<String, String> remoteAccountMap = zNRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
        if (remoteAccountMap == null) {
          logger.debug("ZNRecord={} to read on path={} does not have a simple map with key={}", zNRecord,
              FULL_ACCOUNT_METADATA_PATH, ACCOUNT_METADATA_MAP_KEY);
        } else {
          logger.trace("Start parsing remote account data.");
          AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountService, remoteAccountMap);
          AccountInfoMap oldAccountInfoMap = accountService.updateCache(newAccountInfoMap);
          accountService.notifyAccountUpdateConsumers(newAccountInfoMap, oldAccountInfoMap, isCalledFromListener);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    ZkUpdater zkUpdater = new ZkUpdater(accounts);
    boolean hasSucceeded = helixStore.update(FULL_ACCOUNT_METADATA_PATH, zkUpdater, AccessOption.PERSISTENT);
    zkUpdater.cleanup(hasSucceeded);
    return hasSucceeded;
  }

  /**
   * A {@link DataUpdater} to be used for updating {@link #FULL_ACCOUNT_METADATA_PATH} inside of
   * {@link #updateAccounts(Collection)}
   */
  private class ZkUpdater implements DataUpdater<ZNRecord> {
    private final Collection<Account> accountsToUpdate;
    private Map<String, String> potentialNewState;
    private final Pair<String, Path> backupPrefixAndPath;

    /**
     * @param accountsToUpdate The {@link Account}s to update.
     */
    ZkUpdater(Collection<Account> accountsToUpdate) {
      this.accountsToUpdate = accountsToUpdate;

      Pair<String, Path> backupPrefixAndPath = null;
      if (accountService.backupDirPath != null) {
        try {
          backupPrefixAndPath = accountService.reserveBackupFile();
        } catch (IOException e) {
          logger.error("Error reserving backup file", e);
        }
      }
      this.backupPrefixAndPath = backupPrefixAndPath;
    }

    @Override
    public ZNRecord update(ZNRecord recordFromZk) {
      ZNRecord recordToUpdate;
      if (recordFromZk == null) {
        logger.debug(
            "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
            FULL_ACCOUNT_METADATA_PATH);
        recordToUpdate = new ZNRecord(ZN_RECORD_ID);
      } else {
        recordToUpdate = recordFromZk;
      }
      Map<String, String> accountMap = recordToUpdate.getMapField(ACCOUNT_METADATA_MAP_KEY);
      if (accountMap == null) {
        logger.debug("AccountMap does not exist in ZNRecord when updating accounts. Creating a new accountMap");
        accountMap = new HashMap<>();
      }

      AccountInfoMap remoteAccountInfoMap;
      try {
        remoteAccountInfoMap = new AccountInfoMap(accountService, accountMap);
      } catch (JSONException e) {
        // Do not depend on Helix to log, so log the error message here.
        logger.error("Exception occurred when building AccountInfoMap from accountMap={}", accountMap, e);
        accountService.accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException("Exception occurred when building AccountInfoMap from accountMap", e);
      }
      accountService.maybePersistOldState(backupPrefixAndPath, accountMap);

      // if there is any conflict with the existing record, fail the update. Exception thrown in this updater will
      // be caught by Helix and helixStore#update will return false.
      if (accountService.hasConflictingAccount(accountsToUpdate, remoteAccountInfoMap)) {
        // Throw exception, so that helixStore can capture and terminate the update operation
        throw new IllegalArgumentException(
            "Updating accounts failed because one account to update conflicts with existing accounts");
      } else {
        for (Account account : accountsToUpdate) {
          try {
            accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
          } catch (Exception e) {
            String message = "Updating accounts failed because unexpected exception occurred when updating accountId="
                + account.getId() + " accountName=" + account.getName();
            // Do not depend on Helix to log, so log the error message here.
            logger.error(message, e);
            throw new IllegalStateException(message, e);
          }
        }
        recordToUpdate.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
        potentialNewState = accountMap;
        return recordToUpdate;
      }
    }

    void cleanup(boolean isUpdateSucceeded) {
      if (isUpdateSucceeded) {
        accountService.maybePersistNewState(backupPrefixAndPath, potentialNewState);
      }
    }
  }
}

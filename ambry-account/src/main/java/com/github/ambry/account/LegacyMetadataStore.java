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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A legacy implementation of {@link AccountMetadataStore}, where we store the full set of {@link Account} metadata as a map
 * in a single {@link ZNRecord} in {@link HelixPropertyStore} at {@link #FULL_ACCOUNT_METADATA_PATH}.
 *
 * When updating {@link Account} metadata, it reads the data back from {@link ZNRecord}, and patches the provided accounts
 * to the current ones and save it back to {@link HelixPropertyStore}. In order to keep a total order of all potential concurrent
 * updates to {@link HelixPropertyStore}, we take advantage of {@link HelixPropertyStore#update(String, DataUpdater, int)} function
 * that performs a atomic test-and-set.
 */
class LegacyMetadataStore extends AccountMetadataStore {
  static final String ACCOUNT_METADATA_MAP_KEY = "accountMetadata";
  static final String FULL_ACCOUNT_METADATA_PATH = "/account_metadata/full_data";
  private static final String ZN_RECORD_ID = "full_account_metadata";
  private static final Logger logger = LoggerFactory.getLogger(LegacyMetadataStore.class);

  /**
   * Constructor to create a {@link LegacyMetadataStore}.
   * @param accountServiceMetrics The metrics set to update metrics.
   * @param backupFileManager The {@link BackupFileManager} instance to manage backup files.
   * @param helixStore The {@link HelixPropertyStore} to fetch and update data.
   */
  LegacyMetadataStore(AccountServiceMetrics accountServiceMetrics, BackupFileManager backupFileManager,
      HelixPropertyStore<ZNRecord> helixStore) {
    super(accountServiceMetrics, backupFileManager, helixStore, FULL_ACCOUNT_METADATA_PATH);
  }

  @Override
  Map<String, String> fetchAccountMetadataFromZNRecord(ZNRecord record) {
    Map<String, String> result = record.getMapField(ACCOUNT_METADATA_MAP_KEY);
    if (result == null) {
      logger.info("ZNRecord={} to read on path={} does not have a simple map with key={}", record,
          FULL_ACCOUNT_METADATA_PATH, ACCOUNT_METADATA_MAP_KEY);
    }
    return result;
  }

  @Override
  ZKUpdater createNewZKUpdater(Collection<Account> accounts) {
    return new ZKUpdater(accounts);
  }

  /**
   * A {@link DataUpdater} to be used for updating {@link #FULL_ACCOUNT_METADATA_PATH} inside of
   * {@link #updateAccounts(Collection)}
   */
  private class ZKUpdater implements AccountMetadataStore.ZKUpdater {
    private final Collection<Account> accountsToUpdate;

    /**
     * @param accountsToUpdate The {@link Account}s to update.
     */
    ZKUpdater(Collection<Account> accountsToUpdate) {
      this.accountsToUpdate = accountsToUpdate;
    }

    @Override
    public ZNRecord update(ZNRecord recordFromZk) {
      ZNRecord recordToUpdate;
      if (recordFromZk == null) {
        logger.info(
            "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
            FULL_ACCOUNT_METADATA_PATH);
        recordToUpdate = new ZNRecord(ZN_RECORD_ID);
      } else {
        recordToUpdate = recordFromZk;
      }
      Map<String, String> accountMap = recordToUpdate.getMapField(ACCOUNT_METADATA_MAP_KEY);
      if (accountMap == null) {
        logger.info("AccountMap does not exist in ZNRecord when updating accounts. Creating a new accountMap");
        accountMap = new HashMap<>();
      }

      AccountInfoMap remoteAccountInfoMap;
      String errorMessage = null;
      try {
        remoteAccountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
      } catch (JSONException e) {
        // Do not depend on Helix to log, so log the error message here.
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        errorMessage = "Exception occurred when building AccountInfoMap from accountMap " + accountMap;
        logger.error(errorMessage, e);
        throw new IllegalStateException(errorMessage, e);
      }

      // if there is any conflict with the existing record, fail the update. Exception thrown in this updater will
      // be caught by Helix and helixStore#update will return false.
      if (remoteAccountInfoMap.hasConflictingAccount(accountsToUpdate)) {
        // Throw exception, so that helixStore can capture and terminate the update operation
        errorMessage = "Updating accounts failed because one account to update conflicts with existing accounts";
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      } else {
        for (Account account : accountsToUpdate) {
          try {
            accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
          } catch (Exception e) {
            errorMessage = "Updating accounts failed because unexpected exception occurred when updating accountId="
                + account.getId() + " accountName=" + account.getName();
            logger.error(errorMessage, e);
            throw new IllegalStateException(errorMessage, e);
          }
        }
        recordToUpdate.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
        return recordToUpdate;
      }
    }

    @Override
    public void afterUpdate(boolean isUpdateSucceeded) {
    }
  }
}

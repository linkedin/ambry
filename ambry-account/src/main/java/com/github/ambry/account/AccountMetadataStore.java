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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An AccountMetadataStore is a storage to keep and retrieve {@link Account} metadata. This is the base
 */
abstract class AccountMetadataStore {
  private static final Logger logger = LoggerFactory.getLogger(AccountMetadataStore.class);
  protected final ObjectMapper objectMapper = new ObjectMapper();

  protected final AccountServiceMetrics accountServiceMetrics;
  protected final BackupFileManager backupFileManager;
  protected final String znRecordPath;
  protected final HelixPropertyStore<ZNRecord> helixStore;

  /** Create a new {@link AccountMetadataStore} instance for the subclasses.
   * @param accountServiceMetrics The {@link AccountServiceMetrics}
   * @param backupFileManager The {@link BackupFileManager} to manage the backup files.
   * @param helixStore The {@link HelixPropertyStore} to retrieve and update the {@link ZNRecord}.
   * @param znRecordPath The {@link ZNRecord} path.
   */
  AccountMetadataStore(AccountServiceMetrics accountServiceMetrics, BackupFileManager backupFileManager,
      HelixPropertyStore<ZNRecord> helixStore, String znRecordPath) {
    this.accountServiceMetrics = accountServiceMetrics;
    this.backupFileManager = backupFileManager;
    this.helixStore = helixStore;
    this.znRecordPath = znRecordPath;
  }

  /**
   * ZKUpdater extends the {@link DataUpdater} with another method to perform some clean up logic after
   * an update.
   */
  interface ZKUpdater extends DataUpdater<ZNRecord> {

    /**
     *  Called after {@link HelixPropertyStore} update the {@code znRecordPath} with the {@link ZKUpdater}.
     * @param isUpdateSucceeded The result of the {@link HelixPropertyStore#update}.
     */
    void afterUpdate(boolean isUpdateSucceeded);
  }

  /**
   * Fetch the {@link Account} metadata from the given ZNRecord. It should return null when there is no {@link Account}
   * ever created before. Subclass can assume the {@link ZNRecord} passed in this function is not null;
   * @param record The {@link ZNRecord} fetched from {@code znRecordPath}.
   * @return {@link Account} metadata in a map.
   */
  abstract Map<String, String> fetchAccountMetadataFromZNRecord(ZNRecord record);

  /**
   * Create new {@link ZKUpdater} that will be used to update the accounts.
   * @param accounts The {@link Account} collection to update.
   * @return the Specific {@link ZKUpdater} to update accounts with {@link HelixPropertyStore}.
   */
  abstract ZKUpdater createNewZKUpdater(Collection<Account> accounts);

  /**
   * fetchAccountMetadata would fetch the latest full set of {@link Account} metadata from the store. It returns null
   * when there is no {@link Account} created.
   * @return A collection of {@link Account} metadata.
   */
  Collection<Account> fetchAccountMetadata() {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start reading ZNRecord from path={}", znRecordPath);
    Stat stat = new Stat();
    ZNRecord znRecord = helixStore.get(znRecordPath, stat, AccessOption.PERSISTENT);
    long helixFetchTime = System.currentTimeMillis() - startTimeMs;
    accountServiceMetrics.fetchRemoteAccountTimeInMs.update(helixFetchTime);
    logger.trace("Fetched ZNRecord from path={}, took time={} ms", znRecordPath, helixFetchTime);
    if (znRecord == null) {
      logger.info("The ZNRecord to read does not exist on path={}", znRecordPath);
      return null;
    }
    Map<String, String> newAccountMap = fetchAccountMetadataFromZNRecord(znRecord);
    Map<Short, Account> idToAccountMap = new HashMap<>();
    Map<String, Account> nameToAccountMap = new HashMap<>();
    for (Map.Entry<String, String> entry : newAccountMap.entrySet()) {
      String idKey = entry.getKey();
      if (idKey == null) {
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException("Invalid account record when reading accountMap because idKey=null");
      }
      String valueString = entry.getValue();
      Account account;
      try {
        account = objectMapper.readValue(valueString, Account.class);
      } catch (JsonProcessingException e) {
        logger.error("Failed to deserialize {} to an Account object", valueString, e);
        throw new RuntimeException(e);
      }

      if (account.getId() != Short.parseShort(idKey)) {
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException(
            "Invalid account record when reading accountMap because idKey and accountId do not match. idKey=" + idKey
                + " accountId=" + account.getId());
      }
      if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
        throw new IllegalStateException(
            "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
      }
      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
    if (newAccountMap != null) {
      backupFileManager.persistAccountMap(idToAccountMap.values(), stat.getVersion(), stat.getMtime() / 1000);
    }
    return idToAccountMap.values();
  }

  /**
   * updateAccounts updates the latest full set of {@link Account} metadata and save it in the storage.
   * @param accounts The {@link Account} collection to update. It will not be null or empty.
   * @return false when there is any error.
   */
  boolean updateAccounts(Collection<Account> accounts) {
    ZKUpdater zkUpdater = createNewZKUpdater(accounts);
    boolean hasSucceeded = helixStore.update(znRecordPath, zkUpdater, AccessOption.PERSISTENT);
    zkUpdater.afterUpdate(hasSucceeded);
    return hasSucceeded;
  }
}

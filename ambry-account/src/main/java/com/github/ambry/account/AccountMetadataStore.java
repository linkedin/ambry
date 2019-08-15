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
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AccountMetadataStore is a storage to keep and retrieve {@link Account} metadata. This is the base
 */
abstract class AccountMetadataStore {
  private static final Logger logger = LoggerFactory.getLogger(LegacyMetadataStore.class);

  protected final AccountServiceMetrics accountServiceMetrics;
  protected final LocalBackup backup;
  protected final String znRecordPath;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final ReentrantLock lock = new ReentrantLock();

  AccountMetadataStore(AccountServiceMetrics accountServiceMetrics, LocalBackup backup,
      HelixPropertyStore<ZNRecord> helixStore, String znRecordPath) {
    this.accountServiceMetrics = accountServiceMetrics;
    this.backup = backup;
    this.helixStore = helixStore;
    this.znRecordPath = znRecordPath;
  }

  /**
   * ZKUpdater extends the {@link DataUpdater} with another method to  permform some clean up logic after
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
   * @return {@link Account} metadata in a map.
   */
  Map<String, String> fetchAccountMetadata() {
    // when fetching account metadata, we need to fetch the list of blob ids that point to different versions of
    // account metadata as well the latest version of account metadata.
    lock.lock();
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.trace("Start reading account metadata blob ids list from path={}", znRecordPath);
      ZNRecord znRecord = helixStore.get(znRecordPath, null, AccessOption.PERSISTENT);
      logger.trace("Fetched ZNRecord from path={}, took time={} ms", znRecordPath, startTimeMs);
      if (znRecord == null) {
        logger.debug("The ZNRecord to read does not exist on path={}", znRecordPath);
        return null;
      }
      return fetchAccountMetadataFromZNRecord(znRecord);
    } finally {
      lock.unlock();
    }
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

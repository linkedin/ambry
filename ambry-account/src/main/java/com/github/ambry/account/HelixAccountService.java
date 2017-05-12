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

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *    An implementation of {@link AccountService} that employs {@link HelixPropertyStore} as its underlying storage,
 *    which connects to a ZooKeeper Server. It reads from the {@link HelixPropertyStore} where stores the actual
 *    account metadata, and writes account metadata updates to the {@link HelixPropertyStore}.
 * </p>
 *<p>
 *   All the account metadata are stored on a single {@link ZNRecord} in ZooKeeper, with its path be
 * {@code <rootPath>/<completeAccountMetadataPath>}. {@code <rootPath>} and {@code <completeAccountMetadataPath>}
 * are configured in {@link HelixPropertyStoreConfig}. The {@link ZNRecord} is structured in the form below:
 * {
 *   "accountMetadata" : "A_MAP_OF_ACCOUNT_METADATA"
 * }
 * where "A_SIMPLE_MAP_OF_ACCOUNT_METADATA" is a {@link Map} written into the {@link ZNRecord} as a simple map.
 * Each entry in the map is a pair of (accountId, accountMetadata).
 * </p>
 * <p>
 *   A {@link HelixAccountService} can listen to specific topics through a {@link Notifier}. After subscribing to
 *   topic "accountMetadataChange", every time when a "completeAccountMetadataChange" message is received, the
 *   {@link HelixAccountService} will poll all account metadata from {@code <completeAccountMetadataPath>}, and
 *   update local cache.
 * </p>
 */
public class HelixAccountService implements AccountService, TopicListener<String> {
  public static final String COMPLETE_ACCOUNT_METADATA_CHANGE_MESSAGE = "completeAccountMetadataChange";
  public static final String ACCOUNT_METADATA_CHANGE_TOPIC = "accountMetadataChange";
  public static final String ACCOUNT_METADATA_KEY = "accountMetadata";

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String rootPath;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final String completeAccountMetadataPath;
  private final Map<String, Account> accountNameToAccountMap = new ConcurrentHashMap<>();
  private final Map<Short, Account> accountIdToAccountMap = new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param storeConfig The config needed to start a {@link HelixPropertyStore}.
   * @param storeFactory The factory to generate a {@link HelixPropertyStore}.
   */
  public HelixAccountService(HelixPropertyStoreConfig storeConfig, HelixPropertyStoreFactory<ZNRecord> storeFactory) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("storeConfig cannot be null");
    }
    if (storeFactory == null) {
      throw new IllegalArgumentException("storeFactory cannot be null");
    }
    this.rootPath = storeConfig.rootPath;
    this.completeAccountMetadataPath = storeConfig.completeAccountMetadataPath;
    List<String> subscribedPaths = new ArrayList<>();
    subscribedPaths.add(rootPath + completeAccountMetadataPath);
    this.helixStore =
        storeFactory.getHelixPropertyStore(storeConfig.zkClientConnectString, storeConfig.zkClientSessionTimeoutMs,
            storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath, subscribedPaths);
    readFromHelixStoreAndUpdateLocalCache(completeAccountMetadataPath);
  }

  @Override
  public Account getAccountByName(String accountName) {
    if (accountName == null) {
      throw new IllegalArgumentException("accountName cannot be null.");
    }
    return accountNameToAccountMap.get(accountName);
  }

  @Override
  public Account getAccountById(short id) {
    return accountIdToAccountMap.get(id);
  }

  @Override
  public List<Account> getAllAccounts() {
    return Collections.unmodifiableList(new ArrayList<>(accountIdToAccountMap.values()));
  }

  @Override
  public boolean updateAccounts(List<Account> accounts) {
    if (accounts == null) {
      throw new IllegalArgumentException("accounts cannot be null");
    }
    return helixStore.update(completeAccountMetadataPath, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          currentData = new ZNRecord(String.valueOf(System.currentTimeMillis()));
        }
        Map<String, String> accountMap = currentData.getMapField(ACCOUNT_METADATA_KEY);
        if (accountMap == null) {
          accountMap = new HashMap<>();
        }
        for (Account account : accounts) {
          accountMap.put(String.valueOf(account.getId()), account.getMetadata().toString());
        }
        currentData.setMapField(ACCOUNT_METADATA_KEY, accountMap);
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  @Override
  public void processMessage(String topic, String message) {
    try {
      switch (message) {
        case COMPLETE_ACCOUNT_METADATA_CHANGE_MESSAGE:
          readFromHelixStoreAndUpdateLocalCache(completeAccountMetadataPath);
          break;

        default:
          throw new IllegalArgumentException("Could not understand message" + message);
      }
    } catch (Exception e) {
      logger.error("Failed to process message {} for topic {}.", message, topic, e);
    }
  }

  @Override
  public void close() {
    try {
      cleanLocalCache();
      helixStore.stop();
    } catch (Exception e) {
      logger.error("Exception during closing helix property store.", e);
    }
  }

  /**
   * Reads metadata from the specified path of {@link HelixPropertyStore}.
   * @param path The path to read.
   */
  private void readFromHelixStoreAndUpdateLocalCache(String path) {
    ZNRecord zNRecord = helixStore.get(path, null, AccessOption.PERSISTENT);
    if (zNRecord == null) {
      logger.debug("The ZNRecord to read does not exist on path {}", path);
      return;
    }
    Map<String, String> accountMap = zNRecord.getMapField(ACCOUNT_METADATA_KEY);
    if (accountMap == null) {
      logger.debug("The ZNRecord to read on path {} does not have a map for key {}", path, ACCOUNT_METADATA_KEY);
      return;
    }
    try {
      for (Map.Entry<String, String> entry : accountMap.entrySet()) {
        String key = entry.getKey();
        JSONObject accountObject = new JSONObject(entry.getValue());
        String id = accountObject.getString(Account.ACCOUNT_ID_KEY);
        if (entry.getKey() == null || entry.getValue() == null || !key.equals(id)) {
          logger.error("Invalid record. Account key: {}, account string: {}", entry.getKey(), entry.getValue());
          continue;
        }
        Account account = new Account(accountObject);
        accountIdToAccountMap.put(Short.valueOf(key), account);
        accountNameToAccountMap.put(account.getName(), account);
      }
    } catch (JSONException e) {
      logger.error("Failed to process account metadata read from helix store.", e);
    }
  }

  /**
   * Cleans local cache for accounts.
   */
  private void cleanLocalCache() {
    accountIdToAccountMap.clear();
    accountNameToAccountMap.clear();
  }
}

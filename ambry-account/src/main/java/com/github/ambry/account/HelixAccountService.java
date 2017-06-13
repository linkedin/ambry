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

import com.github.ambry.commons.HelixPropertyStoreFactory;
import com.github.ambry.commons.TopicListener;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   An implementation of {@link AccountService} that employs a {@link HelixPropertyStore} as its underlying storage.
 *   It is internally configured with the information where to fetch a full set of {@link Account} metadata from
 *   the {@link HelixPropertyStore}, and where in the {@link HelixPropertyStore} to update {@link Account} metadata.
 *   The full {@link Account} metadata will be cached locally, so serving an {@link Account} query will not incur
 *   any I/O operation, and the calls to a {@code HelixAccountService} is not blocking.
 * </p>
 * <p>
 *   When a {@link HelixAccountService} starts up, it will automatically fetch a full set of {@link Account} metadata
 *   and store them in the local cache. It also implements the {@link TopicListener} interface, and subscribes to a
 *   {@link #ACCOUNT_METADATA_CHANGE_TOPIC}. Each time when receiving a {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE},
 *   it will fetch the updated full {@link Account} metadata and refresh its local cache.
 * </p>
 * <p>
 *   The full set of the {@link Account} metadata are stored in a single {@link ZNRecord} in {@link HelixPropertyStore},
 *   with the store-relative path be {@link #FULL_ACCOUNT_METADATA_PATH}. The {@link ZNRecord} contains {@link Account}
 *   metadata in a simple map in the following form:
 * </p>
 * <pre>
 * {
 *  {@link #ACCOUNT_METADATA_MAP_KEY} : accountMap &#60String, String&#62
 * }
 * </pre>
 * <p>
 * where {@code accountMap} is a {@link Map} from accountId String to {@link Account} JSON string.
 * </p>
 * <p>
 *   Limited by {@link HelixPropertyStore}, the total size of {@link Account} data stored on the {@link ZNRecord} cannot
 *   exceed 1MB.
 * </p>
 */
public class HelixAccountService implements AccountService, TopicListener<String> {
  static final String FULL_ACCOUNT_METADATA_CHANGE_MESSAGE = "full_account_metadata_change";
  static final String ACCOUNT_METADATA_CHANGE_TOPIC = "account_metadata_change_topic";
  static final String ACCOUNT_METADATA_MAP_KEY = "accountMetadata";
  static final String FULL_ACCOUNT_METADATA_PATH = "/account_metadata/full_data";
  private static final String ZN_RECORD_ID = "full_account_metadata";
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String rootPath;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final Cache cache = new Cache();

  /**
   * Constructor. It fetches the full account metadata set and caches locally.
   * This call is blocking until it fetches all the {@link Account} metadata from {@link HelixPropertyStore}.
   *
   * @param storeConfig The config needed to start a {@link HelixPropertyStore}. Cannot be {@code null}.
   * @param storeFactory The factory to generate a {@link HelixPropertyStore}. Cannot be {@code null}.
   */
  public HelixAccountService(HelixPropertyStoreConfig storeConfig, HelixPropertyStoreFactory<ZNRecord> storeFactory) {
    if (storeConfig == null || storeFactory == null) {
      throw new IllegalArgumentException(
          "storeConfig=" + storeConfig + " and storeFactory=" + storeFactory + " cannot be null");
    }
    long startTimeMs = System.currentTimeMillis();
    logger.info("Starting HelixAccountService with HelixPropertyStore host={} rootPath={}",
        storeConfig.zkClientConnectString, storeConfig.rootPath);
    rootPath = storeConfig.rootPath;
    List<String> subscribedPaths = Collections.singletonList(rootPath + FULL_ACCOUNT_METADATA_PATH);
    helixStore = storeFactory.getHelixPropertyStore(storeConfig, subscribedPaths);
    readFullAccountAndUpdateCache(FULL_ACCOUNT_METADATA_PATH);
    logger.info("HelixAccountService started. {} accounts have been loaded, took time={}ms",
        cache.getUniqueMapPair().getIdToAccountMap().values().size(), System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public Account getAccountByName(String accountName) {
    if (accountName == null) {
      throw new IllegalArgumentException("accountName cannot be null.");
    }
    return cache.getAccountByName(accountName);
  }

  @Override
  public Account getAccountById(short id) {
    return cache.getAccountById(id);
  }

  @Override
  public Collection<Account> getAllAccounts() {
    return cache.getAllAccounts();
  }

  /**
   * {@inheritDoc}
   * <p>
   *   This call is blocking until it completes the operation of updating account metadata to {@link HelixPropertyStore}.
   *   Returning {@code true} indicates that the accounts have been successfully written into {@link HelixPropertyStore},
   *   {@code false} otherwise.
   * </p>
   */
  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    if (accounts == null) {
      throw new IllegalArgumentException("accounts cannot be null");
    }
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating to HelixPropertyStore with accounts={}", accounts);
    checkDuplicateAccountIdOrName(accounts);
    // make a pre check for conflict between the accounts to update and the accounts in cache. Will fail this update
    // the update operation if any conflict exists. There is a slight chance that the account to update conflicts with
    // the accounts in the local cache, but does not conflict with those in the helixPropertyStore. This will happen
    // if some accounts are updated but the local cache is not refreshed.
    Pair<Account, Account> conflictAccountPair = getConflictAccountWithCache(accounts);
    if (conflictAccountPair != null) {
      // @todo should we just log the case without throwing an exception? This case is indeed possible and may not be
      // @todo caused by a programming error.
      throw new IllegalArgumentException(
          "Account to update conflicts with the existing account. accountId=" + conflictAccountPair.getFirst().getId()
              + " accountName=" + conflictAccountPair.getFirst().getName() + " conflicts with existing accountId="
              + conflictAccountPair.getSecond().getId() + " accountName=" + conflictAccountPair.getSecond().getName());
    }

    boolean res = helixStore.update(FULL_ACCOUNT_METADATA_PATH, currentData -> {
      ZNRecord newRecord;
      if (currentData == null) {
        logger.debug(
            "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
            FULL_ACCOUNT_METADATA_PATH);
        newRecord = new ZNRecord(String.valueOf(ZN_RECORD_ID));
      } else {
        newRecord = new ZNRecord(currentData);
      }
      Map<String, String> accountMap = newRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
      if (accountMap == null) {
        logger.debug("AccountMap does not exist in ZNRecord when updating accounts. Creating a new accountMap");
        accountMap = new HashMap<>();
      }
      AccountMapPair accountMapPair = new AccountMapPair(accountMap);
      Pair<Account, Account> conflictPair = getConflictAccount(accounts, accountMapPair);
      // if there is any conflict with the existing record, or any exception occurs during building the new ZNRecord,
      // the current policy is not to make any update.
      if (conflictPair != null) {
        logger.error("AccountId={} accountName={} conflicts with remote accountId={} accountName={}. Stopping update "
                + "operation and no update will be made for accounts={}", conflictPair.getFirst().getId(),
            conflictPair.getFirst().getName(), conflictPair.getSecond().getId(), conflictPair.getSecond().getName(),
            accounts);
      } else {
        boolean noException = true;
        for (Account account : accounts) {
          try {
            accountMap.put(String.valueOf(account.getId()), account.toJson().toString());
          } catch (Exception e) {
            logger.error("Unexpected exception when updating accountId={} accountName={} to ZNRecord={}",
                account.getId(), account.getName(), newRecord, e);
            noException = false;
            break;
          }
        }
        if (noException) {
          newRecord.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
        }
      }
      return newRecord;
    }, AccessOption.PERSISTENT);
    logger.trace("Completes updating accounts, took time={}ms", System.currentTimeMillis() - startTimeMs);
    return res;
  }

  @Override
  public void onMessage(String topic, String message) {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start to process message={} for topic={}", message, topic);
    try {
      switch (message) {
        case FULL_ACCOUNT_METADATA_CHANGE_MESSAGE:
          readFullAccountAndUpdateCache(FULL_ACCOUNT_METADATA_PATH);
          break;

        default:
          throw new IllegalArgumentException("Could not understand message=" + message + " for topic=" + topic);
      }
    } catch (Exception e) {
      logger.error("Failed to process message={} for topic={}.", message, topic, e);
    }
    logger.trace("Complete processing message={} for topic={}, took time={}ms", message, topic,
        System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void close() {
    try {
      cache.close();
      helixStore.stop();
    } catch (Exception e) {
      logger.error("Exception during closing helix property store.", e);
    }
  }

  /**
   * Reads the full set of {@link Account} metadata from {@link HelixPropertyStore}, and update the local cache.
   *
   * @param pathToFullAccountMetadata The path to read the full set of {@link Account} metadata.
   */
  private void readFullAccountAndUpdateCache(String pathToFullAccountMetadata) {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start fetching full account metadata set from path={}", pathToFullAccountMetadata);
    ZNRecord zNRecord = helixStore.get(pathToFullAccountMetadata, null, AccessOption.PERSISTENT);
    if (zNRecord == null) {
      logger.debug("The ZNRecord to read does not exist on path={}, clearing local cache", pathToFullAccountMetadata);
      cache.update(new AccountMapPair());
      return;
    }
    Map<String, String> accountMap = zNRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
    if (accountMap == null) {
      logger.debug("The ZNRecord={} to read on path={} does not have a simple map with key={}, clearing local cache",
          zNRecord, pathToFullAccountMetadata, ACCOUNT_METADATA_MAP_KEY);
      cache.update(new AccountMapPair());
      return;
    }
    cache.update(new AccountMapPair(accountMap));
    logger.trace("Completed fetching full account metadata set from path={}, took time={}ms", pathToFullAccountMetadata,
        System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Checks if there are duplicate accountId or accountName in a given collection of {@link Account}s.
   *
   * @param accounts A collection of {@link Account}s to check.
   * @throws IllegalArgumentException If there are {@link Account}s having conflicting id or names.
   */
  private void checkDuplicateAccountIdOrName(Collection<Account> accounts) {
    if (accounts == null) {
      return;
    }
    Set<Short> idSet = new HashSet<>();
    Set<String> nameSet = new HashSet<>();
    for (Account account : accounts) {
      if (!idSet.add(account.getId()) || !nameSet.add(account.getName())) {
        throw new IllegalArgumentException(
            "Accounts to update have conflicting id or name. Conflicting accountId=" + account.getId() + " accountName="
                + account.getName() + " accounts=" + accounts);
      }
    }
  }

  /**
   * Checks a collection of {@link Account}s, and gets the one that conflicts with the {@link Account}s in the local
   * cache if any. Returning {@code null} means there is no {@link Account} conflicting with any {@link Account} in
   * the cache.
   *
   * @param accounts The collection of {@link Account}s to check.
   * @return A {@link Pair} of {@link Account}s, where the first element is the conflicting {@link Account} in the
   *         given collection, and the second element is the conflicting {@link Account} in the cache.
   */
  private Pair<Account, Account> getConflictAccountWithCache(Collection<Account> accounts) {
    return getConflictAccount(accounts, cache.getUniqueMapPair());
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountMapPair}, according to the JavaDoc specified for {@link #updateAccounts(Collection)}. The method
   * will return immediately once a conflicting {@link Account} is found. Returning {@code null} means there is no
   * {@link Account} in the given collection conflicting with any {@link Account} in the {@link AccountMapPair}.
   *
   * @param accounts A collection of {@link Account}s to check conflict.
   * @param accountMapPair A {@link AccountMapPair} that represents a group of {@link Account}s to check conflict.
   * @return A {@link Pair} of {@link Account}s, where the first item is the conflicting {@link Account} in accounts,
   *         and the second item is the {@link Account} in the {@link AccountMapPair}.
   */
  private Pair<Account, Account> getConflictAccount(Collection<Account> accounts, AccountMapPair accountMapPair) {
    Pair<Account, Account> conflictAccountPair = null;
    for (Account account : accounts) {
      Account remoteAccount = accountMapPair.getIdToAccountMap().get(account.getId());
      // check for case D in the javadoc of updateAccounts(Collection<Account> accounts)
      if (remoteAccount == null && accountMapPair.getNameToAccountMap().containsKey(account.getName())) {
        conflictAccountPair = new Pair(account, accountMapPair.getNameToAccountMap().get(account.getName()));
        break;
      }
      // check for case E in the javadoc of updateAccounts(Collection<Account> accounts)
      if (remoteAccount != null && !account.getName().equals(remoteAccount.getName())
          && accountMapPair.getNameToAccountMap().containsKey(account.getName())) {
        conflictAccountPair = new Pair(account, accountMapPair.getNameToAccountMap().get(account.getName()));
        break;
      }
    }
    return conflictAccountPair;
  }

  /**
   * <p>
   *   A cache to store {@link Account} information locally. It implements the {@link AccountService} interface,
   *   and can respond to the queries for {@link Account}. Internally it maintains a {@link AccountMapPair} that
   *   represents a group of {@link Account}s in the form of an id-to-account map and a name-to-account map.
   *   accountId and accountName in a {@link AccountMapPair} are one-to-one mapped.
   * </p>
   * <p>
   *   Operations on a {@link Cache} is thread-safe.
   * </p>
   */
  private class Cache implements AccountService {
    private final AtomicReference<AccountMapPair> uniqueMapPairRef = new AtomicReference<>();

    /**
     * Constructor that instantiates a cache with no data.
     */
    private Cache() {
      uniqueMapPairRef.set(new AccountMapPair());
    }

    @Override
    public Account getAccountById(short accountId) {
      return uniqueMapPairRef.get().getIdToAccountMap().get(accountId);
    }

    @Override
    public Account getAccountByName(String accountName) {
      return uniqueMapPairRef.get().getNameToAccountMap().get(accountName);
    }

    @Override
    public Collection<Account> getAllAccounts() {
      return Collections.unmodifiableCollection(uniqueMapPairRef.get().getIdToAccountMap().values());
    }

    @Override
    public boolean updateAccounts(Collection<Account> accounts) {
      throw new IllegalStateException("This method is not implemented, and is not expected to be called.");
    }

    @Override
    public void close() throws IOException {
      uniqueMapPairRef.set(new AccountMapPair());
    }

    /**
     * Atomically updates the cache with new id-to-account and name-to-account map. It assumes the {@link Account}s
     * in the two maps have id and name one-to-one mapped.
     * @param accountMapPair A {@link AccountMapPair} to replace the current one.
     */
    private void update(AccountMapPair accountMapPair) {
      uniqueMapPairRef.set(accountMapPair);
    }

    /**
     * Gets the {@link AccountMapPair} in this cache.
     * @return The {@link AccountMapPair} in this cache.
     */
    private AccountMapPair getUniqueMapPair() {
      return uniqueMapPairRef.get();
    }
  }

  /**
   * A representation of a pair of an id-to-account map and a name-to-account map. The ids and names in the
   * AccountMapPair are unique, and are one-to-one mapped. An AccountMapPair guarantees no conflict among the
   * {@link Account}s within it.
   */
  private class AccountMapPair {
    private final Map<String, Account> nameToAccountMap;
    private final Map<Short, Account> idToAccountMap;

    /**
     * Constructor.
     */
    private AccountMapPair() {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
    }

    /**
     * <p>
     *   A constructor that populates a group of {@link Account}s into a {@link Pair} of id-to-account map and
     *   name-to-account map. The group of {@link Account}s exists in the form of a string-to-string map, where
     *   the key is the string form of an {@link Account}'s id, and the value is the string form of the
     *   {@link Account}'s JSON string.
     * </p>
     * <p>
     *   It internally handles any conflicts among the {@link Account}s in the {@code accountMap}, and always removes
     *   a previously conflicting {@link Account} if any.
     * </p>
     * @param accountMap A map of {@link Account}s in the form of (idString, JSONString).
     */
    private AccountMapPair(Map<String, String> accountMap) {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
      for (Map.Entry<String, String> entry : accountMap.entrySet()) {
        String idKey = entry.getKey();
        String valueString = entry.getValue();
        Account account;
        try {
          JSONObject accountJson = new JSONObject(valueString);
          if (idKey == null || accountJson == null) {
            logger.error(
                "Invalid account record when reading accountMap in ZNRecord because either idKey={} or accountJson={}"
                    + "is null", idKey, accountJson);
            continue;
          }
          account = Account.fromJson(accountJson);
          if (account.getId() != Short.valueOf(idKey)) {
            logger.error(
                "Invalid account record when reading accountMap in ZNRecord because idKey and accountId do not match."
                    + "idKey={}, accountId={}", idKey, account.getId());
            continue;
          }
        } catch (Exception e) {
          logger.error("Invalid account record when reading accountMap in ZNRecord. key={}, valueString={}", idKey,
              valueString, e);
          continue;
        }

        Account conflictAccount = nameToAccountMap.get(account.getName());
        if (conflictAccount != null) {
          idToAccountMap.remove(conflictAccount.getId());
          nameToAccountMap.remove(conflictAccount.getName());
          logger.error(
              "Duplicate account id or name exists. id={} name={} conflicts with a previously added id={} name={}",
              account.getId(), account.getName(), conflictAccount.getId(), conflictAccount.getName());
        }
        idToAccountMap.put(account.getId(), account);
        nameToAccountMap.put(account.getName(), account);
      }
    }

    /**
     * Gets the internal nameToAccountMap.
     * @return The internal nameToAccountMap.
     */
    private Map<String, Account> getNameToAccountMap() {
      return nameToAccountMap;
    }

    /**
     * Gets the internal idToAccountMap.
     * @return The internal idToAccountMap.
     */
    private Map<Short, Account> getIdToAccountMap() {
      return idToAccountMap;
    }
  }
}
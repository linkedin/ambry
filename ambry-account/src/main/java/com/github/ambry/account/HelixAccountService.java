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

import com.github.ambry.commons.Notifier;
import com.github.ambry.commons.TopicListener;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   An implementation of {@link AccountService} that employs a {@link HelixPropertyStore} as its underlying storage.
 *   It is internally configured with the store-relative path be {@link #FULL_ACCOUNT_METADATA_PATH} to fetch a full
 *   set of {@link Account} metadata, and the same path to update {@link Account} metadata. The full {@link Account}
 *   metadata will be cached locally, so serving an {@link Account} query will not incur any calls to the remote
 *   {@code ZooKeeper} server, and the calls to a {@code HelixAccountService} is not blocking.
 * </p>
 * <p>
 *   When a {@link HelixAccountService} starts up, it will automatically fetch a full set of {@link Account} metadata
 *   and store them in its local cache. It also takes a {@link Notifier}, and subscribes to a {@link #ACCOUNT_METADATA_CHANGE_TOPIC}.
 *   Each time when receiving a {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE}, it will fetch the updated full
 *   {@link Account} metadata, and refresh its local cache. After every successful operation for updating a collection of
 *   {@link Account}s, it publishes a {@link #FULL_ACCOUNT_METADATA_CHANGE_MESSAGE} for {@link #ACCOUNT_METADATA_CHANGE_TOPIC}
 *   through the {@link Notifier}. If the remote {@link Account} metadata is corrupted or has conflict, {@link HelixAccountService}
 *   will not update its local cache.
 * </p>
 * <p>
 *   The full set of the {@link Account} metadata are stored in a single {@link ZNRecord}. The {@link ZNRecord}
 *   contains {@link Account} metadata in a simple map in the following form:
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
 *   Limited by {@link HelixPropertyStore}, the total size of {@link Account} data stored on a single {@link ZNRecord}
 *   cannot exceed 1MB.
 * </p>
 */
class HelixAccountService implements AccountService {
  static final String ACCOUNT_METADATA_CHANGE_TOPIC = "account_metadata_change_topic";
  static final String FULL_ACCOUNT_METADATA_CHANGE_MESSAGE = "full_account_metadata_change";
  static final String ACCOUNT_METADATA_MAP_KEY = "accountMetadata";
  static final String FULL_ACCOUNT_METADATA_PATH = "/account_metadata/full_data";
  private static final String ZN_RECORD_ID = "full_account_metadata";
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final AccountServiceMetrics accountServiceMetrics;
  private final TopicListener<String> listener;
  private final Notifier<String> notifier;
  private final AtomicReference<AccountInfoMap> accountInfoMapRef = new AtomicReference<>(new AccountInfoMap());
  private volatile boolean isOpen = false;
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Constructor. It fetches the full account metadata set and caches locally.
   * This call is blocking until it fetches all the {@link Account} metadata from {@link HelixPropertyStore}.
   * @param helixStore A {@link HelixPropertyStore} used by the {@code HelixAccountService}. Cannot be {@code null}.
   * @param accountServiceMetrics {@link AccountServiceMetrics} to report metrics. Cannot be {@code null}.
   * @param notifier A {@link Notifier} that will be used to publish message after updating {@link Account}s, and
   *                 listen to {@link Account} change messages. Cannot be {@code null}.
   */
  HelixAccountService(HelixPropertyStore<ZNRecord> helixStore, AccountServiceMetrics accountServiceMetrics,
      Notifier<String> notifier) {
    this.helixStore = Objects.requireNonNull(helixStore, "helixStore cannot be null");
    this.accountServiceMetrics = Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null");
    this.notifier = Objects.requireNonNull(notifier, "notifier cannot be null");
    listener = (topic, message) -> {
      checkOpen();
      logger.trace("Start to process message={} for topic={}", message, topic);
      try {
        switch (message) {
          case FULL_ACCOUNT_METADATA_CHANGE_MESSAGE:
            logger.trace("Start processing message={} for topic={}", message, topic);
            readFullAccountAndUpdateCache(FULL_ACCOUNT_METADATA_PATH);
            logger.trace("Completed processing message={} for topic={}", message, topic);
            break;
          default:
            accountServiceMetrics.unRecognizedMessageErrorCount.inc();
            throw new RuntimeException("Could not understand message=" + message + " for topic=" + topic);
        }
      } catch (Exception e) {
        logger.error("Exception occurred when processing message={} for topic={}.", message, topic, e);
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
      }
    };
    notifier.subscribe(ACCOUNT_METADATA_CHANGE_TOPIC, listener);
    try {
      readFullAccountAndUpdateCache(FULL_ACCOUNT_METADATA_PATH);
    } catch (Exception e) {
      logger.error("Exception occurred when fetching remote account data", e);
      accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
    }
    isOpen = true;
  }

  @Override
  public Account getAccountByName(String accountName) {
    checkOpen();
    Objects.requireNonNull(accountName, "accountName cannot be null.");
    return accountInfoMapRef.get().getAccountByName(accountName);
  }

  @Override
  public Account getAccountById(short id) {
    checkOpen();
    return accountInfoMapRef.get().getAccountById(id);
  }

  @Override
  public Collection<Account> getAllAccounts() {
    checkOpen();
    return accountInfoMapRef.get().getAccounts();
  }

  /**
   * {@inheritDoc}
   * <p>
   *   This call is blocking until it completes the operation of updating account metadata to {@link HelixPropertyStore}.
   * </p>
   * <p>
   *   There is a slight chance that an {@link Account} could be updated successfully, but its value was
   *   set based on an outdated {@link Account}. This can be fixed after {@code generationId} is introduced
   *   to {@link Account}.
   * </p>
   */
  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    checkOpen();
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (hasDuplicateAccountIdOrName(accounts)) {
      logger.debug("Duplicate account id or name exist in the accounts to update");
      accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating to HelixPropertyStore with accounts={}", accounts);
    boolean hasSucceeded = false;
    // make a pre check for conflict between the accounts to update and the accounts in the local cache. Will fail this
    // update operation for all the accounts if any conflict exists. There is a slight chance that the account to update
    // conflicts with the accounts in the local cache, but does not conflict with those in the helixPropertyStore. This
    // will happen if some accounts are updated but the local cache is not refreshed.
    if (hasConflictWithCache(accounts)) {
      logger.debug("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
      accountServiceMetrics.updateAccountErrorCount.inc();
    } else {
      hasSucceeded = helixStore.update(FULL_ACCOUNT_METADATA_PATH, currentData -> {
        ZNRecord newRecord;
        if (currentData == null) {
          logger.debug(
              "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
              FULL_ACCOUNT_METADATA_PATH);
          newRecord = new ZNRecord(ZN_RECORD_ID);
        } else {
          newRecord = currentData;
        }
        Map<String, String> accountMap = newRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
        if (accountMap == null) {
          logger.debug("AccountMap does not exist in ZNRecord when updating accounts. Creating a new accountMap");
          accountMap = new HashMap<>();
        }
        AccountInfoMap remoteAccountInfoMap;
        try {
          remoteAccountInfoMap = new AccountInfoMap(accountMap);
        } catch (JSONException e) {
          // Do not depend on Helix to log, so log the error message here.
          logger.error("Exception occurred when building AccountInfoMap from accountMap={}", accountMap, e);
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException("Exception occurred when building AccountInfoMap from accountMap", e);
        }
        // if there is any conflict with the existing record, fail the update. Exception thrown in this updater will
        // be caught by Helix and helixStore#update will return false.
        if (hasConflictingAccount(accounts, remoteAccountInfoMap)) {
          // Throw exception, so that helixStore can capture and terminate the update operation
          throw new IllegalArgumentException("Updating accounts failed because one account to update conflicts with existing accounts");
        } else {
          for (Account account : accounts) {
            try {
              accountMap.put(String.valueOf(account.getId()), account.toJson().toString());
            } catch (Exception e) {
              String message = "Updating accounts failed because unexpected exception occurred when updating accountId="
                  + account.getId() + " accountName=" + account.getName();
              // Do not depend on Helix to log, so log the error message here.
              logger.error(message, e);
              throw new IllegalStateException(message, e);
            }
          }
          newRecord.setMapField(ACCOUNT_METADATA_MAP_KEY, accountMap);
          return newRecord;
        }
      }, AccessOption.PERSISTENT);
    }
    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    if (hasSucceeded) {
      logger.trace("Completed updating accounts, took time={} ms", timeForUpdate);
      accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
      // notify account changes after successfully update.
      if (notifier.publish(ACCOUNT_METADATA_CHANGE_TOPIC, FULL_ACCOUNT_METADATA_CHANGE_MESSAGE)) {
        logger.trace("Successfully published message for account metadata change");
      } else {
        logger.error("Failed to send notification for account metadata change");
        accountServiceMetrics.notifyAccountDataChangeErrorCount.inc();
      }
    } else {
      logger.error("Failed updating accounts={}", accounts);
      accountServiceMetrics.updateAccountErrorCount.inc();
    }
    return hasSucceeded;
  }

  @Override
  public void close() {
    if (isOpen) {
      try {
        isOpen = false;
        helixStore.stop();
      } catch (Exception e) {
        logger.error("Exception occurred when closing HelixAccountService.", e);
      }
    }
  }

  /**
   * Reads the full set of {@link Account} metadata from {@link HelixPropertyStore}, and update the local cache.
   *
   * @param pathToFullAccountMetadata The path to read the full set of {@link Account} metadata.
   */
  private void readFullAccountAndUpdateCache(String pathToFullAccountMetadata) throws JSONException {
    lock.lock();
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.trace("Start reading full account metadata set from path={}", pathToFullAccountMetadata);
      ZNRecord zNRecord = helixStore.get(pathToFullAccountMetadata, null, AccessOption.PERSISTENT);
      logger.trace("Fetched ZNRecord from path={}, took time={} ms", pathToFullAccountMetadata,
          System.currentTimeMillis() - startTimeMs);
      if (zNRecord == null) {
        logger.debug("The ZNRecord to read does not exist on path={}", pathToFullAccountMetadata);
      } else {
        Map<String, String> remoteAccountMap = zNRecord.getMapField(ACCOUNT_METADATA_MAP_KEY);
        if (remoteAccountMap == null) {
          logger.debug("ZNRecord={} to read on path={} does not have a simple map with key={}", zNRecord,
              pathToFullAccountMetadata, ACCOUNT_METADATA_MAP_KEY);
        } else {
          logger.trace("Start parsing remote account data.");
          AccountInfoMap newAccountInfoMap = new AccountInfoMap(remoteAccountMap);
          accountInfoMapRef.set(newAccountInfoMap);
          logger.trace("Completed updating local cache with number={} accounts",
              newAccountInfoMap.getAccounts().size());
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Checks if there are duplicate accountId or accountName in the given collection of {@link Account}s.
   *
   * @param accounts A collection of {@link Account}s to check duplication.
   * @return {@code true} if there are duplicated accounts in id or name in the given collection of accounts,
   *                      {@code false} otherwise.
   */
  private boolean hasDuplicateAccountIdOrName(Collection<Account> accounts) {
    if (accounts == null) {
      return false;
    }
    Set<Short> idSet = new HashSet<>();
    Set<String> nameSet = new HashSet<>();
    boolean res = false;
    for (Account account : accounts) {
      if (!idSet.add(account.getId()) || !nameSet.add(account.getName())) {
        logger.debug("Accounts to update have conflicting id or name. Conflicting accountId={} accountName={}",
            account.getId(), account.getName());
        res = true;
        break;
      }
    }
    return res;
  }

  /**
   * Checks a collection of {@link Account}s if there is any conflict between the {@link Account}s to update and the
   * {@link Account}s in the local cache.
   *
   * @param accountsToSet The collection of {@link Account}s to check with the local cache.
   * @return {@code true} if there is at least one {@link Account} in {@code accounts} conflicts with the {@link Account}s
   *                      in the cache, {@code false} otherwise.
   */
  private boolean hasConflictWithCache(Collection<Account> accountsToSet) {
    return hasConflictingAccount(accountsToSet, accountInfoMapRef.get());
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountInfoMap}, according to the Javadoc of {@link AccountService}. Two {@link Account}s can be
   * conflicting with each other if they have different account Ids but the same account name.
   *
   * @param accountsToSet The collection of {@link Account}s to check conflict.
   * @param accountInfoMap A {@link AccountInfoMap} that represents a group of {@link Account}s to check conflict.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  private boolean hasConflictingAccount(Collection<Account> accountsToSet, AccountInfoMap accountInfoMap) {
    boolean res = false;
    for (Account account : accountsToSet) {
      Account accountInMap = accountInfoMap.getAccountById(account.getId());

      // @todo if accountInMap is not null, make sure
      // @todo account.generationId() = accountInfoMap.getById(account.getId()).generationId()+1
      // @todo the generationId will be added to Account class.

      // check for case D described in the javadoc of AccountService
      if (accountInMap == null && accountInfoMap.containsName(account.getName())) {
        Account conflictingAccount = accountInfoMap.getAccountByName(account.getName());
        logger.error(
            "Account to update with accountId={} accountName={} conflicts with an existing record with accountId={} accountName={}",
            account.getId(), account.getName(), conflictingAccount.getId(), conflictingAccount.getName());
        res = true;
        break;
      }

      // check for case E described in the javadoc of AccountService
      if (accountInMap != null && !account.getName().equals(accountInMap.getName()) && accountInfoMap.containsName(
          account.getName())) {
        Account conflictingAccount = accountInfoMap.getAccountByName(account.getName());
        logger.error(
            "Account to update with accountId={} accountName={} conflicts with an existing record with accountId={} accountName={}",
            account.getId(), account.getName(), conflictingAccount.getId(), conflictingAccount.getName());
        res = true;
        break;
      }
    }
    return res;
  }

  /**
   * Checks if the {@code HelixAccountService} is open.
   */
  private void checkOpen() {
    if (!isOpen) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }

  /**
   * <p>
   *   A helper class that represents a collection of {@link Account}s, where the ids and names of the
   *   {@link Account}s are one-to-one mapped. An {@code AccountInfoMap} guarantees no duplicated account
   *   id or name, nor conflict among the {@link Account}s within it.
   * </p>
   * <p>
   *   Based on the properties, a {@code AccountInfoMap} internally builds index for {@link Account}s using both
   *   {@link Account}'s id and name as key.
   * </p>
   */
  private class AccountInfoMap {
    private final Map<String, Account> nameToAccountMap;
    private final Map<Short, Account> idToAccountMap;

    /**
     * Constructor for an empty {@code AccountInfoMap}.
     */
    private AccountInfoMap() {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
    }

    /**
     * <p>
     *   Constructs an {@code AccountInfoMap} from a group of {@link Account}s. The {@link Account}s exists
     *   in the form of a string-to-string map, where the key is the string form of an {@link Account}'s id,
     *   and the value is the string form of the {@link Account}'s JSON string.
     * </p>
     * <p>
     *   The source {@link Account}s in the {@code accountMap} may duplicate account ids or names, or corrupted
     *   JSON strings that cannot be parsed as valid {@link JSONObject}. In such cases, construction of
     *   {@code AccountInfoMap} will fail.
     * </p>
     * @param accountMap A map of {@link Account}s in the form of (accountIdString, accountJSONString).
     * @throws JSONException If parsing account data in json fails.
     */
    private AccountInfoMap(Map<String, String> accountMap) throws JSONException {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
      for (Map.Entry<String, String> entry : accountMap.entrySet()) {
        String idKey = entry.getKey();
        String valueString = entry.getValue();
        Account account;
        JSONObject accountJson = new JSONObject(valueString);
        if (idKey == null) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException(
              "Invalid account record when reading accountMap in ZNRecord because idKey=null");
        }
        account = Account.fromJson(accountJson);
        if (account.getId() != Short.valueOf(idKey)) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException(
              "Invalid account record when reading accountMap in ZNRecord because idKey and accountId do not match. idKey="
                  + idKey + " accountId=" + account.getId());
        }
        if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
          throw new IllegalStateException(
              "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
        }
        idToAccountMap.put(account.getId(), account);
        nameToAccountMap.put(account.getName(), account);
      }
    }

    /**
     * Gets {@link Account} by its id.
     * @param id The id to get the {@link Account}.
     * @return The {@link Account} with the given id, or {@code null} if such an {@link Account} does not exist.
     */
    private Account getAccountById(Short id) {
      return idToAccountMap.get(id);
    }

    /**
     * Gets {@link Account} by its name.
     * @param name The id to get the {@link Account}.
     * @return The {@link Account} with the given name, or {@code null} if such an {@link Account} does not exist.
     */
    private Account getAccountByName(String name) {
      return nameToAccountMap.get(name);
    }

    /**
     * Checks if there is an {@link Account} with the given id.
     * @param id The {@link Account} id to check.
     * @return {@code true} if such an {@link Account} exists, {@code false} otherwise.
     */
    private boolean containsId(Short id) {
      return idToAccountMap.containsKey(id);
    }

    /**
     * Checks if there is an {@link Account} with the given name.
     * @param name The {@link Account} name to check.
     * @return {@code true} if such an {@link Account} exists, {@code false} otherwise.
     */
    private boolean containsName(String name) {
      return nameToAccountMap.containsKey(name);
    }

    /**
     * Gets all the {@link Account}s in this {@code AccountInfoMap} in a {@link Collection}.
     * @return A {@link Collection} of all the {@link Account}s in this map.
     */
    private Collection<Account> getAccounts() {
      return Collections.unmodifiableCollection(idToAccountMap.values());
    }
  }
}

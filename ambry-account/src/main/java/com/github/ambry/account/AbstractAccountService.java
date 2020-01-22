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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;


/**
 * Class which provides a basic implementation an {@link AccountService}. The handling of accounts is mostly the same
 * for all the implementations. The only thing the implementations (usually) differ in is what the source is of the
 * accounts (Zookeeper / Helix, local JSON file, etcd, Consul, etc).
 */
abstract class AbstractAccountService implements AccountService {

  private static final Logger logger = LoggerFactory.getLogger(AbstractAccountService.class);

  protected final AtomicReference<AccountInfoMap> accountInfoMapRef;
  protected final ReentrantLock lock = new ReentrantLock();
  protected final CopyOnWriteArraySet<Consumer<Collection<Account>>> accountUpdateConsumers =
      new CopyOnWriteArraySet<>();
  protected final AccountServiceMetrics accountServiceMetrics;

  public AbstractAccountService(AccountServiceMetrics accountServiceMetrics) {
    this.accountServiceMetrics = accountServiceMetrics;

    this.accountInfoMapRef = new AtomicReference<>(new AccountInfoMap(accountServiceMetrics));
  }

  /**
   * Ensures the account service is ready to process requests. Throws an unchecked exception (for example
   * {@link IllegalStateException}) if the account service is not ready to process requests.
   */
  abstract protected void checkOpen();

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

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to subscribe cannot be null");
    return accountUpdateConsumers.add(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to unsubscribe cannot be null");
    return accountUpdateConsumers.remove(accountUpdateConsumer);
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountInfoMap}, according to the Javadoc of {@link AccountService}. Two {@link Account}s can be
   * conflicting with each other if they have different account Ids but the same account name.
   *
   * @param accountsToSet The collection of {@link Account}s to check conflict.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  boolean hasConflictingAccount(Collection<Account> accountsToSet) {
    for (Account account : accountsToSet) {
      // if the account already exists, check that the snapshot version matches the expected value.
      Account accountInMap = getAccountById(account.getId());
      if (accountInMap != null && account.getSnapshotVersion() != accountInMap.getSnapshotVersion()) {
        logger.error(
                "Account to update (accountId={} accountName={}) has an unexpected snapshot version in zk (expected={}, encountered={})",
                account.getId(), account.getName(), account.getSnapshotVersion(), accountInMap.getSnapshotVersion());
        return true;
      }
      // check that there are no other accounts that conflict with the name of the account to update
      // (case D and E from the javadoc)
      Account potentialConflict = getAccountByName(account.getName());
      if (potentialConflict != null && potentialConflict.getId() != account.getId()) {
        logger.error(
                "Account to update (accountId={} accountName={}) conflicts with an existing record (accountId={} accountName={})",
                account.getId(), account.getName(), potentialConflict.getId(), potentialConflict.getName());
        return true;
      }
    }
    return false;
  }

  /**
   * Logs and notifies account update {@link Consumer}s about any new account changes/creations.
   * @param newAccountInfoMap the new {@link AccountInfoMap} that has been set.
   * @param oldAccountInfoMap the {@link AccountInfoMap} that was cached before this change.
   * @param isCalledFromListener {@code true} if the caller is the account update listener, {@@code false} otherwise.
   */
  protected void notifyAccountUpdateConsumers(AccountInfoMap newAccountInfoMap, AccountInfoMap oldAccountInfoMap,
                                            boolean isCalledFromListener) {
    Map<Short, Account> idToUpdatedAccounts = new HashMap<>();
    for (Account newAccount : newAccountInfoMap.getAccounts()) {
      if (!newAccount.equals(oldAccountInfoMap.getAccountById(newAccount.getId()))) {
        idToUpdatedAccounts.put(newAccount.getId(), newAccount);
      }
    }
    if (idToUpdatedAccounts.size() > 0) {
      logger.info("Received updates for {} accounts. Received from listener={}. Account IDs={}",
              idToUpdatedAccounts.size(), isCalledFromListener, idToUpdatedAccounts.keySet());
      // @todo In long run, this metric is not necessary.
      if (isCalledFromListener) {
        accountServiceMetrics.accountUpdatesCapturedByScheduledUpdaterCount.inc();
      }
      Collection<Account> updatedAccounts = Collections.unmodifiableCollection(idToUpdatedAccounts.values());
      for (Consumer<Collection<Account>> accountUpdateConsumer : accountUpdateConsumers) {
        long startTime = System.currentTimeMillis();
        try {
          accountUpdateConsumer.accept(updatedAccounts);
          long consumerExecutionTimeInMs = System.currentTimeMillis() - startTime;
          logger.trace("Consumer={} has been notified for account change, took {} ms", accountUpdateConsumer,
                  consumerExecutionTimeInMs);
          accountServiceMetrics.accountUpdateConsumerTimeInMs.update(consumerExecutionTimeInMs);
        } catch (Exception e) {
          logger.error("Exception occurred when notifying accountUpdateConsumer={}", accountUpdateConsumer, e);
        }
      }
    } else {
      logger.debug("HelixAccountService is updated with 0 updated account");
    }
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
class AccountInfoMap {
  private final Map<String, Account> nameToAccountMap;
  private final Map<Short, Account> idToAccountMap;
  private final static Logger logger = LoggerFactory.getLogger(AccountInfoMap.class);

  /**
   * Constructor for an empty {@code AccountInfoMap}.
   */
  AccountInfoMap(AccountServiceMetrics accountServiceMetrics) {
    this(accountServiceMetrics, new HashMap<>());
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
  AccountInfoMap(AccountServiceMetrics accountServiceMetrics, Map<String, String> accountMap) throws JSONException {
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
   * <p>
   *   Constructs an AccountInfoMap instance from one single JSON file which contains multiple accounts.
   * </p>
   * <p>
   *   This constructor is primarily useful if the source of the accounts is a flat storage for example a local
   *   JSON file.
   * </p>
   *
   * @param accountsJsonString JSON data containing an array of all accounts.
   * @throws JSONException If parsing account data in json fails.
   */
  protected AccountInfoMap(String accountsJsonString) throws JSONException {
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();

    JSONArray accountArray = new JSONArray(accountsJsonString);

    for (int i = 0; i < accountArray.length(); i++) {
      JSONObject accountJson = accountArray.getJSONObject(i);
      Account account = Account.fromJson(accountJson);

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
  Account getAccountById(Short id) {
    return idToAccountMap.get(id);
  }

  /**
   * Gets {@link Account} by its name.
   * @param name The id to get the {@link Account}.
   * @return The {@link Account} with the given name, or {@code null} if such an {@link Account} does not exist.
   */
  Account getAccountByName(String name) {
    return nameToAccountMap.get(name);
  }

  /**
   * Gets all the {@link Account}s in this {@code AccountInfoMap} in a {@link Collection}.
   * @return A {@link Collection} of all the {@link Account}s in this map.
   */
  Collection<Account> getAccounts() {
    return Collections.unmodifiableCollection(idToAccountMap.values());
  }

  /**
   * Return true if there is no accounts in this info map.
   * @return True when there is no accounts.
   */
  boolean isEmpty() {
    return idToAccountMap.isEmpty();
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountInfoMap}, according to the Javadoc of {@link AccountService}. Two {@link Account}s can be
   * conflicting with each other if they have different account Ids but the same account name.
   *
   * @param accountsToSet The collection of {@link Account}s to check conflict.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  boolean hasConflictingAccount(Collection<Account> accountsToSet) {
    for (Account account : accountsToSet) {
      // if the account already exists, check that the snapshot version matches the expected value.
      Account accountInMap = getAccountById(account.getId());
      if (accountInMap != null && account.getSnapshotVersion() != accountInMap.getSnapshotVersion()) {
        logger.error(
                "Account to update (accountId={} accountName={}) has an unexpected snapshot version in zk (expected={}, encountered={})",
                account.getId(), account.getName(), account.getSnapshotVersion(), accountInMap.getSnapshotVersion());
        return true;
      }
      // check that there are no other accounts that conflict with the name of the account to update
      // (case D and E from the javadoc)
      Account potentialConflict = getAccountByName(account.getName());
      if (potentialConflict != null && potentialConflict.getId() != account.getId()) {
        logger.error(
                "Account to update (accountId={} accountName={}) conflicts with an existing record (accountId={} accountName={})",
                account.getId(), account.getName(), potentialConflict.getId(), potentialConflict.getName());
        return true;
      }
    }
    return false;
  }
}

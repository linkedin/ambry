/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   A class that represents a collection of {@link Account}s, where the ids and names of the
 *   {@link Account}s are one-to-one mapped. An {@code AccountInfoMap} guarantees no duplicated account
 *   id or name, nor conflict among the {@link Account}s within it.
 * </p>
 * <p>
 *   Based on the properties, a {@code AccountInfoMap} internally builds index for {@link Account}s using both
 *   {@link Account}'s id and name as key.
 * </p>
 */
class AccountInfoMap {
  private final static Logger logger = LoggerFactory.getLogger(AccountInfoMap.class);
  private final Map<String, Account> nameToAccountMap;
  private final Map<Short, Account> idToAccountMap;
  // used to track last modified time of the accounts and containers in this cache
  private long lastModifiedTime = 0;

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
        throw new IllegalStateException("Invalid account record when reading accountMap because idKey=null");
      }
      account = Account.fromJson(accountJson);
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
   * @param ignoreVersion {@code true} if mismatch in account version can be ignored.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  boolean hasConflictingAccount(Collection<Account> accountsToSet, boolean ignoreVersion) {
    for (Account account : accountsToSet) {
      // if the account already exists, check that the snapshot version matches the expected value.
      Account accountInMap = getAccountById(account.getId());
      if (!ignoreVersion && accountInMap != null && account.getSnapshotVersion() != accountInMap.getSnapshotVersion()) {
        logger.error(
            "Account to update (accountId={} accountName={}) has an unexpected snapshot version in store (expected={}, encountered={})",
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
   * Checks if there are any name/ID conflicts between {@link Container}s being updated with existing {@link Container}s
   * under its parent {@link Account}. Two {@link Container}s can be conflicting with each other if they have
   * different {@link Container} Ids but the same {@link Container} name.
   * @param containersToSet {@link Container}s to check for conflict
   * @param parentAccountId parent {@link Account} id of the {@link Container}.
   * @param ignoreVersion {@code true} if mismatch in account version can be ignored.
   * @return true if there is any container under given parent account with same name but different id.
   */
  boolean hasConflictingContainer(Collection<Container> containersToSet, short parentAccountId, boolean ignoreVersion) {
    for (Container container : containersToSet) {
      // if the container already exists, check that the snapshot version matches the expected value.
      Container containerInMap = getContainerByNameForAccount(container.getParentAccountId(), container.getName());
      if (!ignoreVersion && containerInMap != null
          && container.getSnapshotVersion() != containerInMap.getSnapshotVersion()) {
        logger.error(
            "Container to update in AccountId {} (containerId={} containerName={}) has an unexpected snapshot version in store (expected={}, encountered={})",
            parentAccountId, container.getId(), container.getName(), container.getSnapshotVersion(),
            containerInMap.getSnapshotVersion());
        return true;
      }

      // check that there are no other containers that conflict with the name of the container to update
      if (containerInMap != null && containerInMap.getId() != (container.getId())) {
        logger.error(
            "Container to update in AccountId {} (containerId={} containerName={}) conflicts with an existing record (containerId={} containerName={})",
            parentAccountId, container.getId(), container.getName(), containerInMap.getId(), containerInMap.getName());
        return true;
      }
    }
    return false;
  }

  /**
   * Adds or updates the {@code AccountInfoMap} with the input {@link Collection} of {@link Account}s.
   * @param accounts collection of {@link Account}s to be added.
   */
  void addOrUpdateAccounts(Collection<Account> accounts) {
    for (Account account : accounts) {
      Account accountToUpdate = idToAccountMap.get(account.getId());
      if (accountToUpdate == null) {
        accountToUpdate = account;
      } else {
        // Remove name to Account mapping for existing account as name might have been updated.
        nameToAccountMap.remove(accountToUpdate.getName());
        AccountBuilder accountBuilder = new AccountBuilder(accountToUpdate).name(account.getName())
            .status(account.getStatus())
            .snapshotVersion(account.getSnapshotVersion())
            .lastModifiedTime(account.getLastModifiedTime());
        account.getAllContainers().forEach(accountBuilder::addOrUpdateContainer);
        accountToUpdate = accountBuilder.build();
      }
      idToAccountMap.put(accountToUpdate.getId(), accountToUpdate);
      nameToAccountMap.put(accountToUpdate.getName(), accountToUpdate);
    }
  }

  /**
   * Adds or updates the {@code AccountInfoMap} with the input {@link Collection} of {@link Container}s.
   * @param containers collection of {@link Container}s to be added.
   */
  void addOrUpdateContainers(Collection<Container> containers) {
    for (Container container : containers) {
      addOrUpdateContainer(container.getParentAccountId(), container);
    }
  }

  /**
   * Adds or updates a {@link Container} to a parent {@link Account}.
   * @param accountId The id of the parent {@link Account} for this {@link Container}.
   * @param container {@link Container} to be added.
   * @throws IllegalArgumentException if {@link Account} with provided id doesn't exist.
   */
  void addOrUpdateContainer(short accountId, Container container) {
    Account parentAccount = idToAccountMap.get(accountId);
    if (parentAccount == null) {
      throw new IllegalArgumentException("Account with ID " + accountId + "doesn't exist");
    }
    AccountBuilder accountBuilder = new AccountBuilder(parentAccount).addOrUpdateContainer(container);
    parentAccount = accountBuilder.build();
    idToAccountMap.put(parentAccount.getId(), parentAccount);
    nameToAccountMap.put(parentAccount.getName(), parentAccount);
  }

  /**
   * Gets {@link Container} by its Parent Account id and id.
   * @param accountId The id of the parent {@link Account} for this {@link Container}.
   * @param id The id to get the {@link Container}.
   * @return The {@link Container} with the given id within the parent Account Id, or {@code null} if
   * such a parent {@link Account} or {@link Container} does not exist.
   */
  Container getContainerByIdForAccount(Short accountId, short id) {
    Account parentAccount = idToAccountMap.get(accountId);
    return parentAccount == null ? null : parentAccount.getContainerById(id);
  }

  /**
   * Gets {@link Container} by its name and Parent Account id.
   * @param accountId The id of the parent {@link Account} for this {@link Container}.
   * @param name The name of the {@link Container} to get.
   * @return The {@link Container} with the given name within the parent Account Id, or {@code null} if
   * such a parent {@link Account} or {@link Container} does not exist.
   */
  Container getContainerByNameForAccount(Short accountId, String name) {
    Account parentAccount = idToAccountMap.get(accountId);
    return parentAccount == null ? null : parentAccount.getContainerByName(name);
  }

  /**
   * Gets the last modified time of {@link Account}s and {@link Container}s in the cache
   * @return last modified time.
   */
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Refreshes the lastModifiedTime of {@link Account}s and {@link Container}s in the cache. This is expected to be
   * called after account metadata is fetched from db periodically.
   */
  public void refreshLastModifiedTime() {
    for (Account account : idToAccountMap.values()) {
      lastModifiedTime = Math.max(lastModifiedTime, account.getLastModifiedTime());
      for (Container container : account.getAllContainers()) {
        lastModifiedTime = Math.max(lastModifiedTime, container.getLastModifiedTime());
      }
    }
  }
}

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

import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.AccountUtils.*;
import static com.github.ambry.utils.Utils.*;


/**
 * An implementation of {@link AccountService} that employs MySql database as its underlying storage.
 */
public class MySqlAccountService implements AccountService {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountService.class);
  static final String MYSQL_ACCOUNT_UPDATER_PREFIX = "mysql-account-updater";
  private final AccountServiceMetrics accountServiceMetrics;
  private final MySqlAccountServiceConfig config;
  // in-memory cache for storing account and container metadata
  private final AccountInfoMap accountInfoMap;
  private final ReadWriteLock infoMapLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService scheduler;
  private volatile MySqlAccountStore mySqlAccountStore;

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlAccountServiceConfig config,
      MySqlAccountStore mySqlAccountStore) {
    this.accountServiceMetrics = accountServiceMetrics;
    this.config = config;
    this.mySqlAccountStore = mySqlAccountStore;
    this.accountInfoMap = new AccountInfoMap(accountServiceMetrics);
    this.scheduler =
        config.updaterPollingIntervalSeconds > 0 ? Utils.newScheduler(1, MYSQL_ACCOUNT_UPDATER_PREFIX, false) : null;
    // TODO: create backup manager to manage local back up copies of Account and container metadata and lastModifiedTime
    initialFetchAndSchedule();
    // TODO: Subscribe to notifications from ZK
  }

  /**
   * Creates MySql Account store which establishes connection to database
   * @throws SQLException
   */
  private void createMySqlAccountStore() throws SQLException {
    if (mySqlAccountStore == null) {
      mySqlAccountStore = new MySqlAccountStore(config);
    }
  }

  /**
   * Initialize in-memory cache by fetching all the {@link Account}s and {@link Container}s metadata records.
   * It consists of 2 steps:
   * 1. Check local disk for back up copy and load metadata and last modified time of Accounts/Containers into cache.
   * 2. Fetch added/modified accounts and containers from mysql database since the last modified time (found in step 1)
   *    and load into cache.
   */
  private void initialFetchAndSchedule() {

    // TODO: Check local disk for back up copy and load metadata and last modified time into cache.

    // Fetch added/modified accounts and containers from mysql db since LMT and update cache. Also, schedule to
    // execute the logic periodically.
    fetchAndUpdateCache();
    if (scheduler != null) {
      scheduler.scheduleAtFixedRate(this::fetchAndUpdateCache, config.updaterPollingIntervalSeconds,
          config.updaterPollingIntervalSeconds, TimeUnit.SECONDS);
      logger.info("Background account updater will fetch accounts from mysql db at intervals of {} seconds",
          config.updaterPollingIntervalSeconds);
    }
  }

  /**
   * Fetches all the accounts and containers that have been created or modified since the last sync time and loads into
   * cache.
   */
  private void fetchAndUpdateCache() {
    try {
      // Retry connection to mysql if we couldn't set up previously
      createMySqlAccountStore();

      // get the last modified/sync time of accounts and containers from cache
      long lastModifiedTime = accountInfoMap.getLastModifiedTime();

      // Fetch all added/modified accounts and containers from MySql database since LMT
      Collection<Account> accounts = mySqlAccountStore.getNewAccounts(lastModifiedTime);
      Collection<Container> containers = mySqlAccountStore.getNewContainers(lastModifiedTime);

      // Update cache with fetched accounts and containers
      infoMapLock.writeLock().lock();
      try {
        accountInfoMap.updateAccounts(accounts);
        accountInfoMap.updateContainers(containers);
      } finally {
        infoMapLock.writeLock().unlock();
      }

      // TODO: Find the max LMT in the fetched accounts and containers and update the cache

    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMap.getAccountById(accountId);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMap.getAccountByName(accountName);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  //TODO: Revisit this interface method to see if we can throw exception instead of returning boolean so that caller can
  // distinguish between bad input and system error.
  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      logger.debug("Empty account collection to update.");
      return false;
    }

    if (mySqlAccountStore == null) {
      logger.warn("MySql Account DB store is not accessible");
      return false;
    }

    if (config.updateDisabled) {
      logger.info("Updates have been disabled");
      return false;
    }

    if (hasDuplicateAccountIdOrName(accounts)) {
      logger.error("Duplicate account id or name exist in the accounts to update");
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // Check for name/id/version conflicts between the accounts and containers being updated with those in local cache.
    // There is a slight chance that we have conflicts local cache, but not with MySql database. This will happen if
    // but the local cache is not yet refreshed with latest account info.
    infoMapLock.readLock().lock();
    try {
      if (accountInfoMap.hasConflictingAccount(accounts)) {
        logger.error("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
        //accountServiceMetrics.updateAccountErrorCount.inc();
        return false;
      }
      for (Account account : accounts) {
        if (accountInfoMap.hasConflictingContainer(account.getAllContainers(), account.getId())) {
          logger.error(
              "Containers={} under Account={} conflict with the containers in local cache. Cancel the update operation.",
              account.getAllContainers(), account.getId());
          //accountServiceMetrics.updateAccountErrorCount.inc();
          return false;
        }
      }
    } finally {
      infoMapLock.readLock().unlock();
    }

    // Update database with updated accounts
    try {
      updateAccountsWithMySqlStore(accounts);
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in MySql DB", accounts, e);
      // record failure, parse exception to figure out what we did wrong (eg. id or name collision). If it is id collision,
      // retry with incrementing ID (Account ID generation logic is currently in nuage-ambry, we might need to move it here)
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // Update in-memory cache with updated accounts
    infoMapLock.writeLock().lock();
    try {
      accountInfoMap.updateAccounts(accounts);
    } finally {
      infoMapLock.writeLock().unlock();
    }

    // TODO: persist updated accounts and max timestamp to local back up file.
    // TODO: can notify account updates to other nodes via ZK .

    return true;
  }

  @Override
  public Collection<Account> getAllAccounts() {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMap.getAccounts();
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return false;
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return false;
  }

  @Override
  public void selectInactiveContainersAndMarkInZK(StatsSnapshot statsSnapshot) {
    // TODO: Work with Sophie to implement this method in MySqlAccountService
    throw new UnsupportedOperationException("This method is not supported");
  }

  @Override
  public void close() throws IOException {
    if (scheduler != null) {
      shutDownExecutorService(scheduler, config.updaterShutDownTimeoutSeconds, TimeUnit.SECONDS);
    }
  }

  /**
   * Updates MySql DB with added or modified {@link Account}s
   * @param accounts collection of {@link Account}s
   * @throws SQLException
   */
  private void updateAccountsWithMySqlStore(Collection<Account> accounts) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating accounts={} into MySql DB", accounts);

    // write Accounts and containers to MySql
    for (Account account : accounts) {
      if (getAccountById(account.getId()) == null) {
        // new account (insert the containers and account into db tables)
        mySqlAccountStore.addAccounts(Collections.singletonList(account));
        mySqlAccountStore.addContainers(account.getAllContainers());
      } else {
        // existing account (update account table)
        // TODO: Avoid updating account records if only container information changed.
        mySqlAccountStore.updateAccounts(Collections.singletonList(account));
        updateContainersWithMySqlStore(account.getId(), account.getAllContainers());
      }
    }

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating accounts into MySql DB, took time={} ms", timeForUpdate);
    //accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);

  }

  /**
   * Updates MySql DB with added or modified {@link Container}s of a given account
   * @param accountId id of the {@link Account} for the {@link Container}s
   * @param containers collection of {@link Container}s
   * @throws SQLException
   */
  private void updateContainersWithMySqlStore(short accountId, Collection<Container> containers) throws SQLException {
    //check for account ID in in-memory cache
    Account accountInCache = accountInfoMap.getAccountById(accountId);
    if (accountInCache == null) {
      throw new IllegalArgumentException("Account with ID " + accountId + "doesn't exist");
    }

    for (Container containerToUpdate : containers) {
      Container containerInCache =
          accountInfoMap.getContainerByIdForAccount(containerToUpdate.getParentAccountId(), containerToUpdate.getId());
      if (containerInCache == null) {
        // new container added (insert into container table)
        mySqlAccountStore.addContainers(Collections.singletonList(containerToUpdate));
      } else {
        if (!containerInCache.equals(containerToUpdate)) {
          // existing container modified (update container table)
          mySqlAccountStore.updateContainers(Collections.singletonList(containerToUpdate));
        }
      }
    }
  }
}

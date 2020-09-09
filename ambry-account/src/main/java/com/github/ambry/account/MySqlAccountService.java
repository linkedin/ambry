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
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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

  private MySqlAccountStore mySqlAccountStore = null;
  private final AccountServiceMetrics accountServiceMetrics;
  private final MySqlAccountServiceConfig config;
  // in-memory cache for storing account and container metadata
  private final AccountInfoMap accountInfoMap;
  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountService.class);
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService scheduler;

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlAccountServiceConfig config,
      ScheduledExecutorService scheduler) {
    this.accountServiceMetrics = accountServiceMetrics;
    this.config = config;
    this.scheduler = scheduler;
    accountInfoMap = new AccountInfoMap(accountServiceMetrics);
    try {
      createMySqlAccountStore();
    } catch (SQLException e) {
      logger.error("MySQL account store creation failed", e);
      // Continue account service creation. Cache will initialized with metadata from backup copy on local disk to serve read requests.
      // Write requests will be blocked until MySql DB is up. Connection to MySql DB will be retried in polling thread that fetches new accounts.
    }

    // TODO: create backup manager to manage local back up copies of Account and container metadata and lastModifiedTime

    initialFetchAndSchedule();

    // TODO: Subscribe to notifications from ZK
  }

  /**
   * creates MySql Account store which establishes connection to database
   * @throws SQLException
   */
  private void createMySqlAccountStore() throws SQLException {
    if (mySqlAccountStore == null) {
      try {
        mySqlAccountStore = new MySqlAccountStore(config);
      } catch (SQLException e) {
        // TODO: record failure, parse exception to figure out what we did wrong. If it is a non-transient error like credential issue, we should fail start up
        logger.error("MySQL account store creation failed", e);
        throw e;
      }
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

    // Fetch added/modified accounts and containers from mysql db since LMT and update cache.
    fetchAndUpdateCache();

    //Also, schedule to execute the logic periodically.
    if (scheduler != null) {
      scheduler.scheduleAtFixedRate(this::fetchAndUpdateCache, config.updaterPollingIntervalMs,
          config.updaterPollingIntervalMs, TimeUnit.MILLISECONDS);
      logger.info("Background account updater will fetch accounts from mysql db at intervals of {} ms",
          config.updaterPollingIntervalMs);
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
    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
      return;
    }

    // get the last sync time of accounts and containers in cache
    long lastModifiedTime = accountInfoMap.getLastModifiedTime();

    try {
      // Fetch all added/modified accounts and containers from MySql database since LMT
      List<Account> accounts = mySqlAccountStore.getNewAccounts(lastModifiedTime);
      List<Container> containers = mySqlAccountStore.getNewContainers(lastModifiedTime);
      rwLock.writeLock().lock();
      try {
        accountInfoMap.updateAccounts(accounts);
        accountInfoMap.updateContainers(containers);
      } finally {
        rwLock.writeLock().unlock();
      }

      // TODO: Find the max LMT in the fetched accounts and containers and update the cache

    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccountById(accountId);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccountByName(accountName);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      logger.debug("Empty account collection to update.");
      return false;
    }

    if (mySqlAccountStore == null) {
      logger.info("MySql Account DB store is not accessible");
      return false;
    }

    if (config.updateDisabled) {
      logger.info("Updates has been disabled");
      return false;
    }

    if (hasDuplicateAccountIdOrName(accounts)) {
      logger.error("Duplicate account id or name exist in the accounts to update");
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // Make a pre check for conflict between the accounts to update and the accounts in the local cache. Will fail this
    // update operation for all the accounts if any conflict exists. For existing accounts, there is a chance that the account to update
    // conflicts with the accounts in the local cache, but does not conflict with those in the MySql database. This
    // will happen if some accounts are updated but the local cache is not yet refreshed.
    // TODO: Once we have APIs (and versioning) for updating containers, we will need to check conflicts for containers as well.
    rwLock.readLock().lock();
    try {
      if (accountInfoMap.hasConflictingAccount(accounts)) {
        logger.error("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
        //accountServiceMetrics.updateAccountErrorCount.inc();
        return false;
      }
    } finally {
      rwLock.readLock().unlock();
    }

    try {
      updateAccountsWithMySqlStore(accounts);
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in MySql DB", accounts, e);
      // record failure, parse exception to figure out what we did wrong (eg. id or name collision). If it is id collision,
      // retry with incrementing ID (Account ID generation logic is currently in nuage-ambry, we might need to move it here)
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // update in-memory cache with accounts
    rwLock.writeLock().lock();
    try {
      accountInfoMap.updateAccounts(accounts);
    } finally {
      rwLock.writeLock().unlock();
    }

    // TODO: can notify account updates to other nodes via ZK .

    // TODO: persist updated accounts and max timestamp to local back up file.

    return true;
  }

  @Override
  public Collection<Account> getAllAccounts() {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccounts();
    } finally {
      rwLock.readLock().unlock();
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
      shutDownExecutorService(scheduler, config.updaterShutDownTimeoutMs, TimeUnit.MILLISECONDS);
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

    for (Container container : containers) {
      if (accountInfoMap.getContainerByIdForAccount(container.getParentAccountId(), container.getId()) == null) {
        // new container added (insert into container table)
        mySqlAccountStore.addContainers(Collections.singletonList(container));
      } else {
        // existing container modified (update container table)
        mySqlAccountStore.updateContainers(Collections.singletonList(container));
      }
    }
  }
}

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

import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.account.mysql.MySqlDataAccessor;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class MySqlAccountService extends AbstractAccountService {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountService.class);
  static final String MYSQL_ACCOUNT_UPDATER_PREFIX = "mysql-account-updater";
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final MySqlAccountServiceConfig config;
  // lock to protect in-memory metadata cache
  private final ReadWriteLock infoMapLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService scheduler;
  private final BackupFileManager backupFileManager;
  // TODO: we could have two stores (master for writes/reads and replica for reads during failover)
  private volatile MySqlAccountStore mySqlAccountStore;
  private final MySqlAccountStoreFactory mySqlAccountStoreFactory;
  private boolean needRefresh = false;

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlAccountServiceConfig config,
      MySqlAccountStoreFactory mySqlAccountStoreFactory) throws SQLException, IOException {
    super(config, Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null"));
    this.config = config;
    this.mySqlAccountStoreFactory = mySqlAccountStoreFactory;
    try {
      this.mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore(true);
    } catch (SQLException e) {
      logger.error("MySQL account store creation failed", e);
      // If it is a non-transient error like credential issue, creation should fail.
      // Otherwise, continue account service creation and initialize cache with metadata from local file copy
      // to serve read requests. Connection to MySql DB will be retried during periodic sync. Until then, write
      // requests will be blocked.
      if (MySqlDataAccessor.isCredentialError(e)) {
        // Fatal error, fail fast
        throw e;
      }
    }
    this.scheduler =
        config.updaterPollingIntervalSeconds > 0 ? Utils.newScheduler(1, MYSQL_ACCOUNT_UPDATER_PREFIX, false) : null;
    // create backup file manager for persisting and retrieving Account metadata on local disk
    backupFileManager = new BackupFileManager(accountServiceMetrics, config.backupDir, config.maxBackupFileCount);
    // Initialize cache from backup file on disk
    initCacheFromBackupFile();
    // Fetches added or modified accounts and containers from mysql db and schedules to execute it periodically
    initialFetchAndSchedule();
    // TODO: Subscribe to notifications from ZK
  }

  /**
   * Initializes in-memory {@link AccountInfoMap} with accounts and containers stored in backup copy on local disk.
   */
  private void initCacheFromBackupFile() {
    long aMonthAgo = SystemTime.getInstance().seconds() - TimeUnit.DAYS.toSeconds(30);
    Map<String, String> accountMapFromFile = backupFileManager.getLatestAccountMap(aMonthAgo);
    AccountInfoMap accountInfoMap = null;
    if (accountMapFromFile != null) {
      try {
        accountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMapFromFile);
      } catch (Exception e) {
        logger.warn("Failure in parsing of Account Metadata from local backup file", e);
      }
    }
    if (accountInfoMap == null) {
      accountInfoMap = new AccountInfoMap(accountServiceMetrics);
    }
    // Refresh last modified time of Accounts and Containers in cache
    accountInfoMap.refreshLastModifiedTime();
    accountInfoMapRef.set(accountInfoMap);
  }

  /**
   * Fetches added or modified accounts and containers from mysql database and schedules to execute periodically.
   */
  private void initialFetchAndSchedule() {
    fetchAndUpdateCache();
    if (scheduler != null) {
      scheduler.scheduleAtFixedRate(this::fetchAndUpdateCache, config.updaterPollingIntervalSeconds,
          config.updaterPollingIntervalSeconds, TimeUnit.SECONDS);
      logger.info("Background account updater will fetch accounts from mysql db at intervals of {} seconds",
          config.updaterPollingIntervalSeconds);
    }
  }

  /**
   * Fetches all the accounts and containers that have been created or modified in the mysql database since the
   * last modified/sync time and loads into in-memory {@link AccountInfoMap}.
   */
  synchronized void fetchAndUpdateCache() {
    try {
      // Retry connection to mysql if we couldn't set up previously
      if (mySqlAccountStore == null) {
        try {
          mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore(true);
        } catch (SQLException e) {
          logger.error("MySQL account store creation failed", e);
          //TODO: Add failover logic to retry connection on mysql read replica for fetch operations
        }
      }

      // Find last modified time of Accounts and containers in cache.
      long lastModifiedTime = accountInfoMapRef.get().getLastModifiedTime();

      // Fetch added/modified accounts and containers from MySql database since LMT
      Collection<Account> updatedAccountsInDB = mySqlAccountStore.getNewAccounts(lastModifiedTime);
      Collection<Container> updatedContainersInDB = mySqlAccountStore.getNewContainers(lastModifiedTime);

      if (!updatedAccountsInDB.isEmpty() || !updatedContainersInDB.isEmpty()) {
        // Update cache with fetched accounts and containers
        infoMapLock.writeLock().lock();
        try {
          AccountInfoMap accountInfoMap = accountInfoMapRef.get();
          accountInfoMap.addOrUpdateAccounts(updatedAccountsInDB);
          accountInfoMap.addOrUpdateContainers(updatedContainersInDB);
          // Refresh last modified time of Accounts and Containers in cache
          accountInfoMap.refreshLastModifiedTime();
        } finally {
          infoMapLock.writeLock().unlock();
        }
        // At this point we can safely say cash is refreshed
        needRefresh = false;

        // Persist account metadata in cache to back up file on disk.
        Collection<Account> accountCollection;
        infoMapLock.readLock().lock();
        try {
          accountCollection = accountInfoMapRef.get().getAccounts();
        } finally {
          infoMapLock.readLock().unlock();
        }
        Map<String, String> accountMap = new HashMap<>();
        accountCollection.forEach(
            account -> accountMap.put(Short.toString(account.getId()), account.toJson(false).toString()));
        backupFileManager.persistAccountMap(accountMap, backupFileManager.getLatestVersion() + 1,
            SystemTime.getInstance().seconds());
      }
    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMapRef.get().getAccountById(accountId);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMapRef.get().getAccountByName(accountName);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  //TODO: Revisit this interface method to see if we can throw exception instead of returning boolean so that caller can
  // distinguish between bad input and system error.
  @Override
  public boolean updateAccounts(Collection<Account> accounts) throws AccountServiceException {
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
      accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // Check for name/id/version conflicts between the accounts and containers being updated with those in local cache.
    // There is a slight chance that we have conflicts local cache, but not with MySql database. This will happen if
    // but the local cache is not yet refreshed with latest account info.
    infoMapLock.readLock().lock();
    try {
      AccountInfoMap accountInfoMap = accountInfoMapRef.get();
      if (accountInfoMap.hasConflictingAccount(accounts)) {
        logger.error("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
        accountServiceMetrics.updateAccountErrorCount.inc();
        return false;
      }
      for (Account account : accounts) {
        if (accountInfoMap.hasConflictingContainer(account.getAllContainers(), account.getId())) {
          logger.error(
              "Containers={} under Account={} conflict with the containers in local cache. Cancel the update operation.",
              account.getAllContainers(), account.getId());
          accountServiceMetrics.updateAccountErrorCount.inc();
          return false;
        }
      }
    } finally {
      infoMapLock.readLock().unlock();
    }

    // write added/modified accounts to database
    try {
      updateAccountsWithMySqlStore(accounts);
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in MySql DB", accounts, e);
      try {
        handleSQLException(e);
      } catch (AccountServiceException ase) {
        return false;
      }
    }

    // write added/modified accounts to in-memory cache
    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRef.get().addOrUpdateAccounts(accounts);
    } finally {
      infoMapLock.writeLock().unlock();
    }

    // TODO: can notify account updates to other nodes via ZK .

    return true;
  }

  @Override
  public Collection<Account> getAllAccounts() {
    infoMapLock.readLock().lock();
    try {
      return accountInfoMapRef.get().getAccounts();
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
      shutDownExecutorService(scheduler, config.updaterShutDownTimeoutMinutes, TimeUnit.MINUTES);
    }
    open.set(false);
  }

  @Override
  protected void checkOpen() {
    if (!open.get()) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }

  ExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  public Collection<Container> updateContainers(String accountName, Collection<Container> containers)
      throws AccountServiceException {
    // TODO: make transactional
    try {
      return super.updateContainers(accountName, containers);
    } catch (AccountServiceException ase) {
      if (needRefresh) {
        // refresh and retry (with regenerated containerIds)
        accountServiceMetrics.conflictRetryCount.inc();
        this.fetchAndUpdateCache();
        return super.updateContainers(accountName, containers);
      } else {
        throw ase;
      }
    }
  }

  @Override
  protected void updateResolvedContainers(Account account, Collection<Container> resolvedContainers)
      throws AccountServiceException {
    try {
      updateContainersWithMySqlStore(account.getId(), resolvedContainers);
    } catch (SQLException e) {
      logger.error("Failed updating containers {} in MySql DB", resolvedContainers, e);
      handleSQLException(e);
    }

    // write added/modified containers to in-memory cache
    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRef.get().addOrUpdateContainers(resolvedContainers);
    } finally {
      infoMapLock.writeLock().unlock();
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
    // TODO: make transactional
    for (Account account : accounts) {
      Account existingAccount = getAccountById(account.getId());
      if (existingAccount == null) {
        // new account (insert the containers and account into db tables)
        mySqlAccountStore.addAccount(account);
        mySqlAccountStore.addContainers(account.getAllContainers());
      } else {
        // existing account (update account table)
        // Avoid updating account records if only container information changed.
        if (!AccountCollectionSerde.accountToJsonNoContainers(existingAccount)
            .similar(AccountCollectionSerde.accountToJsonNoContainers(account))) {
          mySqlAccountStore.updateAccount(account);
        }
        updateContainersWithMySqlStore(account.getId(), account.getAllContainers());
      }
    }

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating accounts into MySql DB, took time={} ms", timeForUpdate);
    accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
  }

  /**
   * Updates MySql DB with added or modified {@link Container}s of a given account
   * @param accountId id of the {@link Account} for the {@link Container}s
   * @param containers collection of {@link Container}s
   * @throws SQLException
   */
  private void updateContainersWithMySqlStore(short accountId, Collection<Container> containers) throws SQLException {
    //check for account ID in in-memory cache
    AccountInfoMap accountInfoMap;
    Account accountInCache;
    infoMapLock.readLock().lock();
    try {
      accountInfoMap = accountInfoMapRef.get();
      accountInCache = accountInfoMap.getAccountById(accountId);
    } finally {
      infoMapLock.readLock().unlock();
    }
    if (accountInCache == null) {
      throw new IllegalArgumentException("Account with ID " + accountId + "doesn't exist");
    }

    for (Container containerToUpdate : containers) {
      Container containerInCache =
          accountInfoMap.getContainerByIdForAccount(containerToUpdate.getParentAccountId(), containerToUpdate.getId());
      if (containerInCache == null) {
        // new container added (insert into container table)
        mySqlAccountStore.addContainer(containerToUpdate);
      } else {
        if (!containerInCache.equals(containerToUpdate)) {
          // existing container modified (update container table)
          mySqlAccountStore.updateContainer(containerToUpdate);
        }
      }
    }
  }

  private void handleSQLException(SQLException e) throws AccountServiceException {
    // record failure, parse exception to figure out what we did wrong (eg. id or name collision). If it is id collision,
    // retry with incrementing ID (Account ID generation logic is currently in nuage-ambry, we might need to move it here)
    accountServiceMetrics.updateAccountErrorCount.inc();
    if (e instanceof SQLIntegrityConstraintViolationException) {
      needRefresh = true;
      SQLIntegrityConstraintViolationException icve = (SQLIntegrityConstraintViolationException) e;
      String message;
      if (icve.getMessage().contains("containers.accountContainer")) {
        // Example: Duplicate entry '101-5' for key 'containers.accountContainer'
        // duplicate container id: need to update cache and retry
        message = "Duplicate containerId";
      } else if (icve.getMessage().contains("containers.uniqueName")) {
        // duplicate container name: need to update cache but retry may fail
        message = "Duplicate container name";
      } else {
        message = "Constraint violation";
      }
      throw new AccountServiceException(message, AccountServiceErrorCode.ResourceConflict);
    } else if (MySqlDataAccessor.isCredentialError(e)) {
      // TODO: BadCredentials
      throw new AccountServiceException("Invalid db credentials", AccountServiceErrorCode.InternalError);
    } else {
      throw new AccountServiceException(e.getMessage(), AccountServiceErrorCode.InternalError);
    }
  }
}

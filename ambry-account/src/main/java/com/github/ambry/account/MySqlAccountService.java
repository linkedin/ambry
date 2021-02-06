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
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
  private volatile MySqlAccountStore mySqlAccountStore;
  private final MySqlAccountStoreFactory mySqlAccountStoreFactory;
  private boolean needRefresh = false;
  private long lastSyncTime = -1;

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlAccountServiceConfig config,
      MySqlAccountStoreFactory mySqlAccountStoreFactory) throws SQLException, IOException {
    super(config, Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null"));
    this.config = config;
    this.mySqlAccountStoreFactory = mySqlAccountStoreFactory;
    try {
      this.mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore();
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
    accountServiceMetrics.trackTimeSinceLastSync(this::getTimeInSecondsSinceLastSync);
    accountServiceMetrics.trackContainerCount(this::getContainerCount);
    this.scheduler =
        config.updaterPollingIntervalSeconds > 0 ? Utils.newScheduler(1, MYSQL_ACCOUNT_UPDATER_PREFIX, false) : null;
    // create backup file manager for persisting and retrieving Account metadata on local disk
    backupFileManager = new BackupFileManager(accountServiceMetrics, config.backupDir, config.maxBackupFileCount);
    // Initialize cache from backup file on disk
    initCacheFromBackupFile();
    // Fetches added or modified accounts and containers from mysql db and schedules to execute it periodically
    initialFetchAndSchedule();
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
    fetchAndUpdateCacheNoThrow();
    if (scheduler != null) {
      scheduler.scheduleAtFixedRate(this::fetchAndUpdateCacheNoThrow, config.updaterPollingIntervalSeconds,
          config.updaterPollingIntervalSeconds, TimeUnit.SECONDS);
      logger.info("Background account updater will fetch accounts from mysql db at intervals of {} seconds",
          config.updaterPollingIntervalSeconds);
    }
  }

  /**
   * Fetches all the accounts and containers that have been created or modified in the mysql database since the
   * last modified/sync time and loads into in-memory {@link AccountInfoMap}.
   */
  void fetchAndUpdateCacheNoThrow() {
    try {
      fetchAndUpdateCache();
    } catch (Throwable e) {
      logger.error("fetchAndUpdateCache failed", e);
    }
  }

  /**
   * Fetches all the accounts and containers that have been created or modified in the mysql database since the
   * last modified/sync time and loads into in-memory {@link AccountInfoMap}.
   */
  synchronized void fetchAndUpdateCache() throws SQLException {
    // Retry connection to mysql if we couldn't set up previously
    if (mySqlAccountStore == null) {
      try {
        mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore();
      } catch (SQLException e) {
        logger.error("MySQL account store creation failed: {}", e.getMessage());
        throw e;
      }
    }

    try {
      // Find last modified time of Accounts and containers in cache.
      long lastModifiedTime = accountInfoMapRef.get().getLastModifiedTime();
      logger.info("Syncing from database using lastModifiedTime = {}", new Date(lastModifiedTime));

      long startTimeMs = System.currentTimeMillis();
      // Fetch added/modified accounts and containers from MySql database since LMT
      Collection<Account> updatedAccountsInDB = mySqlAccountStore.getNewAccounts(lastModifiedTime);
      Collection<Container> updatedContainersInDB = mySqlAccountStore.getNewContainers(lastModifiedTime);
      long endTimeMs = System.currentTimeMillis();
      accountServiceMetrics.fetchRemoteAccountTimeInMs.update(endTimeMs - startTimeMs);
      // Close connection and get fresh one next time
      mySqlAccountStore.closeConnection();

      if (!updatedAccountsInDB.isEmpty() || !updatedContainersInDB.isEmpty()) {
        logger.info("Found {} accounts and {} containers", updatedAccountsInDB.size(), updatedContainersInDB.size());
        for (Account account : updatedAccountsInDB) {
          logger.info("Found account {}", account.getName());
        }
        for (Container container : updatedContainersInDB) {
          logger.info("Found container {}", container.getName());
        }
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
        // At this point we can safely say cache is refreshed
        needRefresh = false;
        lastSyncTime = endTimeMs;

        // Notify updated accounts to consumers
        Set<Account> updatedAccounts = new HashSet<>();
        updatedAccountsInDB.forEach(
            account -> updatedAccounts.add(accountInfoMapRef.get().getAccountById(account.getId())));
        updatedContainersInDB.forEach(
            container -> updatedAccounts.add(accountInfoMapRef.get().getAccountById(container.getParentAccountId())));
        notifyAccountUpdateConsumers(updatedAccounts, false);

        // Persist all account metadata to back up file on disk.
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
      } else {
        // Cache is up to date
        needRefresh = false;
        lastSyncTime = endTimeMs;
      }
    } catch (SQLException e) {
      accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
      logger.error("Fetching accounts from MySql DB failed: {}", e.getMessage());
      throw e;
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

  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      throw new IllegalArgumentException("Empty account collection to update.");
    }
    if (mySqlAccountStore == null) {
      throw new AccountServiceException("MySql Account store is not accessible", AccountServiceErrorCode.InternalError);
    }

    if (config.updateDisabled) {
      throw new AccountServiceException("Updates have been disabled", AccountServiceErrorCode.UpdateDisabled);
    }

    if (hasDuplicateAccountIdOrName(accounts)) {
      accountServiceMetrics.updateAccountErrorCount.inc();
      throw new AccountServiceException("Duplicate account id or name exist in the accounts to update",
          AccountServiceErrorCode.ResourceConflict);
    }

    // Check for name/id/version conflicts between the accounts and containers being updated with those in local cache.
    // There is a slight chance that we have conflicts local cache, but not with MySql database. This will happen if
    // but the local cache is not yet refreshed with latest account info.
    infoMapLock.readLock().lock();
    try {
      AccountInfoMap accountInfoMap = accountInfoMapRef.get();
      if (accountInfoMap.hasConflictingAccount(accounts, config.ignoreVersionMismatch)) {
        logger.error("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
        accountServiceMetrics.updateAccountErrorCount.inc();
        throw new AccountServiceException("Input accounts conflict with the accounts in local cache",
            AccountServiceErrorCode.ResourceConflict);
      }
      for (Account account : accounts) {
        if (accountInfoMap.hasConflictingContainer(account.getAllContainers(), account.getId(),
            config.ignoreVersionMismatch)) {
          logger.error(
              "Containers={} under Account={} conflict with the containers in local cache. Cancel the update operation.",
              account.getAllContainers(), account.getId());
          accountServiceMetrics.updateAccountErrorCount.inc();
          throw new AccountServiceException("Containers in account " + account.getId() + " conflict with local cache",
              AccountServiceErrorCode.ResourceConflict);
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
      handleSQLException(e);
    }

    // write added/modified accounts to in-memory cache
    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRef.get().addOrUpdateAccounts(accounts);
    } finally {
      infoMapLock.writeLock().unlock();
    }
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
    try {
      return super.updateContainers(accountName, containers);
    } catch (AccountServiceException ase) {
      if (needRefresh) {
        // refresh and retry (with regenerated containerIds)
        try {
          fetchAndUpdateCache();
        } catch (SQLException e) {
          throw translateSQLException(e);
        }
        accountServiceMetrics.conflictRetryCount.inc();
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
      updateContainersWithMySqlStore(account, resolvedContainers);
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
  void updateAccountsWithMySqlStore(Collection<Account> accounts) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating accounts={} into MySql DB", accounts);

    // Get account and container changes info
    List<AccountUpdateInfo> accountsUpdateInfo = new ArrayList<>();
    for (Account account : accounts) {

      boolean isAccountAdded = false, isAccountUpdated = false;
      List<Container> addedContainers;
      List<Container> updatedContainers = new ArrayList<>();

      Account accountInCache = getAccountById(account.getId());
      if (accountInCache == null) {
        isAccountAdded = true;
        addedContainers = new ArrayList<>(account.getAllContainers());
      } else {
        if (!AccountCollectionSerde.accountToJsonNoContainers(accountInCache)
            .similar(AccountCollectionSerde.accountToJsonNoContainers(account))) {
          isAccountUpdated = true;
        }
        // Get list of added and updated containers in the account.
        Pair<List<Container>, List<Container>> addedOrUpdatedContainers =
            getUpdatedContainers(account, account.getAllContainers());
        addedContainers = addedOrUpdatedContainers.getFirst();
        updatedContainers = addedOrUpdatedContainers.getSecond();
      }

      accountsUpdateInfo.add(
          new AccountUpdateInfo(account, isAccountAdded, isAccountUpdated, addedContainers, updatedContainers));
    }

    // Write changes to MySql db.
    mySqlAccountStore.updateAccounts(accountsUpdateInfo);

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating accounts={} in MySql DB, took time={} ms", accounts, timeForUpdate);
    accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
  }

  /**
   * Updates MySql DB with added or modified {@link Container}s of a given account
   * @param account parent {@link Account} for the {@link Container}s
   * @param containers collection of {@link Container}s
   * @throws SQLException
   */
  private void updateContainersWithMySqlStore(Account account, Collection<Container> containers) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating containers={} for accountId={} into MySql DB", containers, account.getId());

    // Get list of added and updated containers in the account.
    Pair<List<Container>, List<Container>> addedOrUpdatedContainers = getUpdatedContainers(account, containers);
    AccountUpdateInfo accountUpdateInfo =
        new AccountUpdateInfo(account, false, false, addedOrUpdatedContainers.getFirst(),
            addedOrUpdatedContainers.getSecond());

    // Write changes to MySql db.
    mySqlAccountStore.updateAccounts(Collections.singletonList(accountUpdateInfo));

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating containers={} for accountId={} in MySqlDB in time={} ms", containers,
        account.getId(), timeForUpdate);
    accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);
  }

  /**
   * Gets lists of added and updated {@link Container}s in the {@link Account} by comparing with in-memory cache.
   * @param account Id of the account
   * @param containers {@link Container}s added or updated in the account.
   * @return {@link Pair} of lists of added and updated {@link Container}s in the given {@link Account}
   */
  private Pair<List<Container>, List<Container>> getUpdatedContainers(Account account,
      Collection<Container> containers) {

    //check for account in in-memory cache
    Account accountInCache = getAccountById(account.getId());
    if (accountInCache == null) {
      throw new IllegalArgumentException("Account with ID " + account + "doesn't exist");
    }

    List<Container> addedContainers = new ArrayList<>();
    List<Container> updatedContainers = new ArrayList<>();
    for (Container containerToUpdate : containers) {
      Container containerInCache = accountInCache.getContainerById(containerToUpdate.getId());
      if (containerInCache == null) {
        addedContainers.add(containerToUpdate);
      } else {
        if (!containerInCache.equals(containerToUpdate)) {
          updatedContainers.add(containerToUpdate);
        }
      }
    }

    return new Pair<>(addedContainers, updatedContainers);
  }

  /**
   * @return the time in seconds since the last database sync / cache refresh
   */
  public int getTimeInSecondsSinceLastSync() {
    return lastSyncTime > 0 ? (int) (System.currentTimeMillis() - lastSyncTime) / 1000 : Integer.MAX_VALUE;
  }

  /**
   * @return the total number of containers in all accounts.
   */
  public int getContainerCount() {
    return accountInfoMapRef.get().getContainerCount();
  }

  /**
   * Handle a {@link SQLException} and throw the corresponding {@link AccountServiceException}.
   * @param e the input exception.
   * @throws AccountServiceException the translated {@link AccountServiceException}.
   */
  private void handleSQLException(SQLException e) throws AccountServiceException {
    // record failure, parse exception to figure out what we did wrong (eg. id or name collision). If it is id collision,
    // retry with incrementing ID (Account ID generation logic is currently in nuage-ambry, we might need to move it here)
    accountServiceMetrics.updateAccountErrorCount.inc();
    AccountServiceException ase = translateSQLException(e);
    if (ase.getErrorCode() == AccountServiceErrorCode.ResourceConflict) {
      needRefresh = true;
    }
    throw ase;
  }

  /**
   * Translate a {@link SQLException} to a {@link AccountServiceException}.
   * @param e the input exception.
   * @return the corresponding {@link AccountServiceException}.
   */
  public static AccountServiceException translateSQLException(SQLException e) {
    if (e instanceof SQLIntegrityConstraintViolationException || (e instanceof BatchUpdateException
        && e.getCause() instanceof SQLIntegrityConstraintViolationException)) {
      return new AccountServiceException(e.getMessage(), AccountServiceErrorCode.ResourceConflict);
    } else if (MySqlDataAccessor.isCredentialError(e)) {
      return new AccountServiceException("Invalid database credentials", AccountServiceErrorCode.InternalError);
    } else {
      return new AccountServiceException(e.getMessage(), AccountServiceErrorCode.InternalError);
    }
  }
}
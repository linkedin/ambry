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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.mysql.MySqlAccountStore;
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.commons.Notifier;
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
import java.util.LinkedHashMap;
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
  public static final String SEPARATOR = ":";
  // Cache parameters (initial capacity, load factor and max limit) for recentNotFoundContainersCache
  private static final int cacheInitialCapacity = 100;
  private static final float cacheLoadFactor = 0.75f;
  private static final int cacheMaxLimit = 1000;
  static final String MYSQL_ACCOUNT_UPDATER_PREFIX = "mysql-account-updater";
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final MySqlAccountServiceConfig config;
  // lock to protect in-memory metadata cache
  private final ReadWriteLock infoMapLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService scheduler;
  private final BackupFileManager backupFileManager;
  private BackupFileManager backupFileManagerNew = null;
  private volatile MySqlAccountStore mySqlAccountStore;
  private volatile MySqlAccountStore mySqlAccountStoreNew = null;
  private final MySqlAccountStoreFactory mySqlAccountStoreFactory;
  private boolean needRefresh = false;
  private boolean needRefreshNew = false;
  private long lastSyncTime = -1;
  private long lastSyncTimeNew = -1;
  private final Set<String> recentNotFoundContainersCache;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final boolean writeCacheAfterUpdate;

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlAccountServiceConfig config,
      MySqlAccountStoreFactory mySqlAccountStoreFactory, Notifier<String> notifier) throws SQLException, IOException {
    super(config, Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null"), notifier);
    this.config = config;
    this.mySqlAccountStoreFactory = mySqlAccountStoreFactory;
    try {
      this.mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore();
      if (config.enableNewDatabaseForMigration) {
        this.mySqlAccountStoreNew = mySqlAccountStoreFactory.getMySqlAccountStoreNew();
      }
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
    // A local LRA cache of containers not found in mysql db. This is used to avoid repeated queries to db during getContainerByName() calls.
    recentNotFoundContainersCache = Collections.newSetFromMap(
        Collections.synchronizedMap(new LinkedHashMap<String, Boolean>(cacheInitialCapacity, cacheLoadFactor, true) {
          protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > cacheMaxLimit;
          }
        }));
    this.writeCacheAfterUpdate = config.writeCacheAfterUpdate;

    if (config.enableNewDatabaseForMigration) {
      accountServiceMetrics.trackTimeSinceLastSyncNew(this::getTimeInSecondsSinceLastSyncNew);
      accountServiceMetrics.trackContainerCountNew(this::getContainerCountNew);
      backupFileManagerNew = new BackupFileManager(accountServiceMetrics, config.backupDirNew, config.maxBackupFileCount);
    }
    // Initialize cache from backup file on disk
    initCacheFromBackupFile();
    // Fetches added or modified accounts and containers from mysql db and schedules to execute it periodically
    initialFetchAndSchedule();
  }

  /**
   * @return set (LRA cache) of containers not found in recent get attempts. Used in only tests.
   */
  Set<String> getRecentNotFoundContainersCache() {
    return Collections.unmodifiableSet(recentNotFoundContainersCache);
  }

  /**
   * Initializes in-memory {@link AccountInfoMap} with accounts and containers stored in backup copy on local disk.
   */
  private void initCacheFromBackupFile() {
    long aMonthAgo = SystemTime.getInstance().seconds() - TimeUnit.DAYS.toSeconds(30);
    Collection<Account> accounts = backupFileManager.getLatestAccounts(aMonthAgo);
    AccountInfoMap accountInfoMap = null;
    if (accounts != null) {
      try {
        accountInfoMap = new AccountInfoMap(accounts);
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

    if (config.enableNewDatabaseForMigration) {
      Collection<Account> accountsNew = backupFileManagerNew.getLatestAccounts(aMonthAgo);
      AccountInfoMap accountInfoMapNew = null;
      if (accountsNew != null) {
        try {
          accountInfoMapNew = new AccountInfoMap(accountsNew);
        } catch (Exception e) {
          logger.warn("Failure in parsing of Account Metadata from local backup file", e);
        }
      }
      if (accountInfoMapNew == null) {
        accountInfoMapNew = new AccountInfoMap(accountServiceMetrics);
      }
      // Refresh last modified time of Accounts and Containers in cache
      accountInfoMapNew.refreshLastModifiedTime();
      accountInfoMapRefNew.set(accountInfoMapNew);
    }
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
    maybeSubscribeChangeTopic(false);
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
        updateAccountsInCache(updatedAccountsInDB);
        updateContainersInCache(updatedContainersInDB);

        // Refresh last modified time of Accounts and Containers in cache
        accountInfoMapRef.get().refreshLastModifiedTime();

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
        backupFileManager.persistAccountMap(accountCollection, backupFileManager.getLatestVersion() + 1,
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

    if (config.enableNewDatabaseForMigration) {
      // Retry connection to mysql if we couldn't set up previously
      if (mySqlAccountStoreNew == null) {
        try {
          mySqlAccountStoreNew = mySqlAccountStoreFactory.getMySqlAccountStoreNew();
        } catch (SQLException e) {
          logger.error("MySQL account store creation failed: {}", e.getMessage());
          throw e;
        }
      }

      try {
        // Find last modified time of Accounts and containers in cache.
        long lastModifiedTimeNew = accountInfoMapRefNew.get().getLastModifiedTime();
        logger.info("Syncing from database using lastModifiedTimeNew = {}", new Date(lastModifiedTimeNew));

        long startTimeMs = System.currentTimeMillis();
        // Fetch added/modified accounts and containers from MySql database since LMT
        Collection<Account> updatedAccountsInDBNew = mySqlAccountStoreNew.getNewAccounts(lastModifiedTimeNew);
        Collection<Container> updatedContainersInDBNew = mySqlAccountStoreNew.getNewContainers(lastModifiedTimeNew);
        long endTimeMs = System.currentTimeMillis();
        accountServiceMetrics.fetchRemoteAccountTimeInMsNew.update(endTimeMs - startTimeMs);
        // Close connection and get fresh one next time
        mySqlAccountStoreNew.closeConnection();
        if (!updatedAccountsInDBNew.isEmpty() || !updatedContainersInDBNew.isEmpty()) {
          logger.info("Found {} accountsNew and {} containersNew", updatedAccountsInDBNew.size(),
              updatedContainersInDBNew.size());
          for (Account account : updatedAccountsInDBNew) {
            logger.info("Found accountNew {}", account.getName());
          }
          for (Container container : updatedContainersInDBNew) {
            logger.info("Found containerNew {}", container.getName());
          }

          // Update cache with fetched accounts and containers
          updateAccountsInCacheNew(updatedAccountsInDBNew);
          updateContainersInCacheNew(updatedContainersInDBNew);

          // Refresh last modified time of Accounts and Containers in cache
          accountInfoMapRefNew.get().refreshLastModifiedTime();

          // At this point we can safely say cache is refreshed
          needRefreshNew = false;
          lastSyncTimeNew = endTimeMs;

          // Notify updated accounts to consumers
          Set<Account> updatedAccountsNew = new HashSet<>();
          updatedAccountsInDBNew.forEach(
              account -> updatedAccountsNew.add(accountInfoMapRefNew.get().getAccountById(account.getId())));
          updatedContainersInDBNew.forEach(container -> updatedAccountsNew.add(
              accountInfoMapRefNew.get().getAccountById(container.getParentAccountId())));
          notifyAccountUpdateConsumers(updatedAccountsNew, false);

          // Persist all account metadata to back up file on disk.
          Collection<Account> accountCollectionNew;
          infoMapLock.readLock().lock();
          try {
            accountCollectionNew = accountInfoMapRefNew.get().getAccounts();
          } finally {
            infoMapLock.readLock().unlock();
          }
          backupFileManagerNew.persistAccountMap(accountCollectionNew, backupFileManagerNew.getLatestVersion() + 1,
              SystemTime.getInstance().seconds());
        } else {
          // Cache is up to date
          needRefreshNew = false;
          lastSyncTimeNew = endTimeMs;
        }
      } catch (SQLException e) {
        accountServiceMetrics.fetchRemoteAccountErrorCountNew.inc();
        logger.error("Fetching accounts from MySql DB failed: {}", e.getMessage());
        throw e;
      }
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    infoMapLock.readLock().lock();
    try {
      if (config.enableNewDatabaseForMigration) {
        if (accountInfoMapRefNew.get().getAccountById(accountId) != null) {
          logger.trace("get account by Id from new in memory cache, accountId: " + accountId);
          return accountInfoMapRefNew.get().getAccountById(accountId);
        } else {
          logger.trace("get account by Id from old in memory cache, accountId: " + accountId);
          return accountInfoMapRef.get().getAccountById(accountId);
        }
      } else {
        logger.trace("get account by Id from old in memory cache, accountId: " + accountId);
        return accountInfoMapRef.get().getAccountById(accountId);
      }
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  public Account getAccountByIdNew(short accountId) {
    infoMapLock.readLock().lock();
    try {
      logger.trace("get account by Id from new in memory cache, accountId: " + accountId);
      return accountInfoMapRefNew.get().getAccountById(accountId);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  public Account getAccountByIdOld(short accountId) {
    infoMapLock.readLock().lock();
    try {
      logger.trace("get account by Id from new in memory cache, accountId: " + accountId);
      return accountInfoMapRef.get().getAccountById(accountId);
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    infoMapLock.readLock().lock();
    try {
      if (config.enableNewDatabaseForMigration) {
        if (accountInfoMapRefNew.get().getAccountByName(accountName) != null) {
          logger.trace("get account by Id from new in memory cache, accountName: " + accountName);
          return accountInfoMapRefNew.get().getAccountByName(accountName);
        } else {
          logger.trace("get account by Id from old in memory cache, accountName: " + accountName);
          return accountInfoMapRef.get().getAccountByName(accountName);
        }
      } else {
        logger.trace("get account by Id from old in memory cache, accountName: " + accountName);
        return accountInfoMapRef.get().getAccountByName(accountName);
      }
    } finally {
      infoMapLock.readLock().unlock();
    }
  }

  public Account getAccountByNameNew(String accountName) {
    infoMapLock.readLock().lock();
    try {
      logger.trace("get account by name from new in memory cache, accountName: " + accountName);
      return accountInfoMapRefNew.get().getAccountByName(accountName);
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

    if (config.enableNewDatabaseForMigration && mySqlAccountStoreNew == null) {
      throw new AccountServiceException("MySql Account store new is not accessible", AccountServiceErrorCode.InternalError);
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
      if (config.enableNewDatabaseForMigration) {
        AccountInfoMap accountInfoMapNew = accountInfoMapRefNew.get();
        if (accountInfoMapNew.hasConflictingAccount(accounts, config.ignoreVersionMismatch)) {
          logger.error("Accounts={} conflict with the accounts in local cache new. Cancel the update operation.", accounts);
          accountServiceMetrics.updateAccountErrorCountNew.inc();
          throw new AccountServiceException("Input accounts conflict with the accounts in local cache new",
              AccountServiceErrorCode.ResourceConflict);
        }
        for (Account account : accounts) {
          if (accountInfoMapNew.hasConflictingContainer(account.getAllContainers(), account.getId(),
              config.ignoreVersionMismatch)) {
            logger.error(
                "Containers={} under Account={} conflict with the containers in local cache new. Cancel the update operation.",
                account.getAllContainers(), account.getId());
            accountServiceMetrics.updateAccountErrorCountNew.inc();
            throw new AccountServiceException("Containers in account " + account.getId() + " conflict with local cache new",
                AccountServiceErrorCode.ResourceConflict);
          }
        }
      }
    } finally {
      infoMapLock.readLock().unlock();
    }

    // write added/modified accounts to database
    try {
      if (config.enableNewDatabaseForMigration) {
        //only do put or update against new db
        updateAccountsWithMySqlStoreNew(accounts);
      } else {
        updateAccountsWithMySqlStore(accounts);
      }
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in MySql DB", accounts, e);
      handleSQLException(e);
    }
    // Tell the world
    publishChangeNotice();

    if (writeCacheAfterUpdate) {
      // Write accounts to in-memory cache. Snapshot version will be out of date until the next DB refresh.
      if (config.enableNewDatabaseForMigration) {
        //since when config enable, we only update the new db, so only new cache should be updated.
        updateAccountsInCacheNew(accounts);
      } else {
        updateAccountsInCache(accounts);
      }
    }
  }

  @Override
  public Collection<Account> getAllAccounts() {
    infoMapLock.readLock().lock();
    try {
      if (config.enableNewDatabaseForMigration) {
        List<Account> allAccounts = new ArrayList<>(accountInfoMapRefNew.get().getAccounts());
        Collection<Short> allAccountIds = accountInfoMapRefNew.get().getAccountIds();
        for (Account account : accountInfoMapRef.get().getAccounts()) {
          if (!allAccountIds.contains(account.getId())) {
            logger.trace("Account {} is in old cache only", account);
            allAccounts.add(account);
          }
        }
        return allAccounts;
      }
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
    maybeUnsubscribeChangeTopic();
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
  protected void publishChangeNotice() {
    // TODO: can optimize this by sending the account/container payload as the topic message,
    // so subscribers can update their cache and avoid the database query.
    super.publishChangeNotice();
  }

  @Override
  protected void onAccountChangeMessage(String topic, String message) {
    if (!open.get()) {
      // take no action instead of throwing an exception to silence noisy log messages when a message is received while
      // closing the AccountService.
      return;
    }
    try {
      switch (message) {
        case FULL_ACCOUNT_METADATA_CHANGE_MESSAGE:
          logger.info("Processing message={} for topic={}", message, topic);
          fetchAndUpdateCache();
          logger.trace("Completed processing message={} for topic={}", message, topic);
          break;
        default:
          accountServiceMetrics.unrecognizedMessageErrorCount.inc();
          throw new RuntimeException("Could not understand message=" + message + " for topic=" + topic);
      }
    } catch (Exception e) {
      logger.error("Exception occurred when processing message={} for topic={}.", message, topic, e);
      accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
      accountServiceMetrics.fetchRemoteAccountErrorCount.inc();
    }
  }

  @Override
  public Collection<Container> updateContainers(String accountName, Collection<Container> containers)
      throws AccountServiceException {
    if (config.enableNewDatabaseForMigration) {
      try {
        return updateContainersHelper(accountName, containers);
      } catch (AccountServiceException ase) {
        if (needRefresh || needRefreshNew) {
          // refresh and retry (with regenerated containerIds)
          try {
            fetchAndUpdateCache();
          } catch (SQLException e) {
            throw translateSQLException(e);
          }
          accountServiceMetrics.conflictRetryCount.inc();
          return updateContainersHelper(accountName, containers);
        } else {
          throw ase;
        }
      }
    } else {
      try {
        return super.updateContainers(accountName, containers);
      } catch (AccountServiceException ase) {
        if (needRefresh || needRefreshNew) {
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
  }

  private Collection<Container> updateContainersHelper (String accountName, Collection<Container> containers)
      throws AccountServiceException {
    checkOpen();
    // input validation
    if (accountName == null || accountName.isEmpty() || containers == null || containers.isEmpty()) {
      throw new AccountServiceException("Account or container is null or empty", AccountServiceErrorCode.BadRequest);
    }

    Account account = getAccountByName(accountName);
    if (account == null) {
      logger.error("Account {} is not found", accountName);
      throw new AccountServiceException("Account " + accountName + " is not found", AccountServiceErrorCode.NotFound);
    }

    Account accountInCacheOld = getAccountByIdOld(account.getId());
    Account accountInCacheNew = getAccountByIdNew(account.getId());
    //only when the account in old cache but not exist in new cache, we need to upload account from old cache to new db
    //and refresh new cache. After migration is done, we can remove this line.
    if (accountInCacheNew == null && accountInCacheOld != null) {
      accountServiceMetrics.updateAccountFromOldCacheToNewDbCount.inc();
      try {
        uploadAccountFromOldCacheToNewDBAndRefreshNewCache(accountInCacheOld);
      } catch (SQLException e) {
        throw translateSQLException(e);
      }
    }

    //after above, the account should be found in new cache.
    account = getAccountByNameNew(accountName);
    if (account == null) {
      logger.error("Account {} is not found", accountName);
      throw new AccountServiceException("Account " + accountName + " is not found in new cache",
          AccountServiceErrorCode.NotFound);
    }

    List<Container> resolvedContainers = new ArrayList<>();
    List<Container> existingUnchangedContainers = new ArrayList<>();
    // create a hashmap to map the name to existing containers in account
    Map<String, Container> existingContainersInAccount = new HashMap<>();
    account.getAllContainers().forEach(c -> existingContainersInAccount.put(c.getName(), c));

    // Generate container ids for new containers
    short nextContainerId = account.getAllContainers()
        .stream()
        .map(Container::getId)
        .max(Short::compareTo)
        .map(maxId -> (short) (maxId + 1))
        .orElse(config.containerIdStartNumber);

    for (Container container : containers) {
      if (container.getId() == Container.UNKNOWN_CONTAINER_ID) {
        // new container
        Container existingContainer = existingContainersInAccount.get(container.getName());
        if (existingContainer != null) {
          switch (existingContainer.getStatus()) {
            case INACTIVE:
              throw new AccountServiceException(
                  "The container " + container.getName() + " has gone and cannot be restored",
                  AccountServiceErrorCode.ResourceHasGone);
            case DELETE_IN_PROGRESS:
              if (existingContainer.getDeleteTriggerTime() + TimeUnit.DAYS.toMillis(
                  config.containerDeprecationRetentionDays) > System.currentTimeMillis()) {
                throw new AccountServiceException("Create method is not allowed on container " + container.getName()
                    + " as it's in Delete_In_Progress state", AccountServiceErrorCode.MethodNotAllowed);
              } else {
                throw new AccountServiceException(
                    "The container " + container.getName() + " has gone and cannot be restored",
                    AccountServiceErrorCode.ResourceHasGone);
              }
            case ACTIVE:
              // make sure there is no conflicting container (conflicting means a container with same name but different attributes already exists).
              if (existingContainer.isSameContainer(container)) {
                // If an exactly same container already exists, treat as no-op (may be retry after partial failure).
                // But include it in the list returned to caller to provide the containerId.
                String containerStr;
                try {
                  containerStr = objectMapper.writeValueAsString(existingContainer);
                } catch (IOException e) {
                  containerStr = existingContainer.toString();
                }
                logger.info("Request to create container with existing name and properties: {}", containerStr);
                existingUnchangedContainers.add(existingContainer);
              } else {
                throw new AccountServiceException("There is a conflicting container in account " + accountName,
                    AccountServiceErrorCode.ResourceConflict);
              }
          }
        } else {
          resolvedContainers.add(
              new ContainerBuilder(container).setId(nextContainerId).setParentAccountId(account.getId()).build());
          ++nextContainerId;
        }
      } else {
        // existing container
        Container existingContainer = existingContainersInAccount.get(container.getName());
        if (existingContainer == null) {
          throw new AccountServiceException(
              "In account " + accountName + ", container " + container.getName() + " does not exist (containerId "
                  + container.getId() + " was supplied)", AccountServiceErrorCode.NotFound);
        } else if (existingContainer.getId() != container.getId()) {
          throw new AccountServiceException(
              "In account " + accountName + ", container " + container.getName() + " has containerId "
                  + existingContainer.getId() + " (" + container.getId() + " was supplied)",
              AccountServiceErrorCode.ResourceConflict);
        } else if (!config.ignoreVersionMismatch
            && existingContainer.getSnapshotVersion() != container.getSnapshotVersion()) {
          throw new AccountServiceException(
              "In account " + accountName + ", container " + container.getName() + " has version "
                  + existingContainer.getSnapshotVersion() + " (" + container.getSnapshotVersion() + " was supplied)",
              AccountServiceErrorCode.ResourceConflict);
        } else {
          resolvedContainers.add(container);
        }
      }
    }

    if (!resolvedContainers.isEmpty()) {
      updateResolvedContainers(account, resolvedContainers);
    }

    resolvedContainers.addAll(existingUnchangedContainers);
    return resolvedContainers;
  }

  @Override
  public DatasetVersionRecord addDatasetVersion(String accountName, String containerName, String datasetName,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled) throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      return mySqlAccountStore.addDatasetVersion(accountId, containerId, accountName, containerName, datasetName,
          version, timeToLiveInSeconds, creationTimeInMs, datasetVersionTtlEnabled);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public DatasetVersionRecord getDatasetVersion(String accountName, String containerName, String datasetName,
      String version) throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      return mySqlAccountStore.getDatasetVersion(accountId, containerId, accountName, containerName, datasetName,
          version);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public void deleteDatasetVersion(String accountName, String containerName, String datasetName,
      String version) throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      mySqlAccountStore.deleteDatasetVersion(accountId, containerId, datasetName, version);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public List<DatasetVersionRecord> getAllValidVersion(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      return mySqlAccountStore.getAllValidVersion(accountId, containerId, datasetName);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  protected void updateResolvedContainers(Account account, Collection<Container> resolvedContainers)
      throws AccountServiceException {
    try {
      if (config.enableNewDatabaseForMigration) {
        updateContainersWithMySqlStoreNew(account, resolvedContainers);
      } else {
        updateContainersWithMySqlStore(account, resolvedContainers);
      }
    } catch (SQLException e) {
      logger.error("Failed updating containers {} in MySql DB", resolvedContainers, e);
      handleSQLException(e);
    }
    // Tell the world
    publishChangeNotice();

    if (writeCacheAfterUpdate) {
      // Write containers to in-memory cache. Snapshot version will be out of date until the next DB refresh.
      if (config.enableNewDatabaseForMigration) {
        updateContainersInCacheNew(resolvedContainers);
      } else {
        updateContainersInCache(resolvedContainers);
      }
    }
  }

  /**
   * Gets the {@link Container} by its name and parent {@link Account} name by looking up in in-memory cache.
   * If it is not present in in-memory cache, it queries from mysql db and updates the cache.
   * @param accountName the name of account which container belongs to.
   * @param containerName the name of container to get.
   * @return {@link Container} if found in cache or mysql db. Else, returns {@code null}.
   * @throws AccountServiceException
   */
  @Override
  public Container getContainerByName(String accountName, String containerName) throws AccountServiceException {

    if (recentNotFoundContainersCache.contains(accountName + SEPARATOR + containerName)) {
      // If container was not found in recent get attempts, avoid another query to db and return null.
      return null;
    }

    if (config.enableNewDatabaseForMigration) {
      Account account = getAccountByNameNew(accountName);
      Container container = null;
      if (account != null) {
        container = account.getContainerByName(containerName);
      }
      if (account != null && container == null) {
        // If container is not present in the cache, query from mysql db
        try {
          container = mySqlAccountStoreNew.getContainerByName(account.getId(), containerName);
          if (container != null) {
            // write container to in-memory cache
            updateContainersInCacheNew(Collections.singletonList(container));
            logger.info("Container {} in Account {} is not found locally; Fetched from mysql db", containerName,
                accountName);
            accountServiceMetrics.onDemandContainerFetchCountNew.inc();
          } else {
            return getContainerByNameFromOldDbHelper(accountName, containerName);
          }
        } catch (SQLException e) {
          throw translateSQLException(e);
        }
        logger.trace("Found container from new db, container: " + containerName);
        return container;
      } else {
        return getContainerByNameFromOldDbHelper(accountName, containerName);
      }
    } else {
      return getContainerByNameFromOldDbHelper(accountName, containerName);
    }
  }

  /**
   * Get container from old cache or db.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @return the {@link Container}
   * @throws AccountServiceException
   */
  private Container getContainerByNameFromOldDbHelper(String accountName, String containerName) throws AccountServiceException {
    Account account = getAccountByName(accountName);
    if (account == null) {
      return null;
    }
    Container container = account.getContainerByName(containerName);
    if (container == null) {
      // If container is not present in the cache, query from mysql db
      try {
        container = mySqlAccountStore.getContainerByName(account.getId(), containerName);
        if (container != null) {
          // write container to in-memory cache
          updateContainersInCache(Collections.singletonList(container));
          logger.info("Container {} in Account {} is not found locally; Fetched from new mysql db", containerName,
              accountName);
          accountServiceMetrics.onDemandContainerFetchCount.inc();
        } else {
          // Add account_container to not-found LRU cache
          recentNotFoundContainersCache.add(accountName + SEPARATOR + containerName);
          logger.error("Container {} is not found in Account {}", containerName, accountName);
        }
      } catch (SQLException e) {
        throw translateSQLException(e);
      }
    }
    logger.trace("Found container from new db, container: " + containerName);
    return container;
  }

  /**
   * Gets the {@link Container} by its Id and parent {@link Account} Id by looking up in in-memory cache.
   * If it is not present in in-memory cache, it queries from mysql db and updates the cache.
   * @param accountId the name of account which container belongs to.
   * @param containerId the id of container to get.
   * @return {@link Container} if found in cache or mysql db. Else, returns {@code null}.
   * @throws AccountServiceException
   */
  @Override
  public Container getContainerById(short accountId, Short containerId) throws AccountServiceException {
    if (config.enableNewDatabaseForMigration) {
      Account account = getAccountByIdNew(accountId);
      Container container = null;
      if (account != null) {
        container = account.getContainerById(containerId);
      }
      if (account != null && container == null) {
        // If container is not present in the cache, query from mysql db
        try {
          container = mySqlAccountStoreNew.getContainerById(account.getId(), containerId);
          if (container != null) {
            // write container to in-memory cache
            updateContainersInCacheNew(Collections.singletonList(container));
            logger.info("Container Id {} in Account {} is not found locally, fetched from new mysql db", containerId,
                account.getName());
            accountServiceMetrics.onDemandContainerFetchCountNew.inc();
          }
        } catch (SQLException e) {
          throw translateSQLException(e);
        }
        logger.trace("Found container from new db, container: " + containerId);
        return container;
      } else {
        return getContainerByIdFromOldDbHelper(accountId, containerId);
      }
    } else {
      return getContainerByIdFromOldDbHelper(accountId, containerId);
    }
  }

  /**
   * Helper function to support get container id from old cache or db.
   * @param accountId the id of the account.
   * @param containerId the id of the container.
   * @return the {@link Container}
   * @throws AccountServiceException
   */
  private Container getContainerByIdFromOldDbHelper(short accountId, Short containerId) throws AccountServiceException {
    Account account = getAccountById(accountId);
    if (account == null) {
      return null;
    }
    Container container = account.getContainerById(containerId);
    if (container == null) {
      // If container is not present in the cache, query from mysql db
      try {
        container = mySqlAccountStore.getContainerById(accountId, containerId);
        if (container != null) {
          // write container to in-memory cache
          updateContainersInCache(Collections.singletonList(container));
          logger.info("Container Id {} in Account {} is not found locally, fetched from mysql db", containerId,
              account.getName());
          accountServiceMetrics.onDemandContainerFetchCount.inc();
        } else {
          logger.error("Container Id {} is not found in Account {}", containerId, account.getName());
          // Note: We are not using LRU cache for storing recent unsuccessful get attempts by container Ids since this
          // will be called in getBlob() path which should have valid account-container in most cases.
        }
      } catch (SQLException e) {
        throw translateSQLException(e);
      }
    }
    logger.trace("Found container from old db, container: " + containerId);
    return container;
  }

  @Override
  public void addDataset(Dataset dataset) throws AccountServiceException {
    try {
      String accountName = dataset.getAccountName();
      String containerName = dataset.getContainerName();
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      mySqlAccountStore.addDataset(accountId, containerId, dataset);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public void updateDataset(Dataset dataset) throws AccountServiceException {
    try {
      String accountName = dataset.getAccountName();
      String containerName = dataset.getContainerName();
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      mySqlAccountStore.updateDataset(accountId, containerId, dataset);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public Dataset getDataset(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException(
            "Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      return mySqlAccountStore.getDataset(accountId, containerId, accountName, containerName, datasetName);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  @Override
  public void deleteDataset(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    try {
      Container container = getContainerByName(accountName, containerName);
      if (container == null) {
        throw new AccountServiceException("Can't find the container: " + containerName + " in account: " + accountName,
            AccountServiceErrorCode.BadRequest);
      }
      short accountId = container.getParentAccountId();
      short containerId = container.getId();
      mySqlAccountStore.deleteDataset(accountId, containerId, datasetName);
    } catch (SQLException e) {
      throw translateSQLException(e);
    }
  }

  void updateAccountsWithMySqlStoreNew(Collection<Account> accounts) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating accounts={} into new MySql DB", accounts);

    // Get account and container changes info
    List<AccountUpdateInfo> accountsUpdateInfo = new ArrayList<>();

    for (Account account : accounts) {
      boolean isAccountAdded = false, isAccountUpdated = false;
      List<Container> addedContainers;
      List<Container> updatedContainers = new ArrayList<>();
      Account accountInCacheOld = getAccountByIdOld(account.getId());
      Account accountInCacheNew = getAccountByIdNew(account.getId());
      //only when the account in old cache but not exist in new cache, we need to upload account from old cache to new db
      //and refresh new cache. After migration is done, we can remove this line.
      if (accountInCacheNew == null && accountInCacheOld != null) {
        accountServiceMetrics.updateAccountFromOldCacheToNewDbCount.inc();
        uploadAccountFromOldCacheToNewDBAndRefreshNewCache(accountInCacheOld);
      }
      //refresh accountInCacheNew.
      accountInCacheNew = getAccountByIdNew(account.getId());
      //after above action, there's no chance the account exist in old db only.
      if (accountInCacheNew == null) {
        isAccountAdded = true;
        addedContainers = new ArrayList<>(account.getAllContainers());
      } else {
        if (!accountInCacheNew.equalsWithoutContainers(account)) {
          isAccountUpdated = true;
        }
        // Get list of added and updated containers in the account.
        Pair<List<Container>, List<Container>> addedOrUpdatedContainers =
            getUpdatedContainersNew(account, account.getAllContainers());
        addedContainers = addedOrUpdatedContainers.getFirst();
        updatedContainers = addedOrUpdatedContainers.getSecond();
      }
      accountsUpdateInfo.add(
          new AccountUpdateInfo(account, isAccountAdded, isAccountUpdated, addedContainers, updatedContainers));
    }
    mySqlAccountStoreNew.updateAccounts(accountsUpdateInfo);

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating accounts={} in new MySql DB, took time={} ms", accounts, timeForUpdate);
    accountServiceMetrics.updateAccountTimeInMsNew.update(timeForUpdate);
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
        if (!accountInCache.equalsWithoutContainers(account)) {
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
   * Updates MySql DB with added or modified {@link Container}s of a given account
   * @param account parent {@link Account} for the {@link Container}s
   * @param containers collection of {@link Container}s
   * @throws SQLException
   */
  private void updateContainersWithMySqlStoreNew(Account account, Collection<Container> containers) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating containers={} for accountId={} into new MySql DB", containers, account.getId());

    // Get list of added and updated containers in the account.
    Pair<List<Container>, List<Container>> addedOrUpdatedContainers = getUpdatedContainersNew(account, containers);
    AccountUpdateInfo accountUpdateInfo =
        new AccountUpdateInfo(account, false, false, addedOrUpdatedContainers.getFirst(),
            addedOrUpdatedContainers.getSecond());

    // if container only exist in old db, upload to

    // Write changes to MySql db.
    mySqlAccountStoreNew.updateAccounts(Collections.singletonList(accountUpdateInfo));

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating containers={} for accountId={} in MySqlDB in time={} ms", containers,
        account.getId(), timeForUpdate);
    accountServiceMetrics.updateAccountTimeInMsNew.update(timeForUpdate);
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
   * Gets lists of added and updated {@link Container}s in the {@link Account} by comparing with in-memory cache.
   * @param account Id of the account
   * @param containers {@link Container}s added or updated in the account.
   * @return {@link Pair} of lists of added and updated {@link Container}s in the given {@link Account}
   */
  private Pair<List<Container>, List<Container>> getUpdatedContainersNew(Account account,
      Collection<Container> containers) {
    Account accountInCacheNew = getAccountByIdNew(account.getId());

    if (accountInCacheNew == null) {
      throw new IllegalArgumentException("Account with ID " + account + "doesn't exist in new cache after reload");
    }

    List<Container> addedContainers = new ArrayList<>();
    List<Container> updatedContainers = new ArrayList<>();
    for (Container containerToUpdate : containers) {
      Container containerInCache = accountInCacheNew.getContainerById(containerToUpdate.getId());
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

  private void uploadAccountFromOldCacheToNewDBAndRefreshNewCache(Account account) throws SQLException {
    try {
      logger.trace("Upload account from old cache to new db for account: " + account);
      List <Container> addedContainers = new ArrayList<>(account.getAllContainers());
      List<AccountUpdateInfo> accountsUpdateInfo = new ArrayList<>();
      accountsUpdateInfo.add(
          new AccountUpdateInfo(account, true, false, addedContainers, new ArrayList<>()));
      mySqlAccountStoreNew.updateAccounts(accountsUpdateInfo);
      logger.trace("Completed updating account={} in new MySql DB", account);
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in new MySql DB", account, e);
      throw e;
    }
    // Tell the world
    publishChangeNotice();
    if (writeCacheAfterUpdate) {
      // Write accounts to in-memory cache. Snapshot version will be out of date until the next DB refresh.
      updateAccountsInCacheNew(Collections.singletonList(account));
    }
  }

  /**
   * Updates {@link Account}s to in-memory cache.
   * @param accounts added or modified {@link Account}s
   */
  private void updateAccountsInCache(Collection<Account> accounts) {
    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRef.get().addOrUpdateAccounts(accounts);
    } finally {
      infoMapLock.writeLock().unlock();
    }
  }

  /**
   * Updates {@link Account}s to in-memory cache.
   * @param accounts added or modified {@link Account}s
   */
  private void updateAccountsInCacheNew(Collection<Account> accounts) {
    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRefNew.get().addOrUpdateAccounts(accounts);
    } finally {
      infoMapLock.writeLock().unlock();
    }
  }

  /**
   * Updates {@link Container}s to in-memory cache.
   * @param containers added or modified {@link Container}s
   */
  private void updateContainersInCache(Collection<Container> containers) {

    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRef.get().addOrUpdateContainers(containers);
    } finally {
      infoMapLock.writeLock().unlock();
    }

    // Remove containers from not-found LRU cache.
    for (Container container : containers) {
      recentNotFoundContainersCache.remove(
          getAccountById(container.getParentAccountId()).getName() + SEPARATOR + container.getName());
    }
  }

  /**
   * Updates {@link Container}s to in-memory cache.
   * @param containers added or modified {@link Container}s
   */
  private void updateContainersInCacheNew(Collection<Container> containers) {

    infoMapLock.writeLock().lock();
    try {
      accountInfoMapRefNew.get().addOrUpdateContainers(containers);
    } finally {
      infoMapLock.writeLock().unlock();
    }

    // Remove containers from not-found LRU cache.
    for (Container container : containers) {
      recentNotFoundContainersCache.remove(
          getAccountById(container.getParentAccountId()).getName() + SEPARATOR + container.getName());
    }
  }

  /**
   * @return the time in seconds since the last database sync / cache refresh
   */
  public int getTimeInSecondsSinceLastSync() {
    return lastSyncTime > 0 ? (int) (System.currentTimeMillis() - lastSyncTime) / 1000 : Integer.MAX_VALUE;
  }

  /**
   * @return the time in seconds since the last database sync / cache refresh
   */
  public int getTimeInSecondsSinceLastSyncNew() {
    return lastSyncTimeNew > 0 ? (int) (System.currentTimeMillis() - lastSyncTimeNew) / 1000 : Integer.MAX_VALUE;
  }

  /**
   * @return the total number of containers in all accounts.
   */
  public int getContainerCount() {
    return accountInfoMapRef.get().getContainerCount();
  }

  /**
   * @return the total number of containers in all accounts.
   */
  public int getContainerCountNew() {
    return accountInfoMapRefNew.get().getContainerCount();
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
      if (config.enableNewDatabaseForMigration) {
        needRefreshNew = true;
      } else {
        needRefresh = true;
      }
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
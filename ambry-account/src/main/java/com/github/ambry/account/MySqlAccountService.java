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
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final boolean needRefresh = false;
  private final boolean needRefreshNew = false;
  private long lastSyncTime = -1;
  private long lastSyncTimeNew = -1;
  private final Set<String> recentNotFoundContainersCache;
  private Set<String> recentNotFoundContainersCacheNew = null;
  private final AccountMigrationService accountMigrationService;
  private AccountMigrationService accountMigrationServiceNew;
  private AtomicReference<AccountInfoMap> accountInfoMapRefNew;

  public MySqlAccountService(AccountServiceMetricsWrapper accountServiceMetrics, MySqlAccountServiceConfig config,
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
    accountServiceMetrics.trackTimeSinceLastSync(this::getTimeInSecondsSinceLastSync, this::getTimeInSecondsSinceLastSyncNew);
    accountServiceMetrics.trackContainerCount(this::getContainerCount, this::getContainerCountNew);
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

    accountMigrationService =
        new AccountMigrationService(backupFileManager, mySqlAccountStore, needRefresh, lastSyncTime,
            recentNotFoundContainersCache, accountInfoMapRef, accountServiceMetrics.getAccountServiceMetrics(),
            infoMapLock, config, notifier, scheduler);

    if (config.enableNewDatabaseForMigration) {
      backupFileManagerNew =
          new BackupFileManager(accountServiceMetrics, config.backupDirNew, config.maxBackupFileCount);
      recentNotFoundContainersCacheNew = Collections.newSetFromMap(
          Collections.synchronizedMap(new LinkedHashMap<String, Boolean>(cacheInitialCapacity, cacheLoadFactor, true) {
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
              return size() > cacheMaxLimit;
            }
          }));
      accountInfoMapRefNew = new AtomicReference<>(new AccountInfoMap(accountServiceMetrics.getAccountServiceMetricsNew()));
      accountMigrationServiceNew =
          new AccountMigrationService(backupFileManagerNew, mySqlAccountStoreNew, needRefreshNew, lastSyncTimeNew,
              recentNotFoundContainersCacheNew, accountInfoMapRefNew,
              accountServiceMetrics.getAccountServiceMetricsNew(), infoMapLock, config, notifier, scheduler);
    }

    // Initialize cache from backup file on disk
    accountMigrationService.initCacheFromBackupFile();
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.initCacheFromBackupFile();
    }
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
    accountMigrationService.fetchAndUpdateCacheHelper();

    if (config.enableNewDatabaseForMigration) {
      if (mySqlAccountStoreNew == null) {
        try {
          mySqlAccountStoreNew = mySqlAccountStoreFactory.getMySqlAccountStoreNew();
        } catch (SQLException e) {
          logger.error("MySQL account store creation failed: {}", e.getMessage());
          throw e;
        }
      }
      accountMigrationServiceNew.fetchAndUpdateCacheHelper();
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    return accountMigrationService.getAccountById(accountId);
  }

  @Override
  public Account getAccountByName(String accountName) {
    return accountMigrationService.getAccountByName(accountName);
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return accountMigrationService.addAccountUpdateConsumer(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return accountMigrationService.removeAccountUpdateConsumer(accountUpdateConsumer);
  }

  /**
   * This method is used for uploading data to new db for testing purpose only.
   */
  protected Account getAccountByIdNew(short accountId) {
    return accountMigrationServiceNew.getAccountById(accountId);
  }

  /**
   * This method is used for uploading data to new db for testing purpose only.
   */
  protected Account getAccountByIdOld(short accountId) {
    return accountMigrationService.getAccountById(accountId);
  }

  /**
   * This method is used for uploading data to new db for testing purpose only.
   */
  protected Account getAccountByNameNew(String accountName) {
    return accountMigrationServiceNew.getAccountByName(accountName);
  }

  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.updateAccounts(accounts);
      accountMigrationService.updateAccounts(accounts);
    } else {
      accountMigrationService.updateAccounts(accounts);
    }
  }

  @Override
  public Collection<Account> getAllAccounts() {
    return accountMigrationService.getAllAccountsHelper();
  }

  @Override
  public void close() throws IOException {
    accountMigrationService.close();
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.close();
    }
  }

  @Override
  protected void checkOpen() {
    accountMigrationService.checkOpen();
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.checkOpen();
    }
  }

  ExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  protected void publishChangeNotice() {
    // TODO: can optimize this by sending the account/container payload as the topic message,
    // so subscribers can update their cache and avoid the database query.
    accountMigrationService.publishChangeNotice();
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.publishChangeNotice();
    }
  }

  @Override
  protected void onAccountChangeMessage(String topic, String message) {
   accountMigrationService.onAccountChangeMessage(topic, message);
   if (config.enableNewDatabaseForMigration) {
     accountMigrationServiceNew.onAccountChangeMessage(topic, message);
   }
  }

  @Override
  public Set<Container> getContainersByStatus(Container.ContainerStatus containerStatus) {
    return accountMigrationService.getContainersByStatus(containerStatus);
  }

  @Override
  public void selectInactiveContainersAndMarkInStore(AggregatedAccountStorageStats aggregatedAccountStorageStats) {
    accountMigrationService.selectInactiveContainersAndMarkInStore(aggregatedAccountStorageStats);
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.selectInactiveContainersAndMarkInStore(aggregatedAccountStorageStats);
    }
  }

  @Override
  public Collection<Container> updateContainers(String accountName, Collection<Container> containers)
      throws AccountServiceException {
      if (config.enableNewDatabaseForMigration) {
        try {
          if (mySqlAccountStoreNew == null) {
            try {
              mySqlAccountStoreNew = mySqlAccountStoreFactory.getMySqlAccountStoreNew();
            } catch (SQLException e) {
              logger.error("MySQL account store creation failed: {}", e.getMessage());
              throw translateSQLException(e);
            }
          }
          accountMigrationServiceNew.updateContainers(accountName, containers);
        } catch (AccountServiceException e) {
          // if before migration, update container for new db would fail due to the account does not exist.
          logger.trace("This is expected due to the account {} does not exist in new db", accountName);
        }
      }
      if (mySqlAccountStore == null) {
        try {
          mySqlAccountStore = mySqlAccountStoreFactory.getMySqlAccountStore();
        } catch (SQLException e) {
          logger.error("MySQL account store creation failed: {}", e.getMessage());
          throw translateSQLException(e);
        }
      }
      return accountMigrationService.updateContainers(accountName, containers);
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
    accountMigrationService.updateResolvedContainers(account, resolvedContainers);
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.updateResolvedContainers(account, resolvedContainers);
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
    return accountMigrationService.getContainerByNameHelper(accountName, containerName);
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
    return accountMigrationService.getContainerByIdHelper(accountId, containerId);
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

  /**
   * Updates MySql DB with added or modified {@link Account}s
   * @param accounts collection of {@link Account}s
   * @throws SQLException
   */
  void updateAccountsWithMySqlStore(Collection<Account> accounts) throws SQLException {
    accountMigrationService.updateAccountsWithMySqlStore(accounts);
    if (config.enableNewDatabaseForMigration) {
      accountMigrationServiceNew.updateAccountsWithMySqlStore(accounts);
    }
  }

  /**
   * @return the time in seconds since the last database sync / cache refresh
   */
  public int getTimeInSecondsSinceLastSync() {
    return accountMigrationService.getTimeInSecondsSinceLastSync();
  }

  public int getTimeInSecondsSinceLastSyncNew() {
    return accountMigrationService.getTimeInSecondsSinceLastSync();
  }

  /**
   * @return the total number of containers in all accounts.
   */
  public int getContainerCount() {
    return accountMigrationService.getContainerCount();
  }

  public int getContainerCountNew() {
    return accountMigrationServiceNew.getContainerCount();
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
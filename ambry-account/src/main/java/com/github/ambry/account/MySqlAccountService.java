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
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.protocol.DatasetVersionState;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;


/**
 * An implementation of {@link AccountService} that employs MySql database as its underlying storage.
 */
public class MySqlAccountService extends AbstractAccountService {

  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountService.class);
  static final String MYSQL_ACCOUNT_UPDATER_PREFIX = "mysql-account-updater";
  private final MySqlAccountServiceConfig config;
  // lock to protect in-memory metadata cache
  private final ScheduledExecutorService scheduler;
  private volatile MySqlAccountStore mySqlAccountStore;
  private final CachedAccountService cachedAccountService;

  public MySqlAccountService(AccountServiceMetricsWrapper accountServiceMetricsWrapper,
      MySqlAccountServiceConfig config, MySqlAccountStoreFactory mySqlAccountStoreFactory, Notifier<String> notifier)
      throws IOException, SQLException {
    super(config, Objects.requireNonNull(accountServiceMetricsWrapper.getAccountServiceMetrics(),
        "accountServiceMetrics cannot be null"), notifier);
    this.config = config;
    this.scheduler =
        config.updaterPollingIntervalSeconds > 0 ? Utils.newScheduler(1, MYSQL_ACCOUNT_UPDATER_PREFIX, false) : null;
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
    cachedAccountService = new CachedAccountService(mySqlAccountStore,
        callSupplierWithException(mySqlAccountStoreFactory::getMySqlAccountStore),
        accountServiceMetricsWrapper.getAccountServiceMetrics(), config, notifier, config.backupDir, scheduler);
  }

  /**
   * @return set (LRA cache) of containers not found in recent get attempts. Used in only tests.
   */
  Set<String> getRecentNotFoundContainersCache() {
    return cachedAccountService.getRecentNotFoundContainersCache();
  }

  /**
   * Fetches all the accounts and containers that have been created or modified in the mysql database since the
   * last modified/sync time and loads into in-memory {@link AccountInfoMap}.
   */
  synchronized void fetchAndUpdateCache() throws SQLException {
    cachedAccountService.fetchAndUpdateCache();
  }

  @Override
  public Account getAccountById(short accountId) {
    return cachedAccountService.getAccountById(accountId);
  }

  @Override
  public Account getAccountByName(String accountName) {
    return cachedAccountService.getAccountByName(accountName);
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return cachedAccountService.addAccountUpdateConsumer(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return cachedAccountService.removeAccountUpdateConsumer(accountUpdateConsumer);
  }

  protected CachedAccountService getCachedAccountService() {
    return cachedAccountService;
  }

  @Override
  public void updateAccounts(Collection<Account> accounts) throws AccountServiceException {
    cachedAccountService.updateAccounts(accounts);
  }

  @Override
  public Collection<Account> getAllAccounts() {
    return cachedAccountService.getAllAccountsHelper();
  }

  @Override
  public void close() throws IOException {
    if (scheduler != null) {
      shutDownExecutorService(scheduler, config.updaterShutDownTimeoutMinutes, TimeUnit.MINUTES);
    }
    cachedAccountService.close();
  }

  @Override
  protected void checkOpen() {
    cachedAccountService.checkOpen();
  }

  ExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  protected void onAccountChangeMessage(String topic, String message) {
  }

  @Override
  public Set<Container> getContainersByStatus(Container.ContainerStatus containerStatus) {
    return cachedAccountService.getContainersByStatus(containerStatus);
  }

  @Override
  public void selectInactiveContainersAndMarkInStore(AggregatedAccountStorageStats aggregatedAccountStorageStats) {
    cachedAccountService.selectInactiveContainersAndMarkInStore(aggregatedAccountStorageStats);
  }

  @Override
  public Collection<Container> updateContainers(String accountName, Collection<Container> containers)
      throws AccountServiceException {
    return cachedAccountService.updateContainers(accountName, containers);
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
    return cachedAccountService.getContainerByNameHelper(accountName, containerName);
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
    return cachedAccountService.getContainerByIdHelper(accountId, containerId);
  }

  @FunctionalInterface
  public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
  }

  /**
   * call supplier to get the {@link MySqlAccountStore}
   */
  private Supplier<MySqlAccountStore> callSupplierWithException(ThrowingSupplier<MySqlAccountStore, SQLException> ts) {
    return () -> {
      try {
        return ts.get();
      } catch (SQLException e) {
        logger.error("MySQL account store creation failed: {}", e.getMessage());
        throw new RuntimeException("MySQL account store creation failed: {}", e);
      }
    };
  }

  @Override
  public void addDataset(Dataset dataset) throws AccountServiceException {
    cachedAccountService.addDataset(dataset);
  }

  @Override
  public void updateDataset(Dataset dataset) throws AccountServiceException {
    cachedAccountService.updateDataset(dataset);
  }

  @Override
  public Dataset getDataset(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    return cachedAccountService.getDataset(accountName, containerName, datasetName);
  }

  @Override
  public void deleteDataset(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    cachedAccountService.deleteDataset(accountName, containerName, datasetName);
  }

  @Override
  public DatasetVersionRecord addDatasetVersion(String accountName, String containerName, String datasetName,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled,
      DatasetVersionState datasetVersionState) throws AccountServiceException {
    return cachedAccountService.addDatasetVersion(accountName, containerName, datasetName, version, timeToLiveInSeconds,
        creationTimeInMs, datasetVersionTtlEnabled, datasetVersionState);
  }

  @Override
  public void updateDatasetVersionState(String accountName, String containerName, String datasetName, String version,
      DatasetVersionState datasetVersionState) throws AccountServiceException {
    cachedAccountService.updateDatasetVersionState(accountName, containerName, datasetName, version,
        datasetVersionState);
  }

  @Override
  public DatasetVersionRecord getDatasetVersion(String accountName, String containerName, String datasetName,
      String version) throws AccountServiceException {
    return cachedAccountService.getDatasetVersion(accountName, containerName, datasetName, version);
  }

  @Override
  public void deleteDatasetVersion(String accountName, String containerName, String datasetName, String version)
      throws AccountServiceException {
    cachedAccountService.deleteDatasetVersion(accountName, containerName, datasetName, version);
  }

  @Override
  public void updateDatasetVersionTtl(String accountName, String containerName, String datasetName,
      String version) throws AccountServiceException {
    cachedAccountService.updateDatasetVersionTtl(accountName, containerName, datasetName, version);
  }

  @Override
  public List<DatasetVersionRecord> getAllValidVersionsOutOfRetentionCount(String accountName, String containerName,
      String datasetName) throws AccountServiceException {
    return cachedAccountService.getAllValidVersionsOutOfRetentionCount(accountName, containerName, datasetName);
  }

  @Override
  public List<DatasetVersionRecord> getAllValidVersionForDatasetDeletion(String accountName, String containerName, String datasetName)
      throws AccountServiceException {
    return cachedAccountService.getAllValidVersionForDatasetDeletion(accountName, containerName, datasetName);
  }

  @Override
  public Page<String> listAllValidDatasets(String accountName, String containerName, String pageToken)
      throws AccountServiceException {
    return cachedAccountService.listAllValidDatasets(accountName, containerName, pageToken);
  }

  @Override
  public Page<String> listAllValidDatasetVersions(String accountName, String containerName, String datasetName,
      String pageToken) throws AccountServiceException {
    return cachedAccountService.listAllValidDatasetVersions(accountName, containerName, datasetName, pageToken);
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
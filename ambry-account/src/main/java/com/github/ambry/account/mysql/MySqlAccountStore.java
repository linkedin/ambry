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
package com.github.ambry.account.mysql;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.Container;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.protocol.DatasetVersionState;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;


/**
 * Wrapper class to handle MySql store operations on Account and Container tables
 */
public class MySqlAccountStore {

  private final AccountDao accountDao;
  private final DatasetDao datasetDao;
  private final MySqlDataAccessor mySqlDataAccessor;
  private final MySqlAccountServiceConfig config;

  /**
   * Constructor.
   * @param dbEndpoints MySql DB end points
   * @param localDatacenter name of the local data center
   * @param metrics metrics to track mysql operations
   * @param config configuration associated with {@link com.github.ambry.account.MySqlAccountService}
   * @throws SQLException
   */
  public MySqlAccountStore(List<MySqlUtils.DbEndpoint> dbEndpoints, String localDatacenter, MySqlMetrics metrics,
      MySqlAccountServiceConfig config) throws SQLException {
    mySqlDataAccessor = new MySqlDataAccessor(dbEndpoints, localDatacenter, metrics);
    accountDao = new AccountDao(mySqlDataAccessor);
    datasetDao = new DatasetDao(mySqlDataAccessor, config, metrics);
    this.config = config;
  }

  /**
   * Used for tests.
   * @return {@link MySqlDataAccessor}
   */
  public MySqlDataAccessor getMySqlDataAccessor() {
    return mySqlDataAccessor;
  }

  /**
   * Gets all {@link Account}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Account}s
   * @throws SQLException
   */
  public synchronized Collection<Account> getNewAccounts(long updatedSince) throws SQLException {
    return accountDao.getNewAccounts(updatedSince);
  }

  /**
   * Gets all {@link Container}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public synchronized Collection<Container> getNewContainers(long updatedSince) throws SQLException {
    return accountDao.getNewContainers(updatedSince);
  }

  /**
   * Gets all {@link Container}s of a given account
   * @param accountId ID of the account
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public synchronized Collection<Container> getContainersByAccount(short accountId) throws SQLException {
    return accountDao.getContainers(accountId);
  }

  /**
   * Gets container by its name and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerName name of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerByName(int accountId, String containerName) throws SQLException {
    return accountDao.getContainerByName(accountId, containerName);
  }

  /**
   * Gets container by its Id and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerById(int accountId, int containerId) throws SQLException {
    return accountDao.getContainerById(accountId, containerId);
  }

  /**
   * Adds/Updates accounts and their containers to the database in batches atomically using transaction.
   * @param accountsInfo information of updated Accounts
   * @throws SQLException
   */
  public synchronized void updateAccounts(List<AccountUpdateInfo> accountsInfo) throws SQLException {
    accountDao.updateAccounts(accountsInfo, config.dbExecuteBatchSize);
  }

  /**
   * Add dataset to the database.
   * @param accountId the id of the {@link Account}.
   * @param containerId the id of the {@link Container}
   * @param dataset the {@link Dataset}.
   * @throws SQLException
   */
  public synchronized void addDataset(short accountId, short containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    datasetDao.addDataset(accountId, containerId, dataset);
  }

  /**
   * Update dataset to the database.
   * @param accountId the id of the {@link Account}
   * @param containerId the id of the {@link Container}
   * @param dataset the {@link Dataset}.
   * @throws SQLException
   */
  public synchronized void updateDataset(short accountId, short containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    datasetDao.updateDataset(accountId, containerId, dataset);
  }

  /**
   * Get dataset from the database.
   * @param accountId the id of the {@link Account}.
   * @param containerId the id of the {@link Container}
   * @param accountName the name of the {@link Account}.
   * @param containerName the name of the {@link Container}
   * @param datasetName the name of the dataset.
   * @return the {@link Dataset}
   * @throws SQLException
   */
  public synchronized Dataset getDataset(short accountId, short containerId, String accountName, String containerName,
      String datasetName) throws SQLException, AccountServiceException {
    return datasetDao.getDataset(accountId, containerId, accountName, containerName, datasetName);
  }

  /**
   * Delete dataset from the database.
   * @param accountId the id of the {@link Account}.
   * @param containerId the id of the {@link Container}
   * @param datasetName the name of the dataset.
   * @throws AccountServiceException
   * @throws SQLException
   */
  public synchronized void deleteDataset(short accountId, short containerId, String datasetName)
      throws AccountServiceException, SQLException {
    datasetDao.deleteDataset(accountId, containerId, datasetName);
  }

  /**
   * Add a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return the corresponding {@link Dataset}
   * @throws SQLException
   */
  public synchronized DatasetVersionRecord addDatasetVersion(int accountId, int containerId, String accountName,
      String containerName, String datasetName, String version, long timeToLiveInSeconds, long creationTimeInMs,
      boolean datasetVersionTtlEnabled, DatasetVersionState datasetVersionState) throws SQLException, AccountServiceException {
    return datasetDao.addDatasetVersions(accountId, containerId, accountName, containerName, datasetName, version,
        timeToLiveInSeconds, creationTimeInMs, datasetVersionTtlEnabled, datasetVersionState);
  }

  public synchronized void updateDatasetVersionState(int accountId, int containerId, String accountName, String containerName,
      String datasetName, String version, DatasetVersionState datasetVersionState)
      throws SQLException, AccountServiceException {
    datasetDao.updateDatasetVersionState(accountId, containerId, accountName, containerName, datasetName, version,
        datasetVersionState);
  }

  /**
   * Get a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @return the {@link DatasetVersionRecord}
   * @throws SQLException
   */
  public synchronized DatasetVersionRecord getDatasetVersion(short accountId, short containerId, String accountName,
      String containerName, String datasetName, String version) throws SQLException, AccountServiceException {
    return datasetDao.getDatasetVersions(accountId, containerId, accountName, containerName, datasetName, version);
  }

  /**
   * Delete a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @throws SQLException
   * @throws AccountServiceException
   */
  public synchronized void deleteDatasetVersion(short accountId, short containerId, String datasetName, String version)
      throws SQLException, AccountServiceException {
    datasetDao.deleteDatasetVersion(accountId, containerId, datasetName, version);
  }

  /**
   * Update ttl for a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   */
  public synchronized void updateDatasetVersionTtl(short accountId, short containerId, String accountName,
      String containerName, String datasetName, String version) throws SQLException, AccountServiceException {
    datasetDao.updateDatasetVersionTtl(accountId, containerId, accountName, containerName, datasetName, version);
  }

  /**
   * Get a list of dataset versions which is not expired or been deleted for specific dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return a list of dataset versions which is not expired for specific dataset.
   * @throws SQLException
   */
  public synchronized List<DatasetVersionRecord> getAllValidVersionForDatasetDeletion(short accountId, short containerId,
      String datasetName) throws SQLException, AccountServiceException {
    return datasetDao.getAllValidVersionForDatasetDeletion(accountId, containerId, datasetName);
  }

  /**
   * Get all valid datasets of the container start with page token.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param pageToken the start page token, if it's null, will start at beginning.
   * @return the page of the dataset names.
   * @throws SQLException
   */
  public synchronized Page<String> listAllValidDatasets(short accountId, short containerId, String pageToken) throws SQLException {
    return datasetDao.listAllValidDatasets(accountId, containerId, pageToken);
  }

  /**
   * Get all valid dataset versions of the dataset with page token.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the dataset name.
   * @param pageToken the start page token, if it's null, will start at beginning.
   * @return the page of the dataset versions.
   * @throws SQLException
   * @throws AccountServiceException
   */
  public synchronized Page<String> listAllValidDatasetVersions(short accountId, short containerId,
      String datasetName, String pageToken) throws SQLException, AccountServiceException {
    return datasetDao.listAllValidDatasetVersions(accountId, containerId, datasetName, pageToken);
  }

  /**
   * Get all versions from a dataset which has not expired and out of retentionCount by checking the last modified time.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @return a list of {@link DatasetVersionRecord}
   * @throws SQLException
   */
  public synchronized List<DatasetVersionRecord> getAllValidVersionsOutOfRetentionCount(short accountId, short containerId,
      String accountName, String containerName, String datasetName) throws SQLException, AccountServiceException {
     return datasetDao.getAllValidVersionsOutOfRetentionCount(accountId, containerId, accountName, containerName, datasetName);
  }

  /**
   * Helper method to close the active connection, if there is one.
   */
  public void closeConnection() {
    if (mySqlDataAccessor != null) {
      mySqlDataAccessor.closeActiveConnection();
    }
  }
}

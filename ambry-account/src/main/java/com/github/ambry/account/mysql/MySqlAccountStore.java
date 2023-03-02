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
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;


/**
 * Wrapper class to handle MySql store operations on Account and Container tables
 */
public class MySqlAccountStore {

  private final AccountDao accountDao;
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
    accountDao = new AccountDao(mySqlDataAccessor, config);
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
   * Adds/Updates accounts and their containers to the database in batches atomically using transaction.
   * @param accountsInfo information of updated Accounts
   * @throws SQLException
   */
  public void updateAccounts(List<AccountUpdateInfo> accountsInfo) throws SQLException {
    accountDao.updateAccounts(accountsInfo, config.dbExecuteBatchSize);
  }

  /**
   * Add dataset to the database.
   * @param accountId the id of the {@link Account}.
   * @param containerId the id of the {@link Container}
   * @param dataset the {@link Dataset}.
   * @throws SQLException
   */
  public void addDataset(short accountId, short containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    accountDao.addDataset(accountId, containerId, dataset);
  }

  /**
   * Update dataset to the database.
   * @param accountId the id of the {@link Account}
   * @param containerId the id of the {@link Container}
   * @param dataset the {@link Dataset}.
   * @throws SQLException
   */
  public void updateDataset(short accountId, short containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    accountDao.updateDataset(accountId, containerId, dataset);
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
  public Dataset getDataset(short accountId, short containerId, String accountName, String containerName,
      String datasetName) throws SQLException, AccountServiceException {
    return accountDao.getDataset(accountId, containerId, accountName, containerName, datasetName);
  }

  /**
   * Gets all {@link Account}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Account}s
   * @throws SQLException
   */
  public Collection<Account> getNewAccounts(long updatedSince) throws SQLException {
    return accountDao.getNewAccounts(updatedSince);
  }

  /**
   * Gets all {@link Container}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public Collection<Container> getNewContainers(long updatedSince) throws SQLException {
    return accountDao.getNewContainers(updatedSince);
  }

  /**
   * Gets all {@link Container}s of a given account
   * @param accountId ID of the account
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public Collection<Container> getContainersByAccount(short accountId) throws SQLException {
    return accountDao.getContainers(accountId);
  }

  /**
   * Gets container by its name and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerName name of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public Container getContainerByName(int accountId, String containerName) throws SQLException {
    return accountDao.getContainerByName(accountId, containerName);
  }

  /**
   * Gets container by its Id and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public Container getContainerById(int accountId, int containerId) throws SQLException {
    return accountDao.getContainerById(accountId, containerId);
  }

  /**
   * Add a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @param expirationTimeMs the expiration time of the version of the dataset.
   * @return the corresponding {@link Dataset}
   * @throws SQLException
   */
  public DatasetVersionRecord addDatasetVersion(int accountId, int containerId, String accountName, String containerName,
      String datasetName, String version, long expirationTimeMs) throws SQLException, AccountServiceException {
    return accountDao.addDatasetVersions(accountId, containerId, accountName, containerName, datasetName, version,
        expirationTimeMs);
  }

  /**
   * Get a version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @return the {@link DatasetVersionRecord}
   * @throws SQLException
   */
  public DatasetVersionRecord getDatasetVersion(short accountId, short containerId, String datasetName,
      String version) throws SQLException, AccountServiceException {
    return  accountDao.getDatasetVersions(accountId, containerId, datasetName, version);
  }

  /**
   * Get the latest version value of the dataset versions.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return the latest version of the dataset.
   * @throws SQLException
   */
  public long getLatestVersion(short accountId, short containerId, String datasetName)
      throws SQLException, AccountServiceException {
    return accountDao.getLatestVersion(accountId, containerId, datasetName);
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

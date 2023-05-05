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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;
import static com.github.ambry.utils.Utils.*;


/**
 * Account Data Access Object.
 */
public class AccountDao {

  private final MySqlDataAccessor dataAccessor;
  private final MySqlAccountServiceConfig mySqlAccountServiceConfig;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final long MAX_TIMESTAMP_MONOTONIC_VERSION_VALUE = Long.MAX_VALUE;
  private static final long MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE = 999L;
  private static final String LATEST = "LATEST";
  private static final String MAJOR = "MAJOR";
  private static final String MINOR = "MINOR";
  private static final String PATCH = "PATCH";

  // Account table fields
  public static final String ACCOUNT_TABLE = "Accounts";
  public static final String ACCOUNT_INFO = "accountInfo";
  public static final String ACCOUNT_ID = "accountId";

  // Container table fields
  public static final String CONTAINER_TABLE = "Containers";
  public static final String CONTAINER_ID = "containerId";
  public static final String CONTAINER_NAME = "containerName";
  public static final String CONTAINER_INFO = "containerInfo";

  // Dataset table fields
  public static final String DATASET_TABLE = "Datasets";
  public static final String DATASET_NAME = "datasetName";
  public static final String VERSION_SCHEMA = "versionSchema";
  public static final String RETENTION_COUNT = "retentionCount";
  public static final String RETENTION_TIME_IN_SECONDS = "retentionTimeInSeconds";
  public static final String USER_TAGS = "userTags";
  public static final String DELETE_TS = "delete_ts";

  // Dataset version table fields
  public static final String DATASET_VERSION_TABLE = "DatasetVersions";

  // Common fields
  public static final String VERSION = "version";
  public static final String CREATION_TIME = "creationTime";
  public static final String LAST_MODIFIED_TIME = "lastModifiedTime";

  // Account table query strings
  private final String insertAccountsSql;
  private final String getAccountsSinceSql;
  private final String updateAccountsSql;

  // Container table query strings
  private final String insertContainersSql;
  private final String updateContainersSql;
  private final String getContainersSinceSql;
  private final String getContainersByAccountSql;
  private final String getContainerByNameSql;
  private final String getContainerByIdSql;

  // Dataset version table query strings.
  private final String insertDatasetVersionSql;
  private final String getDatasetVersionByNameSql;
  private final String deleteDatasetVersionByIdSql;
  private final String updateDatasetVersionIfExpiredSql;
  private final String listLatestVersionSqlForUpload;
  private final String getLatestVersionSqlForDownload;
  private final String listValidVersionSql;
  private final String listVersionByModifiedTimeSql;

  // Dataset table query strings
  private final String insertDatasetSql;
  private final String getDatasetByNameSql;
  private final String getVersionSchemaSql;
  private final String updateDatasetSql;
  private final String updateDatasetIfExpiredSql;
  private final String deleteDatasetByIdSql;
  /**
   * Types of MySql statements.
   */
  public enum StatementType {
    Select, Insert, Update, Delete
  }

  public AccountDao(MySqlDataAccessor dataAccessor, MySqlAccountServiceConfig mySqlAccountServiceConfig) {
    this.dataAccessor = dataAccessor;
    this.mySqlAccountServiceConfig = mySqlAccountServiceConfig;
    insertAccountsSql =
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(3), now(3))", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getAccountsSinceSql =
        String.format("select %s, %s, %s from %s where %s > ?", ACCOUNT_INFO, VERSION, LAST_MODIFIED_TIME,
            ACCOUNT_TABLE, LAST_MODIFIED_TIME);
    updateAccountsSql =
        String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? ", ACCOUNT_TABLE, ACCOUNT_INFO, VERSION,
            LAST_MODIFIED_TIME, ACCOUNT_ID);
    insertContainersSql =
        String.format("insert into %s (%s, %s, %s, %s, %s) values (?, ?, ?, now(3), now(3))", CONTAINER_TABLE,
            ACCOUNT_ID, CONTAINER_INFO, VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getContainersSinceSql =
        String.format("select %s, %s, %s, %s from %s where %s > ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, LAST_MODIFIED_TIME);
    getContainersByAccountSql =
        String.format("select %s, %s, %s, %s from %s where %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID);
    updateContainersSql =
        String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? AND %s = ? ", CONTAINER_TABLE,
            CONTAINER_INFO, VERSION, LAST_MODIFIED_TIME, ACCOUNT_ID, CONTAINER_ID);
    getContainerByNameSql =
        String.format("select %s, %s, %s, %s from %s where %s = ? and %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID, CONTAINER_NAME);
    getContainerByIdSql =
        String.format("select %s, %s, %s, %s from %s where %s = ? and %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID, CONTAINER_ID);
    insertDatasetSql =
        String.format("insert into %s (%s, %s, %s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, now(3), ?, ?, ?)",
            DATASET_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_COUNT,
            RETENTION_TIME_IN_SECONDS, USER_TAGS);
    updateDatasetIfExpiredSql = String.format(
        "update %s set %s = ?, %s = now(3), %s = ?, %s = ?, %s = ?, %s = NULL where %s = ? and %s = ? and %s = ? and delete_ts < now(3)",
        DATASET_TABLE, VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_COUNT, RETENTION_TIME_IN_SECONDS, USER_TAGS,
        DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //update the dataset field, in order to support partial update, if one parameter is null, keep the original value.
    updateDatasetSql = String.format(
        "update %1$s set %2$s = now(3)," + "`retentionCount` = IFNULL(?, `retentionCount`), "
            + "`retentionTimeInSeconds` = IFNULL(?, `retentionTimeInSeconds`)," + "`userTags` = IFNULL(?, `userTags`)"
            + " where %6$s = ? and %7$s = ? and %8$s = ?", DATASET_TABLE, LAST_MODIFIED_TIME, RETENTION_COUNT,
        RETENTION_TIME_IN_SECONDS, USER_TAGS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    getDatasetByNameSql =
        String.format("select %s, %s, %s, %s, %s, %s , %s from %s where %s = ? and %s = ? and %s = ?", DATASET_NAME,
            VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_COUNT, RETENTION_TIME_IN_SECONDS, USER_TAGS, DELETE_TS, DATASET_TABLE,
            ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    deleteDatasetByIdSql = String.format(
        "update %s set %s = now(3), %s = now(3) where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ?",
        DATASET_TABLE, DELETE_TS, LAST_MODIFIED_TIME, DELETE_TS, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    getVersionSchemaSql =
        String.format("select %s from %s where %s = ? and %s = ? and %s = ?", VERSION_SCHEMA, DATASET_TABLE, ACCOUNT_ID,
            CONTAINER_ID, DATASET_NAME);
    insertDatasetVersionSql =
        String.format("insert into %s (%s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, now(3), ?)", DATASET_VERSION_TABLE,
            ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION, LAST_MODIFIED_TIME, DELETE_TS);
    getLatestVersionSqlForDownload = String.format("select %1$s, %2$s from %3$s "
            + "where (%4$s IS NULL or %4$s > now(3)) and (%5$s, %6$s, %7$s) = (?, ?, ?) ORDER BY %1$s DESC LIMIT 1",
        VERSION, DELETE_TS, DATASET_VERSION_TABLE, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    listLatestVersionSqlForUpload =
        String.format("select %1$s from %2$s " + "where (%4$s, %5$s, %6$s) = (?, ?, ?) ORDER BY %1$s DESC LIMIT 1",
            VERSION, DATASET_VERSION_TABLE, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    listValidVersionSql =
        String.format("select %s, %s from %s " + "where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ?",
            VERSION, DELETE_TS, DATASET_VERSION_TABLE, DELETE_TS, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    // list all valid versions sorted by last modified time, and skip the first N records which is not out of retentionCount.
    listVersionByModifiedTimeSql = String.format("select %s, %s from %s "
            + "where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ? ORDER BY %s DESC LIMIT ?, 100", VERSION,
        DELETE_TS, DATASET_VERSION_TABLE, DELETE_TS, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
        LAST_MODIFIED_TIME);
    getDatasetVersionByNameSql =
        String.format("select %s, %s from %s where %s = ? and %s = ? and %s = ? and %s = ?", LAST_MODIFIED_TIME,
            DELETE_TS, DATASET_VERSION_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION);
    deleteDatasetVersionByIdSql = String.format(
        "update %s set %s = now(3), %s = now(3) where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ? and %s = ?",
        DATASET_VERSION_TABLE, DELETE_TS, LAST_MODIFIED_TIME, DELETE_TS, DELETE_TS, ACCOUNT_ID, CONTAINER_ID,
        DATASET_NAME, VERSION);
    updateDatasetVersionIfExpiredSql = String.format(
        "update %s set %s = ?, %s = now(3), %s = ? where %s = ? and %s = ? and %s = ? and %s = ? and delete_ts < now(3)",
        DATASET_VERSION_TABLE, VERSION, LAST_MODIFIED_TIME, DELETE_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION);
  }

  /**
   * Gets all accounts that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  public synchronized List<Account> getNewAccounts(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    ResultSet rs = null;
    try {
      PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getAccountsSinceSql, false);
      getSinceStatement.setTimestamp(1, sinceTime);
      rs = getSinceStatement.executeQuery();
      List<Account> accounts = convertAccountsResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return accounts;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Convert a query result set to a list of accounts.
   * @param resultSet the result set.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  private List<Account> convertAccountsResultSet(ResultSet resultSet) throws SQLException {
    List<Account> accounts = new ArrayList<>();
    while (resultSet.next()) {
      String accountJson = resultSet.getString(ACCOUNT_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      try {
        Account account = new AccountBuilder(objectMapper.readValue(accountJson, Account.class)).lastModifiedTime(
            lastModifiedTime.getTime()).snapshotVersion(version).build();
        accounts.add(account);
      } catch (IOException e) {
        throw new SQLException(String.format("Faild to deserialize string [{}] to account object", accountJson), e);
      }
    }
    return accounts;
  }

  /**
   * Gets the containers in a specified account.
   * @param accountId the id for the parent account.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public synchronized List<Container> getContainers(int accountId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getByAccountStatement = dataAccessor.getPreparedStatement(getContainersByAccountSql, false);
      getByAccountStatement.setInt(1, accountId);
      rs = getByAccountStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets all containers that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public synchronized List<Container> getNewContainers(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    ResultSet rs = null;
    try {
      PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getContainersSinceSql, false);
      getSinceStatement.setTimestamp(1, sinceTime);
      rs = getSinceStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets container by its name and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerName name of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerByName(int accountId, String containerName) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getContainerByNameStatement = dataAccessor.getPreparedStatement(getContainerByNameSql, false);
      getContainerByNameStatement.setInt(1, accountId);
      getContainerByNameStatement.setString(2, containerName);
      rs = getContainerByNameStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers.isEmpty() ? null : containers.get(0);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets container by its Id and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerById(int accountId, int containerId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getContainerByIdStatement = dataAccessor.getPreparedStatement(getContainerByIdSql, false);
      getContainerByIdStatement.setInt(1, accountId);
      getContainerByIdStatement.setInt(2, containerId);
      rs = getContainerByIdStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers.isEmpty() ? null : containers.get(0);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Add a version of {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @return the {@link DatasetVersionRecord}.
   * @throws SQLException
   */
  public synchronized DatasetVersionRecord addDatasetVersions(int accountId, int containerId, String accountName,
      String containerName, String datasetName, String version, long timeToLiveInSeconds, long creationTimeInMs,
      boolean datasetVersionTtlEnabled)
      throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    long versionNumber = 0;
    Dataset dataset = null;
    DatasetVersionRecord datasetVersionRecord;
    try {
      dataset = getDataset(accountId, containerId, accountName, containerName, datasetName);
      dataAccessor.getDatabaseConnection(true);
      if (isAutoCreatedVersionForUpload(version)) {
        datasetVersionRecord =
            retryWithLatestAutoIncrementedVersion(accountId, containerId, dataset, version, timeToLiveInSeconds,
                creationTimeInMs, datasetVersionTtlEnabled);
      } else {
        versionNumber = getVersionBasedOnSchema(version, dataset.getVersionSchema());
        datasetVersionRecord =
            addDatasetVersionHelper(accountId, containerId, dataset, version, timeToLiveInSeconds, creationTimeInMs,
                datasetVersionTtlEnabled, versionNumber);
      }
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
      return datasetVersionRecord;
    } catch (SQLException | AccountServiceException e) {
      if (e instanceof SQLIntegrityConstraintViolationException) {
        try {
          datasetVersionRecord =
              updateDatasetVersionHelper(accountId, containerId, dataset, version, timeToLiveInSeconds,
                  creationTimeInMs, datasetVersionTtlEnabled, versionNumber);
          dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
          return datasetVersionRecord;
        } catch (SQLException | AccountServiceException ex) {
          dataAccessor.onException(ex, Write);
          throw ex;
        }
      }
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Helper function to support add dataset verison.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @return the {@link DatasetVersionRecord} and the latest version.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private DatasetVersionRecord addDatasetVersionHelper(int accountId, int containerId, Dataset dataset, String version,
      long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled, long versionNumber)
      throws SQLException, AccountServiceException {
    dataAccessor.getDatabaseConnection(true);
    PreparedStatement insertDatasetVersionStatement = dataAccessor.getPreparedStatement(insertDatasetVersionSql, true);
    long newExpirationTimeMs = executeAddDatasetVersionStatement(insertDatasetVersionStatement, accountId, containerId,
        dataset.getDatasetName(), versionNumber, timeToLiveInSeconds, creationTimeInMs, dataset,
        datasetVersionTtlEnabled);
    return new DatasetVersionRecord(accountId, containerId, dataset.getDatasetName(), version, newExpirationTimeMs);
  }

  /**
   * Retry and add the latest auto incremented dataset version
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @return the {@link DatasetVersionRecord} with the latest auto incremented dataset version
   * @throws AccountServiceException
   * @throws SQLException
   */
  private DatasetVersionRecord retryWithLatestAutoIncrementedVersion(int accountId, int containerId, Dataset dataset,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled)
      throws AccountServiceException, SQLException {
    long versionNumber;
    while (true) {
      try {
        versionNumber =
            getAutoIncrementedVersionBasedOnLatestAvailableVersion(accountId, containerId, dataset.getVersionSchema(),
                dataset.getDatasetName(), version);
        version = convertVersionValueToVersion(versionNumber, dataset.getVersionSchema());
        //always retry with the latest version + 1 until it has been successfully uploading without conflict or failed with exception.
        return addDatasetVersionHelper(accountId, containerId, dataset, version, timeToLiveInSeconds, creationTimeInMs,
            datasetVersionTtlEnabled, versionNumber);
      } catch (SQLException | AccountServiceException ex) {
        // if the version already exist in database, retry with the new latest version +1.
        if (!(ex instanceof SQLIntegrityConstraintViolationException)) {
          throw ex;
        }
      }
    }
  }

  /**
   * Helper function to support update dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @param versionNumber the version number converted by version string.
   * @return
   * @throws SQLException
   * @throws AccountServiceException
   */
  private DatasetVersionRecord updateDatasetVersionHelper(int accountId, int containerId, Dataset dataset,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled,
      long versionNumber) throws SQLException, AccountServiceException {
    PreparedStatement updateDatasetVersionSqlIfExpiredStatement =
        dataAccessor.getPreparedStatement(updateDatasetVersionIfExpiredSql, true);
    long newExpirationTimeMs =
        executeUpdateDatasetVersionIfExpiredSqlStatement(updateDatasetVersionSqlIfExpiredStatement, accountId,
            containerId, dataset.getDatasetName(), versionNumber, timeToLiveInSeconds, creationTimeInMs, dataset,
            datasetVersionTtlEnabled);
    return new DatasetVersionRecord(accountId, containerId, dataset.getDatasetName(), version, newExpirationTimeMs);
  }

  /**
   * Get a version of {@link Dataset} based on supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @return the {@link DatasetVersionRecord}
   * @throws SQLException
   */
  public synchronized DatasetVersionRecord getDatasetVersions(short accountId, short containerId, String accountName,
      String containerName, String datasetName, String version) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      //if dataset is deleted, we should not be able to get any dataset version.
      Dataset dataset = getDataset(accountId, containerId, accountName, containerName, datasetName);
      Dataset.VersionSchema versionSchema = dataset.getVersionSchema();
      dataAccessor.getDatabaseConnection(false);
      PreparedStatement getDatasetVersionStatement =
          dataAccessor.getPreparedStatement(getDatasetVersionByNameSql, false);
      long versionValue;
      DatasetVersionRecord result;
      if (isAutoCreatedVersionForDownload(version)) {
        PreparedStatement getLatestVersionStatement =
            dataAccessor.getPreparedStatement(getLatestVersionSqlForDownload, false);
        result =
            executeGetLatestVersionStatementForDownload(getLatestVersionStatement, accountId, containerId, datasetName,
                versionSchema);
      } else {
        versionValue = getVersionBasedOnSchema(version, versionSchema);
        result = executeGetDatasetVersionStatement(getDatasetVersionStatement, accountId, containerId, datasetName,
            versionValue, version);
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return result;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Delete {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @throws SQLException
   */
  public synchronized void deleteDatasetVersion(int accountId, int containerId, String datasetName, String version)
      throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement getVersionSchemaStatement = dataAccessor.getPreparedStatement(getVersionSchemaSql, false);
      Dataset.VersionSchema versionSchema =
          executeGetVersionSchema(getVersionSchemaStatement, accountId, containerId, datasetName);
      dataAccessor.getDatabaseConnection(true);
      PreparedStatement deleteDatasetVersionStatement =
          dataAccessor.getPreparedStatement(deleteDatasetVersionByIdSql, true);
      long versionValue = getVersionBasedOnSchema(version, versionSchema);
      executeDeleteDatasetVersionStatement(deleteDatasetVersionStatement, accountId, containerId, datasetName,
          versionValue);
      dataAccessor.onSuccess(Delete, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Delete);
      throw e;
    }
  }

  /**
   * Execute insert dataset version statement to add a dataset version.
   * @param statement the mysql statement to add dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs The time at which the blob is created.
   * @param dataset the {@link Dataset} including the metadata.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @throws SQLException
   */
  private long executeAddDatasetVersionStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long version, long timeToLiveInSeconds, long creationTimeInMs, Dataset dataset,
      boolean datasetVersionTtlEnabled) throws SQLException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    statement.setLong(4, version);
    long updatedExpirationTimeMs;
    if (datasetVersionTtlEnabled || dataset.getRetentionTimeInSeconds() == null) {
      updatedExpirationTimeMs = Utils.addSecondsToEpochTime(creationTimeInMs, timeToLiveInSeconds);
    } else {
      updatedExpirationTimeMs = Utils.addSecondsToEpochTime(creationTimeInMs, dataset.getRetentionTimeInSeconds());
    }
    if (updatedExpirationTimeMs == Infinite_Time) {
      statement.setTimestamp(5, null);
    } else {
      statement.setTimestamp(5, new Timestamp(updatedExpirationTimeMs));
    }
    statement.executeUpdate();
    return updatedExpirationTimeMs;
  }

  /**
   * Execute deleteDatasetVersionStatement to delete Dataset version.
   * @param statement the mysql statement to delete dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version value of the dataset.
   * @throws SQLException the {@link Dataset} including the metadata.
   * @throws AccountServiceException
   */
  private void executeDeleteDatasetVersionStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName, long versionValue) throws SQLException, AccountServiceException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    statement.setLong(4, versionValue);
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException(
          "Dataset not found qualified record to delete for account: " + accountId + " container: " + containerId
              + " dataset: " + datasetName + " version: " + versionValue, AccountServiceErrorCode.NotFound);
    }
  }

  /**
   * Execute updateDatasetSqlIfExpiredStatement to update dataset version if it gets expired.
   * @param statement the mysql statement to update dataset version if it gets expired.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param version the version value of the dataset.
   * @param timeToLiveInSeconds The dataset version level ttl.
   * @param creationTimeInMs the creation time of the dataset version.
   * @param dataset the {@link Dataset} including the metadata.
   * @param datasetVersionTtlEnabled set to true if dataset version ttl want to override the dataset level default ttl.
   * @return the updated expiration time.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private long executeUpdateDatasetVersionIfExpiredSqlStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName, long version, long timeToLiveInSeconds, long creationTimeInMs,
      Dataset dataset, boolean datasetVersionTtlEnabled) throws SQLException, AccountServiceException {
    statement.setLong(1, version);
    long updatedExpirationTimeMs;
    if (datasetVersionTtlEnabled || dataset.getRetentionTimeInSeconds() == null) {
      updatedExpirationTimeMs = Utils.addSecondsToEpochTime(creationTimeInMs, timeToLiveInSeconds);
    } else {
      updatedExpirationTimeMs = Utils.addSecondsToEpochTime(creationTimeInMs, dataset.getRetentionTimeInSeconds());
    }
    if (updatedExpirationTimeMs == Infinite_Time) {
      statement.setTimestamp(2, null);
    } else {
      statement.setTimestamp(2, new Timestamp(updatedExpirationTimeMs));
    }
    statement.setInt(3, accountId);
    statement.setInt(4, containerId);
    statement.setString(5, datasetName);
    statement.setLong(6, version);
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException("The dataset version already exist", AccountServiceErrorCode.ResourceConflict);
    }
    return updatedExpirationTimeMs;
  }

  /**
   * Add {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @throws SQLException
   */
  public synchronized void addDataset(int accountId, int containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    try {
      dataAccessor.getDatabaseConnection(true);
      PreparedStatement insertDatasetStatement = dataAccessor.getPreparedStatement(insertDatasetSql, true);
      executeAddDatasetStatement(insertDatasetStatement, accountId, containerId, dataset);
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      if (e instanceof SQLIntegrityConstraintViolationException) {
        PreparedStatement updateDatasetSqlIfExpiredStatement =
            dataAccessor.getPreparedStatement(updateDatasetIfExpiredSql, true);
        if (executeUpdateDatasetSqlIfExpiredStatement(updateDatasetSqlIfExpiredStatement, accountId, containerId,
            dataset) <= 0) {
          throw new AccountServiceException("The dataset already exist", AccountServiceErrorCode.ResourceConflict);
        } else {
          dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
          return;
        }
      }
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Update {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @throws SQLException
   */
  public synchronized void updateDataset(int accountId, int containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(true);
      PreparedStatement updateDatasetStatement = dataAccessor.getPreparedStatement(updateDatasetSql, true);
      int rowCount = executeUpdateDatasetStatement(updateDatasetStatement, accountId, containerId, dataset);
      if (rowCount == 0) {
        throw new AccountServiceException("Can't update the dataset if it does not exist. Dataset: " + dataset,
            AccountServiceErrorCode.NotFound);
      }
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Get {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @return the {@link Dataset}
   * @throws SQLException
   */
  public synchronized Dataset getDataset(int accountId, int containerId, String accountName, String containerName,
      String datasetName) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(false);
      PreparedStatement getDatasetStatement = dataAccessor.getPreparedStatement(getDatasetByNameSql, false);
      Dataset dataset =
          executeGetDatasetStatement(getDatasetStatement, accountId, containerId, accountName, containerName,
              datasetName);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return dataset;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Delete {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @throws SQLException
   */
  public synchronized void deleteDataset(int accountId, int containerId, String datasetName)
      throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(true);
      PreparedStatement deleteDatasetStatement = dataAccessor.getPreparedStatement(deleteDatasetByIdSql, true);
      executeDeleteDatasetStatement(deleteDatasetStatement, accountId, containerId, datasetName);
      dataAccessor.onSuccess(Delete, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Delete);
      throw e;
    }
  }

  /**
   * Get the auto incremented version based on latest version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param versionSchema the {@link com.github.ambry.account.Dataset.VersionSchema} of the dataset.
   * @param datasetName the name of the dataset.
   * @return the latest version.
   * @throws SQLException
   */
  private synchronized long getAutoIncrementedVersionBasedOnLatestAvailableVersion(int accountId, int containerId,
      Dataset.VersionSchema versionSchema, String datasetName, String version)
      throws SQLException, AccountServiceException {
    long latestVersionValue;
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(false);
      PreparedStatement listLatestVersionStatement =
          dataAccessor.getPreparedStatement(listLatestVersionSqlForUpload, false);
      try {
        latestVersionValue =
            executeListLatestVersionStatementForUpload(listLatestVersionStatement, accountId, containerId, datasetName);
      } catch (AccountServiceException e) {
        if (AccountServiceErrorCode.NotFound.equals(e.getErrorCode())) {
          latestVersionValue = 0;
        } else {
          throw e;
        }
      }
      long autoIncrementedVersionBasedOnSchema =
          generateAutoIncrementedVersionBasedOnSchema(latestVersionValue, versionSchema, version);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return autoIncrementedVersionBasedOnSchema;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Get a list of dataset versions which is not expired or been deleted which belongs to the related dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return a list of dataset versions which is not expired or been deleted.
   * @throws SQLException
   */
  public synchronized List<DatasetVersionRecord> getAllValidVersion(short accountId, short containerId,
      String datasetName) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(false);
      PreparedStatement listAllValidVersionStatement = dataAccessor.getPreparedStatement(listValidVersionSql, false);
      List<DatasetVersionRecord> datasetVersionRecords =
          executeListAllValidVersionStatement(listAllValidVersionStatement, accountId, containerId, datasetName);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return datasetVersionRecords;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Get all versions from a dataset which has not expired and out of retentionCount by checking the last modified time.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @return a list of {@link DatasetVersionRecord}
   */
  public synchronized List<DatasetVersionRecord> getAllValidVersionsOutOfRetentionCount(short accountId,
      short containerId, String accountName, String containerName, String datasetName)
      throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    Integer retentionCount;
    Dataset.VersionSchema versionSchema;
    try {
      //if dataset is deleted, we should not be able to get any dataset version.
      Dataset dataset = getDataset(accountId, containerId, accountName, containerName, datasetName);
      retentionCount = dataset.getRetentionCount();
      versionSchema = dataset.getVersionSchema();
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
    if (retentionCount != null) {
      try {
        dataAccessor.getDatabaseConnection(false);
        PreparedStatement listAllValidVersionsOrderedByLastModifiedTimeStatement =
            dataAccessor.getPreparedStatement(listVersionByModifiedTimeSql, false);
        List<DatasetVersionRecord> datasetVersionRecordList =
            executeListAllValidVersionsOrderedByLastModifiedTimeStatement(
                listAllValidVersionsOrderedByLastModifiedTimeStatement, accountId, containerId, datasetName, retentionCount, versionSchema);
        dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
        return datasetVersionRecordList;
      } catch (SQLException e) {
        dataAccessor.onException(e, Read);
        throw e;
      }
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * Execute the listAllValidVersionsOrderedByLastModifiedTimeStatement and fileter all versions out of retentionCount.
   * @param statement the listVersionByModifiedTimeSql statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param retentionCount the retention count of dataset.
   * @param versionSchema the {@link com.github.ambry.account.Dataset.VersionSchema} of the datset.
   * @return a list of versions from a dataset which has not expired and out of retentionCount by checking the last modified time.
   * @throws SQLException
   */
  private List<DatasetVersionRecord> executeListAllValidVersionsOrderedByLastModifiedTimeStatement(
      PreparedStatement statement, int accountId, int containerId, String datasetName, int retentionCount,
      Dataset.VersionSchema versionSchema) throws SQLException {
    ResultSet resultSet = null;
    List<DatasetVersionRecord> datasetVersionRecordList = new ArrayList<>();
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setInt(4, retentionCount);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        long versionValue = resultSet.getLong(VERSION);
        Timestamp deletionTime = resultSet.getTimestamp(DELETE_TS);
        String version = convertVersionValueToVersion(versionValue, versionSchema);
        datasetVersionRecordList.add(
            new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime)));
      }
      return datasetVersionRecordList;
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Convert version value from DB to string version.
   * @param versionValue the value of the version from DB.
   * @param versionSchema the {@link com.github.ambry.account.Dataset.VersionSchema} from dataset level.
   * @return the converted string format version.
   */
  private String convertVersionValueToVersion(long versionValue, Dataset.VersionSchema versionSchema) {
    String version;
    switch (versionSchema) {
      case TIMESTAMP:
      case MONOTONIC:
        version = Long.toString(versionValue);
        return version;
      case SEMANTIC:
        //Given a version number MAJOR.MINOR.PATCH, increment the:
        //1.MAJOR version when you make incompatible API changes
        //2.MINOR version when you add functionality in a backwards compatible manner
        //3.PATCH version when you make backwards compatible bug fixes
        //The MAJOR,MINOR and PATCH version are non-negative value.
        long major = versionValue / 1000000L;
        long minor = (versionValue % 1000000L) / 1000L;
        long patch = versionValue % 1000L;
        version = String.format("%d.%d.%d", major, minor, patch);
        return version;
      default:
        throw new IllegalArgumentException("Unsupported version schema: " + versionSchema);
    }
  }

  /**
   * Execute the listVersionSql statement.
   * @param statement the statement to list the latest version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return the latest version of the dataset.
   * @throws SQLException
   */
  private long executeListLatestVersionStatementForUpload(PreparedStatement statement, int accountId, int containerId,
      String datasetName) throws SQLException, AccountServiceException {
    ResultSet resultSet = null;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Latest version not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName, AccountServiceErrorCode.NotFound);
      }
      return resultSet.getLong(VERSION);
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Execute the getLatestVersionSqlForDownload statement.
   * @param statement the statement to list the latest version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return the latest version of the dataset.
   * @throws SQLException
   */
  private DatasetVersionRecord executeGetLatestVersionStatementForDownload(PreparedStatement statement, int accountId,
      int containerId, String datasetName, Dataset.VersionSchema versionSchema)
      throws SQLException, AccountServiceException {
    ResultSet resultSet = null;
    String version;
    Timestamp deletionTime;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Latest version not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName, AccountServiceErrorCode.NotFound);
      }
      long versionValue = resultSet.getLong(VERSION);
      version = convertVersionValueToVersion(versionValue, versionSchema);
      deletionTime = resultSet.getTimestamp(DELETE_TS);
    } finally {
      closeQuietly(resultSet);
    }
    return new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime));
  }

  /**
   * Generate the auto incremented version based on the latest valid version and the version schema.
   * @param versionValue the latest version value for current dataset.
   * @param versionSchema the {@link com.github.ambry.account.Dataset.VersionSchema} of the dataset.
   * @param version the version string to indicate if user provide LATEST, MAJOR, MINOR or PATCH option.
   * @return the auto incremented value base on latest version.
   */
  private long generateAutoIncrementedVersionBasedOnSchema(long versionValue, Dataset.VersionSchema versionSchema,
      String version) {
    long autoIncrementedVersionValue;
    switch (versionSchema) {
      case TIMESTAMP:
      case MONOTONIC:
        if (!LATEST.equals(version)) {
          throw new IllegalArgumentException(
              "The current version string " + version + " is not supported for Timestamp or Monotonic version schema");
        }
        autoIncrementedVersionValue = versionValue + 1;
        if (autoIncrementedVersionValue >= MAX_TIMESTAMP_MONOTONIC_VERSION_VALUE) {
          throw new IllegalArgumentException(
              "The largest version for Timestamp and Monotonic version should be less than "
                  + MAX_TIMESTAMP_MONOTONIC_VERSION_VALUE + ", currentVersion: " + versionValue);
        }
        return autoIncrementedVersionValue;
      case SEMANTIC:
        //Given a version number MAJOR.MINOR.PATCH, increment the:
        //1.MAJOR version when you make incompatible API changes
        //2.MINOR version when you add functionality in a backwards compatible manner
        //3.PATCH version when you make backwards compatible bug fixes
        //The MAJOR,MINOR and PATCH version are non-negative value.
        if (!MAJOR.equals(version) && !MINOR.equals(version) && !PATCH.equals(version)) {
          throw new IllegalArgumentException(
              "The current version string " + version + " is not supported for Semantic version schema");
        }
        long majorVersion = versionValue / 1000000;
        long minorVersion = ((versionValue % 1000000) / 1000);
        long patchVersion = versionValue % 1000;
        if (MAJOR.equals(version)) {
          majorVersion += 1;
          minorVersion = 0;
          patchVersion = 0;
          if (majorVersion > MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Major version should be less than "
                + MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE + ", currentMajorVersion: " + majorVersion);
          }
        } else if (MINOR.equals(version)) {
          minorVersion += 1;
          patchVersion = 0;
          if (minorVersion > MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Minor version should be less than "
                + MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE + ", currentMinorVersion: " + minorVersion);
          }
        } else {
          patchVersion += 1;
          if (patchVersion > MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Patch version should be less than "
                + MAX_SEMANTIC_MAJOR_MINOR_PATH_VERSION_VALUE + ", currentPatchVersion: " + patchVersion);
          }
        }
        return majorVersion * 1000000 + minorVersion * 1000 + patchVersion;
      default:
        throw new IllegalArgumentException("Unsupported version schema: " + versionSchema);
    }
  }

  /**
   * @param version the version string.
   * @return {@code} true if the version is auto created version for upload.
   */
  private boolean isAutoCreatedVersionForUpload(String version) {
    return LATEST.equals(version) || MAJOR.equals(version) || MINOR.equals(version) || PATCH.equals(version);
  }

  /**
   * @param version the version string.
   * @return {@code} true if the version is auto created version for download.
   */
  private boolean isAutoCreatedVersionForDownload(String version) {
    return LATEST.equals(version);
  }

  /**
   * Execute listValidVersionSql statement.
   * @param statement the statement to list all dataset versions which is not expired or been deleted which belongs to
   * the related dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return a list of all dataset versions which is not expired or been deleted which belongs to the related dataset.
   * @throws SQLException
   */
  private List<DatasetVersionRecord> executeListAllValidVersionStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName) throws SQLException {
    ResultSet resultSet = null;
    List<DatasetVersionRecord> datasetVersionRecords = new ArrayList<>();
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String version = resultSet.getString(VERSION);
        Timestamp deletionTime = resultSet.getTimestamp(DELETE_TS);
        DatasetVersionRecord datasetVersionRecord =
            new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime));
        datasetVersionRecords.add(datasetVersionRecord);
      }
      return datasetVersionRecords;
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Convert a query result set to a list of containers.
   * @param resultSet the result set.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  private List<Container> convertContainersResultSet(ResultSet resultSet) throws SQLException {
    List<Container> containers = new ArrayList<>();
    while (resultSet.next()) {
      int accountId = resultSet.getInt(ACCOUNT_ID);
      String containerJson = resultSet.getString(CONTAINER_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      try {
        Container deserialized = objectMapper.readValue(containerJson, Container.class);
        Container container = new ContainerBuilder(deserialized).setParentAccountId((short) accountId)
            .setLastModifiedTime(lastModifiedTime.getTime())
            .setSnapshotVersion(version)
            .build();
        containers.add(container);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
    return containers;
  }

  /**
   * Adds/Updates accounts and their containers to the database in batches atomically using transaction.
   * @param accountsInfo information of updated Accounts
   * @param batchSize number of statements to be executed in one batch
   * @throws SQLException
   */
  public synchronized void updateAccounts(List<AccountUpdateInfo> accountsInfo, int batchSize) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();

      AccountUpdateBatch accountUpdateBatch =
          new AccountUpdateBatch(dataAccessor.getPreparedStatement(insertAccountsSql, true),
              dataAccessor.getPreparedStatement(updateAccountsSql, true),
              dataAccessor.getPreparedStatement(insertContainersSql, true),
              dataAccessor.getPreparedStatement(updateContainersSql, true));

      // Disable auto commits
      dataAccessor.setAutoCommit(false);

      int batchCount = 0;
      for (AccountUpdateInfo accountUpdateInfo : accountsInfo) {
        // Get account and container changes information
        Account account = accountUpdateInfo.getAccount();
        boolean isAccountAdded = accountUpdateInfo.isAdded();
        boolean isAccountUpdated = accountUpdateInfo.isUpdated();
        List<Container> addedContainers = accountUpdateInfo.getAddedContainers();
        List<Container> updatedContainers = accountUpdateInfo.getUpdatedContainers();

        // Number of changes in the account.
        int accountUpdateCount =
            (isAccountAdded ? 1 : 0) + (isAccountUpdated ? 1 : 0) + addedContainers.size() + updatedContainers.size();

        // Commit transaction with previous batch inserts/updates if it either of following is true.
        // a) Total batch count of previous #accounts/containers is equal to or greater than configured batch size.
        //    Note: It is possible for count to be greater than configured batch size when number of containers in
        //    previous account exceeds the configured batch size. We allow it to ensure an account is committed atomically.
        // b) Adding account and its containers in current iteration to total batch count > configured batch size
        if (batchCount >= batchSize || (batchCount > 0 && batchCount + accountUpdateCount > batchSize)) {
          accountUpdateBatch.maybeExecuteBatch();
          dataAccessor.commit();
          batchCount = 0;
        }

        // Add account to insert/update batch if it was either added or modified.
        if (isAccountAdded) {
          accountUpdateBatch.addAccount(account);
        } else if (isAccountUpdated) {
          accountUpdateBatch.updateAccount(account);
        }
        // Add new containers for batch inserts
        for (Container container : addedContainers) {
          accountUpdateBatch.addContainer(account.getId(), container);
        }
        // Add updated containers for batch updates
        for (Container container : updatedContainers) {
          accountUpdateBatch.updateContainer(account.getId(), container);
        }

        batchCount += accountUpdateCount;
      }

      // Commit transaction with pending batch inserts/updates
      if (batchCount > 0) {
        accountUpdateBatch.maybeExecuteBatch();
        dataAccessor.commit();
      }

      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      //rollback the current transaction.
      dataAccessor.rollback();
      dataAccessor.onException(e, Write);
      throw e;
    } finally {
      // Close the connection to ensure subsequent queries are made in a new transaction and return the latest data
      dataAccessor.closeActiveConnection();
    }
  }

  /**
   * Binds parameters to prepare statements for {@link Account} inserts/updates
   * @param statement {@link PreparedStatement} for mysql queries
   * @param account {@link Account} being added to mysql
   * @param statementType {@link StatementType} of mysql query such as insert or update.
   * @throws SQLException
   */
  private void bindAccount(PreparedStatement statement, Account account, StatementType statementType)
      throws SQLException {
    String accountInJson;
    try {
      accountInJson = new String(AccountCollectionSerde.serializeAccountsInJsonNoContainers(account));
    } catch (IOException e) {
      throw new SQLException("Fail to serialize account: " + account.toString(), e);
    }
    switch (statementType) {
      case Insert:
        statement.setString(1, accountInJson);
        statement.setInt(2, account.getSnapshotVersion());
        break;
      case Update:
        statement.setString(1, accountInJson);
        statement.setInt(2, (account.getSnapshotVersion() + 1));
        statement.setInt(3, account.getId());
        break;
    }
  }

  /**
   * Binds parameters to prepare statements for {@link Container} inserts/updates
   * @param statement {@link PreparedStatement} for mysql queries
   * @param accountId Id of {@link Account} whose {@link Container} is being added to mysql
   * @param container {@link Container} being added to mysql
   * @param statementType {@link StatementType} of mysql query such as insert or update.
   * @throws SQLException
   */
  private void bindContainer(PreparedStatement statement, int accountId, Container container,
      StatementType statementType) throws SQLException {
    String containerInJson;
    try {
      containerInJson = objectMapper.writeValueAsString(container);
    } catch (IOException e) {
      throw new SQLException("Fail to serialize container: " + container.toString(), e);
    }
    switch (statementType) {
      case Insert:
        statement.setInt(1, accountId);
        statement.setString(2, containerInJson);
        statement.setInt(3, container.getSnapshotVersion());
        break;
      case Update:
        statement.setString(1, containerInJson);
        statement.setInt(2, (container.getSnapshotVersion() + 1));
        statement.setInt(3, accountId);
        statement.setInt(4, container.getId());
    }
  }

  /**
   * Execute insertDatasetStatement to add Dataset.
   * @param statement the mysql statement to add dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @throws SQLException
   */
  private void executeAddDatasetStatement(PreparedStatement statement, int accountId, int containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    String datasetName = dataset.getDatasetName();
    if (dataset.getVersionSchema() == null) {
      throw new AccountServiceException("Must set the version schema info during dataset creation",
          AccountServiceErrorCode.BadRequest);
    }
    int schemaVersionOrdinal = dataset.getVersionSchema().ordinal();
    Integer retentionCount = dataset.getRetentionCount();
    Long retentionTimeInSeconds = dataset.getRetentionTimeInSeconds();
    Map<String, String> userTags = dataset.getUserTags();
    String userTagsInJson;
    try {
      userTagsInJson = objectMapper.writeValueAsString(userTags);
    } catch (IOException e) {
      throw new AccountServiceException("Fail to serialize user tags : " + userTags.toString(),
          AccountServiceErrorCode.BadRequest);
    }
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    statement.setInt(4, schemaVersionOrdinal);
    if (retentionCount != null) {
      statement.setInt(5, retentionCount);
    } else {
      statement.setObject(5, null);
    }
    if (retentionTimeInSeconds != null) {
      statement.setLong(6, retentionTimeInSeconds);
    } else {
      statement.setObject(6, null);
    }
    if (userTags != null) {
      statement.setString(7, userTagsInJson);
    } else {
      statement.setString(7, null);
    }
    statement.executeUpdate();
  }

  /**
   * Execute updateDatasetSqlIfExpired statement.
   * @param statement the updateDatasetSqlIfExpired statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}.
   * @return the number of rows been updated.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private int executeUpdateDatasetSqlIfExpiredStatement(PreparedStatement statement, int accountId, int containerId, Dataset dataset)
      throws SQLException, AccountServiceException {
    String datasetName = dataset.getDatasetName();
    if (dataset.getVersionSchema() == null) {
      throw new AccountServiceException("Must set the version schema info during dataset creation",
          AccountServiceErrorCode.BadRequest);
    }
    int schemaVersionOrdinal = dataset.getVersionSchema().ordinal();
    Integer retentionCount = dataset.getRetentionCount();
    Long retentionTimeInSeconds = dataset.getRetentionTimeInSeconds();
    Map<String, String> userTags = dataset.getUserTags();
    String userTagsInJson;
    try {
      userTagsInJson = objectMapper.writeValueAsString(userTags);
    } catch (IOException e) {
      throw new AccountServiceException("Fail to serialize user tags : " + userTags.toString(),
          AccountServiceErrorCode.BadRequest);
    }
    statement.setInt(1, schemaVersionOrdinal);
    if (retentionCount != null) {
      statement.setInt(2, retentionCount);
    } else {
      statement.setObject(2, null);
    }
    if (retentionTimeInSeconds != null) {
      statement.setLong(3, retentionTimeInSeconds);
    } else {
      statement.setObject(3, null);
    }
    if (userTags != null) {
      statement.setString(4, userTagsInJson);
    } else {
      statement.setString(4, null);
    }
    statement.setInt(5, accountId);
    statement.setInt(6, containerId);
    statement.setString(7, datasetName);
    return statement.executeUpdate();
  }

  /**
   * Execute updateDatasetStatement to update Dataset.
   * @param statement the mysql statement to update dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @return the number of rows been updated.
   * @throws SQLException
   */
  private int executeUpdateDatasetStatement(PreparedStatement statement, int accountId, int containerId,
      Dataset dataset) throws SQLException, AccountServiceException {
    Dataset.VersionSchema versionSchema = dataset.getVersionSchema();
    if (versionSchema != null) {
      throw new AccountServiceException("version Schema can't be updated once the dataset has been created",
          AccountServiceErrorCode.BadRequest);
    }
    Map<String, String> userTags = dataset.getUserTags();
    String userTagsInJson = null;
    if (userTags != null) {
      try {
        userTagsInJson = objectMapper.writeValueAsString(userTags);
      } catch (IOException e) {
        throw new AccountServiceException("Fail to serialize user tags : " + userTags,
            AccountServiceErrorCode.BadRequest);
      }
    }
    Integer retentionCount = dataset.getRetentionCount();
    if (retentionCount != null) {
      statement.setInt(1, retentionCount);
    } else {
      statement.setObject(1, null);
    }
    Long retentionTimeInSeconds = dataset.getRetentionTimeInSeconds();
    if (retentionTimeInSeconds != null) {
      statement.setLong(2, retentionTimeInSeconds);
    } else {
      statement.setObject(2, null);
    }
    statement.setString(3, userTagsInJson);
    statement.setInt(4, accountId);
    statement.setInt(5, containerId);
    statement.setString(6, dataset.getDatasetName());
    return statement.executeUpdate();
  }

  /**
   * Execute getDatasetStatement to get Dataset.
   * @param statement the mysql statement to get dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @return the {@link Dataset}
   * @throws SQLException
   */
  private Dataset executeGetDatasetStatement(PreparedStatement statement, int accountId, int containerId,
      String accountName, String containerName, String datasetName) throws SQLException, AccountServiceException {
    Dataset.VersionSchema versionSchema;
    Integer retentionCount;
    Long retentionTimeInSeconds;
    Timestamp deletionTime;
    Map<String, String> userTags = null;
    ResultSet resultSet = null;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Dataset not found for account: " + accountId + " container: " + containerId + " dataset: " + datasetName,
            AccountServiceErrorCode.NotFound);
      }
      deletionTime = resultSet.getTimestamp(DELETE_TS);
      long currentTime = System.currentTimeMillis();
      if (compareTimestamp(deletionTime, currentTime) <= 0) {
        throw new AccountServiceException(
            "Dataset expired for account: " + accountId + " container: " + containerId + " dataset: " + datasetName,
            AccountServiceErrorCode.Deleted);
      }
      versionSchema = Dataset.VersionSchema.values()[resultSet.getInt(VERSION_SCHEMA)];
      retentionCount = resultSet.getObject(RETENTION_COUNT, Integer.class);
      retentionTimeInSeconds = resultSet.getObject(RETENTION_TIME_IN_SECONDS, Long.class);
      String userTagsInJson = resultSet.getString(USER_TAGS);
      if (userTagsInJson != null) {
        try {
          userTags = objectMapper.readValue(userTagsInJson, Map.class);
        } catch (IOException e) {
          throw new AccountServiceException("Fail to deserialize user tags : " + userTagsInJson,
              AccountServiceErrorCode.BadRequest);
        }
      }
    } finally {
      //If result set is not created in a try-with-resources block, it needs to be closed in a finally block.
      closeQuietly(resultSet);
    }
    return new Dataset(accountName, containerName, datasetName, versionSchema, retentionCount, retentionTimeInSeconds, userTags);
  }

  /**
   * Execute deleteDatasetStatement to delete Dataset.
   * @param statement the mysql statement to delete dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   */
  private void executeDeleteDatasetStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName) throws AccountServiceException, SQLException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException(
          "Dataset not found qualified record to delete for account: " + accountId + " container: " + containerId
              + " dataset: " + datasetName, AccountServiceErrorCode.NotFound);
    }
  }

  /**
   * Get version schema corresponding to the dataset.
   * @param statement the mysql statement to get the version schema of the dataset.
   * @param accountId the id of the parent account.
   * @param containerId the if of the container.
   * @param datasetName the name of the dataset.
   * @return the name of the dataset.
   * @throws SQLException
   */
  private Dataset.VersionSchema executeGetVersionSchema(PreparedStatement statement, int accountId, int containerId,
      String datasetName) throws SQLException, AccountServiceException {
    ResultSet resultSet = null;
    Dataset.VersionSchema versionSchema;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Version Schema not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName, AccountServiceErrorCode.NotFound);
      }
      versionSchema = Dataset.VersionSchema.values()[resultSet.getInt(VERSION_SCHEMA)];
    } finally {
      //If result set is not created in a try-with-resources block, it needs to be closed in a finally block.
      closeQuietly(resultSet);
    }
    return versionSchema;
  }

  /**
   * Convert string version to long value.
   * @param version The string version.
   * @param versionSchema The {@link com.github.ambry.account.Dataset.VersionSchema} of the dataset.
   * @return the long value of version.
   */
  private long getVersionBasedOnSchema(String version, Dataset.VersionSchema versionSchema) {
    switch (versionSchema) {
      case TIMESTAMP:
      case MONOTONIC:
        long versionToLong;
        try {
          versionToLong = Long.parseLong(version);
        } catch (Exception e) {
          throw new IllegalArgumentException("The version is not numeric, version: " + version);
        }
        return versionToLong;
      case SEMANTIC:
        //Given a version number MAJOR.MINOR.PATCH, increment the:
        //1.MAJOR version when you make incompatible API changes
        //2.MINOR version when you add functionality in a backwards compatible manner
        //3.PATCH version when you make backwards compatible bug fixes
        //The MAJOR,MINOR and PATCH version are non-negative value.
        return sanityCheckAndConvertSemanticVersion(version);
      default:
        throw new IllegalArgumentException("Unsupported version schema: " + versionSchema);
    }
  }

  /**
   * Sanity check for semantic version.
   * @param version The version from request.
   * @return the converted version which will be stored in DatasetVersions DB.
   */
  private long sanityCheckAndConvertSemanticVersion(String version) {
    long majorVersion;
    long minorVersion;
    long patchVersion;
    try {
      String[] versionArray = version.split("[.]", 3);
      patchVersion = Long.parseLong(versionArray[2]);
      minorVersion = Long.parseLong(versionArray[1]);
      majorVersion = Long.parseLong(versionArray[0]);
      if (patchVersion < 0 || minorVersion < 0 || majorVersion < 0) {
        throw new IllegalArgumentException(
            "The major, minor or patch version is less than 0, major version: " + majorVersion + " minor version: "
                + minorVersion + " patch version: " + patchVersion);
      }
      if (patchVersion > 999 || minorVersion > 999) {
        throw new IllegalArgumentException(
            "The minor or patch version exceeded max allowed number: 999, minor version: " + minorVersion
                + " patch version: " + patchVersion);
      }
      //We set the max major version same as patch and minor version based on current use cases.
      if (majorVersion > mySqlAccountServiceConfig.maxMajorVersionForSemanticSchemaDataset) {
        throw new IllegalArgumentException("The major version exceeded max allowed number: "
            + mySqlAccountServiceConfig.maxMajorVersionForSemanticSchemaDataset + " major version: " + majorVersion);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Version does not match the semantic version format MAJOR.MINOR.PATCH, version: " + version);
    }
    return majorVersion * 1000000 + minorVersion * 1000 + patchVersion;
  }

  /**
   * Execute the get dataset version statement to get the dataset version.
   * @param statement the mysql statement to get dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version long value of the dataset.
   * @param version the String type version of the dataset.
   * @return the {@link DatasetVersionRecord}
   * @throws SQLException
   */
  private DatasetVersionRecord executeGetDatasetVersionStatement(PreparedStatement statement, short accountId,
      short containerId, String datasetName, long versionValue, String version)
      throws SQLException, AccountServiceException {
    ResultSet resultSet = null;
    Timestamp deletionTime;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setLong(4, versionValue);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Dataset version not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName + " version: " + version, AccountServiceErrorCode.NotFound);
      }
      deletionTime = resultSet.getTimestamp(DELETE_TS);
      long currentTime = System.currentTimeMillis();
      if (compareTimestamp(deletionTime, currentTime) <= 0) {
        throw new AccountServiceException(
            "Dataset version expired for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName + " version: " + version, AccountServiceErrorCode.Deleted);
      }
    } finally {
      closeQuietly(resultSet);
    }
    return new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime));
  }

  /**
   * Compare a nullable timestamp against {@code otherTimeMs}.
   * @param timestamp the nullable {@link Timestamp} to compare.
   * @param otherTimeMs the time value to compare against.
   * @return -1 if the timestamp is earlier than {@code otherTimeMs}, 0 if the times are equal, and 1 if
   *         {@code otherTimeMs} is later than the timestamp. {@code null} is considered greater than any other time.
   */
  private static int compareTimestamp(Timestamp timestamp, long otherTimeMs) {
    return Utils.compareTimes(timestampToMs(timestamp), otherTimeMs);
  }

  /**
   * @param timestamp a {@link Timestamp}, can be null.
   * @return the milliseconds since the epoch if {@code timestamp} is non-null, or {@link Utils#Infinite_Time} if null.
   */
  private static long timestampToMs(Timestamp timestamp) {
    return timestamp == null ? Utils.Infinite_Time : timestamp.getTime();
  }

  /**
   * Helper class to do batch inserts and updates of {@link Account}s and {@link Container}s.
   */
  class AccountUpdateBatch {

    private int insertAccountCount = 0;
    private int updateAccountCount = 0;
    private int insertContainerCount = 0;
    private int updateContainerCount = 0;
    private final PreparedStatement insertAccountStatement;
    private final PreparedStatement updateAccountStatement;
    private final PreparedStatement insertContainerStatement;
    private final PreparedStatement updateContainerStatement;

    public AccountUpdateBatch(PreparedStatement insertAccountStatement, PreparedStatement updateAccountStatement,
        PreparedStatement insertContainerStatement, PreparedStatement updateContainerStatement) {
      this.insertAccountStatement = insertAccountStatement;
      this.updateAccountStatement = updateAccountStatement;
      this.insertContainerStatement = insertContainerStatement;
      this.updateContainerStatement = updateContainerStatement;
    }

    /**
     * Executes batch inserts and updates of {@link Account}s and {@link Container}s.
     * @throws SQLException
     */
    public void maybeExecuteBatch() throws SQLException {
      if (insertAccountStatement != null && insertAccountCount > 0) {
        insertAccountStatement.executeBatch();
        insertAccountCount = 0;
      }
      if (updateAccountStatement != null && updateAccountCount > 0) {
        updateAccountStatement.executeBatch();
        updateAccountCount = 0;
      }
      if (insertContainerStatement != null && insertContainerCount > 0) {
        insertContainerStatement.executeBatch();
        insertContainerCount = 0;
      }
      if (updateContainerStatement != null && updateContainerCount > 0) {
        updateContainerStatement.executeBatch();
        updateContainerCount = 0;
      }
    }

    /**
     * Adds {@link Account} to its insert {@link PreparedStatement}'s batch.
     * @param account {@link Account} to be inserted.
     * @throws SQLException
     */
    public void addAccount(Account account) throws SQLException {
      bindAccount(insertAccountStatement, account, StatementType.Insert);
      insertAccountStatement.addBatch();
      ++insertAccountCount;
    }

    /**
     * Adds {@link Account} to its update {@link PreparedStatement}'s batch.
     * @param account {@link Account} to be updated.
     * @throws SQLException
     */
    public void updateAccount(Account account) throws SQLException {
      bindAccount(updateAccountStatement, account, StatementType.Update);
      updateAccountStatement.addBatch();
      ++updateAccountCount;
    }

    /**
     * Adds {@link Container} to its insert {@link PreparedStatement}'s batch.
     * @param accountId account id of the Container.
     * @param container {@link Container} to be inserted.
     * @throws SQLException
     */
    public void addContainer(int accountId, Container container) throws SQLException {
      bindContainer(insertContainerStatement, accountId, container, StatementType.Insert);
      insertContainerStatement.addBatch();
      ++insertContainerCount;
    }

    /**
     * Adds {@link Container} to its update {@link PreparedStatement}'s batch.
     * @param accountId account id of the Container.
     * @param container {@link Container} to be updated.
     * @throws SQLException
     */
    public void updateContainer(int accountId, Container container) throws SQLException {
      bindContainer(updateContainerStatement, accountId, container, StatementType.Update);
      updateContainerStatement.addBatch();
      ++updateContainerCount;
    }
  }
}
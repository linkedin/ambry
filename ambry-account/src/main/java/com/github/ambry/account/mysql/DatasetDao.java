/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetRetentionPolicy;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.protocol.DatasetVersionState;
import java.util.Set;

import static com.github.ambry.account.Dataset.VersionSchema.*;
import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;
import static com.github.ambry.utils.Utils.*;


public class DatasetDao {
  private final MySqlDataAccessor dataAccessor;
  private final MySqlAccountServiceConfig mySqlAccountServiceConfig;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final MySqlMetrics metrics;
  private static final long MAX_TIMESTAMP_MONOTONIC_VERSION_VALUE = Long.MAX_VALUE;
  private static final long MAX_SEMANTIC_VERSION_PART_VALUE = 999L;
  private static final String LATEST = "LATEST";
  private static final String MAJOR = "MAJOR";
  private static final String MINOR = "MINOR";
  private static final String PATCH = "PATCH";
  private static final String REVISION = "REVISION";

  // Account table fields
  public static final String ACCOUNT_ID = "accountId";

  // Container table fields
  public static final String CONTAINER_ID = "containerId";
  public static final String CONTAINER_NAME = "containerName";

  // Dataset table fields
  public static final String DATASET_TABLE = "Datasets";
  public static final String DATASET_NAME = "datasetName";
  public static final String VERSION_SCHEMA = "versionSchema";
  public static final String RETENTION_POLICY = "retentionPolicy";
  public static final String RETENTION_COUNT = "retentionCount";
  public static final String RETENTION_TIME_IN_SECONDS = "retentionTimeInSeconds";
  public static final String USER_TAGS = "userTags";
  public static final String DELETED_TS = "deleted_ts";
  public static final String RENAME_FROM = "rename_from";

  // Dataset version table fields
  public static final String DATASET_VERSION_TABLE = "DatasetVersions";
  public static final String DATASET_VERSION_STATE = "datasetVersionState";

  // Common fields
  public static final String VERSION = "version";
  public static final String CREATION_TIME = "creationTime";
  public static final String LAST_MODIFIED_TIME = "lastModifiedTime";

  // Dataset version table query strings.
  private final String insertDatasetVersionSql;
  private final String updateDatasetVersionStateSql;
  private final String updateDatasetVersionStateAndDeletedTsSql;
  private final String getDatasetVersionByNameSql;
  private final String deleteDatasetVersionByIdSql;
  private final String deleteDatasetVersionForDatasetDeleteByIdSql;
  private final String updateDatasetVersionIfExpiredSql;
  private final String listLatestVersionSqlForUpload;
  private final String getLatestVersionSqlForDownload;
  private final String listValidVersionForDatasetDeletionSql;
  private final String listVersionByModifiedTimeAndFilterByRetentionSql;
  private final String listValidDatasetVersionsByPageSql;
  private final String listValidDatasetVersionsByListSql;
  private final String updateDatasetVersionTtlSql;
  private final String copyToNewDatasetVersionSql;

  // Dataset table query strings
  private final String insertDatasetSql;
  private final String getDatasetByNameSql;
  private final String getVersionSchemaSql;
  private final String updateDatasetSql;
  private final String updateDatasetIfExpiredSql;
  private final String deleteDatasetByIdSql;
  private final String listValidDatasetsSql;
  private final String getDatasetVersionRenameFromSql;

  public DatasetDao(MySqlDataAccessor dataAccessor, MySqlAccountServiceConfig mySqlAccountServiceConfig,
      MySqlMetrics metrics) {
    this.dataAccessor = dataAccessor;
    this.mySqlAccountServiceConfig = mySqlAccountServiceConfig;
    this.metrics = metrics;
    //adding a dataset to db, we only support permanent dataset, and the only way to delete dataset is by calling deleteDatasetByIdSql
    insertDatasetSql =
        String.format("insert into %s (%s, %s, %s, %s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, now(3), ?, ?, ?, ?)",
            DATASET_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_POLICY,
            RETENTION_COUNT, RETENTION_TIME_IN_SECONDS, USER_TAGS);
    //if dataset is expired, we can create a dataset with same primary key, but we have to use update instead of insert.
    //need to make sure the deleted_ts is set back to null as the only way to delete a dataset is by calling deleteDatasetByIdSql.
    updateDatasetIfExpiredSql = String.format(
    "update %s set %s = ?, %s = now(3), %s = ?, %s = ?, %s = ?, %s = ?, %s = NULL where %s = ? and %s = ? and %s = ? and deleted_ts < now(3)",
        DATASET_TABLE, VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_POLICY, RETENTION_COUNT,
        RETENTION_TIME_IN_SECONDS, USER_TAGS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //update the dataset field, in order to support partial update, if one parameter is null, keep the original value.
    updateDatasetSql = String.format(
        "update %1$s set %2$s = now(3)," + "`retentionPolicy` = IFNULL(?, `retentionPolicy`), "
            + "`retentionCount` = IFNULL(?, `retentionCount`), "
            + "`retentionTimeInSeconds` = IFNULL(?, `retentionTimeInSeconds`)," + "`userTags` = IFNULL(?, `userTags`)"
            + " where %7$s = ? and %8$s = ? and %9$s = ?", DATASET_TABLE, LAST_MODIFIED_TIME, RETENTION_POLICY,
        RETENTION_COUNT, RETENTION_TIME_IN_SECONDS, USER_TAGS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //get the dataset metadata.
    getDatasetByNameSql =
        String.format("select %s, %s, %s, %s, %s, %s, %s, %s from %s where %s = ? and %s = ? and %s = ?", DATASET_NAME,
            VERSION_SCHEMA, LAST_MODIFIED_TIME, RETENTION_POLICY, RETENTION_COUNT,
            RETENTION_TIME_IN_SECONDS, USER_TAGS, DELETED_TS, DATASET_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //delete the dataset.
    deleteDatasetByIdSql = String.format(
    "update %s set %s = now(3), %s = now(3) where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ?",
        DATASET_TABLE, DELETED_TS, LAST_MODIFIED_TIME, DELETED_TS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //list all the datasets under a container.
    listValidDatasetsSql = String.format(
        "select %1$s from %2$s where (%3$s IS NULL or %3$s > now(3)) and %4$s = ? and %5$s = ? and (%1$s >= ? or ? IS NULL) "
            + "ORDER BY %1$s ASC LIMIT ?", DATASET_NAME, DATASET_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID);
    //get the version schema of a dataset. When we upload/download a dataset version, we need to check the version schema first.
    getVersionSchemaSql =
        String.format("select %s from %s where %s = ? and %s = ? and %s = ?", VERSION_SCHEMA, DATASET_TABLE, ACCOUNT_ID,
            CONTAINER_ID, DATASET_NAME);
    //insert a dataset version into db.
    insertDatasetVersionSql =
        String.format("insert into %s (%s, %s, %s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, now(3), now(3), ?, ?)",
            DATASET_VERSION_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION, CREATION_TIME, LAST_MODIFIED_TIME,
            DELETED_TS, DATASET_VERSION_STATE);
    //copy a dataset version from a source version.
    //We need to update the modify time so counter based purge policy won't delete it.
    copyToNewDatasetVersionSql = String.format(
        "INSERT INTO %1$s (%2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s, %10$s) "
            + "SELECT %2$s, %3$s, %4$s, ?, %6$s, NOW(3), %8$s, %9$s, ? "
            + "FROM %1$s WHERE %2$s = ? AND %3$s = ? AND %4$s = ? AND %5$s = ? AND %9$s = ? "
            + "AND (%8$s IS NULL OR %8$s > NOW(3))",
        DATASET_VERSION_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION, CREATION_TIME, LAST_MODIFIED_TIME,
        DELETED_TS, DATASET_VERSION_STATE, RENAME_FROM);
    //dataset version has in_progress and ready states.
    //when we put the dataset version, the flow is add dataset version in_progress -> add named blob -> add regular blob -> update dataset version to ready state.
    //Only the dataset version in ready state will be considered as a valid version.
    updateDatasetVersionStateSql =
        String.format("update %s set %s = ?, %s = now(3) where %s = ? and %s = ? and %s = ? and %s = ?",
            DATASET_VERSION_TABLE, DATASET_VERSION_STATE, LAST_MODIFIED_TIME, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
            VERSION);
    updateDatasetVersionStateAndDeletedTsSql =
        String.format("update %s set %s = ?, %s = now(3), %s = NULL where %s = ? and %s = ? and %s = ? and %s = ?",
            DATASET_VERSION_TABLE, DATASET_VERSION_STATE, LAST_MODIFIED_TIME, DELETED_TS, ACCOUNT_ID, CONTAINER_ID,
            DATASET_NAME, VERSION);
    //get the current latest version to download when user provide version == LATEST/MAJOR/MINOR/PATCH
    getLatestVersionSqlForDownload = String.format("select %1$s, %2$s, %9$s from %3$s "
            + "where (%4$s IS NULL or %4$s > now(3)) and (%5$s, %6$s, %7$s, %8$s) = (?, ?, ?, ?) ORDER BY %1$s DESC LIMIT 1",
        VERSION, DELETED_TS, DATASET_VERSION_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
        DATASET_VERSION_STATE, RENAME_FROM);
    //get the latest version + 1 for upload when user provide version == LATEST/MAJOR/MINOR/PATCH
    listLatestVersionSqlForUpload = String.format("select %1$s from %2$s "
            + "where (%3$s IS NULL or %3$s > now(3)) and (%4$s, %5$s, %6$s) = (?, ?, ?) ORDER BY %1$s DESC LIMIT 1", VERSION,
        DATASET_VERSION_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    //when we delete a dataset, we will delete all the versions under the dataset. This is to list all versions under a dataset.
    listValidVersionForDatasetDeletionSql = String.format(
        "select %s, %s, %s from %s where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ?", VERSION,
        RENAME_FROM, DELETED_TS, DATASET_VERSION_TABLE, DELETED_TS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME);
    // list all valid versions sorted by last modified time, and skip the first N records which is not out of retentionCount.
    listVersionByModifiedTimeAndFilterByRetentionSql = String.format("select %s, %s, %s from %s "
            + "where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ? and %s = ? ORDER BY %s DESC LIMIT ?, 100",
        VERSION, RENAME_FROM, DELETED_TS, DATASET_VERSION_TABLE, DELETED_TS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
        DATASET_VERSION_STATE, LAST_MODIFIED_TIME);
    //list dataset versions under a dataset by page.
    listValidDatasetVersionsByPageSql = String.format("select %1$s from %2$s "
            + "where (%3$s IS NULL or %3$s > now(3)) and %4$s = ? and %5$s = ? and %6$s = ? and %7$s = ? and %1$s >= ? "
            + "ORDER BY %1$s ASC LIMIT ?", VERSION, DATASET_VERSION_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
        DATASET_VERSION_STATE);
    //this is used for customized retention policy where we need to provide all versions under a dataset ordered by version number.
    listValidDatasetVersionsByListSql = String.format("select %1$s, %3$s, %8$s, %9$s from %2$s "
            + "where (%3$s IS NULL or %3$s > now(3)) and %4$s = ? and %5$s = ? and %6$s = ? and %7$s = ? "
            + "ORDER BY %1$s ASC", VERSION, DATASET_VERSION_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME,
        DATASET_VERSION_STATE, CREATION_TIME, RENAME_FROM);
    //this is to update the dataset version to permanent.
    updateDatasetVersionTtlSql = String.format(
        "update %s set %s = NULL where %s = ? and %s = ? and %s = ? and %s = ? and (deleted_ts is NULL or deleted_ts > now(3)) and %s = ?",
        DATASET_VERSION_TABLE, DELETED_TS, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION, DATASET_VERSION_STATE);
    //get the dataset version.
    getDatasetVersionByNameSql =
        String.format("select %s, %s, %s from %s where %s = ? and %s = ? and %s = ? and %s = ? and %s = ?", LAST_MODIFIED_TIME,
            DELETED_TS, RENAME_FROM, DATASET_VERSION_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION, DATASET_VERSION_STATE);
    //delete a dataset version
    deleteDatasetVersionByIdSql = String.format(
        "update %s set %s = now(3), %s = now(3) where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ? and %s = ? and %s <> ?",
        DATASET_VERSION_TABLE, DELETED_TS, LAST_MODIFIED_TIME, DELETED_TS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID,
        DATASET_NAME, VERSION, DATASET_VERSION_STATE);
    deleteDatasetVersionForDatasetDeleteByIdSql = String.format(
        "update %s set %s = now(3), %s = now(3) where (%s IS NULL or %s > now(3)) and %s = ? and %s = ? and %s = ? and %s = ?",
        DATASET_VERSION_TABLE, DELETED_TS, LAST_MODIFIED_TIME, DELETED_TS, DELETED_TS, ACCOUNT_ID, CONTAINER_ID,
        DATASET_NAME, VERSION);
    //if dataset version is expired, we can create a dataset version with same primary key, but we have to use update instead of insert.
    updateDatasetVersionIfExpiredSql = String.format(
        "update %s set %s = ?, %s = now(3), %s = now(3), %s = ?, %s = ?, %s = NULL where %s = ? and %s = ? and %s = ? and %s = ? and deleted_ts < now(3)",
        DATASET_VERSION_TABLE, VERSION, CREATION_TIME, LAST_MODIFIED_TIME, DELETED_TS, DATASET_VERSION_STATE,
        RENAME_FROM, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION);
    getDatasetVersionRenameFromSql =
        String.format("SELECT %1$s FROM %2$s WHERE %3$s = ? AND %4$s = ? AND %5$s = ? AND %6$s = ?", RENAME_FROM,
            DATASET_VERSION_TABLE, ACCOUNT_ID, CONTAINER_ID, DATASET_NAME, VERSION);
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
    String retentionPolicy = dataset.getRetentionPolicy();
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
    if (retentionPolicy != null) {
      statement.setString(5, retentionPolicy);
    } else {
      //set to the default
      statement.setString(5, DEFAULT_RETENTION_POLICY);
    }
    if (retentionCount != null) {
      statement.setInt(6, retentionCount);
    } else {
      statement.setObject(6, null);
    }
    if (retentionTimeInSeconds != null) {
      statement.setLong(7, retentionTimeInSeconds);
    } else {
      statement.setObject(7, null);
    }
    if (userTags != null) {
      statement.setString(8, userTagsInJson);
    } else {
      statement.setString(8, null);
    }
    statement.executeUpdate();
  }

  /**
   * Update the dataset version state.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @param datasetVersionState the {@link DatasetVersionState}
   * @throws SQLException
   * @throws AccountServiceException
   */
  public void updateDatasetVersionState(int accountId, int containerId, String accountName,
      String containerName, String datasetName, String version, DatasetVersionState datasetVersionState)
      throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    try {
      Dataset dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      long versionNumber = getVersionBasedOnSchema(version, dataset.getVersionSchema());
      PreparedStatement updateDatasetVersionStateStatement =
          dataAccessor.getPreparedStatement(updateDatasetVersionStateSql, true);
      executeUpdateDatasetVersionStateStatement(updateDatasetVersionStateStatement, accountId, containerId,
          dataset.getDatasetName(), versionNumber, datasetVersionState);
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Write);
      throw e;
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return the {@link DatasetVersionRecord}.
   * @throws SQLException
   */
  public DatasetVersionRecord addDatasetVersions(int accountId, int containerId, String accountName,
      String containerName, String datasetName, String version, long timeToLiveInSeconds, long creationTimeInMs,
      boolean datasetVersionTtlEnabled, DatasetVersionState datasetVersionState)
      throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    long versionNumber = 0;
    Dataset dataset = null;
    DatasetVersionRecord datasetVersionRecord;
    try {
      dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      if (isAutoCreatedVersionForUpload(version)) {
        datasetVersionRecord =
            retryWithLatestAutoIncrementedVersion(accountId, containerId, dataset, version, timeToLiveInSeconds,
                creationTimeInMs, datasetVersionTtlEnabled, datasetVersionState);
      } else {
        versionNumber = getVersionBasedOnSchema(version, dataset.getVersionSchema());
        datasetVersionRecord =
            addDatasetVersionHelper(accountId, containerId, dataset, version, timeToLiveInSeconds, creationTimeInMs,
                datasetVersionTtlEnabled, versionNumber, datasetVersionState);
      }
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
      return datasetVersionRecord;
    } catch (SQLException | AccountServiceException e) {
      if (e instanceof SQLIntegrityConstraintViolationException) {
        try {
          datasetVersionRecord =
              updateDatasetVersionHelper(accountId, containerId, dataset, version, timeToLiveInSeconds,
                  creationTimeInMs, datasetVersionTtlEnabled, versionNumber, datasetVersionState);
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
   * Rename a dataset version of {@link Dataset}
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param sourceVersion the source version to rename.
   * @param targetVersion the target version to rename.
   * @throws SQLException
   * @throws AccountServiceException
   */
  public void renameDatasetVersion(int accountId, int containerId, String accountName, String containerName,
      String datasetName, String sourceVersion, String targetVersion) throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    try {
      Dataset dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      if (!SEMANTIC_LONG.equals(dataset.getVersionSchema())) {
        throw new IllegalArgumentException("Rename API only supported for SEMANTIC_LONG schema");
      }
      if (isAutoIncrVersion(sourceVersion) || isAutoIncrVersion(targetVersion)) {
        throw new IllegalArgumentException("Rename API can't rename from/to auto incr version");
      }
      renameDatasetVersionHelper(accountId, containerId, datasetName, dataset.getVersionSchema(), sourceVersion,
          targetVersion);
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return the {@link DatasetVersionRecord} and the latest version.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private DatasetVersionRecord addDatasetVersionHelper(int accountId, int containerId, Dataset dataset, String version,
      long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled, long versionNumber,
      DatasetVersionState datasetVersionState)
      throws SQLException, AccountServiceException {
    PreparedStatement insertDatasetVersionStatement = dataAccessor.getPreparedStatement(insertDatasetVersionSql, true);
    long newExpirationTimeMs = executeAddDatasetVersionStatement(insertDatasetVersionStatement, accountId, containerId,
        dataset.getDatasetName(), versionNumber, timeToLiveInSeconds, creationTimeInMs, dataset,
        datasetVersionTtlEnabled, datasetVersionState);
    return new DatasetVersionRecord(accountId, containerId, dataset.getDatasetName(), version, newExpirationTimeMs,
        null);
  }

  /**
   * Helper function to rename a dataset version from source version to target version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param sourceVersion the source version to rename.
   * @param targetVersion the target version to rename.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private void renameDatasetVersionHelper(int accountId, int containerId, String datasetName,
      Dataset.VersionSchema versionSchema, String sourceVersion, String targetVersion)
      throws SQLException, AccountServiceException {
    long sourceVersionValue = getVersionBasedOnSchema(sourceVersion, versionSchema);
    long targetVersionValue = getVersionBasedOnSchema(targetVersion, versionSchema);
    String renameFromSourceVersion =
        getDatasetVersionRenameFromHelper(accountId, containerId, datasetName, sourceVersionValue);
    //sanity check for the renameFromSourceVersion
    if (renameFromSourceVersion != null) {
      getVersionBasedOnSchema(renameFromSourceVersion, versionSchema);
    }
    try {
      // Disable auto commits
      dataAccessor.setAutoCommit(false);
      PreparedStatement copyToNewDatasetVersionStatement =
          dataAccessor.getPreparedStatement(copyToNewDatasetVersionSql, true);
      //copy the source version to the new target version.
      //if source version has a rename field, set to the rename field
      String renameFromForTarget = renameFromSourceVersion != null? renameFromSourceVersion : sourceVersion;
      executeCopyToNewDatasetVersionStatement(copyToNewDatasetVersionStatement, accountId, containerId, datasetName,
          sourceVersionValue, targetVersionValue, renameFromForTarget);
      //mark the original dataset version state to renamed.
      PreparedStatement updateDatasetVersionStateAndDeletedTsStatement =
          dataAccessor.getPreparedStatement(updateDatasetVersionStateAndDeletedTsSql, true);
      executeUpdateDatasetVersionStateAndDeletedStatement(updateDatasetVersionStateAndDeletedTsStatement, accountId,
          containerId, datasetName, sourceVersionValue, DatasetVersionState.RENAMED);
      dataAccessor.commit();
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.rollback();
      dataAccessor.onException(e, Write);
      if (e instanceof SQLIntegrityConstraintViolationException) {
        throw new AccountServiceException(e.getMessage(), AccountServiceErrorCode.ResourceConflict);
      }
      throw e;
    } finally {
      // Close the connection to ensure subsequent queries are made in a new transaction and return the latest data
      dataAccessor.closeActiveConnection();
    }
  }

  private String getDatasetVersionRenameFromHelper(int accountId, int containerId, String datasetName,
      long sourceVersionValue) throws SQLException, AccountServiceException {
    PreparedStatement getDatasetVersionRenameFromStatement =
        dataAccessor.getPreparedStatement(getDatasetVersionRenameFromSql, true);
    try {
      return executeGetDatasetVersionRenameStatement(getDatasetVersionRenameFromStatement, accountId, containerId,
          datasetName, sourceVersionValue);
    } catch (AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  private String executeGetDatasetVersionRenameStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long sourceVersionValue) throws SQLException, AccountServiceException {
    ResultSet resultSet = null;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setLong(4, sourceVersionValue);
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Version not found for account: " + accountId + " container: " + containerId + " dataset: " + datasetName
                + " version: " + sourceVersionValue, AccountServiceErrorCode.NotFound);
      }
      return resultSet.getString(RENAME_FROM);
    } finally {
      closeQuietly(resultSet);
    }
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return the {@link DatasetVersionRecord} with the latest auto incremented dataset version
   * @throws AccountServiceException
   * @throws SQLException
   */
  private DatasetVersionRecord retryWithLatestAutoIncrementedVersion(int accountId, int containerId, Dataset dataset,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled,
      DatasetVersionState datasetVersionState)
      throws AccountServiceException, SQLException {
    long versionNumber;
    while (true) {
      try {
        versionNumber =
            getAutoIncrementedVersionBasedOnLatestAvailableVersionHelper(accountId, containerId, dataset.getVersionSchema(),
                dataset.getDatasetName(), version);
        version = convertVersionValueToVersion(versionNumber, dataset.getVersionSchema());
        //always retry with the latest version + 1 until it has been successfully uploading without conflict or failed with exception.
        return addDatasetVersions(accountId, containerId, dataset.getAccountName(), dataset.getContainerName(),
            dataset.getDatasetName(), version, timeToLiveInSeconds, creationTimeInMs, datasetVersionTtlEnabled,
            datasetVersionState);
      } catch (SQLException | AccountServiceException ex) {
        // if the version already exist in database, retry with the new latest version +1.
        if (!(ex instanceof SQLIntegrityConstraintViolationException)) {
          throw ex;
        }
      }
    }
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
  public DatasetVersionRecord getDatasetVersions(short accountId, short containerId, String accountName,
      String containerName, String datasetName, String version) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      //if dataset is deleted, we should not be able to get any dataset version.
      Dataset dataset;
      Dataset.VersionSchema versionSchema;
      long versionValue;
      DatasetVersionRecord result;
      if (isAutoCreatedVersionForDownload(version)) {
        dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
        versionSchema = dataset.getVersionSchema();
        PreparedStatement getLatestVersionStatement =
            dataAccessor.getPreparedStatement(getLatestVersionSqlForDownload, true);
        result =
            executeGetLatestVersionStatementForDownload(getLatestVersionStatement, accountId, containerId, datasetName,
                versionSchema);
      } else {
        dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, false);
        versionSchema = dataset.getVersionSchema();
        PreparedStatement getDatasetVersionStatement =
            dataAccessor.getPreparedStatement(getDatasetVersionByNameSql, false);
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
  public void deleteDatasetVersion(int accountId, int containerId, String datasetName, String version)
      throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement getVersionSchemaStatement = dataAccessor.getPreparedStatement(getVersionSchemaSql, true);
      Dataset.VersionSchema versionSchema =
          executeGetVersionSchema(getVersionSchemaStatement, accountId, containerId, datasetName);
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
   * Delete a dataset version for dataset deletion.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @throws SQLException
   * @throws AccountServiceException
   */
  public void deleteDatasetVersionForDatasetDelete(int accountId, int containerId, String datasetName, String version)
      throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement getVersionSchemaStatement = dataAccessor.getPreparedStatement(getVersionSchemaSql, true);
      Dataset.VersionSchema versionSchema =
          executeGetVersionSchema(getVersionSchemaStatement, accountId, containerId, datasetName);
      PreparedStatement deleteDatasetVersionStatement =
          dataAccessor.getPreparedStatement(deleteDatasetVersionForDatasetDeleteByIdSql, true);
      long versionValue = getVersionBasedOnSchema(version, versionSchema);
      executeDeleteDatasetVersionForDatasetDeleteStatement(deleteDatasetVersionStatement, accountId, containerId,
          datasetName, versionValue);
      dataAccessor.onSuccess(Delete, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Delete);
      throw e;
    }
  }

  /**
   * Update ttl for dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param datasetName the name of the dataset.
   * @param version the version of the dataset.
   * @throws SQLException
   */
  public void updateDatasetVersionTtl(int accountId, int containerId, String accountName, String containerName,
      String datasetName, String version) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      Dataset dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      long versionNumber = getVersionBasedOnSchema(version, dataset.getVersionSchema());
      PreparedStatement versionTtlUpdateSchemaStatement =
          dataAccessor.getPreparedStatement(updateDatasetVersionTtlSql, true);
      executeVersionTtlUpdateStatement(versionTtlUpdateSchemaStatement, accountId, containerId, datasetName,
          versionNumber);
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Delete);
      throw e;
    }
  }

  /**
   * Add {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param dataset the {@link Dataset}
   * @throws SQLException
   */
  public void addDataset(int accountId, int containerId, Dataset dataset)
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
  public void updateDataset(int accountId, int containerId, Dataset dataset)
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
   * Delete {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @throws SQLException
   */
  public void deleteDataset(int accountId, int containerId, String datasetName)
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
   * Get {@link Dataset} based on the supplied properties.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @return the {@link Dataset}
   * @throws SQLException
   */
  public Dataset getDataset(int accountId, int containerId, String accountName, String containerName,
      String datasetName) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      Dataset dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return dataset;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Get a list of dataset versions which is not expired or been deleted which belongs to the related dataset.
   * Since this is used for delete all version under a dataset, we should list all version including the in_progress states.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @return a list of dataset versions which is not expired or been deleted.
   * @throws SQLException
   */
  public List<DatasetVersionRecord> getAllValidVersionForDatasetDeletion(short accountId,
      short containerId, String datasetName) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement getVersionSchemaStatement = dataAccessor.getPreparedStatement(getVersionSchemaSql, false);
      Dataset.VersionSchema versionSchema =
          executeGetVersionSchema(getVersionSchemaStatement, accountId, containerId, datasetName);
      PreparedStatement listAllValidVersionForDatasetDeletionStatement =
          dataAccessor.getPreparedStatement(listValidVersionForDatasetDeletionSql, false);
      List<DatasetVersionRecord> datasetVersionRecords =
          executeListAllValidVersionForDatasetDeletionStatement(listAllValidVersionForDatasetDeletionStatement,
              accountId, containerId, datasetName, versionSchema);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return datasetVersionRecords;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * List all valid datasets under a container.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param pageToken the pageToken if total datasets number has reached out to limit.
   * @return a page of dataset names.
   * @throws SQLException
   */
  public Page<String> listAllValidDatasets(short accountId, short containerId, String pageToken)
      throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      dataAccessor.getDatabaseConnection(false);
      PreparedStatement listAllValidDatasetsStatement = dataAccessor.getPreparedStatement(listValidDatasetsSql, false);
      Page<String> datasetNameList =
          executeListAllValidDatasetsStatement(listAllValidDatasetsStatement, accountId, containerId, pageToken);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return datasetNameList;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * List all valid dataset versions under a dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param pageToken the pageToken if total datasets number has reached out to limit.
   * @return a page of dataset versions.
   * @throws SQLException
   * @throws AccountServiceException
   */
  public Page<String> listAllValidDatasetVersions(short accountId, short containerId, String datasetName,
      String pageToken) throws SQLException, AccountServiceException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement getVersionSchemaStatement = dataAccessor.getPreparedStatement(getVersionSchemaSql, false);
      Dataset.VersionSchema versionSchema =
          executeGetVersionSchema(getVersionSchemaStatement, accountId, containerId, datasetName);
      PreparedStatement listAllValidDatasetVersionsStatement =
          dataAccessor.getPreparedStatement(listValidDatasetVersionsByPageSql, false);
      Page<String> datasetVersionList =
          executeListAllValidDatasetVersionsStatement(listAllValidDatasetVersionsStatement, accountId, containerId,
              datasetName, pageToken, versionSchema);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return datasetVersionList;
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * List all valid version records.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionSchema the version schema of the dataset.
   * @return a list of all dataset versions.
   * @throws SQLException
   */
  public List<DatasetVersionRecord> listValidDatasetVersionsByListStatement(short accountId, short containerId,
      String datasetName, Dataset.VersionSchema versionSchema) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement listValidDatasetVersionsByListSchemaStatement =
          dataAccessor.getPreparedStatement(listValidDatasetVersionsByListSql, true);
      List<DatasetVersionRecord> results =
          executeListValidDatasetVersionsByListStatement(listValidDatasetVersionsByListSchemaStatement, accountId,
              containerId, datasetName, versionSchema);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return results;
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
  public List<DatasetVersionRecord> getAllValidVersionsOutOfRetentionCount(short accountId, short containerId,
      String accountName, String containerName, String datasetName) throws SQLException, AccountServiceException {
    long startTimeMs = System.currentTimeMillis();
    Integer retentionCount;
    Dataset.VersionSchema versionSchema;
    String retentionPolicy;
    List<DatasetVersionRecord> datasetVersionRecordList;
    try {
      //if dataset is deleted, we should not be able to get any dataset version.
      Dataset dataset = getDatasetHelper(accountId, containerId, accountName, containerName, datasetName, true);
      retentionCount = dataset.getRetentionCount();
      versionSchema = dataset.getVersionSchema();
      retentionPolicy = dataset.getRetentionPolicy();
      datasetVersionRecordList = new ArrayList<>();
      if (retentionCount == null) {
        dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
        return datasetVersionRecordList;
      }
      if (DEFAULT_RETENTION_POLICY.equals(retentionPolicy)) {
        datasetVersionRecordList =
            getDatasetVersionOutOfRetentionByDefault(accountId, containerId, datasetName, retentionCount,
                versionSchema);
        dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
        return datasetVersionRecordList;
      } else {
        List<DatasetVersionRecord> allValidDatasetVersionsOrderedByVersion =
            listValidDatasetVersionsByListStatement(accountId, containerId, datasetName, versionSchema);
        DatasetRetentionPolicy customizedRetentionPolicy = Utils.getObj(retentionPolicy);
        datasetVersionRecordList =
            customizedRetentionPolicy.getDatasetVersionOutOfRetention(allValidDatasetVersionsOrderedByVersion,
                retentionCount, versionSchema);
        dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
        return datasetVersionRecordList;
      }
    } catch (SQLException | AccountServiceException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } catch (ReflectiveOperationException e) {
      dataAccessor.onException(e, Read);
      metrics.constructRetentionPolicyFailureCount.inc();
      throw new IllegalStateException("Unable to construct the dataset retention policy");
    }
  }

  /**
   * Get dataset version out of retention by default retention policy.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param retentionCount the retention count of the dataset.
   * @param versionSchema the version schema of the dataset.
   * @return the list of dataset version out of retention by default retention policy.
   * @throws SQLException
   */
  private List<DatasetVersionRecord> getDatasetVersionOutOfRetentionByDefault(short accountId, short containerId,
      String datasetName, Integer retentionCount, Dataset.VersionSchema versionSchema) throws SQLException {
    PreparedStatement listAllValidVersionsOrderedByLastModifiedTimeStatement =
        dataAccessor.getPreparedStatement(listVersionByModifiedTimeAndFilterByRetentionSql, true);
    return executeListAllValidVersionsOrderedByLastModifiedTimeStatement(
        listAllValidVersionsOrderedByLastModifiedTimeStatement, accountId, containerId, datasetName, retentionCount,
        versionSchema);
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
      statement.setInt(4, DatasetVersionState.READY.ordinal());
      statement.setInt(5, retentionCount);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        long versionValue = resultSet.getLong(VERSION);
        String renameFrom = resultSet.getString(RENAME_FROM);
        Timestamp deletionTime = resultSet.getTimestamp(DELETED_TS);
        String version = convertVersionValueToVersion(versionValue, versionSchema);
        datasetVersionRecordList.add(
            new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime),
                renameFrom));
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
        version = convertVersionToSemantic(versionValue);
        return version;
      case SEMANTIC_LONG:
        version = convertVersionToSemanticLong(versionValue);
        return version;
      default:
        throw new IllegalArgumentException("Unsupported version schema: " + versionSchema);
    }
  }

  /**
   * Produces a string-represented value in the for {major}.{minor}.{patch}
   * @param versionValue the value of the version from DB.
   * @return the converted string format version.
   */
  private String convertVersionToSemantic(long versionValue) {
    //Given a version number MAJOR.MINOR.PATCH, increment the:
    //1.MAJOR version when you make incompatible API changes
    //2.MINOR version when you add functionality in a backwards compatible manner
    //3.PATCH version when you make backwards compatible bug fixes
    //The MAJOR,MINOR and PATCH version are non-negative value.
    long major = versionValue / 1000000L;
    long minor = (versionValue % 1000000L) / 1000L;
    long patch = versionValue % 1000L;
    return String.format("%d.%d.%d", major, minor, patch);
  }

  /**
   * Produces a string-represented value in the for {major}.{minor}.{patch}.{revision} from a long value, by performing
   * the following calculation: A * 10^9 + B * 10^6 + C * 10^3 + D, where version is A.B.C.D.
   * @param versionValue the value of the version from DB.
   * @return the converted string format version.
   */
  private String convertVersionToSemanticLong(long versionValue) {
    //Given a version number MAJOR.MINOR.PATCH.REVISION, increment the:
    //1.MAJOR version when you make incompatible API changes
    //2.MINOR version when you add functionality in a backwards compatible manner
    //3.PATCH version when you make backwards compatible bug fixes
    //4.REVISION   version when you make fixes atop an original major.minor.patch version.
    //The MAJOR, MINOR, PATCH, and REVISION version are non-negative value.
    long major = versionValue / 1000000000L;
    long minor = (versionValue % 1000000000L) / 1000000L;
    long patch = (versionValue % 1000000L) / 1000L;
    long revision = versionValue % 1000L;
    return String.format("%d.%d.%d.%d", major, minor, patch, revision);
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
      //we should list all versions including in_progress version in order to get the latest version for next upload.
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
    String renameFrom;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setInt(4, DatasetVersionState.READY.ordinal());
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Latest version not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName, AccountServiceErrorCode.NotFound);
      }
      long versionValue = resultSet.getLong(VERSION);
      version = convertVersionValueToVersion(versionValue, versionSchema);
      renameFrom = resultSet.getString(RENAME_FROM);
      deletionTime = resultSet.getTimestamp(DELETED_TS);
    } finally {
      closeQuietly(resultSet);
    }
    return new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime),
        renameFrom);
  }

  /**
   * Generate the auto incremented version based on the latest valid version and the version schema.
   * @param versionValue the latest version value for current dataset.
   * @param versionSchema the {@link com.github.ambry.account.Dataset.VersionSchema} of the dataset.
   * @param version the version string to indicate if user provide LATEST, MAJOR, MINOR, PATCH, or REVISION option.
   * @return the auto incremented value base on latest version.
   */
  private long generateAutoIncrementedVersionBasedOnSchema(long versionValue, Dataset.VersionSchema versionSchema,
      String version) {
    long autoIncrementedVersionValue;
    switch (versionSchema) {
      case TIMESTAMP:
        throw new IllegalArgumentException("We don't support latest version for timestamp schema");
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
          if (majorVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Major version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentMajorVersion: " + majorVersion);
          }
        } else if (MINOR.equals(version)) {
          minorVersion += 1;
          patchVersion = 0;
          if (minorVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Minor version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentMinorVersion: " + minorVersion);
          }
        } else {
          patchVersion += 1;
          if (patchVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Patch version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentPatchVersion: " + patchVersion);
          }
        }
        return majorVersion * 1000000 + minorVersion * 1000 + patchVersion;
      case SEMANTIC_LONG:
        //Given a version number MAJOR.MINOR.PATCH, increment the:
        //1.MAJOR version when you make incompatible API changes
        //2.MINOR version when you add functionality in a backwards compatible manner
        //3.PATCH version when you make backwards compatible bug fixes
        //4.REVISION   version when you make fixes atop an original major.minor.patch version.
        //The MAJOR, MINOR, PATCH, and REVISION version are non-negative value.
        if (!MAJOR.equals(version) && !MINOR.equals(version) && !PATCH.equals(version) && !REVISION.equals(version)) {
          throw new IllegalArgumentException(
              "The current version string " + version + " is not supported for Semantic Long version schema");
        }
        long majorLongVersion = versionValue / 1000000000L;
        long minorLongVersion = (versionValue % 1000000000L) / 1000000L;
        long patchLongVersion = (versionValue % 1000000L) / 1000L;
        long revisionLongVersion = versionValue % 1000L;
        if (MAJOR.equals(version)) {
          majorLongVersion += 1;
          minorLongVersion = 0;
          patchLongVersion = 0;
          revisionLongVersion = 0;
          if (majorLongVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Major version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentMajorVersion: " + majorLongVersion);
          }
        } else if (MINOR.equals(version)) {
          minorLongVersion += 1;
          patchLongVersion = 0;
          revisionLongVersion = 0;
          if (minorLongVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Minor version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentMinorVersion: " + minorLongVersion);
          }
        } else if (PATCH.equals(version)) {
          patchLongVersion += 1;
          revisionLongVersion = 0;
          if (patchLongVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Patch version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentPatchVersion: " + patchLongVersion);
          }
        } else {
          revisionLongVersion += 1;
          if (revisionLongVersion > MAX_SEMANTIC_VERSION_PART_VALUE) {
            throw new IllegalArgumentException("The largest version for Semantic Revision version should be less than "
                + MAX_SEMANTIC_VERSION_PART_VALUE + ", currentRevisionVersion: " + revisionLongVersion);
          }
        }
        return majorLongVersion * 1000000000 + minorLongVersion * 1000000 + patchLongVersion * 1000 + revisionLongVersion;
      default:
        throw new IllegalArgumentException("Unsupported version schema: " + versionSchema);
    }
  }

  /**
   * @param version the version string.
   * @return {@code} true if the version is auto created version for upload.
   */
  private boolean isAutoCreatedVersionForUpload(String version) {
    return LATEST.equals(version)
        || MAJOR.equals(version)
        || MINOR.equals(version)
        || PATCH.equals(version)
        || REVISION.equals(version);
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
  private List<DatasetVersionRecord> executeListAllValidVersionForDatasetDeletionStatement(PreparedStatement statement,
      int accountId, int containerId, String datasetName, Dataset.VersionSchema versionSchema) throws SQLException {
    ResultSet resultSet = null;
    List<DatasetVersionRecord> datasetVersionRecords = new ArrayList<>();
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        long versionValue = resultSet.getLong(VERSION);
        String version = convertVersionValueToVersion(versionValue, versionSchema);
        String renameFrom = resultSet.getString(RENAME_FROM);
        Timestamp deletionTime = resultSet.getTimestamp(DELETED_TS);
        DatasetVersionRecord datasetVersionRecord =
            new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime),
                renameFrom);
        datasetVersionRecords.add(datasetVersionRecord);
      }
      return datasetVersionRecords;
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Execute listValidDatasetsSql statement.
   * @param statement the listValidDatasetsSql statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param pageToken the page token.
   * @return the page of the dataset names.
   * @throws SQLException
   */
  private Page<String> executeListAllValidDatasetsStatement(PreparedStatement statement, int accountId,
      int containerId, String pageToken) throws SQLException {
    ResultSet resultSet = null;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, pageToken);
      statement.setString(4, pageToken);
      statement.setInt(5, mySqlAccountServiceConfig.listDatasetsMaxResult + 1);
      resultSet = statement.executeQuery();
      List<String> entries = new ArrayList<>();
      String nextContinuationToken = null;
      int resultIndex = 0;
      while (resultSet.next()) {
        if (resultIndex == mySqlAccountServiceConfig.listDatasetsMaxResult) {
          nextContinuationToken = resultSet.getString(1);
          break;
        }
        resultIndex++;
        entries.add(resultSet.getString(1));
      }
      return new Page<>(entries, nextContinuationToken);
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Execute listValidDatasetVersionsSql statement.
   * @param statement the listValidDatasetVersionsSql statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param pageToken the page token.
   * @param versionSchema the dataset version schema.
   * @return the page of the dataset versions.
   * @throws SQLException
   */
  private Page<String> executeListAllValidDatasetVersionsStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName, String pageToken, Dataset.VersionSchema versionSchema) throws SQLException {
    ResultSet resultSet = null;
    try {
      long pageTokenValue = pageToken == null? 0 : getVersionBasedOnSchema(pageToken, versionSchema);
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setInt(4, DatasetVersionState.READY.ordinal());
      statement.setLong(5, pageTokenValue);
      statement.setInt(6, mySqlAccountServiceConfig.listDatasetVersionsMaxResult + 1);
      resultSet = statement.executeQuery();
      List<String> entries = new ArrayList<>();
      String nextContinuationToken = null;
      int resultIndex = 0;
      while (resultSet.next()) {
        long versionValue = resultSet.getLong(VERSION);
        String version = convertVersionValueToVersion(versionValue, versionSchema);
        if (resultIndex == mySqlAccountServiceConfig.listDatasetVersionsMaxResult) {
          nextContinuationToken = version;
          break;
        }
        resultIndex++;
        entries.add(version);
      }
      return new Page<>(entries, nextContinuationToken);
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Execute listValidDatasetVersionsByListSql statement.
   * @param statement the listValidDatasetVersionsByListSql statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionSchema the version schema of the dataset.
   * @return the list of all valid dataset versions under a dataset.
   * @throws SQLException
   */
  private List<DatasetVersionRecord> executeListValidDatasetVersionsByListStatement(PreparedStatement statement,
      int accountId, int containerId, String datasetName, Dataset.VersionSchema versionSchema) throws SQLException {
    ResultSet resultSet = null;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setInt(4, DatasetVersionState.READY.ordinal());
      resultSet = statement.executeQuery();
      List<DatasetVersionRecord> entries = new ArrayList<>();
      while (resultSet.next()) {
        long versionValue = resultSet.getLong(VERSION);
        long expirationTimeMs = timestampToMs(resultSet.getTimestamp(DELETED_TS));
        long creationTimeMs = timestampToMs(resultSet.getTimestamp(CREATION_TIME));
        String rename_from = resultSet.getString(RENAME_FROM);
        String version = convertVersionValueToVersion(versionValue, versionSchema);
        entries.add(
            new DatasetVersionRecord(accountId, containerId, datasetName, version, expirationTimeMs, creationTimeMs,
                rename_from));
      }
      return entries;
    } finally {
      closeQuietly(resultSet);
    }
  }

  /**
   * Get the auto incremented version based on latest version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param versionSchema the {@link Dataset.VersionSchema} of the dataset.
   * @param datasetName the name of the dataset.
   * @return the latest version.
   * @throws SQLException
   */
  private long getAutoIncrementedVersionBasedOnLatestAvailableVersionHelper(int accountId, int containerId,
      Dataset.VersionSchema versionSchema, String datasetName, String version)
      throws SQLException, AccountServiceException {
    long latestVersionValue;
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement listLatestVersionStatement =
          dataAccessor.getPreparedStatement(listLatestVersionSqlForUpload, true);
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @throws SQLException
   */
  private long executeAddDatasetVersionStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long version, long timeToLiveInSeconds, long creationTimeInMs, Dataset dataset,
      boolean datasetVersionTtlEnabled, DatasetVersionState datasetVersionState) throws SQLException {
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
    statement.setInt(6, datasetVersionState.ordinal());
    statement.executeUpdate();
    return updatedExpirationTimeMs;
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return the updated expiration time.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private long executeUpdateDatasetVersionIfExpiredSqlStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName, long version, long timeToLiveInSeconds, long creationTimeInMs,
      Dataset dataset, boolean datasetVersionTtlEnabled, DatasetVersionState datasetVersionState)
      throws SQLException, AccountServiceException {
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
    statement.setInt(3, datasetVersionState.ordinal());
    statement.setInt(4, accountId);
    statement.setInt(5, containerId);
    statement.setString(6, datasetName);
    statement.setLong(7, version);
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException("The dataset version already exist", AccountServiceErrorCode.ResourceConflict);
    }
    return updatedExpirationTimeMs;
  }

  /**
   * Execute the updateDatasetVersionStateSql statement.
   * @param statement the updateDatasetVersionStateSql statement.
   * @param statement the mysql statement to delete dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version value of the dataset.
   * @param datasetVersionState the {@link DatasetVersionState}
   * @throws SQLException
   */
  private void executeUpdateDatasetVersionStateStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long versionValue, DatasetVersionState datasetVersionState) throws SQLException {
    statement.setInt(1, datasetVersionState.ordinal());
    statement.setInt(2, accountId);
    statement.setInt(3, containerId);
    statement.setString(4, datasetName);
    statement.setLong(5, versionValue);
    statement.executeUpdate();
  }

  /**
   * Execute the updateDatasetVersionStateAndDeletedTsSql statement.
   * @param statement the updateDatasetVersionStateSql statement.
   * @param statement the mysql statement to delete dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version value of the dataset.
   * @param datasetVersionState the {@link DatasetVersionState}
   * @throws SQLException
   */
  private void executeUpdateDatasetVersionStateAndDeletedStatement(PreparedStatement statement, int accountId,
      int containerId, String datasetName, long versionValue, DatasetVersionState datasetVersionState)
      throws SQLException {
    statement.setInt(1, datasetVersionState.ordinal());
    statement.setInt(2, accountId);
    statement.setInt(3, containerId);
    statement.setString(4, datasetName);
    statement.setLong(5, versionValue);
    statement.executeUpdate();
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
  private void executeDeleteDatasetVersionStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long versionValue) throws SQLException, AccountServiceException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    statement.setLong(4, versionValue);
    statement.setInt(5, DatasetVersionState.RENAMED.ordinal());
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException(
          "Dataset not found qualified record to delete for account: " + accountId + " container: " + containerId
              + " dataset: " + datasetName + " version: " + versionValue, AccountServiceErrorCode.NotFound);
    }
  }

  /**
   * Execute deleteDatasetVersionForDatasetDeleteByIdSql to delete Dataset version when delete a dataset.
   * @param statement the mysql statement to delete dataset version.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version value of the dataset.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private void executeDeleteDatasetVersionForDatasetDeleteStatement(PreparedStatement statement, int accountId,
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
   * Execute CopyToNewDatasetVersionStatement to copy the source version to a target version.
   * @param statement the rename dataset statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param sourceVersionValue the long value of source version.
   * @param targetVersionValue the long value of target version.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private void executeCopyToNewDatasetVersionStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long sourceVersionValue, long targetVersionValue, String renameFrom)
      throws SQLException, AccountServiceException {
    statement.setLong(1, targetVersionValue);
    statement.setString(2, renameFrom);
    statement.setInt(3, accountId);
    statement.setInt(4, containerId);
    statement.setString(5, datasetName);
    statement.setLong(6, sourceVersionValue);
    statement.setInt(7, DatasetVersionState.READY.ordinal());
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException(
          "Can't find source version to rename for account: " + accountId + " container: " + containerId + " dataset: "
              + datasetName + " version: " + sourceVersionValue, AccountServiceErrorCode.NotFound);
    }
  }

  /**
   * Execute version ttl update statement.
   * @param statement the updateDatasetVersionTtlSql statement.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param datasetName the name of the dataset.
   * @param versionValue the version value of the dataset.
   * @throws SQLException
   * @throws AccountServiceException
   */
  private void executeVersionTtlUpdateStatement(PreparedStatement statement, int accountId, int containerId,
      String datasetName, long versionValue) throws SQLException, AccountServiceException {
    statement.setInt(1, accountId);
    statement.setInt(2, containerId);
    statement.setString(3, datasetName);
    statement.setLong(4, versionValue);
    //only update ttl against a ready status record.
    statement.setInt(5, DatasetVersionState.READY.ordinal());
    int count = statement.executeUpdate();
    if (count <= 0) {
      throw new AccountServiceException(
          "Dataset can not be ttl updated for account: " + accountId + " container: " + containerId + " dataset: "
              + datasetName + " version: " + versionValue, AccountServiceErrorCode.NotFound);
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
   * @param datasetVersionState the {@link DatasetVersionState}
   * @return
   * @throws SQLException
   * @throws AccountServiceException
   */
  private DatasetVersionRecord updateDatasetVersionHelper(int accountId, int containerId, Dataset dataset,
      String version, long timeToLiveInSeconds, long creationTimeInMs, boolean datasetVersionTtlEnabled,
      long versionNumber, DatasetVersionState datasetVersionState) throws SQLException, AccountServiceException {
    PreparedStatement updateDatasetVersionSqlIfExpiredStatement =
        dataAccessor.getPreparedStatement(updateDatasetVersionIfExpiredSql, true);
    long newExpirationTimeMs =
        executeUpdateDatasetVersionIfExpiredSqlStatement(updateDatasetVersionSqlIfExpiredStatement, accountId,
            containerId, dataset.getDatasetName(), versionNumber, timeToLiveInSeconds, creationTimeInMs, dataset,
            datasetVersionTtlEnabled, datasetVersionState);
    return new DatasetVersionRecord(accountId, containerId, dataset.getDatasetName(), version, newExpirationTimeMs,
        null);
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
    String retentionPolicy = dataset.getRetentionPolicy();
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
    if (retentionPolicy != null) {
      statement.setString(2, retentionPolicy);
    } else {
      statement.setString(2, DEFAULT_RETENTION_POLICY);
    }
    if (retentionCount != null) {
      statement.setInt(3, retentionCount);
    } else {
      statement.setObject(3, null);
    }
    if (retentionTimeInSeconds != null) {
      statement.setLong(4, retentionTimeInSeconds);
    } else {
      statement.setObject(4, null);
    }
    if (userTags != null) {
      statement.setString(5, userTagsInJson);
    } else {
      statement.setString(5, null);
    }
    statement.setInt(6, accountId);
    statement.setInt(7, containerId);
    statement.setString(8, datasetName);
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
    String retentionPolicy = dataset.getRetentionPolicy();
    if (retentionPolicy != null) {
      statement.setString(1, retentionPolicy);
    } else {
      //For update, do not set DEFAULT_RETENTION_POLICY as default.
      statement.setString(1, null);
    }
    Integer retentionCount = dataset.getRetentionCount();
    if (retentionCount != null) {
      statement.setInt(2, retentionCount);
    } else {
      statement.setObject(2, null);
    }
    Long retentionTimeInSeconds = dataset.getRetentionTimeInSeconds();
    if (retentionTimeInSeconds != null) {
      statement.setLong(3, retentionTimeInSeconds);
    } else {
      statement.setObject(3, null);
    }
    statement.setString(4, userTagsInJson);
    statement.setInt(5, accountId);
    statement.setInt(6, containerId);
    statement.setString(7, dataset.getDatasetName());
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
    String retentionPolicy;
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
      deletionTime = resultSet.getTimestamp(DELETED_TS);
      long currentTime = System.currentTimeMillis();
      if (compareTimestamp(deletionTime, currentTime) <= 0) {
        throw new AccountServiceException(
            "Dataset expired for account: " + accountId + " container: " + containerId + " dataset: " + datasetName,
            AccountServiceErrorCode.Deleted);
      }
      versionSchema = Dataset.VersionSchema.values()[resultSet.getInt(VERSION_SCHEMA)];
      retentionPolicy = resultSet.getObject(RETENTION_POLICY, String.class);
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
    return new Dataset(accountName, containerName, datasetName, versionSchema, retentionPolicy, retentionCount,
        retentionTimeInSeconds, userTags);
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
      case SEMANTIC_LONG:
        return sanityCheckAndConvertSemanticLongVersion(version);
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
   * Sanity check for semantic long version.
   * @param version The version from request.
   * @return the converted version which will be stored in DatasetVersions DB.
   */
  private long sanityCheckAndConvertSemanticLongVersion(String version) {
    long majorVersion;
    long minorVersion;
    long patchVersion;
    long revisionVersion;
    try {
      String[] versionArray = version.split("[.]", 4);
      revisionVersion = Long.parseLong(versionArray[3]);
      patchVersion = Long.parseLong(versionArray[2]);
      minorVersion = Long.parseLong(versionArray[1]);
      majorVersion = Long.parseLong(versionArray[0]);
      if (revisionVersion < 0 || patchVersion < 0 || minorVersion < 0 || majorVersion < 0) {
        throw new IllegalArgumentException(
            "The major, minor, patch, or revision version is less than 0, major version: " + majorVersion + " minor version: "
                + minorVersion + " patch version: " + patchVersion + " revision version: " + revisionVersion);
      }
      if (revisionVersion > 999 || patchVersion > 999 || minorVersion > 999) {
        throw new IllegalArgumentException(
            "The minor, patch, or revision version exceeded max allowed number: 999, minor version: " + minorVersion
                + " patch version: " + patchVersion + " revision version: " + revisionVersion);
      }
      //We set the max major version same as revision/patch/minor version based on current use cases.
      if (majorVersion > mySqlAccountServiceConfig.maxMajorVersionForSemanticSchemaDataset) {
        throw new IllegalArgumentException("The major version exceeded max allowed number: "
            + mySqlAccountServiceConfig.maxMajorVersionForSemanticSchemaDataset + " major version: " + majorVersion);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Version does not match the semantic long version format MAJOR.MINOR.PATCH.REVISION, version: " + version);
    }
    return majorVersion * 1000000000 + minorVersion * 1000000 + patchVersion * 1000 + revisionVersion;
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
    String renameFrom;
    try {
      statement.setInt(1, accountId);
      statement.setInt(2, containerId);
      statement.setString(3, datasetName);
      statement.setLong(4, versionValue);
      statement.setInt(5, DatasetVersionState.READY.ordinal());
      resultSet = statement.executeQuery();
      if (!resultSet.next()) {
        throw new AccountServiceException(
            "Dataset version not found for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName + " version: " + version, AccountServiceErrorCode.NotFound);
      }
      renameFrom = resultSet.getString(RENAME_FROM);
      deletionTime = resultSet.getTimestamp(DELETED_TS);
      long currentTime = System.currentTimeMillis();
      if (compareTimestamp(deletionTime, currentTime) <= 0) {
        throw new AccountServiceException(
            "Dataset version expired for account: " + accountId + " container: " + containerId + " dataset: "
                + datasetName + " version: " + version, AccountServiceErrorCode.Deleted);
      }
    } finally {
      closeQuietly(resultSet);
    }
    return new DatasetVersionRecord(accountId, containerId, datasetName, version, timestampToMs(deletionTime),
        renameFrom);
  }


  /**
   * Helper function to get the dataset.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @param accountName the name for the parent account.
   * @param containerName the name for the container.
   * @param datasetName the name of the dataset.
   * @param needWritable if we need writable for connection.
   * @return the {@link Dataset}
   * @throws SQLException
   * @throws AccountServiceException
   */
  private Dataset getDatasetHelper(int accountId, int containerId, String accountName, String containerName,
      String datasetName, boolean needWritable) throws SQLException, AccountServiceException {
    PreparedStatement getDatasetStatement = dataAccessor.getPreparedStatement(getDatasetByNameSql, needWritable);
    return executeGetDatasetStatement(getDatasetStatement, accountId, containerId, accountName, containerName,
        datasetName);
  }

  private boolean isAutoIncrVersion(String version) {
    final Set<String> ALL_SEMANTIC_COMPONENTS = new HashSet<>(Arrays.asList(LATEST, MAJOR, MINOR, PATCH, REVISION));
    return ALL_SEMANTIC_COMPONENTS.contains(version);
  }
}

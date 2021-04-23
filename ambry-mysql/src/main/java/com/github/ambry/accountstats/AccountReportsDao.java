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
package com.github.ambry.accountstats;

import com.github.ambry.mysql.BatchUpdater;
import com.github.ambry.mysql.MySqlDataAccessor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


/**
 * AccountReports Data Access Object.
 */
public class AccountReportsDao {
  public static final String ACCOUNT_REPORTS_TABLE = "AccountReports";
  public static final String CLUSTER_NAME_COLUMN = "clusterName";
  public static final String HOSTNAME_COLUMN = "hostname";
  public static final String PARTITION_ID_COLUMN = "partitionId";
  public static final String ACCOUNT_ID_COLUMN = "accountId";
  public static final String CONTAINER_ID_COLUMN = "containerId";
  public static final String STORAGE_USAGE_COLUMN = "storageUsage";
  public static final String UPDATED_AT_COLUMN = "updatedAt";

  private static final Logger logger = LoggerFactory.getLogger(AccountReportsDao.class);
  private static final String insertSql = String.format(
      "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE %s=?, %s=NOW()",
      ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN,
      CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN);
  private static final String querySqlForClusterAndHost =
      String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = ? AND %s = ?", PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN,
          CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN, ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
          HOSTNAME_COLUMN);
  private static final String deletePartitionSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
          HOSTNAME_COLUMN, PARTITION_ID_COLUMN);
  private static final String deleteAccountSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
          HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN);
  private static final String deleteContainerSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE,
          CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN);
  private final MySqlDataAccessor dataAccessor;

  /**
   * Constructor to create a {@link AccountReportsDao}.
   * @param dataAccessor The underlying {@link MySqlDataAccessor}.
   */
  AccountReportsDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param clusterName the name of target cluster
   * @param hostname the name of target host
   * @param partitionId The partition id of this account/container usage.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   */
  void updateStorageUsage(String clusterName, String hostname, int partitionId, short accountId, short containerId,
      long storageUsage) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql, true);
      insertStatement.setString(1, clusterName);
      insertStatement.setString(2, hostname);
      // The data type of partition id, account id and container id are not SMALLINT, but INT in MySQL, for
      // future extension
      insertStatement.setInt(3, partitionId);
      insertStatement.setInt(4, accountId);
      insertStatement.setInt(5, containerId);
      insertStatement.setLong(6, storageUsage);
      insertStatement.setLong(7, storageUsage);
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error(String.format("Failed to execute updated on %s, with parameter %d %d %d %d", ACCOUNT_REPORTS_TABLE,
          partitionId, accountId, containerId, storageUsage), e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clusterName} and {@code hostname}. The result will be applied to the
   * {@link ContainerUsageFunction}.
   * @param clusterName The clusterName.
   * @param hostname The hostname.
   * @param func The {@link ContainerUsageFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryStorageUsageForHost(String clusterName, String hostname, ContainerUsageFunction func) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(querySqlForClusterAndHost, false);
      queryStatement.setString(1, clusterName);
      queryStatement.setString(2, hostname);
      ResultSet resultSet = queryStatement.executeQuery();
      while (resultSet.next()) {
        int partitionId = resultSet.getInt(PARTITION_ID_COLUMN);
        int accountId = resultSet.getInt(ACCOUNT_ID_COLUMN);
        int containerId = resultSet.getInt(CONTAINER_ID_COLUMN);
        long storageUsage = resultSet.getLong(STORAGE_USAGE_COLUMN);
        long updatedAtMs = resultSet.getTimestamp(UPDATED_AT_COLUMN).getTime();
        func.apply(partitionId, (short) accountId, (short) containerId, storageUsage, updatedAtMs);
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      logger.error(
          String.format("Failed to execute query on %s, with parameter %s %s", ACCOUNT_REPORTS_TABLE, clusterName,
              hostname), e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clusterName}, {@code hostname} and {@code partitionId}.
   * @param clusterName The clusterName
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @throws SQLException
   */
  void deleteStorageUsageForPartition(String clusterName, String hostname, int partitionId) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement deleteStatement = dataAccessor.getPreparedStatement(deletePartitionSql, true);
      deleteStatement.setString(1, clusterName);
      deleteStatement.setString(2, hostname);
      deleteStatement.setInt(3, partitionId);
      deleteStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error("Failed to execute DELETE on {}, with parameter {}", ACCOUNT_REPORTS_TABLE, partitionId, e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clusterName}, {@code hostname}, {@code partitionId} and {@code accountId}.
   * @param clusterName The clusterName
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @param accountId The accountId
   * @throws SQLException
   */
  void deleteStorageUsageForAccount(String clusterName, String hostname, int partitionId, int accountId)
      throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement deleteStatement = dataAccessor.getPreparedStatement(deleteAccountSql, true);
      deleteStatement.setString(1, clusterName);
      deleteStatement.setString(2, hostname);
      deleteStatement.setInt(3, partitionId);
      deleteStatement.setInt(4, accountId);
      deleteStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error("Failed to execute DELETE on {}, with parameter {}, {}", ACCOUNT_REPORTS_TABLE, partitionId,
          accountId, e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clusterName}, {@code hostname}, {@code partitionId},
   * {@code accountId} and {@code containerId}.
   * @param clusterName The clusterName
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @param accountId The accountId
   * @param containerId The containerId
   * @throws SQLException
   */
  void deleteStorageUsageForContainer(String clusterName, String hostname, int partitionId, int accountId,
      int containerId) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement deleteStatement = dataAccessor.getPreparedStatement(deleteContainerSql, true);
      deleteStatement.setString(1, clusterName);
      deleteStatement.setString(2, hostname);
      deleteStatement.setInt(3, partitionId);
      deleteStatement.setInt(4, accountId);
      deleteStatement.setInt(5, containerId);
      deleteStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error("Failed to execute DELETE on {}, with parameter {}, {}, {}", ACCOUNT_REPORTS_TABLE, partitionId,
          accountId, containerId, e);
      throw e;
    }
  }

  /**
   * A batch updater to update storage usage.
   */
  class StorageBatchUpdater extends BatchUpdater {

    /**
     * Constructor to instantiate a {@link StorageBatchUpdater}.
     * @param maxBatchSize The max batch size.
     * @throws SQLException
     */
    public StorageBatchUpdater(int maxBatchSize) throws SQLException {
      super(dataAccessor, insertSql, ACCOUNT_REPORTS_TABLE, maxBatchSize);
    }

    /**
     * Supply values to the prepared statement and add it to the batch updater.
     * @param clusterName the cluster name
     * @param hostname the hostname
     * @param partitionId The partition id of this account/container usage.
     * @param accountId The account id.
     * @param containerId The container id.
     * @param storageUsage The storage usage in bytes.
     * @throws SQLException
     */
    public void addUpdateToBatch(String clusterName, String hostname, int partitionId, short accountId,
        short containerId, long storageUsage) throws SQLException {
      addUpdateToBatch(statement -> {
        statement.setString(1, clusterName);
        statement.setString(2, hostname);
        statement.setInt(3, partitionId);
        statement.setInt(4, accountId);
        statement.setInt(5, containerId);
        statement.setLong(6, storageUsage);
        statement.setLong(7, storageUsage);
      });
    }
  }
}

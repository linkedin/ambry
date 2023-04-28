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
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  public static final String PHYSICAL_STORAGE_USAGE_COLUMN = "physicalStorageUsage";
  public static final String NUMBER_OF_BLOBS_COLUMN = "numberOfBlobs";
  public static final String UPDATED_AT_COLUMN = "updatedAt";

  private static final Logger logger = LoggerFactory.getLogger(AccountReportsDao.class);
  private static final String insertSql = String.format(
      "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE %s=?, %s=?, %s=?, %s=NOW()",
      ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN,
      CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN,
      UPDATED_AT_COLUMN, STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN,
      UPDATED_AT_COLUMN);
  private static final String querySqlForClusterAndHost =
      String.format("SELECT %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s = ? AND %s = ?", PARTITION_ID_COLUMN,
          ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN,
          NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN, ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN);
  private static final String deleteHostSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=?", ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN);
  private static final String deletePartitionSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
          HOSTNAME_COLUMN, PARTITION_ID_COLUMN);
  private static final String deleteAccountSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
          HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN);
  private static final String deleteContainerSql =
      String.format("DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=? AND %s=?", ACCOUNT_REPORTS_TABLE,
          CLUSTER_NAME_COLUMN, HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN);
  private final DataSource dataSource;
  private final MySqlMetrics metrics;

  /**
   * Constructor to create a {@link AccountReportsDao}.
   * @param dataSource The {@link DataSource}.
   * @param metrics The {@link MySqlMetrics}.
   */
  AccountReportsDao(DataSource dataSource, MySqlMetrics metrics) {
    this.dataSource = Objects.requireNonNull(dataSource, "DataSource is empty");
    this.metrics = Objects.requireNonNull(metrics, "MySqlMetrics is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param clustername the name of target cluster
   * @param hostname the name of target host
   * @param partitionId The partition id of this account/container usage.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   */
  void updateStorageUsage(String clustername, String hostname, int partitionId, short accountId, short containerId,
      long storageUsage) throws SQLException {
    // TODO: adding real physical storage usage and number of blobs here
    final long physicalStorageUsage = storageUsage;
    final long numberOfBlobs = 1L;
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
        long startTimeMs = System.currentTimeMillis();
        insertStatement.setString(1, clustername);
        insertStatement.setString(2, hostname);
        // The data type of partition id, account id and container id are not SMALLINT, but INT in MySQL, for
        // future extension
        insertStatement.setInt(3, partitionId);
        insertStatement.setInt(4, accountId);
        insertStatement.setInt(5, containerId);
        insertStatement.setLong(6, storageUsage);
        insertStatement.setLong(7, physicalStorageUsage);
        insertStatement.setLong(8, numberOfBlobs);
        insertStatement.setLong(9, storageUsage);
        insertStatement.setLong(10, physicalStorageUsage);
        insertStatement.setLong(11, numberOfBlobs);
        insertStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error(String.format("Failed to execute updated on %s, with parameter %d %d %d %d", ACCOUNT_REPORTS_TABLE,
          partitionId, accountId, containerId, storageUsage), e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clustername} and {@code hostname}. The result will be applied to the
   * {@link ContainerStorageStatsFunction}.
   * @param clustername The clustername.
   * @param hostname The hostname.
   * @param func The {@link ContainerStorageStatsFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryStorageUsageForHost(String clustername, String hostname, ContainerStorageStatsFunction func)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(querySqlForClusterAndHost)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clustername);
        queryStatement.setString(2, hostname);
        try (ResultSet resultSet = queryStatement.executeQuery()) {
          while (resultSet.next()) {
            int partitionId = resultSet.getInt(PARTITION_ID_COLUMN);
            int accountId = resultSet.getInt(ACCOUNT_ID_COLUMN);
            int containerId = resultSet.getInt(CONTAINER_ID_COLUMN);
            long storageUsage = resultSet.getLong(STORAGE_USAGE_COLUMN);
            final long physicalStorageUsage = resultSet.getLong(PHYSICAL_STORAGE_USAGE_COLUMN);
            final long numberOfBlobs = resultSet.getLong(NUMBER_OF_BLOBS_COLUMN);
            long updatedAtMs = resultSet.getTimestamp(UPDATED_AT_COLUMN).getTime();
            func.apply(partitionId, (short) accountId,
                new ContainerStorageStats((short) containerId, storageUsage, physicalStorageUsage, numberOfBlobs),
                updatedAtMs);
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error(
          String.format("Failed to execute query on %s, with parameter %s %s", ACCOUNT_REPORTS_TABLE, clustername,
              hostname), e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clustername}, {@code hostname}.
   * @param clustername The clustername
   * @param hostname The hostname
   * @throws SQLException
   */
  void deleteStorageUsageForHost(String clustername, String hostname) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteHostSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clustername);
        deleteStatement.setString(2, hostname);
        deleteStatement.executeUpdate();
        metrics.deleteTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.deleteSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.deleteFailureCount.inc();
      logger.error("Failed to execute DELETE on {}, with parameter {}", ACCOUNT_REPORTS_TABLE, hostname, e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clustername}, {@code hostname} and {@code partitionId}.
   * @param clustername The clustername
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @throws SQLException
   */
  void deleteStorageUsageForPartition(String clustername, String hostname, int partitionId) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deletePartitionSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clustername);
        deleteStatement.setString(2, hostname);
        deleteStatement.setInt(3, partitionId);
        deleteStatement.executeUpdate();
        metrics.deleteTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.deleteSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.deleteFailureCount.inc();
      logger.error("Failed to execute DELETE on {}, with parameter {}", ACCOUNT_REPORTS_TABLE, partitionId, e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clustername}, {@code hostname}, {@code partitionId} and {@code accountId}.
   * @param clustername The clustername
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @param accountId The accountId
   * @throws SQLException
   */
  void deleteStorageUsageForAccount(String clustername, String hostname, int partitionId, int accountId)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteAccountSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clustername);
        deleteStatement.setString(2, hostname);
        deleteStatement.setInt(3, partitionId);
        deleteStatement.setInt(4, accountId);
        deleteStatement.executeUpdate();
        metrics.deleteTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.deleteSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.deleteFailureCount.inc();
      logger.error("Failed to execute DELETE on {}, with parameter {}, {}", ACCOUNT_REPORTS_TABLE, partitionId,
          accountId, e);
      throw e;
    }
  }

  /**
   * Delete container storage usage rows for given {@code clustername}, {@code hostname}, {@code partitionId},
   * {@code accountId} and {@code containerId}.
   * @param clustername The clustername
   * @param hostname The hostname
   * @param partitionId The partitionId
   * @param accountId The accountId
   * @param containerId The containerId
   * @throws SQLException
   */
  void deleteStorageUsageForContainer(String clustername, String hostname, int partitionId, int accountId,
      int containerId) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteContainerSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clustername);
        deleteStatement.setString(2, hostname);
        deleteStatement.setInt(3, partitionId);
        deleteStatement.setInt(4, accountId);
        deleteStatement.setInt(5, containerId);
        deleteStatement.executeUpdate();
        metrics.deleteTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.deleteSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.deleteFailureCount.inc();
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
      super(dataSource.getConnection(), metrics, insertSql, ACCOUNT_REPORTS_TABLE, maxBatchSize);
    }

    /**
     * Supply values to the prepared statement and add it to the batch updater.
     * @param clustername the cluster name
     * @param hostname the hostname
     * @param partitionId The partition id of this account/container usage.
     * @param accountId The account id.
     * @param containerStats The {@link ContainerStorageStats}.
     * @throws SQLException
     */
    public void addUpdateToBatch(String clustername, String hostname, int partitionId, short accountId,
        ContainerStorageStats containerStats) throws SQLException {
      addUpdateToBatch(statement -> {
        statement.setString(1, clustername);
        statement.setString(2, hostname);
        statement.setInt(3, partitionId);
        statement.setInt(4, accountId);
        statement.setInt(5, containerStats.getContainerId());
        statement.setLong(6, containerStats.getLogicalStorageUsage());
        statement.setLong(7, containerStats.getPhysicalStorageUsage());
        statement.setLong(8, containerStats.getNumberOfBlobs());
        statement.setLong(9, containerStats.getLogicalStorageUsage());
        statement.setLong(10, containerStats.getPhysicalStorageUsage());
        statement.setLong(11, containerStats.getNumberOfBlobs());
      });
    }
  }
}

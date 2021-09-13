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
 * AggregatedAccountReports Data Access Object. This object has to deal with three tables
 * <ol>
 * <li>AggregatedAccountReports</li>
 * <li>MonthlyAggregatedAccountReports</li>
 * <li>AggregatedAccountReportsMonth</li>
 * </ol>
 * <p/>
 * These tables have similar names, but they serve different purposes.
 * <ul>
 *   <li>AggregatedAccountReports saves the aggregated container storage usage for each cluster. It will be updated as frequent as the aggregation task is scheduled.</li>
 *   <li>MonthlyAggregatedAccountReports makes a copy of data from AggregatedAccountReports at the beginning of every month</li>
 *   <li>AggregatedAccountReportsMonth records which month the data is copied to MonthlyAggregatedAccountReports</li>
 * </ul>
 */
public class AggregatedAccountReportsDao {
  public static final String AGGREGATED_ACCOUNT_REPORTS_TABLE = "AggregatedAccountReports";
  public static final String MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE = "MonthlyAggregatedAccountReports";
  public static final String AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE = "AggregatedAccountReportsMonth";
  public static final String CLUSTER_NAME_COLUMN = "clusterName";
  public static final String ACCOUNT_ID_COLUMN = "accountId";
  public static final String CONTAINER_ID_COLUMN = "containerId";
  public static final String STORAGE_USAGE_COLUMN = "storageUsage";
  public static final String PHYSICAL_STORAGE_USAGE_COLUMN = "physicalStorageUsage";
  public static final String NUMBER_OF_BLOBS_COLUMN = "numberOfBlobs";
  public static final String UPDATED_AT_COLUMN = "updatedAt";
  public static final String MONTH_COLUMN = "month";

  private static final Logger logger = LoggerFactory.getLogger(AccountReportsDao.class);
  private static final String insertSql = String.format(
      "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE %s=?, %s=?, %s=?, %s=NOW()",
      AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
      STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN,
      STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN);
  private static final String queryUsageSqlForCluster =
      String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = ?", ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
          STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, AGGREGATED_ACCOUNT_REPORTS_TABLE,
          CLUSTER_NAME_COLUMN);
  private static final String queryMonthlyUsageSqlForCluster =
      String.format("SELECT %s, %s, %s, %s, %s FROM %s WHERE %s = ?", ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
          STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN,
          MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN);
  private static final String copySqlForCluster = String.format(
      "INSERT %s SELECT * FROM %s WHERE %s = ? ON DUPLICATE KEY UPDATE %s=%s.%s, %s=%s.%s, %s=%s.%s, %s=%s.%s",
      MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE, AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN,
      STORAGE_USAGE_COLUMN, AGGREGATED_ACCOUNT_REPORTS_TABLE, STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN,
      AGGREGATED_ACCOUNT_REPORTS_TABLE, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN,
      AGGREGATED_ACCOUNT_REPORTS_TABLE, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN, AGGREGATED_ACCOUNT_REPORTS_TABLE,
      UPDATED_AT_COLUMN);
  private static final String deleteMonthlySqlForCluster =
      String.format("DELETE FROM %s WHERE %s=?", MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN);
  private static final String queryMonthSqlForCluster =
      String.format("SELECT %s FROM %s WHERE %s = ?", MONTH_COLUMN, AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          CLUSTER_NAME_COLUMN);
  private static final String insertMonthSql =
      String.format("INSERT INTO %s (%s, %s) VALUES (?, ?) ON DUPLICATE KEY UPDATE %s=?",
          AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE, CLUSTER_NAME_COLUMN, MONTH_COLUMN, MONTH_COLUMN);
  private static final String deleteSql =
      String.format("DELETE FROM %s WHERE %s = ? AND %s = ? AND %s = ?", AGGREGATED_ACCOUNT_REPORTS_TABLE,
          CLUSTER_NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN);

  private final DataSource dataSource;
  private final MySqlMetrics metrics;

  /**
   * Constructor to create {@link AggregatedAccountReportsDao}.
   * @param dataSource The {@link DataSource}.
   * @param metrics The {@link MySqlMetrics}.
   */
  AggregatedAccountReportsDao(DataSource dataSource, MySqlMetrics metrics) {
    this.dataSource = Objects.requireNonNull(dataSource, "DataSource is empty");
    this.metrics = Objects.requireNonNull(metrics, "MySqlMetrics is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param clusterName The clusterName
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   * @throws SQLException
   */
  void updateStorageUsage(String clusterName, short accountId, short containerId, long storageUsage)
      throws SQLException {
    // TODO: adding real physical storage usage and number of blobs here
    final long physicalStorageUsage = storageUsage;
    final long numberOfBlobs = 1L;
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement insertStatement = connection.prepareStatement(insertSql)) {
        long startTimeMs = System.currentTimeMillis();
        insertStatement.setString(1, clusterName);
        // The data type of partition id, account id and container id are not SMALLINT, but INT in MySQL, for
        // future extension
        insertStatement.setInt(2, accountId);
        insertStatement.setInt(3, containerId);
        insertStatement.setLong(4, storageUsage);
        insertStatement.setLong(5, physicalStorageUsage);
        insertStatement.setLong(6, numberOfBlobs);
        insertStatement.setLong(7, storageUsage);
        insertStatement.setLong(8, physicalStorageUsage);
        insertStatement.setLong(9, numberOfBlobs);
        insertStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error(
          String.format("Failed to execute updated on %s, with parameter %d %d %d", AGGREGATED_ACCOUNT_REPORTS_TABLE,
              accountId, containerId, storageUsage), e);
      throw e;
    }
  }

  /**
   * Delete the storage usage row for the given account/container.
   * @param clusterName The clusterName
   * @param accountId The account id
   * @param containerId The container id
   * @throws SQLException
   */
  void deleteStorageUsage(String clusterName, short accountId, short containerId) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clusterName);
        deleteStatement.setInt(2, accountId);
        deleteStatement.setInt(3, containerId);
        deleteStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to execute delete {}, with parameter {} {}", AGGREGATED_ACCOUNT_REPORTS_TABLE, accountId,
          containerId, e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clusterName}. The result will be applied to the {@link AggregatedContainerStorageStatsFunction}.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerStorageStatsFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryContainerUsageForCluster(String clusterName, AggregatedContainerStorageStatsFunction func)
      throws SQLException {
    queryContainerUsageForClusterInternal(false, clusterName, func);
  }

  /**
   * Query container storage usage for given {@code clusterName}. The result will be applied to the {@link AggregatedContainerStorageStatsFunction}.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerStorageStatsFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryMonthlyContainerUsageForCluster(String clusterName, AggregatedContainerStorageStatsFunction func)
      throws SQLException {
    queryContainerUsageForClusterInternal(true, clusterName, func);
  }

  /**
   * Query container storage for the given {@code clusterName}. The result will be applied to the {@link AggregatedContainerStorageStatsFunction}.
   * @param forMonthly True to return the monthly snapshot of the container storage usage.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerStorageStatsFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  private void queryContainerUsageForClusterInternal(boolean forMonthly, String clusterName,
      AggregatedContainerStorageStatsFunction func) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      String sqlStatement = forMonthly ? queryMonthlyUsageSqlForCluster : queryUsageSqlForCluster;
      try (PreparedStatement queryStatement = connection.prepareStatement(sqlStatement)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet resultSet = queryStatement.executeQuery()) {
          while (resultSet.next()) {
            int accountId = resultSet.getInt(ACCOUNT_ID_COLUMN);
            int containerId = resultSet.getInt(CONTAINER_ID_COLUMN);
            long storageUsage = resultSet.getLong(STORAGE_USAGE_COLUMN);
            final long physicalStorageUsage = resultSet.getLong(PHYSICAL_STORAGE_USAGE_COLUMN);
            final long numberOfBlobs = resultSet.getLong(NUMBER_OF_BLOBS_COLUMN);
            func.apply((short) accountId,
                new ContainerStorageStats((short) containerId, storageUsage, physicalStorageUsage, numberOfBlobs));
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error(String.format("Failed to execute query on %s, with parameter %s", AGGREGATED_ACCOUNT_REPORTS_TABLE,
          clusterName), e);
      throw e;
    }
  }

  /**
   * Copy rows in table {@link #AGGREGATED_ACCOUNT_REPORTS_TABLE} to {@link #MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE}.
   * @param clusterName the clusterName.
   * @throws SQLException
   */
  void copyAggregatedUsageToMonthlyAggregatedTableForCluster(String clusterName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(copySqlForCluster)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        queryStatement.executeUpdate();
        metrics.copyTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.copySuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.copyFailureCount.inc();
      logger.error(
          String.format("Failed to execute copy on %s, with parameter %s", MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
              clusterName), e);
      throw e;
    }
  }

  /**
   * Delete the rows in table {@link #MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE}.
   * @param clusterName The cluster name.
   * @throws SQLException
   */
  void deleteAggregatedUsageFromMonthlyAggregatedTableForCluster(String clusterName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteMonthlySqlForCluster)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clusterName);
        deleteStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to execute delete on {}, with parameter {}", MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
          clusterName, e);
      throw e;
    }
  }

  /**
   * Return the month value in table {@link #AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE}.
   * @param clusterName the clusterName.
   * @return The month value in string format, like "2020-01".
   * @throws SQLException
   */
  String queryMonthForCluster(String clusterName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(queryMonthSqlForCluster)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet resultSet = queryStatement.executeQuery()) {
          metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
          metrics.readSuccessCount.inc();
          if (resultSet.next()) {
            return resultSet.getString(MONTH_COLUMN);
          }
        }
        return "";
      }
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error(
          String.format("Failed to execute query on %s, with parameter %s", AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
              clusterName), e);
      throw e;
    }
  }

  /**
   * Update the month value in table {@link #AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE}.
   * @param clusterName the clusterName.
   * @param monthValue the month value in string format, like "2020-01".
   * @throws SQLException
   */
  void updateMonth(String clusterName, String monthValue) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement insertStatement = connection.prepareStatement(insertMonthSql)) {
        long startTimeMs = System.currentTimeMillis();
        insertStatement.setString(1, clusterName);
        insertStatement.setString(2, monthValue);
        insertStatement.setString(3, monthValue);
        insertStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error(
          String.format("Failed to execute updated on %s, with parameter %s %s", AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
              clusterName, monthValue), e);
      throw e;
    }
  }

  /**
   * A batch updater to update aggregate storage usage.
   */
  class AggregatedStorageBatchUpdater extends BatchUpdater {
    /**
     * Constructor to instantiate a {@link AggregatedStorageBatchUpdater}.
     * @param maxBatchSize The max batch size.
     * @throws SQLException
     */
    public AggregatedStorageBatchUpdater(int maxBatchSize) throws SQLException {
      super(dataSource.getConnection(), metrics, insertSql, AGGREGATED_ACCOUNT_REPORTS_TABLE, maxBatchSize);
    }

    /**
     * Supply values to the prepared statement and add it to the batch updater.
     * @param accountId The account id.
     * @param containerStats The {@link ContainerStorageStats}.
     * @throws SQLException
     */
    public void addUpdateToBatch(String clusterName, short accountId, ContainerStorageStats containerStats)
        throws SQLException {
      addUpdateToBatch(statement -> {
        statement.setString(1, clusterName);
        statement.setInt(2, accountId);
        statement.setInt(3, containerStats.getContainerId());
        statement.setLong(4, containerStats.getLogicalStorageUsage());
        statement.setLong(5, containerStats.getPhysicalStorageUsage());
        statement.setLong(6, containerStats.getNumberOfBlobs());
        statement.setLong(7, containerStats.getLogicalStorageUsage());
        statement.setLong(8, containerStats.getPhysicalStorageUsage());
        statement.setLong(9, containerStats.getNumberOfBlobs());
      });
    }
  }
}

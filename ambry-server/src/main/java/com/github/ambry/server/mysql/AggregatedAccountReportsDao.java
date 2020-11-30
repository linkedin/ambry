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
package com.github.ambry.server.mysql;

import com.github.ambry.mysql.MySqlDataAccessor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


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
  public static final String UPDATED_AT_COLUMN = "updatedAt";
  public static final String MONTH_COLUMN = "month";

  private static final Logger logger = LoggerFactory.getLogger(AccountReportsDao.class);
  private static final String insertSql =
      String.format("INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, NOW())", AGGREGATED_ACCOUNT_REPORTS_TABLE,
          CLUSTER_NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN);
  private static final String queryUsageSqlForCluster =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s = ?", ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
          STORAGE_USAGE_COLUMN, AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN);
  private static final String queryMonthlyUsageSqlForCluster =
      String.format("SELECT %s, %s, %s FROM %s WHERE %s = ?", ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
          STORAGE_USAGE_COLUMN, MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN);
  private static final String copySqlForCluster =
      String.format("INSERT %s SELECT * FROM %s WHERE %s = ?", MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
          AGGREGATED_ACCOUNT_REPORTS_TABLE, CLUSTER_NAME_COLUMN);
  private static final String queryMonthSqlForCluster =
      String.format("SELECT %s FROM %s WHERE %s = ?", MONTH_COLUMN, AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          CLUSTER_NAME_COLUMN);
  private static final String insertMonthSql =
      String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)", AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          CLUSTER_NAME_COLUMN, MONTH_COLUMN);

  private final MySqlDataAccessor dataAccessor;
  private final String clusterName;

  /**
   * Constructor to create {@link AggregatedAccountReportsDao}.
   * @param dataAccessor The {@link MySqlDataAccessor}.
   * @param clusterName The clusterName.
   */
  AggregatedAccountReportsDao(MySqlDataAccessor dataAccessor, String clusterName) {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
    this.clusterName = Objects.requireNonNull(clusterName, "clusterName is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   * @throws SQLException
   */
  void updateStorageUsage(short accountId, short containerId, long storageUsage) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql, true);
      insertStatement.setString(1, clusterName);
      // The data type of partition id, account id and container id are not SMALLINT, but INT in MySQL, for
      // future extension
      insertStatement.setInt(2, accountId);
      insertStatement.setInt(3, containerId);
      insertStatement.setLong(4, storageUsage);
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error(
          String.format("Failed to execute updated on %s, with parameter %d %d %d", AGGREGATED_ACCOUNT_REPORTS_TABLE,
              accountId, containerId, storageUsage), e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clusterName}. The result will be applied to the {@link AggregatedContainerUsageFunction}.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerUsageFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryContainerUsageForCluster(String clusterName, AggregatedContainerUsageFunction func) throws SQLException {
    queryContainerUsageForClusterInternal(false, clusterName, func);
  }

  /**
   * Query container storage usage for given {@code clusterName}. The result will be applied to the {@link AggregatedContainerUsageFunction}.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerUsageFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  void queryMonthlyContainerUsageForCluster(String clusterName, AggregatedContainerUsageFunction func)
      throws SQLException {
    queryContainerUsageForClusterInternal(true, clusterName, func);
  }

  /**
   * Query container storage for the given {@code clusterName}. The result will be applied to the {@link AggregatedContainerUsageFunction}.
   * @param forMonthly True to return the monthly snapshot of the container storage usage.
   * @param clusterName The clusterName.
   * @param func The {@link AggregatedContainerUsageFunction} to call to process each container storage usage.
   * @throws SQLException
   */
  private void queryContainerUsageForClusterInternal(boolean forMonthly, String clusterName,
      AggregatedContainerUsageFunction func) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      String sqlStatement = forMonthly ? queryMonthlyUsageSqlForCluster : queryUsageSqlForCluster;
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(sqlStatement, false);
      queryStatement.setString(1, clusterName);
      ResultSet resultSet = queryStatement.executeQuery();
      if (resultSet != null) {
        while (resultSet.next()) {
          int accountId = resultSet.getInt(ACCOUNT_ID_COLUMN);
          int containerId = resultSet.getInt(CONTAINER_ID_COLUMN);
          long storageUsage = resultSet.getLong(STORAGE_USAGE_COLUMN);
          func.apply((short) accountId, (short) containerId, storageUsage);
        }
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
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
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(copySqlForCluster, true);
      queryStatement.setString(1, clusterName);
      queryStatement.executeUpdate();
      dataAccessor.onSuccess(Copy, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Copy);
      logger.error(
          String.format("Failed to execute copy on %s, with parameter %s", MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
              clusterName), e);
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
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(queryMonthSqlForCluster, false);
      queryStatement.setString(1, clusterName);
      ResultSet resultSet = queryStatement.executeQuery();
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      if (resultSet != null) {
        resultSet.next();
        return resultSet.getString(MONTH_COLUMN);
      }
      return "";
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
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
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertMonthSql, true);
      insertStatement.setString(1, clusterName);
      insertStatement.setString(2, monthValue);
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error(
          String.format("Failed to execute updated on %s, with parameter %s %s", AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
              clusterName, monthValue), e);
      throw e;
    }
  }
}

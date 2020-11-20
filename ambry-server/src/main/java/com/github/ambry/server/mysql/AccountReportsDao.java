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
 * AccountReports Data Access Object.
 */
public class AccountReportsDao {
  public static final String ACCOUNT_REPORTS_TABLE = "AccountReports";
  public static final String CLUSTERNAME_COLUMN = "clusterName";
  public static final String HOSTNAME_COLUMN = "hostname";
  public static final String PARTITION_ID_COLUMN = "partitionId";
  public static final String ACCOUNT_ID_COLUMN = "accountId";
  public static final String CONTAINER_ID_COLUMN = "containerId";
  public static final String STORAGE_USAGE_COLUMN = "storageUsage";
  public static final String UPDATED_AT_COLUMN = "updatedAt";

  private static final Logger logger = LoggerFactory.getLogger(AccountReportsDao.class);
  private static final String insertSql =
      String.format("INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?, NOW())",
          ACCOUNT_REPORTS_TABLE, CLUSTERNAME_COLUMN, HOSTNAME_COLUMN, PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN,
          CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN);
  private static final String querySqlForClusterAndHost =
      String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s = ? AND %s = ?", PARTITION_ID_COLUMN, ACCOUNT_ID_COLUMN,
          CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN, ACCOUNT_REPORTS_TABLE, CLUSTERNAME_COLUMN, HOSTNAME_COLUMN);
  private final MySqlDataAccessor dataAccessor;
  private final String clusterName;
  private final String hostname;

  /**
   * Constructor to create a {@link AccountReportsDao}.
   * @param dataAccessor The underlying {@link MySqlDataAccessor}.
   * @param clusterName The name of the cluster this host is belonging to, like Ambry-test.
   * @param hostname The name of the host. It should include the hostname and the port.
   */
  public AccountReportsDao(MySqlDataAccessor dataAccessor, String clusterName, String hostname) {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
    this.clusterName = Objects.requireNonNull(clusterName, "clusterName is empty");
    this.hostname = Objects.requireNonNull(hostname, "hostname is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param partitionId The partition id of this account/container usage.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   */
  public void updateStorageUsage(short partitionId, short accountId, short containerId, long storageUsage)
      throws SQLException {
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
  public void queryStorageUsageForHost(String clusterName, String hostname, ContainerUsageFunction func)
      throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(querySqlForClusterAndHost, false);
      queryStatement.setString(1, clusterName);
      queryStatement.setString(2, hostname);
      ResultSet resultSet = queryStatement.executeQuery();
      if (resultSet != null) {
        while (resultSet.next()) {
          int partitionId = resultSet.getInt(PARTITION_ID_COLUMN);
          int accountId = resultSet.getInt(ACCOUNT_ID_COLUMN);
          int containerId = resultSet.getInt(CONTAINER_ID_COLUMN);
          long storageUsage = resultSet.getLong(STORAGE_USAGE_COLUMN);
          func.apply((short) partitionId, (short) accountId, (short) containerId, storageUsage);
        }
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
}

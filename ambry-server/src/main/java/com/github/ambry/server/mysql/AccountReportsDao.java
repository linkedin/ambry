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
  public static final String CLUSTERNAME_COLUMN = "clustername";
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
  private final MySqlDataAccessor dataAccessor;
  private final String clustername;
  private final String hostname;

  /**
   * Constructor to create a {@link AccountReportsDao}.
   * @param dataAccessor The underlying {@link MySqlDataAccessor}.
   * @param clustername The name of the cluster this host is belonging to, like Ambry-prod, Ambry-video.
   * @param hostname The name of the host. It should include the hostname and the port.
   */
  public AccountReportsDao(MySqlDataAccessor dataAccessor, String clustername, String hostname) {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
    this.clustername = Objects.requireNonNull(clustername, "clustername is empty");
    this.hostname = Objects.requireNonNull(hostname, "hostname is empty");
  }

  /**
   * Update the storage usage for the given account/container.
   * @param partitionId The partition id of this account/container usage.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   */
  public void updateStorageUsage(short partitionId, short accountId, short containerId, long storageUsage) {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql, true);
      insertStatement.setString(1, clustername);
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
      logger.error(
          String.format("Failed to execute updated on %s, with parameter %d %d %d %d %d", ACCOUNT_REPORTS_TABLE,
              partitionId, accountId, containerId, storageUsage), e);
    }
  }
}

/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


/**
 * PartitionClassNames, Partitions, AggregatedPartitionClassReports Data Access Object.
 */
public class PartitionClassReportsDao {
  public static final String PARTITION_CLASS_NAMES_TABLE = "PartitionClassNames";
  public static final String PARTITIONS_TABLE = "Partitions";
  public static final String AGGREGATED_PARTITION_CLASS_REPORTS_TABLE = "AggregatedPartitionClassReports";
  private static final String ID_COLUMN = "id";
  private static final String NAME_COLUMN = "name";
  private static final String PARTITION_CLASS_ID_COLUMN = "partitionClassId";
  private static final String CLUSTER_NAME_COLUMN = "clusterName";
  private static final String ACCOUNT_ID_COLUMN = "accountId";
  private static final String CONTAINER_ID_COLUMN = "containerId";
  private static final String STORAGE_USAGE_COLUMN = "storageUsage";
  private static final String UPDATED_AT_COLUMN = "updatedAt";

  private static final String insertPartitionClassNamesSql =
      String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)", PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN,
          NAME_COLUMN);

  private static final String queryPartitionClassNamesSql =
      String.format("SELECT %s, %s FROM %s WHERE %s = ?", ID_COLUMN, NAME_COLUMN, PARTITION_CLASS_NAMES_TABLE,
          CLUSTER_NAME_COLUMN);

  private static final String insertPartitionIdSql = String.format("INSERT INTO %s VALUES (?, ?, ?)", PARTITIONS_TABLE);

  private static final String queryPartitionIdsSql =
      String.format("SELECT %s FROM %s WHERE %s = ?", ID_COLUMN, PARTITIONS_TABLE, CLUSTER_NAME_COLUMN);

  private static final String queryPartitionIdAndNamesSql = String.format(
      "SELECT %1$s.%2$s, %3$s.%4$s FROM %1$s INNER JOIN %3$s ON %1$s.%5$s = %3$s.%2$s AND %1$s.%6$s = %3$s.%6$s "
          + "WHERE %1$s.%6$s = ?", PARTITIONS_TABLE, ID_COLUMN, PARTITION_CLASS_NAMES_TABLE, NAME_COLUMN,
      PARTITION_CLASS_ID_COLUMN, CLUSTER_NAME_COLUMN);

  private static final String insertAggregatedSql = String.format(
      "INSERT INTO %s SELECT %s, ?, ?, ?, NOW() FROM %s WHERE %s=? AND %s=? ON DUPLICATE KEY UPDATE %s=?, %s=NOW()",
      AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, ID_COLUMN, PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN,
      NAME_COLUMN, STORAGE_USAGE_COLUMN, UPDATED_AT_COLUMN);

  private static final String queryAggregatedSql = String.format(
      "SELECT TableA.%s, %s, %s, %s, %s FROM %s " + "INNER JOIN (SELECT * FROM %s WHERE %s = ?) TableA"
          + " WHERE TableA.%s = %s.%s;", NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN,
      UPDATED_AT_COLUMN, AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN,
      ID_COLUMN, AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_ID_COLUMN);

  private static final Logger logger = LoggerFactory.getLogger(PartitionClassReportsDao.class);
  private final MySqlDataAccessor dataAccessor;

  /**
   * Constructor to instantiate a {@link PartitionClassReportsDao}.
   * @param dataAccessor
   */
  PartitionClassReportsDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = Objects.requireNonNull(dataAccessor, "MySqlDataAccessor is empty");
  }

  /**
   * Query partition class names and their corresponding id for given {@code clusterName}. An empty map will be returned
   * if there is not rows for the given {@code clusterName}.
   * @param clusterName The clusterName to query partition class names for
   * @return A map whose key is the partition class name and the value is the corresponding id.
   * @throws SQLException
   */
  Map<String, Short> queryPartitionClassNames(String clusterName) throws SQLException {
    Map<String, Short> result = new HashMap<>();
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(queryPartitionClassNamesSql, false);
      queryStatement.setString(1, clusterName);
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          short partitionClassId = rs.getShort(1);
          String className = rs.getString(2);
          result.put(className, partitionClassId);
        }
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return result;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      logger.error("Failed to execute query of {}, with parameter {}", queryPartitionClassNamesSql, clusterName, e);
      throw e;
    }
  }

  /**
   * Insert the partition class name under given {@code clusterName}. Same partition class name can be inserted to different
   * clusterNames. If partition class name is already inserted under clusterName, this method would treat it as a no-op.
   * @param clusterName The clusterName.
   * @param partitionClassName The partition class name.
   * @throws SQLException
   */
  void insertPartitionClassName(String clusterName, String partitionClassName) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertPartitionClassNamesSql, true);
      insertStatement.setString(1, clusterName);
      insertStatement.setString(2, partitionClassName);
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLIntegrityConstraintViolationException e) {
      // If somehow the unique pair(clusterName, partitionClassName) is inserted, consider this as a success
      // ignore it
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error("Failed to execute of {}, with parameter {} {}", insertPartitionClassNamesSql, clusterName,
          partitionClassName, e);
      throw e;
    }
  }

  /**
   * Query partition class names and partitions that belong to certain partition class name for the given {@code clusterName}.
   * An empty map will be returned if there is no partition names and ids for the given {@code clusterName}.
   * @param clusterName the clusterName
   * @return A map whose key is the partition class name and the value is a set of partition ids.
   * @throws SQLException
   */
  Map<String, Set<Short>> queryPartitionNameAndIds(String clusterName) throws SQLException {
    Map<String, Set<Short>> nameAndIds = new HashMap<>();
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(queryPartitionIdAndNamesSql, false);
      queryStatement.setString(1, clusterName);
      try (ResultSet resultSet = queryStatement.executeQuery()) {
        while (resultSet.next()) {
          int partitionId = resultSet.getInt(1);
          String partitionClassName = resultSet.getString(2);
          nameAndIds.computeIfAbsent(partitionClassName, k -> new HashSet<>()).add((short) partitionId);
        }
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return nameAndIds;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      logger.error("Failed to execute query {}, with parameter {}", queryPartitionIdAndNamesSql, clusterName, e);
      throw e;
    }
  }

  /**
   * Query the partition ids belonging to the given {@code clusterName}. An empty set will be returned if there is no
   * partitions for the given {@code clusterName}.
   * @param clusterName The clusterName.
   * @return
   * @throws SQLException
   */
  Set<Short> queryPartitionIds(String clusterName) throws SQLException {
    Set<Short> partitionIds = new HashSet<>();
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(queryPartitionIdsSql, false);
      queryStatement.setString(1, clusterName);
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          partitionIds.add((short) rs.getInt(1));
        }
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return partitionIds;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      logger.error("Failed to execute query {}, with parameter {}", queryPartitionIdsSql, clusterName, e);
      throw e;
    }
  }

  /**
   * Insert partition id with it's partition class id of given {@code clusterName}. If the partition id already
   * exist for given {@code clusterName}, then this will be a no-op.
   * @param clusterName The clusterName
   * @param partitionId The id of the partition
   * @param partitionClassId The id of the partition class name
   * @throws SQLException
   */
  void insertPartitionId(String clusterName, short partitionId, short partitionClassId) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertPartitionIdSql, true);
      insertStatement.setString(1, clusterName);
      insertStatement.setInt(2, partitionId);
      insertStatement.setShort(3, partitionClassId);
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLIntegrityConstraintViolationException e) {
      // If somehow the unique pair(clusterName, partitionId) is inserted, consider this as a success
      // ignore it
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      logger.error("Failed to execute of {}, with parameter {} {} {}", insertPartitionIdSql, clusterName, partitionId,
          partitionClassId, e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clusterName}. This usage is aggregated under each partition class name.
   * The results will be applied to the given {@link PartitionClassContainerUsageFunction}.
   * @param clusterName The clusterName.
   * @param func The callback to apply query results
   * @throws SQLException
   */
  void queryAggregatedPartitionClassReport(String clusterName, PartitionClassContainerUsageFunction func)
      throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement queryStatement = dataAccessor.getPreparedStatement(queryAggregatedSql, false);
      queryStatement.setString(1, clusterName);
      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          String className = rs.getString(1);
          int accountId = rs.getInt(2);
          int containerId = rs.getInt(3);
          long usage = rs.getLong(4);
          long updatedAt = rs.getTimestamp(5).getTime();
          func.apply(className, (short) accountId, (short) containerId, usage, updatedAt);
        }
      }
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      logger.error("Failed to execute query {}, with parameter {}", queryAggregatedSql, clusterName, e);
      throw e;
    }
  }

  /**
   * A batch updater to update aggregated partition class container storage usage.
   */
  class StorageBatchUpdater extends BatchUpdater {

    /**
     * Constructor to instantiate a {@link BatchUpdater}.
     * @param maxBatchSize The max batch size.
     * @throws SQLException
     */
    public StorageBatchUpdater(int maxBatchSize) throws SQLException {
      super(dataAccessor, insertAggregatedSql, "AggregatedPartitionClassReports", maxBatchSize);
    }

    /**
     * Supply values to the prepared statement and add it to batch updater.
     * @param clusterName the clusterName.
     * @param partitionClassName the partition class name
     * @param accountId the account id
     * @param containerId the container id
     * @param usage the storage usage
     * @throws SQLException
     */
    public void addUpdateToBatch(String clusterName, String partitionClassName, short accountId, short containerId,
        long usage) throws SQLException {
      addUpdateToBatch(statement -> {
        statement.setInt(1, accountId);
        statement.setInt(2, containerId);
        statement.setLong(3, usage);
        statement.setString(4, clusterName);
        statement.setString(5, partitionClassName);
        statement.setLong(6, usage);
      });
    }
  }
}

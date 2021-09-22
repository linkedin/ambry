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
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PartitionClassNames, Partitions, AggregatedPartitionClassReports Data Access Object.
 */
public class PartitionClassReportsDao {
  private static final Logger logger = LoggerFactory.getLogger(PartitionClassReportsDao.class);

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
  private static final String PHYSICAL_STORAGE_USAGE_COLUMN = "physicalStorageUsage";
  private static final String NUMBER_OF_BLOBS_COLUMN = "numberOfBlobs";
  private static final String UPDATED_AT_COLUMN = "updatedAt";

  /**
   * eg: INSERT INTO PartitionClassNames(clusterName, name) VALUES ("ambry-test", "default");
   */
  private static final String insertPartitionClassNamesSql =
      String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)", PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN,
          NAME_COLUMN);

  /**
   * eg: SELECT id, name FROM PartitionClassNames WHERE clusterName = "ambry-test";
   */
  private static final String queryPartitionClassNamesSql =
      String.format("SELECT %s, %s FROM %s WHERE %s = ?", ID_COLUMN, NAME_COLUMN, PARTITION_CLASS_NAMES_TABLE,
          CLUSTER_NAME_COLUMN);

  /**
   * eg: INSERT INTO Partitions (clusterName, id, partitionClassId) VALUES("ambry-test", 12, 1);
   * 12 is the partition id and 1 is the partition class id from PartitionClassNames.id.
   */
  private static final String insertPartitionIdSql =
      String.format("INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)", PARTITIONS_TABLE, CLUSTER_NAME_COLUMN, ID_COLUMN,
          PARTITION_CLASS_ID_COLUMN);

  /**
   * eg: SELECT id FROM Partitions WHERE clusterName = "ambry-test";
   */
  private static final String queryPartitionIdsSql =
      String.format("SELECT %s FROM %s WHERE %s = ?", ID_COLUMN, PARTITIONS_TABLE, CLUSTER_NAME_COLUMN);

  /**
   * eg: SELECT Partitions.id, PartitionClassNames.name
   *     FROM Partitions INNER JOIN PartitiionClassNames
   *     ON Partitions.partitionClassId = PartitionClassNames.id
   *     AND Partitions.clusterName = PartitionClassName.clusterName
   *     WHERE Partitions.clusterName = "ambry-test";
   */
  private static final String queryPartitionIdAndNamesSql = String.format(
      "SELECT %1$s.%2$s, %3$s.%4$s FROM %1$s INNER JOIN %3$s ON %1$s.%5$s = %3$s.%2$s AND %1$s.%6$s = %3$s.%6$s "
          + "WHERE %1$s.%6$s = ?", PARTITIONS_TABLE, ID_COLUMN, PARTITION_CLASS_NAMES_TABLE, NAME_COLUMN,
      PARTITION_CLASS_ID_COLUMN, CLUSTER_NAME_COLUMN);

  /**
   * eg: INSERT INTO AggregatedPartitionClassReports (partitionClassId, accountId, containerId, storageUsage, physicalStorageUsage, numberOfBlobs, updatedAt)
   *     SELECT id, 101, 201, 12345, 23456, 321, NOW()
   *     FROM PartitionClassNames WHERE clusterName = "ambry-test" AND name = "default"
   *     ON DUPLICATE KEY UPDATE storageUsage=12345, physicalStorageUsage=23456, numberOfBlobs=321, updatedAt=NOW();
   *  101 is the account id, 201 is the container id, 12345 is the storage usage, 23456 is the physical storage usage, 321 is the number of blobs.
   */
  private static final String insertAggregatedSql = String.format(
      "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s) SELECT %s, ?, ?, ?, ?, ?, NOW() FROM %s WHERE %s=? AND %s=? ON DUPLICATE KEY UPDATE %s=?, %s=?, %s=?, %s=NOW()",
      AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_ID_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN,
      STORAGE_USAGE_COLUMN, PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN, ID_COLUMN,
      PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN, NAME_COLUMN, STORAGE_USAGE_COLUMN,
      PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN);

  /**
   * eg : DELETE FROM AggregatedPartitionClassReports
   *      WHERE
   *      PartitionClassId IN (SELECT id FROM PartitionClassNames WHERE clusterName = "ambry-test" AND name = "default")
   *      AND accountId = 101
   *      AND containerId = 201;
   */
  private static final String deleteAggregatedSql =
      String.format("DELETE FROM %s WHERE %s IN (SELECT %s FROM %s WHERE %s=? AND %s=?) AND %s=? AND %s=?",
          AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_ID_COLUMN, ID_COLUMN, PARTITION_CLASS_NAMES_TABLE,
          CLUSTER_NAME_COLUMN, NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN);

  /**
   * eg: SELECT TableA.name, accountId, containerId, storageUsage, physicalStorageUsage, numberOfBlobs, updatedAt
   *     FROM AggregatedPartitionClassReport INNER JOIN (
   *         SELECT * FROM PartitionClassNames WHERE clusterName="ambry-test"
   *     ) TableA
   *     WHERE TableA.id = AggregatedPartitionClassReports.partitionClassId;
   */
  private static final String queryAggregatedSql = String.format(
      "SELECT TableA.%s, %s, %s, %s, %s, %s, %s FROM %s " + "INNER JOIN (SELECT * FROM %s WHERE %s = ?) TableA"
          + " WHERE TableA.%s = %s.%s;", NAME_COLUMN, ACCOUNT_ID_COLUMN, CONTAINER_ID_COLUMN, STORAGE_USAGE_COLUMN,
      PHYSICAL_STORAGE_USAGE_COLUMN, NUMBER_OF_BLOBS_COLUMN, UPDATED_AT_COLUMN,
      AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_NAMES_TABLE, CLUSTER_NAME_COLUMN, ID_COLUMN,
      AGGREGATED_PARTITION_CLASS_REPORTS_TABLE, PARTITION_CLASS_ID_COLUMN);

  private final DataSource dataSource;
  private final MySqlMetrics metrics;

  /**
   * Constructor to instantiate a {@link PartitionClassReportsDao}.
   * @param dataSource The {@link DataSource}.
   * @param metrics The {@link MySqlMetrics}.
   */
  PartitionClassReportsDao(DataSource dataSource, MySqlMetrics metrics) {
    this.dataSource = Objects.requireNonNull(dataSource, "DataSource is empty");
    this.metrics = Objects.requireNonNull(metrics, "MySqlMetrics is empty");
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
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(queryPartitionClassNamesSql)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet rs = queryStatement.executeQuery()) {
          while (rs.next()) {
            short partitionClassId = rs.getShort(ID_COLUMN);
            String partitionClassName = rs.getString(NAME_COLUMN);
            result.put(partitionClassName, partitionClassId);
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
      return result;
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
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
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement insertStatement = connection.prepareStatement(insertPartitionClassNamesSql)) {
        long startTimeMs = System.currentTimeMillis();
        insertStatement.setString(1, clusterName);
        insertStatement.setString(2, partitionClassName);
        insertStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      // If somehow the unique pair(clusterName, partitionClassName) is inserted, consider this as a success
      // ignore it
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
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
  Map<String, Set<Integer>> queryPartitionNameAndIds(String clusterName) throws SQLException {
    Map<String, Set<Integer>> nameAndIds = new HashMap<>();
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(queryPartitionIdAndNamesSql)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet resultSet = queryStatement.executeQuery()) {
          while (resultSet.next()) {
            int partitionId = resultSet.getInt(ID_COLUMN);
            String partitionClassName = resultSet.getString(NAME_COLUMN);
            nameAndIds.computeIfAbsent(partitionClassName, k -> new HashSet<>()).add(partitionId);
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
      return nameAndIds;
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error("Failed to execute query {}, with parameter {}", queryPartitionIdAndNamesSql, clusterName, e);
      throw e;
    }
  }

  /**
   * Query the partition ids belonging to the given {@code clusterName}. An empty set will be returned if there is no
   * partitions for the given {@code clusterName}.
   * @param clusterName The clusterName.
   * @return A set of partition ids of the given {@code clusterName}.
   * @throws SQLException
   */
  Set<Integer> queryPartitionIds(String clusterName) throws SQLException {
    Set<Integer> partitionIds = new HashSet<>();
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(queryPartitionIdsSql)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet rs = queryStatement.executeQuery()) {
          while (rs.next()) {
            partitionIds.add(rs.getInt(ID_COLUMN));
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
      return partitionIds;
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
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
  void insertPartitionId(String clusterName, int partitionId, short partitionClassId) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement insertStatement = connection.prepareStatement(insertPartitionIdSql)) {
        long startTimeMs = System.currentTimeMillis();
        insertStatement.setString(1, clusterName);
        insertStatement.setInt(2, partitionId);
        insertStatement.setShort(3, partitionClassId);
        insertStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      // If somehow the unique pair(clusterName, partitionId) is inserted, consider this as a success
      // ignore it
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to execute of {}, with parameter {} {} {}", insertPartitionIdSql, clusterName, partitionId,
          partitionClassId, e);
      throw e;
    }
  }

  /**
   * Query container storage usage for given {@code clusterName}. This usage is aggregated under each partition class name.
   * The results will be applied to the given {@link PartitionClassContainerStorageStatsFunction}.
   * @param clusterName The clusterName.
   * @param func The callback to apply query results
   * @throws SQLException
   */
  void queryAggregatedPartitionClassReport(String clusterName, PartitionClassContainerStorageStatsFunction func)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement queryStatement = connection.prepareStatement(queryAggregatedSql)) {
        long startTimeMs = System.currentTimeMillis();
        queryStatement.setString(1, clusterName);
        try (ResultSet rs = queryStatement.executeQuery()) {
          while (rs.next()) {
            String partitionClassName = rs.getString(NAME_COLUMN);
            int accountId = rs.getInt(ACCOUNT_ID_COLUMN);
            int containerId = rs.getInt(CONTAINER_ID_COLUMN);
            long usage = rs.getLong(STORAGE_USAGE_COLUMN);
            final long physicalStorageUsage = rs.getLong(PHYSICAL_STORAGE_USAGE_COLUMN);
            final long numberOfBlobs = rs.getLong(NUMBER_OF_BLOBS_COLUMN);
            long updatedAt = rs.getTimestamp(UPDATED_AT_COLUMN).getTime();
            func.apply(partitionClassName, (short) accountId,
                new ContainerStorageStats((short) containerId, usage, physicalStorageUsage, numberOfBlobs), updatedAt);
          }
        }
        metrics.readTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.readSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.readFailureCount.inc();
      logger.error("Failed to execute query {}, with parameter {}", queryAggregatedSql, clusterName, e);
      throw e;
    }
  }

  /**
   * Delete storage usage from aggregated partition class reports.
   * @param clusterName The clusterName.
   * @param partitionClassName The partition class name
   * @param accountId The account id
   * @param containerId The container id
   * @throws SQLException
   */
  void deleteAggregatedStorageUsage(String clusterName, String partitionClassName, short accountId, short containerId)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement deleteStatement = connection.prepareStatement(deleteAggregatedSql)) {
        long startTimeMs = System.currentTimeMillis();
        deleteStatement.setString(1, clusterName);
        deleteStatement.setString(2, partitionClassName);
        deleteStatement.setInt(3, accountId);
        deleteStatement.setInt(4, containerId);
        deleteStatement.executeUpdate();
        metrics.writeTimeMs.update(System.currentTimeMillis() - startTimeMs);
        metrics.writeSuccessCount.inc();
      }
    } catch (SQLException e) {
      metrics.writeFailureCount.inc();
      logger.error("Failed to execute of {}, with parameter {} {} {} {}", deleteAggregatedSql, clusterName,
          partitionClassName, accountId, containerId, e);
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
      super(dataSource.getConnection(), metrics, insertAggregatedSql, AGGREGATED_PARTITION_CLASS_REPORTS_TABLE,
          maxBatchSize);
    }

    /**
     * Supply values to the prepared statement and add it to batch updater.
     * @param clusterName the clusterName.
     * @param partitionClassName the partition class name
     * @param accountId the account id
     * @param containerStats The {@link ContainerStorageStats}.
     * @throws SQLException
     */
    public void addUpdateToBatch(String clusterName, String partitionClassName, short accountId,
        ContainerStorageStats containerStats) throws SQLException {
      addUpdateToBatch(statement -> {
        statement.setInt(1, accountId);
        statement.setInt(2, containerStats.getContainerId());
        statement.setLong(3, containerStats.getLogicalStorageUsage());
        statement.setLong(4, containerStats.getPhysicalStorageUsage());
        statement.setLong(5, containerStats.getNumberOfBlobs());
        statement.setString(6, clusterName);
        statement.setString(7, partitionClassName);
        statement.setLong(8, containerStats.getLogicalStorageUsage());
        statement.setLong(9, containerStats.getPhysicalStorageUsage());
        statement.setLong(10, containerStats.getNumberOfBlobs());
      });
    }
  }
}

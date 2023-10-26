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
 *
 */
package com.github.ambry.repair;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.MysqlRepairRequestsDbConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.repair.RepairRequestRecord.*;


/**
 * MysqlRepairRequestsDb, using mysql db to implement the RepairRequestsDb
 * The class to manipulate the AmbryRepairRequests DB.
 * The AmbryRepairRequests DB has the records of {@link RepairRequestRecord}.
 * These are partially failed requests which need to get fixed.
 */
public class MysqlRepairRequestsDb implements RepairRequestsDb {
  private static final Logger logger = LoggerFactory.getLogger(MysqlRepairRequestsDb.class);

  // table name
  public static final String REPAIR_REQUESTS_TABLE = "ambry_repair_requests";

  // column names
  private static final String BLOB_ID = "blobId";
  private static final String PARTITION_ID = "partitionId";
  private static final String SOURCE_HOST_NAME = "sourceHostName";
  private static final String SOURCE_HOST_PORT = "sourceHostPort";
  private static final String OPERATION_TYPE = "operationType";
  private static final String OPERATION_TIME = "operationTime";
  private static final String LIFE_VERSION = "lifeVersion";
  private static final String EXPIRATION_TYPE = "expirationTime";

  /**
   * Select the records for one partition with the oldest operation time.
   */
  // @formatter:off
  private static final String GET_REQUESTS_QUERY = String.format(""
      + "SELECT %s, %s, %s, %s, %s, %s, %s "
      + "FROM %s "
      + "WHERE %s = ? "
      + "ORDER BY %s ASC "
      + "LIMIT ?",
      BLOB_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
      REPAIR_REQUESTS_TABLE,
      PARTITION_ID,
      OPERATION_TIME);
  // @formatter:on

  /**
   * Select the records for one partition but exclude the record with this source replica(hostname + hostport)
   * support query pagination
   */
  // @formatter:off
  private static final String GET_REQUESTS_QUERY_EXCLUDE_SOURCE_REPLICA_BY_PAGE = String.format(""
      + "SELECT %s, %s, %s, %s, %s, %s, %s "
      + "FROM %s "
      + "WHERE %s = ? and (%s != ? or %s != ?) and (%s >= ?) "
      + "ORDER BY %s ASC "
      + "LIMIT ?",
      BLOB_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
      REPAIR_REQUESTS_TABLE,
      PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TIME,
      OPERATION_TIME);
  // @formatter:on

  /**
   * Select the partition which are under repair and it has TtlUpdate to repair
   */
  // @formatter:off
  private static final String GET_PARTITIONS_QUERY_EXCLUDE_SOURCE_REPLICA_TEMPLATE = String.format(""
      + "SELECT DISTINCT %s "
      + "FROM %s "
      + "WHERE %s = ? and (%s != ? or %s != ?) and %s IN ",
      PARTITION_ID,
      REPAIR_REQUESTS_TABLE,
      OPERATION_TYPE, SOURCE_HOST_NAME, SOURCE_HOST_PORT, PARTITION_ID);
  // @formatter:on

  /**
   * Attempt to insert a RepairRequestRecord to the database
   */
  // @formatter:off
  private static final String INSERT_QUERY = String.format(""
      + "INSERT INTO %1$s "
      + "(%2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      REPAIR_REQUESTS_TABLE,
      BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE);
  // @formatter:on

  /**
   * Delete a record from the database
   */
  // @formatter:off
  private static final String DELETE_QUERY = String.format(""
      + "DELETE FROM %s "
      + "WHERE %s=? AND %s=?",
      REPAIR_REQUESTS_TABLE,
      BLOB_ID, OPERATION_TYPE);
  // @formatter:on

  private final DataSource dataSource;
  private final MysqlRepairRequestsDbConfig config;
  private final Metrics metrics;
  private final Time time;

  public MysqlRepairRequestsDb(DataSource dataSource, MysqlRepairRequestsDbConfig config,
      MetricRegistry metricsRegistry, Time time) {
    this.dataSource = dataSource;
    this.config = config;
    this.metrics = new Metrics(metricsRegistry);
    this.time = time;
  }

  /**
   * Remove one {@link RepairRequestRecord} from the database
   * @param blobId the blob id
   * @param operationType the operation time, either TtlUpdate or Delete
   */
  @Override
  public void removeRepairRequests(String blobId, OperationType operationType) throws SQLException {
    long startTime = time.milliseconds();
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(DELETE_QUERY)) {
        statement.setBytes(1, Base64.decodeBase64(blobId));
        statement.setShort(2, (short) operationType.ordinal());
        statement.executeUpdate();
      }
      metrics.repairDbRemoveRequestTimeInMs.update(time.milliseconds() - startTime);
    } catch (SQLException e) {
      metrics.repairDbErrorRemoveCount.inc();
      logger.error("failed to delete record from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Insert one {@link RepairRequestRecord}
   * @param record the record to insert
   */
  @Override
  public void putRepairRequests(RepairRequestRecord record) throws SQLException {
    long startTime = time.milliseconds();
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {
        statement.setBytes(1, Base64.decodeBase64(record.getBlobId()));
        statement.setLong(2, record.getPartitionId());
        statement.setString(3, record.getSourceHostName());
        statement.setInt(4, record.getSourceHostPort());
        statement.setShort(5, (short) record.getOperationType().ordinal());
        statement.setTimestamp(6, new Timestamp(record.getOperationTimeMs()));
        statement.setShort(7, record.getLifeVersion());
        if (record.getExpirationTimeMs() != Utils.Infinite_Time) {
          statement.setTimestamp(8, new Timestamp(record.getExpirationTimeMs()));
        } else {
          statement.setTimestamp(8, null);
        }
        statement.executeUpdate();
      }
      metrics.repairDbPutRequestTimeInMs.update(time.milliseconds() - startTime);
    } catch (SQLException e) {
      metrics.repairDbErrorPutCount.inc();
      logger.error("failed to insert record to {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Select the records for one partition ordered by the operation time.
   * @param partitionId partition id
   * @return the oldest {@link RepairRequestRecord}s.
   */
  @Override
  public List<RepairRequestRecord> getRepairRequestsForPartition(long partitionId) throws SQLException {
    long startTime = time.milliseconds();
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(GET_REQUESTS_QUERY)) {
        statement.setLong(1, partitionId);
        statement.setInt(2, config.listMaxResults);
        try (ResultSet resultSet = statement.executeQuery()) {
          List<RepairRequestRecord> result = new ArrayList<>();
          while (resultSet.next()) {
            String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
            String sourceHostName = resultSet.getString(2);
            int sourceHostPort = resultSet.getInt(3);
            OperationType operationType = OperationType.values()[resultSet.getShort(4)];
            Timestamp operationTime = resultSet.getTimestamp(5);
            short lifeVersion = resultSet.getShort(6);
            Timestamp expirationTime = resultSet.getTimestamp(7);
            RepairRequestRecord record =
                new RepairRequestRecord(blobId, partitionId, sourceHostName, sourceHostPort, operationType,
                    operationTime.getTime(), lifeVersion,
                    expirationTime != null ? expirationTime.getTime() : Utils.Infinite_Time);
            result.add(record);
          }
          metrics.repairDbGetRequestTimeInMs.update(time.milliseconds() - startTime);
          return result;
        }
      }
    } catch (SQLException e) {
      metrics.repairDbErrorGetCount.inc();
      logger.error("failed to get records from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Select the records from one partition but exclude the record with this source name and port.
   * @param partitionId partition id
   * @param sourceHostName the host name of the source replica
   * @param sourceHostPort the host port of the source replica
   * @param pageToken the token for the next page
   * @return pair of the oldest {@link RepairRequestRecord}s and token for the next page
   */
  @Override
  public Pair<List<RepairRequestRecord>, Long> getRepairRequestsExcludingHost(long partitionId, String sourceHostName,
      int sourceHostPort, long pageToken) throws SQLException {
    long startTime = time.milliseconds();
    Long nextPageToken = (long) 0;
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(
          GET_REQUESTS_QUERY_EXCLUDE_SOURCE_REPLICA_BY_PAGE)) {
        statement.setLong(1, partitionId);
        statement.setString(2, sourceHostName);
        statement.setInt(3, sourceHostPort);
        statement.setTimestamp(4, new Timestamp(pageToken));
        statement.setInt(5, config.listMaxResults + 1);
        try (ResultSet resultSet = statement.executeQuery()) {
          List<RepairRequestRecord> result = new ArrayList<>();
          while (resultSet.next()) {
            String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
            String hostName = resultSet.getString(2);
            int hostPort = resultSet.getInt(3);
            OperationType operationType = OperationType.values()[resultSet.getShort(4)];
            Timestamp operationTime = resultSet.getTimestamp(5);
            short lifeVersion = resultSet.getShort(6);
            Timestamp expirationTime = resultSet.getTimestamp(7);
            RepairRequestRecord record =
                new RepairRequestRecord(blobId, partitionId, hostName, hostPort, operationType, operationTime.getTime(),
                    lifeVersion, expirationTime != null ? expirationTime.getTime() : Utils.Infinite_Time);
            if (result.size() < config.listMaxResults) {
              result.add(record);
            }
            // update the pagination token
            // LOCAL_CONSISTENCY_TODO
            // one risk is that all the records has the same operation time and RepairRequest handler fails to fix any of them.
            // then, the next page won't advance
            nextPageToken = record.getOperationTimeMs();
          }
          metrics.repairDbGetRequestTimeInMs.update(time.milliseconds() - startTime);

          // restart from the beginning.
          if (result.size() < config.listMaxResults) {
            nextPageToken = (long) 0;
          }
          return new Pair(result, nextPageToken);
        }
      }
    } catch (SQLException e) {
      metrics.repairDbErrorGetCount.inc();
      logger.error("failed to get records from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Get the partitions which have TtlUpdate requests to repair.
   * @param sourceHostName the host name of the source replica
   * @param sourceHostPort the host port of the source replica
   * @param partitions the partitions to check
   * @return the partitions which have TtlUpdate requests to repair.
   */
  @Override
  public Set<Long> getPartitionsNeedRepair(String sourceHostName, int sourceHostPort, List<Long> partitions)
      throws SQLException {
    long startTime = time.milliseconds();
    String partitionsStr = partitions.stream().map(n -> n.toString()).collect(Collectors.joining(","));
    String query = String.format(GET_PARTITIONS_QUERY_EXCLUDE_SOURCE_REPLICA_TEMPLATE + " (%s) ", partitionsStr);

    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(query)) {
        statement.setShort(1, (short) OperationType.TtlUpdateRequest.ordinal());
        statement.setString(2, sourceHostName);
        statement.setInt(3, sourceHostPort);
        try (ResultSet resultSet = statement.executeQuery()) {
          Set<Long> result = new HashSet<>();
          while (resultSet.next()) {
            Long p = resultSet.getLong(1);
            result.add(p);
          }
          metrics.repairDbGetPartitionTimeInMs.update(time.milliseconds() - startTime);
          return result;
        }
      }
    } catch (SQLException e) {
      metrics.repairDbErrorGetCount.inc();
      logger.error("failed to get records from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * The max number of result sets it will return for each query
   * @return the max number of result sets to return
   */
  @Override
  public int getListMaxResults() {
    return config.listMaxResults;
  }

  /**
   * Exposed for integration test usage.
   * @return the {@link Connection}.
   */
  public DataSource getDataSource() {
    return dataSource;
  }

  @Override
  public void close() {
    // close the DataSource
    if (dataSource instanceof AutoCloseable) {
      try {
        ((AutoCloseable) dataSource).close();
      } catch (Exception e) {
        logger.error("Failed to close data source: ", e);
      }
    }
  }

  private static class Metrics {
    public final Counter repairDbErrorRemoveCount;
    public final Counter repairDbErrorPutCount;
    public final Counter repairDbErrorGetCount;

    public final Histogram repairDbGetRequestTimeInMs;
    public final Histogram repairDbGetPartitionTimeInMs;
    public final Histogram repairDbPutRequestTimeInMs;
    public final Histogram repairDbRemoveRequestTimeInMs;

    /**
     * Constructor to create the Metrics.
     * @param metricRegistry The {@link MetricRegistry}.
     */
    public Metrics(MetricRegistry metricRegistry) {
      repairDbErrorRemoveCount =
          metricRegistry.counter(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbErrorRemoveCount"));
      repairDbErrorPutCount =
          metricRegistry.counter(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbErrorPutCount"));
      repairDbErrorGetCount =
          metricRegistry.counter(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbErrorGetCount"));

      repairDbGetRequestTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbGetRequestTimeInMs"));
      repairDbGetPartitionTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbGetPartitionTimeInMs"));
      repairDbPutRequestTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbPutRequestTimeInMs"));
      repairDbRemoveRequestTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(MysqlRepairRequestsDb.class, "RepairDbRemoveRequestTimeInMs"));
    }
  }
}

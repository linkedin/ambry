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

import com.github.ambry.config.MysqlRepairRequestsDbConfig;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.repair.RepairRequestRecord.OperationType;


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

  // LOCAL_CONSISTENCY_TODO continue the Get from one token
  /**
   * Select the records for one partition with the oldest operation time.
   */
  // @formatter:off
  private static final String GET_QUERY = String.format(""
      + "SELECT %s, %s, %s, %s, %s, %s, %s, %s "
      + "FROM %s "
      + "WHERE %s = ? "
      + "ORDER BY %s ASC "
      + "LIMIT ?",
      BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
      REPAIR_REQUESTS_TABLE,
      PARTITION_ID,
      OPERATION_TIME);
  // @formatter:on

  /**
   * Select the records for one partition but exclude the record with this source replica(hostname + hostport)
   */
  // @formatter:off
  private static final String GET_QUERY_EXCLUDE_SOURCE_REPLICA = String.format(""
          + "SELECT %s, %s, %s, %s, %s, %s, %s, %s "
          + "FROM %s "
          + "WHERE %s = ? and (%s != ? or %s != ?) "
          + "ORDER BY %s ASC "
          + "LIMIT ?",
      BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
      REPAIR_REQUESTS_TABLE,
      PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT,
      OPERATION_TIME);
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

  public MysqlRepairRequestsDb(DataSource dataSource, MysqlRepairRequestsDbConfig config) {
    this.dataSource = dataSource;
    this.config = config;
  }

  /**
   * Remove one {@link RepairRequestRecord} from the database
   * @param blobId the blob id
   * @param operationType the operation time, either TtlUpdate or Delete
   */
  @Override
  public void removeRepairRequests(String blobId, OperationType operationType) throws SQLException {
    // private static final String DELETE_QUERY = String.format(""
    //     + "DELETE FROM %s "
    //     + "WHERE %s=? AND %s=?",
    //     REPAIR_REQUESTS_TABLE,
    //     BLOB_ID, OPERATION_TYPE);
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(DELETE_QUERY)) {
        statement.setBytes(1, Base64.decodeBase64(blobId));
        statement.setShort(2, (short) operationType.ordinal());
        statement.executeUpdate();
      }
    } catch (SQLException e) {
      // LOCAL_CONSISTENCY_TODO: add metrics for all the SQLException.
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
    // private static final String INSERT_QUERY = String.format(""
    //    + "INSERT INTO %1$s "
    //    + "(%2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    //    REPAIR_REQUESTS_TABLE,
    //    BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE);
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
    } catch (SQLException e) {
      logger.error("failed to insert record to {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Select the records from one partition ordered by the operation time.
   * @param partitionId partition id
   * @return the oldest {@link RepairRequestRecord}s.
   */
  @Override
  public List<RepairRequestRecord> getRepairRequests(long partitionId) throws SQLException {
    // private static final String GET_QUERY = String.format(""
    //    + "SELECT %s, %s, %s, %s, %s, %s, %s, %s "
    //    + "FROM %s "
    //    + "WHERE %s = ? "
    //    + "ORDER BY %s ASC "
    //    + "LIMIT ?",
    //    BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
    //    REPAIR_REQUESTS_TABLE,
    //    PARTITION_ID,
    //    OPERATION_TIME);
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(GET_QUERY)) {
        statement.setLong(1, partitionId);
        statement.setInt(2, config.listMaxResults);
        try (ResultSet resultSet = statement.executeQuery()) {
          List<RepairRequestRecord> result = new ArrayList<>();
          while (resultSet.next()) {
            String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
            // resultSet.getLong(2) is the partition id.
            String sourceHostName = resultSet.getString(3);
            int sourceHostPort = resultSet.getInt(4);
            OperationType operationType = OperationType.values()[resultSet.getShort(5)];
            Timestamp operationTime = resultSet.getTimestamp(6);
            short lifeVersion = resultSet.getShort(7);
            Timestamp expirationTime = resultSet.getTimestamp(8);
            RepairRequestRecord record =
                new RepairRequestRecord(blobId, partitionId, sourceHostName, sourceHostPort, operationType,
                    operationTime.getTime(), lifeVersion,
                    expirationTime != null ? expirationTime.getTime() : Utils.Infinite_Time);
            result.add(record);
          }
          return result;
        }
      }
    } catch (SQLException e) {
      logger.error("failed to get records from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
  }

  /**
   * Select the records from one partition but exclude the record with this source name and port.
   * @param partitionId partition id
   * @param sourceHostName the host name of the source replica
   * @param sourceHostPort the host port of the source replica
   * @return the oldest {@link RepairRequestRecord}s.
   */
  @Override
  public List<RepairRequestRecord> getRepairRequests(long partitionId, String sourceHostName, int sourceHostPort)
      throws SQLException {
    // private static final String GET_QUERY_EXCLUDE_SOURCE_REPLICA = String.format(""
    //    + "SELECT %s, %s, %s, %s, %s, %s, %s, %s "
    //    + "FROM %s "
    //    + "WHERE %s = ? and (%s != ? or %s != ?) "
    //    + "ORDER BY %s ASC "
    //    + "LIMIT ?",
    //    BLOB_ID, PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT, OPERATION_TYPE, OPERATION_TIME, LIFE_VERSION, EXPIRATION_TYPE,
    //    REPAIR_REQUESTS_TABLE,
    //    PARTITION_ID, SOURCE_HOST_NAME, SOURCE_HOST_PORT,
    //    OPERATION_TIME);
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(GET_QUERY_EXCLUDE_SOURCE_REPLICA)) {
        statement.setLong(1, partitionId);
        statement.setString(2, sourceHostName);
        statement.setInt(3, sourceHostPort);
        statement.setInt(4, config.listMaxResults);
        try (ResultSet resultSet = statement.executeQuery()) {
          List<RepairRequestRecord> result = new ArrayList<>();
          while (resultSet.next()) {
            String blobId = Base64.encodeBase64URLSafeString(resultSet.getBytes(1));
            // resultSet.getLong(2) is the partition id.
            String hostName = resultSet.getString(3);
            int hostPort = resultSet.getInt(4);
            OperationType operationType = OperationType.values()[resultSet.getShort(5)];
            Timestamp operationTime = resultSet.getTimestamp(6);
            short lifeVersion = resultSet.getShort(7);
            Timestamp expirationTime = resultSet.getTimestamp(8);
            RepairRequestRecord record =
                new RepairRequestRecord(blobId, partitionId, hostName, hostPort, operationType, operationTime.getTime(),
                    lifeVersion, expirationTime != null ? expirationTime.getTime() : Utils.Infinite_Time);
            result.add(record);
          }
          return result;
        }
      }
    } catch (SQLException e) {
      logger.error("failed to get records from {} due to {}", dataSource, e.getMessage());
      throw e;
    }
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

  }
}

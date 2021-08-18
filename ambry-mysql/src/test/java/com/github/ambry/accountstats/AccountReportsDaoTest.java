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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link AccountReportsDao}.
 */
@RunWith(MockitoJUnitRunner.class)
public class AccountReportsDaoTest {
  private final MySqlMetrics metrics;
  private final Connection mockConnection;
  private final PreparedStatement mockInsertStatement;
  private final PreparedStatement mockQueryStatement;
  private final AccountReportsDao accountReportsDao;
  private static final String clusterName = "Ambry-test";
  private static final String hostname = "test.ambry.com";

  private final int queryPartitionId = 10;
  private final int queryAccountId = 100;
  private final int queryContainerId = 8;
  private final long queryStorageUsage = 12345;

  public AccountReportsDaoTest() throws SQLException {
    // Mock insert statement
    mockInsertStatement = mock(PreparedStatement.class);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);

    // Mock select statement
    mockQueryStatement = mock(PreparedStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt(eq(AccountReportsDao.PARTITION_ID_COLUMN))).thenReturn(queryPartitionId);
    when(mockResultSet.getInt(eq(AccountReportsDao.ACCOUNT_ID_COLUMN))).thenReturn(queryAccountId);
    when(mockResultSet.getInt(eq(AccountReportsDao.CONTAINER_ID_COLUMN))).thenReturn(queryContainerId);
    when(mockResultSet.getLong(eq(AccountReportsDao.STORAGE_USAGE_COLUMN))).thenReturn(queryStorageUsage);
    when(mockResultSet.getLong(eq(AccountReportsDao.PHYSICAL_STORAGE_USAGE_COLUMN))).thenReturn(queryStorageUsage);
    when(mockResultSet.getLong(eq(AccountReportsDao.NUMBER_OF_BLOBS_COLUMN))).thenReturn(1L);
    when(mockResultSet.getTimestamp(eq(AccountReportsDao.UPDATED_AT_COLUMN))).thenReturn(
        new Timestamp(SystemTime.getInstance().milliseconds()));
    when(mockQueryStatement.executeQuery()).thenReturn(mockResultSet);

    // Set mocked statements in the mock connection
    mockConnection = mock(Connection.class);
    when(mockConnection.prepareStatement(contains("INSERT"))).thenReturn(mockInsertStatement);
    when(mockConnection.prepareStatement(startsWith("SELECT"))).thenReturn(mockQueryStatement);

    metrics = new MySqlMetrics(AccountReportsDao.class, new MetricRegistry());
    accountReportsDao = new AccountReportsDao(getDataSource(mockConnection), metrics);
  }

  /**
   * Utility to get a {@link DataSource}.
   * @param mockConnection the connection to use.
   * @return the {@link DataSource}.
   * @throws SQLException
   */
  static DataSource getDataSource(Connection mockConnection) throws SQLException {
    DataSource mockDataSource = mock(DataSource.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    return mockDataSource;
  }

  @Test
  public void testUpdateStorageUsage() throws Exception {
    short partitionId = 1;
    short accountId = 100;
    short containerId = 8;
    long storageUsage = 100000;
    accountReportsDao.updateStorageUsage(clusterName, hostname, partitionId, accountId, containerId, storageUsage);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
    // Run second time to reuse statement
    accountReportsDao.updateStorageUsage(clusterName, hostname, partitionId, accountId, containerId, storageUsage);
    assertEquals("Write success count should be 2", 2, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testUpdateStorageUsageWithException() throws Exception {
    when(mockInsertStatement.executeUpdate()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> accountReportsDao.updateStorageUsage(clusterName, hostname, (short) 1, (short) 1000, (short) 8, 100000),
        null);
    assertEquals("Write failure count should be 1", 1, metrics.writeFailureCount.getCount());
  }

  @Test
  public void testQueryStorageUsageForHost() throws Exception {
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerStats, updatedAt) -> {
          assertEquals("Partition id mismatch", queryPartitionId, partitionId);
          assertEquals("Account id mismatch", queryAccountId, accountId);
          assertEquals("Container id mismatch", queryContainerId, containerStats.getContainerId());
          assertEquals("Storage usage mismatch", queryStorageUsage, containerStats.getLogicalStorageUsage());
          assertEquals("Physical storage usage mismatch", queryStorageUsage, containerStats.getPhysicalStorageUsage());
          assertEquals("Number of blobs mismatch", 1L, containerStats.getNumberOfBlobs());
        });
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testQueryStorageUsageForHostWithException() throws Exception {
    when(mockQueryStatement.executeQuery()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> accountReportsDao.queryStorageUsageForHost(clusterName, hostname, null), null);
    assertEquals("Read failure count should be 1", 1, metrics.readFailureCount.getCount());
  }
}

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
import com.github.ambry.utils.TestUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import javax.sql.DataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link AggregatedAccountReportsDao}.
 */
@RunWith(MockitoJUnitRunner.class)
public class AggregatedAccountReportsDaoTest {
  private final MySqlMetrics metrics;
  private final Connection mockConnection;
  private final PreparedStatement mockInsertAggregatedStatement;
  private final PreparedStatement mockInsertCopyStatement;
  private final PreparedStatement mockInsertMonthStatement;
  private final PreparedStatement mockQueryAggregatedStatement;
  private final PreparedStatement mockQueryMonthStatement;
  private final AggregatedAccountReportsDao aggregatedAccountReportsDao;
  private static final String clusterName = "Ambry-test";

  private final int queryAccountId = 1000;
  private final int queryContainerId = 8;
  private final long queryStorageUsage = 123456;
  private final String queryMonthValue = "2020-01";

  public AggregatedAccountReportsDaoTest() throws SQLException {
    // Mock inserts - don't stub executeUpdate() here, let each test stub as needed
    mockInsertAggregatedStatement = mock(PreparedStatement.class);
    mockInsertCopyStatement = mock(PreparedStatement.class);
    mockInsertMonthStatement = mock(PreparedStatement.class);

    // Mock select statement
    mockQueryAggregatedStatement = mock(PreparedStatement.class);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt(eq(AggregatedAccountReportsDao.ACCOUNT_ID_COLUMN))).thenReturn(queryAccountId);
    when(mockResultSet.getInt(eq(AggregatedAccountReportsDao.CONTAINER_ID_COLUMN))).thenReturn(queryContainerId);
    when(mockResultSet.getLong(eq(AggregatedAccountReportsDao.STORAGE_USAGE_COLUMN))).thenReturn(queryStorageUsage);
    when(mockResultSet.getLong(eq(AggregatedAccountReportsDao.PHYSICAL_STORAGE_USAGE_COLUMN))).thenReturn(
        queryStorageUsage);
    when(mockResultSet.getLong(eq(AggregatedAccountReportsDao.NUMBER_OF_BLOBS_COLUMN))).thenReturn(1L);
    when(mockQueryAggregatedStatement.executeQuery()).thenReturn(mockResultSet);

    mockQueryMonthStatement = mock(PreparedStatement.class);
    mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString(eq(AggregatedAccountReportsDao.MONTH_COLUMN))).thenReturn(queryMonthValue);
    when(mockQueryMonthStatement.executeQuery()).thenReturn(mockResultSet);

    // Set mocked statements in the mock connection
    // Order matters in Mockito 5 - more specific matchers should come first
    // Using lenient() for shared setup that may not be used by all tests
    mockConnection = mock(Connection.class);
    lenient().when(mockConnection.prepareStatement(
        startsWith("INSERT " + AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE + " SELECT"))).thenReturn(
        mockInsertCopyStatement);
    lenient().when(mockConnection.prepareStatement(
        startsWith("INSERT INTO " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE))).thenReturn(
        mockInsertAggregatedStatement);
    lenient().when(mockConnection.prepareStatement(
        startsWith("INSERT INTO " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE))).thenReturn(
        mockInsertMonthStatement);
    lenient().when(mockConnection.prepareStatement(
        matches("SELECT.+" + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE + ".+"))).thenReturn(
        mockQueryMonthStatement);
    lenient().when(mockConnection.prepareStatement(
        matches("SELECT.+" + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE + " .+"))).thenReturn(
        mockQueryAggregatedStatement);
    lenient().when(mockConnection.prepareStatement(
        matches("SELECT.+" + AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE + ".+"))).thenReturn(
        mockQueryAggregatedStatement);

    metrics = new MySqlMetrics(AggregatedAccountReportsDao.class, new MetricRegistry());
    aggregatedAccountReportsDao = new AggregatedAccountReportsDao(getDataSource(mockConnection), metrics);
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
  public void testInsertAggregatedStats() throws Exception {
    when(mockInsertAggregatedStatement.executeUpdate()).thenReturn(1);
    short accountId = 10;
    short containerId = 1;
    long storageUsage = 1000;
    aggregatedAccountReportsDao.updateStorageUsage(clusterName, accountId, containerId, storageUsage);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
    // Run second time to reuse statement
    aggregatedAccountReportsDao.updateStorageUsage(clusterName, accountId, containerId, storageUsage);
    assertEquals("Write success count should be 2", 2, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testInsertAggregatedStatsWithException() throws Exception {
    doThrow(new SQLTransientConnectionException()).when(mockInsertAggregatedStatement).executeUpdate();
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> aggregatedAccountReportsDao.updateStorageUsage(clusterName, (short) 1, (short) 1000, 100000), null);
    assertEquals("Write failure count should be 1", 1, metrics.writeFailureCount.getCount());
  }

  @Test
  public void testInsertCopy() throws Exception {
    lenient().when(mockInsertCopyStatement.executeUpdate()).thenReturn(1);
    aggregatedAccountReportsDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Copy success count should be 1", 1, metrics.copySuccessCount.getCount());

    // Run second time to reuse statement
    aggregatedAccountReportsDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName);
    assertEquals("Copy success count should be 2", 2, metrics.copySuccessCount.getCount());
  }

  @Test
  public void testInsertCopyWithException() throws Exception {
    // Create fresh mocks for this test to avoid interaction with constructor setup
    Connection testConnection = mock(Connection.class);
    PreparedStatement testCopyStatement = mock(PreparedStatement.class);
    DataSource testDataSource = mock(DataSource.class);

    // Set up the mock chain: dataSource -> connection -> preparedStatement
    when(testDataSource.getConnection()).thenReturn(testConnection);
    when(testConnection.prepareStatement(anyString())).thenReturn(testCopyStatement);

    // Configure the statement to throw SQLException when executeUpdate is called
    when(testCopyStatement.executeUpdate()).thenThrow(new SQLTransientConnectionException("Test exception"));

    // Create a new DAO instance with the test datasource
    AggregatedAccountReportsDao testDao = new AggregatedAccountReportsDao(testDataSource, metrics);

    // Record metrics before the test
    long copyFailureCountBefore = metrics.copyFailureCount.getCount();

    // Execute and verify the exception is thrown
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> testDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName), null);

    // Verify the failure metric was incremented
    assertEquals("Copy failure count should increment by 1",
        copyFailureCountBefore + 1, metrics.copyFailureCount.getCount());

    // Verify the interactions occurred in the expected order
    verify(testDataSource).getConnection();
    verify(testConnection).prepareStatement(anyString());
    verify(testCopyStatement).setString(1, clusterName);
    verify(testCopyStatement).executeUpdate();
  }


  @Test
  public void testInsertMonth() throws Exception {
    when(mockInsertMonthStatement.executeUpdate()).thenReturn(1);
    long writeSuccessCountBefore = metrics.writeSuccessCount.getCount();
    aggregatedAccountReportsDao.updateMonth(clusterName, "2020-01");
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be " + (writeSuccessCountBefore + 1), writeSuccessCountBefore + 1,
        metrics.writeSuccessCount.getCount());

    aggregatedAccountReportsDao.updateMonth(clusterName, "2020-01");
    assertEquals("Write success count should be " + (writeSuccessCountBefore + 2), writeSuccessCountBefore + 2,
        metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testInsertMonthWithException() throws Exception {
    long writeFailureCountBefore = metrics.writeFailureCount.getCount();
    doThrow(new SQLTransientConnectionException()).when(mockInsertMonthStatement).executeUpdate();
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> aggregatedAccountReportsDao.updateMonth(clusterName, "2020-01"), null);
    assertEquals("Write failure count should be " + (writeFailureCountBefore + 1), writeFailureCountBefore + 1,
        metrics.writeFailureCount.getCount());
  }

  @Test
  public void testQueryAggregatedStats() throws Exception {
    long readSuccessCountBefore = metrics.readSuccessCount.getCount();
    aggregatedAccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerStats) -> {
      assertEquals(queryAccountId, accountId);
      assertEquals(queryContainerId, containerStats.getContainerId());
      assertEquals(queryStorageUsage, containerStats.getLogicalStorageUsage());
      assertEquals(queryStorageUsage, containerStats.getPhysicalStorageUsage());
      assertEquals(1L, containerStats.getNumberOfBlobs());
    });
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Read success count should be " + (readSuccessCountBefore + 1), (readSuccessCountBefore + 1),
        metrics.readSuccessCount.getCount());
  }

  @Test
  public void testQueryAggregatedStatsWithException() throws Exception {
    long readFailureCountBefore = metrics.readFailureCount.getCount();
    doThrow(new SQLTransientConnectionException()).when(mockQueryAggregatedStatement).executeQuery();
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> aggregatedAccountReportsDao.queryContainerUsageForCluster(clusterName, null), null);
    assertEquals("Read failure count should be " + (readFailureCountBefore + 1), readFailureCountBefore + 1,
        metrics.readFailureCount.getCount());
  }

  @Test
  public void testQueryMonthlyAggregatedStats() throws Exception {
    long readSuccessCountBefore = metrics.readSuccessCount.getCount();
    aggregatedAccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName, (accountId, containerStats) -> {
      assertEquals(queryAccountId, accountId);
      assertEquals(queryContainerId, containerStats.getContainerId());
      assertEquals(queryStorageUsage, containerStats.getLogicalStorageUsage());
      assertEquals(queryStorageUsage, containerStats.getPhysicalStorageUsage());
      assertEquals(1L, containerStats.getNumberOfBlobs());
    });
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Read success count should be " + (readSuccessCountBefore + 1), (readSuccessCountBefore + 1),
        metrics.readSuccessCount.getCount());
  }

  @Test
  public void testQueryMonthlyAggregatedStatsWithException() throws Exception {
    long readFailureCountBefore = metrics.readFailureCount.getCount();
    doThrow(new SQLTransientConnectionException()).when(mockQueryAggregatedStatement).executeQuery();
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> aggregatedAccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName, null), null);
    assertEquals("Read failure count should be " + (readFailureCountBefore + 1), readFailureCountBefore + 1,
        metrics.readFailureCount.getCount());
  }

  @Test
  public void testQueryMonth() throws Exception {
    long readSuccessCountBefore = metrics.readSuccessCount.getCount();
    String monthValue = aggregatedAccountReportsDao.queryMonthForCluster(clusterName);
    assertTrue(queryMonthValue.equals(monthValue));
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Read success count should be " + (readSuccessCountBefore + 1), readSuccessCountBefore + 1,
        metrics.readSuccessCount.getCount());
  }

  @Test
  public void testQueryMonthWithException() throws Exception {
    long readFailureCountBefore = metrics.readFailureCount.getCount();
    doThrow(new SQLTransientConnectionException()).when(mockQueryMonthStatement).executeQuery();
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> aggregatedAccountReportsDao.queryMonthForCluster(clusterName), null);
    assertEquals("Read failure count should be " + (readFailureCountBefore + 1), readFailureCountBefore + 1,
        metrics.readFailureCount.getCount());
  }
}

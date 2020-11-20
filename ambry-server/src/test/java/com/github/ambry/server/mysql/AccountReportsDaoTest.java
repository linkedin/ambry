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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.utils.TestUtils;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Collections;
import java.util.Properties;
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
  private final MySqlDataAccessor dataAccessor;
  private final Connection mockConnection;
  private final PreparedStatement mockInsertStatement;
  private final AccountReportsDao accountReportsDao;
  private static final String clustername = "Ambry-test";
  private static final String hostname = "test.ambry.com";

  public AccountReportsDaoTest() throws SQLException {
    mockConnection = mock(Connection.class);
    mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("INSERT"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);
    metrics = new MySqlMetrics(AccountReportsDao.class, new MetricRegistry());
    dataAccessor = getDataAccessor(mockConnection, metrics);
    accountReportsDao = new AccountReportsDao(dataAccessor, clustername, hostname);
  }

  /**
   * Utility to get a {@link MySqlDataAccessor}.
   * @param mockConnection the connection to use.
   * @return the {@link MySqlDataAccessor}.
   * @throws SQLException
   */
  static MySqlDataAccessor getDataAccessor(Connection mockConnection, MySqlMetrics metrics) throws SQLException {
    Driver mockDriver = mock(Driver.class);
    when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint("jdbc:mysql://localhost/ambry_container_storage_stats", "dc1", true, "ambry",
            "ambry");
    return new MySqlDataAccessor(Collections.singletonList(dbEndpoint), mockDriver, metrics);
  }

  @Test
  public void testUpdateStorageUsage() throws Exception {
    short partitionId = 1;
    short accountId = 100;
    short containerId = 8;
    long storageUsage = 100000;
    accountReportsDao.updateStorageUsage(partitionId, accountId, containerId, storageUsage);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
    // Run second time to reuse statement
    accountReportsDao.updateStorageUsage(partitionId, accountId, containerId, storageUsage);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 2", 2, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testUpdateStorageUsageWithException() throws Exception {
    when(mockInsertStatement.executeUpdate()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> accountReportsDao.updateStorageUsage((short) 1, (short) 1000, (short) 8, 100000), null);
    assertEquals("Write failure count should be 1", 1, metrics.writeFailureCount.getCount());
  }
}

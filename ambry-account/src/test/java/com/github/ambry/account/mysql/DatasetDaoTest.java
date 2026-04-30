/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account.mysql;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Properties;
import org.junit.Test;

import static com.github.ambry.account.mysql.AccountDaoTest.getDataAccessor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/** Unit test for {@link DatasetDao}. */
public class DatasetDaoTest {

  /**
   * Regression for prod incident on 2026-04-29: a NPE thrown from the MySQL JDBC driver
   * while reading the {@code versionSchema} column (ResultSetImpl.findColumn -> getInt)
   * surfaced as HTTP 500 from {@code GET /named/<account>/<container>/<dataset>}
   * because the exception bubbled past {@link DatasetDao#getDataset} uncaught. Verify the
   * narrow catch translates it into {@link AccountServiceException} with
   * {@link AccountServiceErrorCode#NotFound}, increments the operability counter, and
   * keeps the dataset identifiers in the exception message.
   */
  @Test
  public void testGetDatasetMapsJdbcNpeOnVersionSchemaToNotFound() throws Exception {
    Connection mockConnection = mock(Connection.class);
    MySqlMetrics metrics = new MySqlMetrics(DatasetDao.class, new MetricRegistry());
    MySqlDataAccessor dataAccessor = getDataAccessor(mockConnection, metrics);
    PreparedStatement mockGetDatasetStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("from " + DatasetDao.DATASET_TABLE))).thenReturn(
        mockGetDatasetStatement);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    // Future-dated deletion ts so the deleted-check passes and we reach the column read.
    when(mockResultSet.getTimestamp(eq(DatasetDao.DELETED_TS))).thenReturn(
        new Timestamp(System.currentTimeMillis() + 60_000L));
    // Reproduce the JDBC driver NPE seen in prod.
    when(mockResultSet.getInt(eq(DatasetDao.VERSION_SCHEMA))).thenThrow(new NullPointerException());
    when(mockGetDatasetStatement.executeQuery()).thenReturn(mockResultSet);

    Properties props = new Properties();
    props.setProperty(MySqlAccountServiceConfig.DB_INFO, "");
    MySqlAccountServiceConfig config = new MySqlAccountServiceConfig(new VerifiableProperties(props));
    DatasetDao dao = new DatasetDao(dataAccessor, config, metrics);

    long npeCountBefore = metrics.datasetRowReadNpeCount.getCount();
    try {
      dao.getDataset(1, 2, "acct", "cont", "ds");
      fail("Expected AccountServiceException");
    } catch (AccountServiceException e) {
      assertEquals(AccountServiceErrorCode.NotFound, e.getErrorCode());
      String message = e.getMessage();
      assertTrue("message should include accountId, got: " + message, message.contains("1"));
      assertTrue("message should include containerId, got: " + message, message.contains("2"));
      assertTrue("message should include datasetName, got: " + message, message.contains("ds"));
    }
    assertEquals("datasetRowReadNpeCount should increment by 1", npeCountBefore + 1,
        metrics.datasetRowReadNpeCount.getCount());
  }
}

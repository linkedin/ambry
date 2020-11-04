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
package com.github.ambry.account.mysql;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Timestamp;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/** Unit test for ContainerDao class */
@RunWith(MockitoJUnitRunner.class)
public class ContainerDaoTest {

  private final short accountId = 101;
  private final short containerId = 1;
  private final String containerName = "state-backup";
  private final Container testContainer;
  private final String containerJson;
  private final String accountName = "samza";
  private final MySqlDataAccessor dataAccessor;
  private final Connection mockConnection;
  private final ContainerDao containerDao;
  private final MySqlAccountStoreMetrics metrics;
  private final PreparedStatement mockInsertStatement;
  private final PreparedStatement mockQueryStatement;

  public ContainerDaoTest() throws SQLException {
    metrics = new MySqlAccountStoreMetrics(new MetricRegistry());
    testContainer =
        new ContainerBuilder(containerId, containerName, Container.ContainerStatus.ACTIVE, "", accountId).build();
    containerJson = testContainer.toJson().toString();
    mockConnection = mock(Connection.class);
    mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("insert into"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);
    mockQueryStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(startsWith("select"))).thenReturn(mockQueryStatement);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockQueryStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getInt(eq(ContainerDao.ACCOUNT_ID))).thenReturn((int) accountId);
    when(mockResultSet.getString(eq(ContainerDao.CONTAINER_INFO))).thenReturn(containerJson);
    when(mockResultSet.getTimestamp(eq(ContainerDao.LAST_MODIFIED_TIME))).thenReturn(
        new Timestamp(SystemTime.getInstance().milliseconds()));
    dataAccessor = AccountDaoTest.getDataAccessor(mockConnection, metrics);
    containerDao = new ContainerDao(dataAccessor);
  }

  @Test
  public void testAddContainer() throws Exception {
    containerDao.addContainer(accountId, testContainer);
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testGetContainersForAccount() throws Exception {
    List<Container> containerList = containerDao.getContainers(accountId);
    assertEquals(1, containerList.size());
    assertEquals(testContainer, containerList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testGetNewContainers() throws Exception {
    List<Container> containerList = containerDao.getNewContainers(0);
    assertEquals(1, containerList.size());
    assertEquals(testContainer, containerList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testAddContainerWithException() throws Exception {
    when(mockInsertStatement.executeUpdate()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class,
        () -> containerDao.addContainer(accountId, testContainer), null);
    assertEquals("Write failure count should be 1", 1, metrics.writeFailureCount.getCount());
  }

  @Test
  public void testGetNewContainersWithException() throws Exception {
    when(mockQueryStatement.executeQuery()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class, () -> containerDao.getNewContainers(0L), null);
    assertEquals("Read failure count should be 1", 1, metrics.readFailureCount.getCount());
  }
}

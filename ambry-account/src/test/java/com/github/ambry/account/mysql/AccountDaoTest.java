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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/** Unit test for AccountDao class */
@RunWith(MockitoJUnitRunner.class)
public class AccountDaoTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final short accountId = 101;
  private final Account testAccount;
  private final Container testContainer;
  private final AccountUpdateInfo testAccountInfo;
  private final AccountDao accountDao;
  private final MySqlMetrics metrics;
  private final PreparedStatement mockAccountInsertStatement;
  private final PreparedStatement mockAccountUpdateStatement;
  private final PreparedStatement mockAccountQueryStatement;
  private final PreparedStatement mockContainerInsertStatement;
  private final PreparedStatement mockContainerQueryStatement;
  private final PreparedStatement mockContainerUpdateStatement;

  public AccountDaoTest() throws Exception {
    long lastModifiedTime = SystemTime.getInstance().milliseconds();
    metrics = new MySqlMetrics(MySqlAccountStore.class, new MetricRegistry());
    testContainer =
        new ContainerBuilder((short) 1, "state-backup", Container.ContainerStatus.ACTIVE, "", accountId).build();
    testAccount =
        new AccountBuilder(accountId, "samza", Account.AccountStatus.ACTIVE).addOrUpdateContainer(testContainer)
            .lastModifiedTime(lastModifiedTime)
            .build();
    testAccountInfo = new AccountUpdateInfo(testAccount, true, false, new ArrayList<>(testAccount.getAllContainers()),
        new ArrayList<>());

    Connection mockConnection = mock(Connection.class);
    MySqlDataAccessor dataAccessor = getDataAccessor(mockConnection, metrics);
    accountDao = new AccountDao(dataAccessor, null);

    //Account mock statements
    String accountJson = new String(AccountCollectionSerde.serializeAccountsInJsonNoContainers(testAccount));
    mockAccountInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("insert into Accounts"))).thenReturn(mockAccountInsertStatement);
    when(mockAccountInsertStatement.executeBatch()).thenReturn(new int[]{1});
    mockAccountQueryStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("from Accounts"))).thenReturn(mockAccountQueryStatement);
    mockAccountUpdateStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("update Accounts"))).thenReturn(mockAccountUpdateStatement);
    when(mockAccountUpdateStatement.executeBatch()).thenReturn(new int[]{1});
    ResultSet mockAccountResultSet = mock(ResultSet.class);
    when(mockAccountResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockAccountResultSet.getString(eq(AccountDao.ACCOUNT_INFO))).thenReturn(accountJson);
    when(mockAccountResultSet.getTimestamp(eq(AccountDao.LAST_MODIFIED_TIME))).thenReturn(
        new Timestamp(lastModifiedTime));
    when(mockAccountQueryStatement.executeQuery()).thenReturn(mockAccountResultSet);

    // Container mock statements
    String containerJson = objectMapper.writeValueAsString(testContainer);
    mockContainerInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("insert into Containers"))).thenReturn(mockContainerInsertStatement);
    when(mockContainerInsertStatement.executeBatch()).thenReturn(new int[]{1});
    mockContainerQueryStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("from Containers"))).thenReturn(mockContainerQueryStatement);
    mockContainerUpdateStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("update Containers"))).thenReturn(mockContainerUpdateStatement);
    when(mockContainerUpdateStatement.executeBatch()).thenReturn(new int[]{1});
    ResultSet mockContainerResultSet = mock(ResultSet.class);
    when(mockContainerResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockContainerResultSet.getInt(eq(AccountDao.ACCOUNT_ID))).thenReturn((int) accountId);
    when(mockContainerResultSet.getString(eq(AccountDao.CONTAINER_INFO))).thenReturn(containerJson);
    when(mockContainerResultSet.getTimestamp(eq(AccountDao.LAST_MODIFIED_TIME))).thenReturn(
        new Timestamp(SystemTime.getInstance().milliseconds()));
    when(mockContainerQueryStatement.executeQuery()).thenReturn(mockContainerResultSet);
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
        new MySqlUtils.DbEndpoint("jdbc:mysql://localhost/AccountMetadata", "dc1", true, "ambry", "ambry");
    return new MySqlDataAccessor(Collections.singletonList(dbEndpoint), mockDriver, metrics);
  }

  @Test
  public void testAddAccount() throws Exception {
    int updates = 0;
    accountDao.updateAccounts(Collections.singletonList(testAccountInfo), 50);
    verify(mockAccountInsertStatement, times(++updates)).executeBatch();
    assertEquals("Write success count should be 1", updates, metrics.writeSuccessCount.getCount());
    // Run second time to reuse statement
    accountDao.updateAccounts(Collections.singletonList(testAccountInfo), 50);
    verify(mockAccountInsertStatement, times(++updates)).executeBatch();
    assertEquals("Write success count should be 2", updates, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testGetAccounts() throws Exception {
    List<Account> accountList = accountDao.getNewAccounts(0L);
    assertEquals(1, accountList.size());
    Account testAccountNoContainers = new AccountBuilder(testAccount).containers(null).build();
    assertEquals(testAccountNoContainers, accountList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testAddAccountWithException() throws Exception {
    when(mockAccountInsertStatement.executeBatch()).thenThrow(new BatchUpdateException());
    TestUtils.assertException(BatchUpdateException.class,
        () -> accountDao.updateAccounts(Collections.singletonList(testAccountInfo), 50), null);
  }

  @Test
  public void testGetAccountsWithException() throws Exception {
    when(mockAccountQueryStatement.executeQuery()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class, () -> accountDao.getNewAccounts(0L), null);
    assertEquals("Read failure count should be 1", 1, metrics.readFailureCount.getCount());
  }

  @Test
  public void testAddContainer() throws Exception {
    int updates = 0;
    accountDao.updateAccounts(Collections.singletonList(testAccountInfo), 50);
    verify(mockContainerInsertStatement, times(++updates)).executeBatch();
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testGetContainersForAccount() throws Exception {
    List<Container> containerList = accountDao.getContainers(accountId);
    assertEquals(1, containerList.size());
    assertEquals(testContainer, containerList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testGetNewContainers() throws Exception {
    List<Container> containerList = accountDao.getNewContainers(0);
    assertEquals(1, containerList.size());
    assertEquals(testContainer, containerList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testAddContainerWithException() throws Exception {
    when(mockContainerInsertStatement.executeBatch()).thenThrow(new BatchUpdateException());
    TestUtils.assertException(BatchUpdateException.class,
        () -> accountDao.updateAccounts(Collections.singletonList(testAccountInfo), 50), null);
  }

  @Test
  public void testGetNewContainersWithException() throws Exception {
    when(mockContainerQueryStatement.executeQuery()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class, () -> accountDao.getNewContainers(0L), null);
    assertEquals("Read failure count should be 1", 1, metrics.readFailureCount.getCount());
  }

  @Test
  public void testBatchOperations() throws SQLException {
    List<AccountUpdateInfo> accountUpdateInfos = new ArrayList<>();
    int size = 11;
    int batchSize = 5;

    // test batch account inserts
    for (int i = 1; i <= size; i++) {
      Account account = new AccountBuilder((short) i, "test account " + i, Account.AccountStatus.ACTIVE).build();
      accountUpdateInfos.add(new AccountUpdateInfo(account, true, false, new ArrayList<>(), new ArrayList<>()));
    }
    accountDao.updateAccounts(accountUpdateInfos, batchSize);
    verify(mockAccountInsertStatement, times(size)).addBatch();
    verify(mockAccountInsertStatement, times(size / batchSize + 1)).executeBatch();

    // test batch account updates
    accountUpdateInfos.clear();
    for (int i = 1; i <= size; i++) {
      Account account =
          new AccountBuilder((short) i, "test account " + i, Account.AccountStatus.ACTIVE).snapshotVersion(1).build();
      accountUpdateInfos.add(new AccountUpdateInfo(account, false, true, new ArrayList<>(), new ArrayList<>()));
    }
    accountDao.updateAccounts(accountUpdateInfos, batchSize);
    verify(mockAccountUpdateStatement, times(size)).addBatch();
    verify(mockAccountUpdateStatement, times(size / batchSize + 1)).executeBatch();

    Account account = new AccountBuilder((short) 1, "test account " + 1, Account.AccountStatus.ACTIVE).build();
    List<Container> containers = new ArrayList<>();
    for (int i = 1; i <= size; i++) {
      containers.add(new ContainerBuilder((short) i, "test container " + i, Container.ContainerStatus.ACTIVE, "",
          (short) 1).build());
    }
    // test batch container inserts
    accountUpdateInfos.clear();
    accountUpdateInfos.add(new AccountUpdateInfo(account, false, false, containers, new ArrayList<>()));
    accountDao.updateAccounts(accountUpdateInfos, batchSize);
    verify(mockContainerInsertStatement, times(size)).addBatch();
    // Execute batch should be invoked only once since all containers belong to same account
    verify(mockContainerInsertStatement, times(1)).executeBatch();

    // test batch container updates
    accountUpdateInfos.clear();
    accountUpdateInfos.add(new AccountUpdateInfo(account, false, false, new ArrayList<>(), containers));
    accountDao.updateAccounts(accountUpdateInfos, batchSize);
    verify(mockContainerUpdateStatement, times(size)).addBatch();
    // Execute batch should be invoked only once since all containers belong to same account
    verify(mockContainerUpdateStatement, times(1)).executeBatch();
  }
}
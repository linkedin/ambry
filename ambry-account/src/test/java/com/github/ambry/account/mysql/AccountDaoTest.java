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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Timestamp;
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

  private final short accountId = 101;
  private final String accountName = "samza";
  private final Account testAccount;
  private final String accountJson;
  private final MySqlDataAccessor dataAccessor;
  private final Connection mockConnection;
  private final AccountDao accountDao;
  private final MySqlAccountStoreMetrics metrics;
  private final PreparedStatement mockInsertStatement;
  private final PreparedStatement mockQueryStatement;

  public AccountDaoTest() throws SQLException {
    metrics = new MySqlAccountStoreMetrics(new MetricRegistry());
    testAccount = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).build();
    accountJson = AccountCollectionSerde.accountToJsonNoContainers(testAccount).toString();
    mockConnection = mock(Connection.class);
    mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("insert into"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);
    mockQueryStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(startsWith("select"))).thenReturn(mockQueryStatement);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString(eq(AccountDao.ACCOUNT_INFO))).thenReturn(accountJson);
    when(mockResultSet.getTimestamp(eq(AccountDao.LAST_MODIFIED_TIME))).thenReturn(
        new Timestamp(SystemTime.getInstance().milliseconds()));
    when(mockQueryStatement.executeQuery()).thenReturn(mockResultSet);
    dataAccessor = getDataAccessor(mockConnection, metrics);
    accountDao = new AccountDao(dataAccessor);
  }

  /**
   * Utility to get a {@link MySqlDataAccessor}.
   * @param mockConnection the connection to use.
   * @return the {@link MySqlDataAccessor}.
   * @throws SQLException
   */
  static MySqlDataAccessor getDataAccessor(Connection mockConnection, MySqlAccountStoreMetrics metrics)
      throws SQLException {
    Driver mockDriver = mock(Driver.class);
    when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint("jdbc:mysql://localhost/AccountMetadata", "dc1", true, "ambry", "ambry");
    return new MySqlDataAccessor(Collections.singletonList(dbEndpoint), mockDriver, metrics);
  }

  @Test
  public void testAddAccount() throws Exception {
    accountDao.addAccount(testAccount);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
    // Run second time to reuse statement
    accountDao.addAccount(testAccount);
    verify(mockConnection).prepareStatement(anyString());
    assertEquals("Write success count should be 2", 2, metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testGetAccounts() throws Exception {
    List<Account> accountList = accountDao.getNewAccounts(0L);
    assertEquals(1, accountList.size());
    assertEquals(testAccount, accountList.get(0));
    assertEquals("Read success count should be 1", 1, metrics.readSuccessCount.getCount());
  }

  @Test
  public void testAddAccountWithException() throws Exception {
    when(mockInsertStatement.executeUpdate()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class, () -> accountDao.addAccount(testAccount), null);
    assertEquals("Write failure count should be 1", 1, metrics.writeFailureCount.getCount());
  }

  @Test
  public void testGetAccountsWithException() throws Exception {
    when(mockQueryStatement.executeQuery()).thenThrow(new SQLTransientConnectionException());
    TestUtils.assertException(SQLTransientConnectionException.class, () -> accountDao.getNewAccounts(0L), null);
    assertEquals("Read failure count should be 1", 1, metrics.readFailureCount.getCount());
  }
}

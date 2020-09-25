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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountSerdeUtils;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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

  public AccountDaoTest() throws SQLException {
    testAccount = new AccountBuilder(accountId, accountName, Account.AccountStatus.ACTIVE).build();
    accountJson = AccountSerdeUtils.accountToJson(testAccount, true);
    mockConnection = mock(Connection.class);
    PreparedStatement mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("insert into"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);
    PreparedStatement mockQueryStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(startsWith("select"))).thenReturn(mockQueryStatement);
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString(eq(AccountDao.ACCOUNT_INFO))).thenReturn(accountJson);
    when(mockResultSet.getTimestamp(eq(AccountDao.LAST_MODIFIED_TIME))).thenReturn(
        new Timestamp(SystemTime.getInstance().milliseconds()));
    when(mockQueryStatement.executeQuery()).thenReturn(mockResultSet);
    dataAccessor = getDataAccessor(mockConnection);
    accountDao = new AccountDao(dataAccessor);
  }

  /**
   * Utility to get a {@link MySqlDataAccessor}.
   * @param mockConnection the connection to use.
   * @return the {@link MySqlDataAccessor}.
   * @throws SQLException
   */
  static MySqlDataAccessor getDataAccessor(Connection mockConnection) throws SQLException {
    Driver mockDriver = mock(Driver.class);
    when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
    Properties properties = new Properties();
    properties.setProperty(MySqlAccountServiceConfig.DB_URL, "jdbc:mysql://localhost/AccountMetadata");
    properties.setProperty(MySqlAccountServiceConfig.DB_USER, "ambry");
    properties.setProperty(MySqlAccountServiceConfig.DB_PASSWORD, "ambry");
    MySqlAccountServiceConfig config = new MySqlAccountServiceConfig(new VerifiableProperties(properties));
    MySqlDataAccessor dataAccessor = new MySqlDataAccessor(config, mockDriver);
    when(dataAccessor.getDatabaseConnection()).thenReturn(mockConnection);
    return dataAccessor;
  }

  @Test
  public void testAddAccount() throws Exception {
    accountDao.addAccount(testAccount);
    verify(mockConnection).prepareStatement(anyString());
    // Run second time to reuse statement
    accountDao.addAccount(testAccount);
    verify(mockConnection).prepareStatement(anyString());
  }

  @Test
  public void testGetAccounts() throws Exception {
    List<Account> accountList = accountDao.getNewAccounts(0L);
    assertEquals(1, accountList.size());
    assertEquals(testAccount, accountList.get(0));
  }
}

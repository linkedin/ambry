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
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.Container;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;


/**
 * Wrapper class to handle MySql store operations on Account and Container tables
 */
public class MySqlAccountStore {

  private final AccountDao accountDao;
  private final MySqlDataAccessor mySqlDataAccessor;
  private final MySqlAccountServiceConfig config;

  /**
   * Constructor.
   * @param dbEndpoints MySql DB end points
   * @param localDatacenter name of the local data center
   * @param metrics metrics to track mysql operations
   * @param config configuration associated with {@link com.github.ambry.account.MySqlAccountService}
   * @throws SQLException
   */
  public MySqlAccountStore(List<MySqlUtils.DbEndpoint> dbEndpoints, String localDatacenter, MySqlMetrics metrics,
      MySqlAccountServiceConfig config) throws SQLException {
    mySqlDataAccessor = new MySqlDataAccessor(dbEndpoints, localDatacenter, metrics);
    accountDao = new AccountDao(mySqlDataAccessor);
    this.config = config;
  }

  /**
   * Used for tests.
   * @return {@link MySqlDataAccessor}
   */
  public MySqlDataAccessor getMySqlDataAccessor() {
    return mySqlDataAccessor;
  }

  /**
   * Adds/Updates accounts and their containers to the database in batches atomically using transaction.
   * @param accountsInfo information of updated Accounts
   * @throws SQLException
   */
  public void updateAccounts(List<AccountUpdateInfo> accountsInfo) throws SQLException {
    accountDao.updateAccounts(accountsInfo, config.dbExecuteBatchSize);
  }

  /**
   * Gets all {@link Account}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Account}s
   * @throws SQLException
   */
  public Collection<Account> getNewAccounts(long updatedSince) throws SQLException {
    return accountDao.getNewAccounts(updatedSince);
  }

  /**
   * Gets all {@link Container}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public Collection<Container> getNewContainers(long updatedSince) throws SQLException {
    return accountDao.getNewContainers(updatedSince);
  }

  /**
   * Gets all {@link Container}s of a given account
   * @param accountId ID of the account
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public Collection<Container> getContainersByAccount(short accountId) throws SQLException {
    return accountDao.getContainers(accountId);
  }

  /**
   * Helper method to close the active connection, if there is one.
   */
  public void closeConnection() {
    if (mySqlDataAccessor != null) {
      mySqlDataAccessor.closeActiveConnection();
    }
  }
}

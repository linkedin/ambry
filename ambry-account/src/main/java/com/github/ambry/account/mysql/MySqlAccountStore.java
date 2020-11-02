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
import com.github.ambry.account.Container;
import java.sql.SQLException;
import java.util.Collection;


/**
 * Wrapper class to handle MySql store operations on Account and Container tables
 */
public class MySqlAccountStore {

  private final AccountDao accountDao;
  private final ContainerDao containerDao;
  private final MySqlDataAccessor mySqlDataAccessor;

  /**
   * Constructor.
   * @param dbEndpoint MySql DB end point
   * @param metrics metrics to track mysql operations
   * @throws SQLException
   */
  public MySqlAccountStore(MySqlUtils.DbEndpoint dbEndpoint, MySqlAccountStoreMetrics metrics) throws SQLException {
    mySqlDataAccessor = new MySqlDataAccessor(dbEndpoint, metrics);
    accountDao = new AccountDao(mySqlDataAccessor);
    containerDao = new ContainerDao(mySqlDataAccessor);
  }

  /**
   * Used for tests.
   * @return {@link MySqlDataAccessor}
   */
  public MySqlDataAccessor getMySqlDataAccessor() {
    return mySqlDataAccessor;
  }

  /**
   * Adds new {@link Account}s to Account table in MySql DB
   * @param accounts collection of {@link Account}s to be inserted
   * @throws SQLException
   */
  public void addAccounts(Collection<Account> accounts) throws SQLException {
    for (Account account : accounts) {
      addAccount(account);
    }
  }

  /**
   * Adds new {@link Account} to Account table in MySql DB
   * @param account {@link Account} to be inserted
   * @throws SQLException
   */
  public void addAccount(Account account) throws SQLException {
    accountDao.addAccount(account);
  }

  /**
   * Adds new {@link Container}s to Container table in MySql DB
   * @param containers collection of {@link Container}s to be inserted
   * @throws SQLException
   */
  public void addContainers(Collection<Container> containers) throws SQLException {
    for (Container container : containers) {
      addContainer(container);
    }
  }

  /**
   * Adds new {@link Container} to Container table in MySql DB
   * @param container {@link Container} to be inserted
   * @throws SQLException
   */
  public void addContainer(Container container) throws SQLException {
    containerDao.addContainer(container.getParentAccountId(), container);
  }

  /**
   * Updates existing {@link Account}s in Account table in MySql DB
   * @param accounts collection of {@link Account}s to be updated
   * @throws SQLException
   */
  public void updateAccounts(Collection<Account> accounts) throws SQLException {
    for (Account account : accounts) {
      accountDao.updateAccount(account);
    }
  }

  /**
   * Updates existing {@link Account} in Account table in MySql DB
   * @param account {@link Account} to be updated
   * @throws SQLException
   */
  public void updateAccount(Account account) throws SQLException {
    accountDao.updateAccount(account);
  }

  /**
   * Updates existing {@link Container}s in Container table in MySql DB
   * @param containers collection of {@link Container}s to be updated
   * @throws SQLException
   */
  public void updateContainers(Collection<Container> containers) throws SQLException {
    for (Container container : containers) {
      containerDao.updateContainer(container.getParentAccountId(), container);
    }
  }

  /**
   * Updates existing {@link Container} in Container table in MySql DB
   * @param container {@link Container} to be updated
   * @throws SQLException
   */
  public void updateContainer(Container container) throws SQLException {
    containerDao.updateContainer(container.getParentAccountId(), container);
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
    return containerDao.getNewContainers(updatedSince);
  }

  /**
   * Gets all {@link Container}s of a given account
   * @param accountId ID of the account
   * @return a collection of {@link Container}s
   * @throws SQLException
   */
  public Collection<Container> getContainersByAccount(short accountId) throws SQLException {
    return containerDao.getContainers(accountId);
  }
}

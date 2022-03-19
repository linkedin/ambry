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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountUtils.AccountUpdateInfo;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.mysql.MySqlDataAccessor;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;
import static com.github.ambry.utils.Utils.*;


/**
 * Account Data Access Object.
 */
public class AccountDao {

  private final MySqlDataAccessor dataAccessor;
  private final ObjectMapper objectMapper = new ObjectMapper();

  // Account table fields
  public static final String ACCOUNT_TABLE = "Accounts";
  public static final String ACCOUNT_INFO = "accountInfo";
  public static final String ACCOUNT_ID = "accountId";

  // Container table fields
  public static final String CONTAINER_TABLE = "Containers";
  public static final String CONTAINER_ID = "containerId";
  public static final String CONTAINER_NAME = "containerName";
  public static final String CONTAINER_INFO = "containerInfo";

  // Common fields
  public static final String VERSION = "version";
  public static final String CREATION_TIME = "creationTime";
  public static final String LAST_MODIFIED_TIME = "lastModifiedTime";

  // Account table query strings
  private final String insertAccountsSql;
  private final String getAccountsSinceSql;
  private final String updateAccountsSql;

  // Container table query strings
  private final String insertContainersSql;
  private final String updateContainersSql;
  private final String getContainersSinceSql;
  private final String getContainersByAccountSql;
  private final String getContainerByNameSql;
  private final String getContainerByIdSql;

  /**
   * Types of MySql statements.
   */
  public enum StatementType {
    Select, Insert, Update, Delete
  }

  public AccountDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = dataAccessor;
    insertAccountsSql =
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(3), now(3))", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getAccountsSinceSql =
        String.format("select %s, %s, %s from %s where %s > ?", ACCOUNT_INFO, VERSION, LAST_MODIFIED_TIME,
            ACCOUNT_TABLE, LAST_MODIFIED_TIME);
    updateAccountsSql =
        String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? ", ACCOUNT_TABLE, ACCOUNT_INFO, VERSION,
            LAST_MODIFIED_TIME, ACCOUNT_ID);
    insertContainersSql =
        String.format("insert into %s (%s, %s, %s, %s, %s) values (?, ?, ?, now(3), now(3))", CONTAINER_TABLE,
            ACCOUNT_ID, CONTAINER_INFO, VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getContainersSinceSql =
        String.format("select %s, %s, %s, %s from %s where %s > ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, LAST_MODIFIED_TIME);
    getContainersByAccountSql =
        String.format("select %s, %s, %s, %s from %s where %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID);
    updateContainersSql =
        String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? AND %s = ? ", CONTAINER_TABLE,
            CONTAINER_INFO, VERSION, LAST_MODIFIED_TIME, ACCOUNT_ID, CONTAINER_ID);
    getContainerByNameSql =
        String.format("select %s, %s, %s, %s from %s where %s = ? and %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID, CONTAINER_NAME);
    getContainerByIdSql =
        String.format("select %s, %s, %s, %s from %s where %s = ? and %s = ?", ACCOUNT_ID, CONTAINER_INFO, VERSION,
            LAST_MODIFIED_TIME, CONTAINER_TABLE, ACCOUNT_ID, CONTAINER_ID);
  }

  /**
   * Gets all accounts that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  public synchronized List<Account> getNewAccounts(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    ResultSet rs = null;
    try {
      PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getAccountsSinceSql, false);
      getSinceStatement.setTimestamp(1, sinceTime);
      rs = getSinceStatement.executeQuery();
      List<Account> accounts = convertAccountsResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return accounts;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Convert a query result set to a list of accounts.
   * @param resultSet the result set.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  private List<Account> convertAccountsResultSet(ResultSet resultSet) throws SQLException {
    List<Account> accounts = new ArrayList<>();
    while (resultSet.next()) {
      String accountJson = resultSet.getString(ACCOUNT_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      try {
        Account account = new AccountBuilder(objectMapper.readValue(accountJson, Account.class)).lastModifiedTime(
            lastModifiedTime.getTime()).snapshotVersion(version).build();
        accounts.add(account);
      } catch (IOException e) {
        throw new SQLException(String.format("Faild to deserialize string [{}] to account object", accountJson), e);
      }
    }
    return accounts;
  }

  /**
   * Gets the containers in a specified account.
   * @param accountId the id for the parent account.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public synchronized List<Container> getContainers(int accountId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getByAccountStatement = dataAccessor.getPreparedStatement(getContainersByAccountSql, false);
      getByAccountStatement.setInt(1, accountId);
      rs = getByAccountStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets all containers that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  public synchronized List<Container> getNewContainers(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    ResultSet rs = null;
    try {
      PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getContainersSinceSql, false);
      getSinceStatement.setTimestamp(1, sinceTime);
      rs = getSinceStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets container by its name and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerName name of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerByName(int accountId, String containerName) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getContainerByNameStatement = dataAccessor.getPreparedStatement(getContainerByNameSql, false);
      getContainerByNameStatement.setInt(1, accountId);
      getContainerByNameStatement.setString(2, containerName);
      rs = getContainerByNameStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers.isEmpty() ? null : containers.get(0);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Gets container by its Id and parent account Id.
   * @param accountId the id for the parent account.
   * @param containerId the id of the container.
   * @return {@link Container} if found in mysql db or {@code null} if it doesn't exist.
   * @throws SQLException
   */
  public synchronized Container getContainerById(int accountId, int containerId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    ResultSet rs = null;
    try {
      PreparedStatement getContainerByIdStatement = dataAccessor.getPreparedStatement(getContainerByIdSql, false);
      getContainerByIdStatement.setInt(1, accountId);
      getContainerByIdStatement.setInt(2, containerId);
      rs = getContainerByIdStatement.executeQuery();
      List<Container> containers = convertContainersResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return containers.isEmpty() ? null : containers.get(0);
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    } finally {
      closeQuietly(rs);
    }
  }

  /**
   * Convert a query result set to a list of containers.
   * @param resultSet the result set.
   * @return a list of {@link Container}s.
   * @throws SQLException
   */
  private List<Container> convertContainersResultSet(ResultSet resultSet) throws SQLException {
    List<Container> containers = new ArrayList<>();
    while (resultSet.next()) {
      int accountId = resultSet.getInt(ACCOUNT_ID);
      String containerJson = resultSet.getString(CONTAINER_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      try {
        Container deserialized = objectMapper.readValue(containerJson, Container.class);
        Container container = new ContainerBuilder(deserialized).setParentAccountId((short) accountId)
            .setLastModifiedTime(lastModifiedTime.getTime())
            .setSnapshotVersion(version)
            .build();
        containers.add(container);
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
    return containers;
  }

  /**
   * Adds/Updates accounts and their containers to the database in batches atomically using transaction.
   * @param accountsInfo information of updated Accounts
   * @param batchSize number of statements to be executed in one batch
   * @throws SQLException
   */
  public synchronized void updateAccounts(List<AccountUpdateInfo> accountsInfo, int batchSize) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();

      AccountUpdateBatch accountUpdateBatch =
          new AccountUpdateBatch(dataAccessor.getPreparedStatement(insertAccountsSql, true),
              dataAccessor.getPreparedStatement(updateAccountsSql, true),
              dataAccessor.getPreparedStatement(insertContainersSql, true),
              dataAccessor.getPreparedStatement(updateContainersSql, true));

      // Disable auto commits
      dataAccessor.setAutoCommit(false);

      int batchCount = 0;
      for (AccountUpdateInfo accountUpdateInfo : accountsInfo) {
        // Get account and container changes information
        Account account = accountUpdateInfo.getAccount();
        boolean isAccountAdded = accountUpdateInfo.isAdded();
        boolean isAccountUpdated = accountUpdateInfo.isUpdated();
        List<Container> addedContainers = accountUpdateInfo.getAddedContainers();
        List<Container> updatedContainers = accountUpdateInfo.getUpdatedContainers();

        // Number of changes in the account.
        int accountUpdateCount =
            (isAccountAdded ? 1 : 0) + (isAccountUpdated ? 1 : 0) + addedContainers.size() + updatedContainers.size();

        // Commit transaction with previous batch inserts/updates if it either of following is true.
        // a) Total batch count of previous #accounts/containers is equal to or greater than configured batch size.
        //    Note: It is possible for count to be greater than configured batch size when number of containers in
        //    previous account exceeds the configured batch size. We allow it to ensure an account is committed atomically.
        // b) Adding account and its containers in current iteration to total batch count > configured batch size
        if (batchCount >= batchSize || (batchCount > 0 && batchCount + accountUpdateCount > batchSize)) {
          accountUpdateBatch.maybeExecuteBatch();
          dataAccessor.commit();
          batchCount = 0;
        }

        // Add account to insert/update batch if it was either added or modified.
        if (isAccountAdded) {
          accountUpdateBatch.addAccount(account);
        } else if (isAccountUpdated) {
          accountUpdateBatch.updateAccount(account);
        }
        // Add new containers for batch inserts
        for (Container container : addedContainers) {
          accountUpdateBatch.addContainer(account.getId(), container);
        }
        // Add updated containers for batch updates
        for (Container container : updatedContainers) {
          accountUpdateBatch.updateContainer(account.getId(), container);
        }

        batchCount += accountUpdateCount;
      }

      // Commit transaction with pending batch inserts/updates
      if (batchCount > 0) {
        accountUpdateBatch.maybeExecuteBatch();
        dataAccessor.commit();
      }

      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      //rollback the current transaction.
      dataAccessor.rollback();
      dataAccessor.onException(e, Write);
      throw e;
    } finally {
      // Close the connection to ensure subsequent queries are made in a new transaction and return the latest data
      dataAccessor.closeActiveConnection();
    }
  }

  /**
   * Binds parameters to prepare statements for {@link Account} inserts/updates
   * @param statement {@link PreparedStatement} for mysql queries
   * @param account {@link Account} being added to mysql
   * @param statementType {@link StatementType} of mysql query such as insert or update.
   * @throws SQLException
   */
  private void bindAccount(PreparedStatement statement, Account account, StatementType statementType)
      throws SQLException {
    String accountInJson;
    try {
      accountInJson = new String(AccountCollectionSerde.serializeAccountsInJsonNoContainers(account));
    } catch (IOException e) {
      throw new SQLException("Fail to serialize account: " + account.toString(), e);
    }
    switch (statementType) {
      case Insert:
        statement.setString(1, accountInJson);
        statement.setInt(2, account.getSnapshotVersion());
        break;
      case Update:
        statement.setString(1, accountInJson);
        statement.setInt(2, (account.getSnapshotVersion() + 1));
        statement.setInt(3, account.getId());
        break;
    }
  }

  /**
   * Binds parameters to prepare statements for {@link Container} inserts/updates
   * @param statement {@link PreparedStatement} for mysql queries
   * @param accountId Id of {@link Account} whose {@link Container} is being added to mysql
   * @param container {@link Container} being added to mysql
   * @param statementType {@link StatementType} of mysql query such as insert or update.
   * @throws SQLException
   */
  private void bindContainer(PreparedStatement statement, int accountId, Container container,
      StatementType statementType) throws SQLException {
    String containerInJson;
    try {
      containerInJson = objectMapper.writeValueAsString(container);
    } catch (IOException e) {
      throw new SQLException("Fail to serialize container: " + container.toString(), e);
    }
    switch (statementType) {
      case Insert:
        statement.setInt(1, accountId);
        statement.setString(2, containerInJson);
        statement.setInt(3, container.getSnapshotVersion());
        break;
      case Update:
        statement.setString(1, containerInJson);
        statement.setInt(2, (container.getSnapshotVersion() + 1));
        statement.setInt(3, accountId);
        statement.setInt(4, container.getId());
    }
  }

  /**
   * Helper class to do batch inserts and updates of {@link Account}s and {@link Container}s.
   */
  class AccountUpdateBatch {

    private int insertAccountCount = 0;
    private int updateAccountCount = 0;
    private int insertContainerCount = 0;
    private int updateContainerCount = 0;
    private final PreparedStatement insertAccountStatement;
    private final PreparedStatement updateAccountStatement;
    private final PreparedStatement insertContainerStatement;
    private final PreparedStatement updateContainerStatement;

    public AccountUpdateBatch(PreparedStatement insertAccountStatement, PreparedStatement updateAccountStatement,
        PreparedStatement insertContainerStatement, PreparedStatement updateContainerStatement) {
      this.insertAccountStatement = insertAccountStatement;
      this.updateAccountStatement = updateAccountStatement;
      this.insertContainerStatement = insertContainerStatement;
      this.updateContainerStatement = updateContainerStatement;
    }

    /**
     * Executes batch inserts and updates of {@link Account}s and {@link Container}s.
     * @throws SQLException
     */
    public void maybeExecuteBatch() throws SQLException {
      if (insertAccountStatement != null && insertAccountCount > 0) {
        insertAccountStatement.executeBatch();
        insertAccountCount = 0;
      }
      if (updateAccountStatement != null && updateAccountCount > 0) {
        updateAccountStatement.executeBatch();
        updateAccountCount = 0;
      }
      if (insertContainerStatement != null && insertContainerCount > 0) {
        insertContainerStatement.executeBatch();
        insertContainerCount = 0;
      }
      if (updateContainerStatement != null && updateContainerCount > 0) {
        updateContainerStatement.executeBatch();
        updateContainerCount = 0;
      }
    }

    /**
     * Adds {@link Account} to its insert {@link PreparedStatement}'s batch.
     * @param account {@link Account} to be inserted.
     * @throws SQLException
     */
    public void addAccount(Account account) throws SQLException {
      bindAccount(insertAccountStatement, account, StatementType.Insert);
      insertAccountStatement.addBatch();
      ++insertAccountCount;
    }

    /**
     * Adds {@link Account} to its update {@link PreparedStatement}'s batch.
     * @param account {@link Account} to be updated.
     * @throws SQLException
     */
    public void updateAccount(Account account) throws SQLException {
      bindAccount(updateAccountStatement, account, StatementType.Update);
      updateAccountStatement.addBatch();
      ++updateAccountCount;
    }

    /**
     * Adds {@link Container} to its insert {@link PreparedStatement}'s batch.
     * @param accountId account id of the Container.
     * @param container {@link Container} to be inserted.
     * @throws SQLException
     */
    public void addContainer(int accountId, Container container) throws SQLException {
      bindContainer(insertContainerStatement, accountId, container, StatementType.Insert);
      insertContainerStatement.addBatch();
      ++insertContainerCount;
    }

    /**
     * Adds {@link Container} to its update {@link PreparedStatement}'s batch.
     * @param accountId account id of the Container.
     * @param container {@link Container} to be updated.
     * @throws SQLException
     */
    public void updateContainer(int accountId, Container container) throws SQLException {
      bindContainer(updateContainerStatement, accountId, container, StatementType.Update);
      updateContainerStatement.addBatch();
      ++updateContainerCount;
    }
  }
}
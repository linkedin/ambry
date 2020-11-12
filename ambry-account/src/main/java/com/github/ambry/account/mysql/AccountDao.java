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
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.mysql.MySqlDataAccessor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

import static com.github.ambry.mysql.MySqlDataAccessor.OperationType.*;


/**
 * Account Data Access Object.
 */
public class AccountDao {

  public static final String ACCOUNT_TABLE = "Accounts";
  public static final String ACCOUNT_INFO = "accountInfo";
  public static final String VERSION = "version";
  public static final String CREATION_TIME = "creationTime";
  public static final String LAST_MODIFIED_TIME = "lastModifiedTime";
  public static final String ACCOUNT_ID = "accountId";

  private final MySqlDataAccessor dataAccessor;
  private final String insertSql;
  private final String getSinceSql;
  private final String updateSql;

  public AccountDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = dataAccessor;
    insertSql =
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(3), now(3))", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getSinceSql = String.format("select %s, %s, %s from %s where %s > ?", ACCOUNT_INFO, VERSION, LAST_MODIFIED_TIME,
        ACCOUNT_TABLE, LAST_MODIFIED_TIME);
    updateSql =
        String.format("update %s set %s = ?, %s = ?, %s = now(3) where %s = ? ", ACCOUNT_TABLE, ACCOUNT_INFO, VERSION,
            LAST_MODIFIED_TIME, ACCOUNT_ID);
  }

  /**
   * Add an account to the database.
   * @param account the account to insert.
   * @throws SQLException
   */
  public void addAccount(Account account) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql, true);
      insertStatement.setString(1, AccountCollectionSerde.accountToJsonNoContainers(account).toString());
      insertStatement.setInt(2, account.getSnapshotVersion());
      insertStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Gets all accounts that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  public List<Account> getNewAccounts(long updatedSince) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Timestamp sinceTime = new Timestamp(updatedSince);
    PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getSinceSql, false);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      List<Account> accounts = convertResultSet(rs);
      dataAccessor.onSuccess(Read, System.currentTimeMillis() - startTimeMs);
      return accounts;
    } catch (SQLException e) {
      dataAccessor.onException(e, Read);
      throw e;
    }
  }

  /**
   * Updates an existing account in the database.
   * @param account the account to update.
   * @throws SQLException
   */
  public void updateAccount(Account account) throws SQLException {
    try {
      long startTimeMs = System.currentTimeMillis();
      PreparedStatement updateStatement = dataAccessor.getPreparedStatement(updateSql, true);
      updateStatement.setString(1, AccountCollectionSerde.accountToJsonNoContainers(account).toString());
      updateStatement.setInt(2, account.getSnapshotVersion());
      updateStatement.setInt(3, account.getId());
      updateStatement.executeUpdate();
      dataAccessor.onSuccess(Write, System.currentTimeMillis() - startTimeMs);
    } catch (SQLException e) {
      dataAccessor.onException(e, Write);
      throw e;
    }
  }

  /**
   * Convert a query result set to a list of accounts.
   * @param resultSet the result set.
   * @return a list of {@link Account}s.
   * @throws SQLException
   */
  private List<Account> convertResultSet(ResultSet resultSet) throws SQLException {
    List<Account> accounts = new ArrayList<>();
    while (resultSet.next()) {
      String accountJson = resultSet.getString(ACCOUNT_INFO);
      Timestamp lastModifiedTime = resultSet.getTimestamp(LAST_MODIFIED_TIME);
      int version = resultSet.getInt(VERSION);
      Account account =
          new AccountBuilder(Account.fromJson(new JSONObject(accountJson))).lastModifiedTime(lastModifiedTime.getTime())
              .snapshotVersion(version)
              .build();
      accounts.add(account);
    }
    return accounts;
  }
}

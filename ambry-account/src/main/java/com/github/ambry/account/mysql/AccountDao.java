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
import com.github.ambry.account.AccountSerdeUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


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
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(), now())", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getSinceSql = String.format("select %s, %s from %s where %s > ?", ACCOUNT_INFO, LAST_MODIFIED_TIME, ACCOUNT_TABLE,
        LAST_MODIFIED_TIME);
    updateSql =
        String.format("update %s set %s = ?, %s = ?, %s = now() where %s = ? ", ACCOUNT_TABLE, ACCOUNT_INFO, VERSION,
            LAST_MODIFIED_TIME, ACCOUNT_ID);
  }

  /**
   * Add an account to the database.
   * @param account the account to insert.
   * @throws SQLException
   */
  public void addAccount(Account account) throws SQLException {
    try {
      PreparedStatement insertStatement = dataAccessor.getPreparedStatement(insertSql);
      insertStatement.setString(1, AccountSerdeUtils.accountToJson(account, true));
      insertStatement.setInt(2, account.getSnapshotVersion());
      insertStatement.executeUpdate();
    } catch (SQLException e) {
      // TODO: record failure, parse exception to figure out what we did wrong (eg. id or name collision)
      // For now, assume connection issue.
      dataAccessor.reset();
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
    Timestamp sinceTime = new Timestamp(updatedSince);
    PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getSinceSql);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      return convertResultSet(rs);
    } catch (SQLException e) {
      // TODO: record failure, parse exception to figure out what we did wrong (eg. id or name collision)
      // For now, assume connection issue.
      dataAccessor.reset();
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
      PreparedStatement updateStatement = dataAccessor.getPreparedStatement(updateSql);
      updateStatement.setString(1, AccountSerdeUtils.accountToJson(account, true));
      updateStatement.setInt(2, account.getSnapshotVersion());
      updateStatement.setInt(3, account.getId());
      updateStatement.executeUpdate();
    } catch (SQLException e) {
      // TODO: record failure, parse exception to figure out what we did wrong (eg. id or name collision)
      // For now, assume connection issue.
      dataAccessor.reset();
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
      Account account = AccountSerdeUtils.accountFromJson(accountJson);
      //account.setLastModifiedTime(lastModifiedTime);
      accounts.add(account);
    }
    return accounts;
  }
}

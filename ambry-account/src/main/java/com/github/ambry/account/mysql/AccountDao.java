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
import com.github.ambry.account.Container;
import java.sql.Connection;
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

  private final MySqlDataAccessor dataAccessor;
  private final String insertSql;
  private final String getSinceSql;

  public AccountDao(MySqlDataAccessor dataAccessor) {
    this.dataAccessor = dataAccessor;
    insertSql =
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(), now())", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    getSinceSql =
        String.format("select %s, %s from %s where %s > ?", ACCOUNT_INFO, LAST_MODIFIED_TIME, ACCOUNT_TABLE,
            LAST_MODIFIED_TIME);
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
   * @return a list of {@link Account}.
   * @throws SQLException
   */
  public List<Account> getNewAccounts(long updatedSince) throws SQLException {
    List<Account> accounts = new ArrayList<>();
    Timestamp sinceTime = new Timestamp(updatedSince);
    PreparedStatement getSinceStatement = dataAccessor.getPreparedStatement(getSinceSql);
    getSinceStatement.setTimestamp(1, sinceTime);
    try (ResultSet rs = getSinceStatement.executeQuery()) {
      while (rs.next()) {
        String accountJson = rs.getString(ACCOUNT_INFO);
        Timestamp lastModifiedTime = rs.getTimestamp(LAST_MODIFIED_TIME);
        Account account = AccountSerdeUtils.accountFromJson(accountJson);
        //account.setLastModifiedTime(lastModifiedTime);
        accounts.add(account);
      }
      return accounts;
    } catch (SQLException e) {
      // record failure, parse exception, ...
      dataAccessor.reset();
      throw e;
    }
  }
}

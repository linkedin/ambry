package com.github.ambry.account.mysql;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountSerdeUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


public class AccountTable {

  public static final String ACCOUNT_TABLE = "AccountMetadata";
  public static final String ACCOUNT_INFO = "ACCOUNT_INFO";
  public static final String VERSION = "VERSION";
  public static final String CREATION_TIME = "CREATION_TIME";
  public static final String LAST_MODIFIED_TIME = "LAST_MODIFIED_TIME";

  private final MySqlDataAccessor dataAccessor;
  private final Connection dbConnection;
  private final PreparedStatement insertStatement;
  private final PreparedStatement getSinceStatement;

  public AccountTable(MySqlDataAccessor dataAccessor) throws SQLException {
    this.dataAccessor = dataAccessor;
    this.dbConnection = dataAccessor.getDatabaseConnection();

    String insertSql =
        String.format("insert into %s (%s, %s, %s, %s) values (?, ?, now(), now())", ACCOUNT_TABLE, ACCOUNT_INFO,
            VERSION, CREATION_TIME, LAST_MODIFIED_TIME);
    insertStatement = dbConnection.prepareStatement(insertSql);

    String getSinceSql =
        String.format("select %s, %s from %s where %s > ?", ACCOUNT_INFO, LAST_MODIFIED_TIME, ACCOUNT_TABLE,
            LAST_MODIFIED_TIME);
    getSinceStatement = dbConnection.prepareStatement(getSinceSql);
  }

  public void addAccount(Account account) throws SQLException {
    try {
      insertStatement.setString(1, AccountSerdeUtils.accountToJson(account).toString());
      insertStatement.setInt(2, account.getSnapshotVersion());
      insertStatement.executeUpdate();
    } catch (SQLException e) {
      // record failure, parse exception to figure out what we did wrong (eg. id or name collision)
      throw e;
    }
  }

  public List<Account> getNewAccounts(long updatedSince) throws SQLException {
    try {
      List<Account> accounts = new ArrayList<>();
      Timestamp sinceTime = new Timestamp(updatedSince);
      getSinceStatement.setTimestamp(1, sinceTime);
      ResultSet rs = getSinceStatement.executeQuery();
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
      throw e;
    }
  }
}

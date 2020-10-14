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

import com.github.ambry.account.AccountServiceErrorCode;
import com.github.ambry.account.AccountServiceException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data Accessor to connect to MySql database.
 */
public class MySqlDataAccessor {

  private static final Logger logger = LoggerFactory.getLogger(MySqlDataAccessor.class);
  private static final String INDEX_ACCOUNT_CONTAINER = "containers.accountContainer";
  private static final String INDEX_CONTAINER_NAME = "containers.uniqueName";
  private final String mysqlUrl;
  private final String mysqlUser;
  private final String mysqlPassword;
  private final Driver mysqlDriver;
  private Connection activeConnection;
  private final Map<String, PreparedStatement> statementCache = new HashMap<>();

  /** Production constructor */
  public MySqlDataAccessor(MySqlUtils.DbEndpoint dbEndpoint) throws SQLException {
    mysqlUrl = dbEndpoint.getUrl();
    mysqlUser = dbEndpoint.getUsername();
    mysqlPassword = dbEndpoint.getPassword();
    // Initialize driver
    mysqlDriver = DriverManager.getDriver(mysqlUrl);
    // AccountService needs to work if mysql is down.  Mysql can also reboot.
    try {
      getDatabaseConnection();
    } catch (SQLException e) {
      if (isCredentialError(e)) {
        throw e;
      } else {
        // try again later
      }
    }
  }

  /** Test constructor */
  public MySqlDataAccessor(MySqlUtils.DbEndpoint dbEndpoint, Driver mysqlDriver) {
    mysqlUrl = dbEndpoint.getUrl();
    mysqlUser = dbEndpoint.getUsername();
    mysqlPassword = dbEndpoint.getPassword();
    this.mysqlDriver = mysqlDriver;
  }

  /**
   * @return a JDBC {@link Connection} to the database.  An existing connection will be reused.
   * @throws SQLException
   */
  public synchronized Connection getDatabaseConnection() throws SQLException {
    if (activeConnection != null && activeConnection.isValid(5)) {
      return activeConnection;
    }
    if (activeConnection != null) {
      activeConnection.close();
    }
    Properties credentials = new Properties();
    credentials.setProperty("user", mysqlUser);
    credentials.setProperty("password", mysqlPassword);
    activeConnection = mysqlDriver.connect(mysqlUrl, credentials);
    return activeConnection;
  }

  /**
   * @return a {@link PreparedStatement} using the supplied SQL text.
   * @param sql the SQL text to use.
   * @throws SQLException
   */
  public synchronized PreparedStatement getPreparedStatement(String sql) throws SQLException {
    PreparedStatement statement = statementCache.get(sql);
    if (statement != null) {
      return statement;
    }
    Connection connection = getDatabaseConnection();
    statement = connection.prepareStatement(sql);
    statementCache.put(sql, statement);
    return statement;
  }

  /**
   * Translate a {@link SQLException} to a {@link AccountServiceException}.
   * @param e the input exception.
   * @return the corresponding {@link AccountServiceException}.
   */
  public static AccountServiceException translateSQLException(SQLException e) {
    if (e instanceof SQLIntegrityConstraintViolationException) {
      SQLIntegrityConstraintViolationException icve = (SQLIntegrityConstraintViolationException) e;
      String message;
      if (icve.getMessage().contains(INDEX_ACCOUNT_CONTAINER)) {
        // Example: Duplicate entry '101-5' for key 'containers.accountContainer'
        message = "Duplicate containerId";
      } else if (icve.getMessage().contains(INDEX_CONTAINER_NAME)) {
        // duplicate container name: need to update cache but retry may fail
        message = "Duplicate container name";
      } else {
        message = "Constraint violation";
      }
      return new AccountServiceException(message, AccountServiceErrorCode.ResourceConflict);
    } else if (MySqlDataAccessor.isCredentialError(e)) {
      return new AccountServiceException("Invalid database credentials", AccountServiceErrorCode.InternalError);
    } else {
      return new AccountServiceException(e.getMessage(), AccountServiceErrorCode.InternalError);
    }
  }

  /**
   * @return true if the exception indicates invalid database credentials.
   * @param e the {@link SQLException}
   */
  public static boolean isCredentialError(SQLException e) {
    return e.getErrorCode() == MysqlErrorNumbers.ER_ACCESS_DENIED_ERROR;
  }

  /**
   * Handle a SQL exception on a database operation.
   * @param e the {@link SQLException} encountered.
   */
  void onException(SQLException e) {
    if (e instanceof SQLTransientConnectionException) {
      reset();
    }
  }

  /**
   * Reset to initial state.
   * This should be called after a failed database operation.
   */
  synchronized void reset() {
    for (PreparedStatement statement : statementCache.values()) {
      try {
        statement.close();
      } catch (SQLException e) {
        logger.error("Closing prepared statement", e);
      }
    }
    statementCache.clear();
    if (activeConnection != null) {
      try {
        activeConnection.close();
      } catch (SQLException e) {
        logger.error("Closing connection", e);
      }
    }
  }
}

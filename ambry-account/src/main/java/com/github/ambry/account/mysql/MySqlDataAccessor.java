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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
  private final String mysqlUrl;
  private final String mysqlUser;
  private final String mysqlPassword;
  private final Driver mysqlDriver;
  private Connection activeConnection;
  private final Map<String, PreparedStatement> statementCache = new HashMap<>();

  /** Production constructor */
  public MySqlDataAccessor(MySqlConfig config) throws SQLException {
    // TODO: this will become a list of url's with info on which ones are writeable
    // Q: Can we assume url's share credentials or does each need its own?
    mysqlUrl = config.mysqlUrl;
    mysqlUser = config.mysqlUser;
    mysqlPassword = config.mysqlPassword;
    // Initialize driver
    mysqlDriver = DriverManager.getDriver(mysqlUrl);
    // AccountService needs to work if mysql is down.  Mysql can also reboot.
    try {
      getDatabaseConnection();
    } catch (SQLException e) {
      // try again later
    }
  }

  /** Test constructor */
  public MySqlDataAccessor(MySqlConfig config, Driver mysqlDriver) {
    mysqlUrl = config.mysqlUrl;
    mysqlUser = config.mysqlUser;
    mysqlPassword = config.mysqlPassword;
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
    // TODO: when we have list of hosts, try them in order and track whether the one we get can handle writes
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

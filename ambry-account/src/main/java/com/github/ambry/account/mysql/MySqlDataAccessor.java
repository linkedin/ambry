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
import java.sql.SQLException;
import java.util.Properties;


public class MySqlDataAccessor {

  private final String mysqlUrl;
  private final String mysqlUser;
  private final String mysqlPassword;
  private final Driver mysqlDriver;
  private Connection activeConnection;

  public MySqlDataAccessor(MySqlConfig config) throws SQLException {
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

  public synchronized Connection getDatabaseConnection() throws SQLException {
    if (activeConnection == null || !activeConnection.isValid(5)) {
      Properties credentials = new Properties();
      credentials.setProperty("user", mysqlUser);
      credentials.setProperty("password", mysqlPassword);
      activeConnection = mysqlDriver.connect(mysqlUrl, credentials);
    }
    return activeConnection;
  }
}

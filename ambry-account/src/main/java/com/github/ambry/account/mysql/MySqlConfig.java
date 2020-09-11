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

import com.github.ambry.config.Config;
import com.github.ambry.config.VerifiableProperties;


/**
 * Config for MySql database connection.
 */
public class MySqlConfig {

  public static final String MYSQL_URL = "mysql.url";
  public static final String MYSQL_USER = "mysql.user";
  public static final String MYSQL_PASSWORD = "mysql.password";

  @Config(MYSQL_URL)
  public final String mysqlUrl;

  @Config(MYSQL_USER)
  public final String mysqlUser;

  @Config(MYSQL_PASSWORD)
  public final String mysqlPassword;

  public MySqlConfig(VerifiableProperties verifiableProperties) {
    mysqlUrl = verifiableProperties.getString(MYSQL_URL);
    mysqlUser = verifiableProperties.getString(MYSQL_USER);
    mysqlPassword = verifiableProperties.getString(MYSQL_PASSWORD);
  }
}

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
package com.github.ambry.config;

/**
 * Config for {@link MySqlAccountServiceConfig}
 */
public class MySqlAccountServiceConfig {
  public static final String MYSQL_ACCOUNT_SERVICE_PREFIX = "mysql.account.service.";
  public static final String DB_URL = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.url";
  public static final String DB_USER = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.user";
  public static final String DB_PASSWORD = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.password";
  public static final String UPDATER_POLLING_INTERVAL_MS_KEY =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.polling.interval.ms";
  public static final String UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.shut.down.timeout.ms";
  public static final String BACKUP_DIRECTORY_KEY = MYSQL_ACCOUNT_SERVICE_PREFIX + "backup.dir";
  public static final String UPDATE_DISABLED = MYSQL_ACCOUNT_SERVICE_PREFIX + "update.disabled";

  // TODO: Might need to take an array of URLs which would have one write (master) and multiple read urls (backup)
  @Config(DB_URL)
  @Default("")
  public final String dbUrl;

  @Config(DB_USER)
  @Default("")
  public final String dbUser;

  @Config(DB_PASSWORD)
  @Default("")
  public final String dbPassword;

  /**
   * The time interval in milli seconds between two consecutive account pulling for the background account updater of
   * {@code MySqlAccountService}. Setting to 0 will disable it.
   */
  @Config(UPDATER_POLLING_INTERVAL_MS_KEY)
  @Default("2 * 1000")
  public final int updaterPollingIntervalMs;

  /**
   * The timeout in ms to shut down the account updater of {@code MySqlAccountService}.
   */
  @Config(UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY)
  @Default("5 * 1000")
  public final int updaterShutDownTimeoutMs;

  /**
   * The directory on the local machine where account data backups will be stored before updating accounts.
   * If this string is empty, backups will be disabled.
   */
  @Config(BACKUP_DIRECTORY_KEY)
  @Default("")
  public final String backupDir;

  /**
   * If true, MySqlAccountService would reject all the requests to update accounts.
   */
  @Config(UPDATE_DISABLED)
  @Default("false")
  public final boolean updateDisabled;

  public MySqlAccountServiceConfig(VerifiableProperties verifiableProperties) {
    dbUrl = verifiableProperties.getString(DB_URL, "");
    dbUser = verifiableProperties.getString(DB_USER, "");
    dbPassword = verifiableProperties.getString(DB_PASSWORD, "");
    updaterPollingIntervalMs =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_MS_KEY, 2 * 1000, 0, Integer.MAX_VALUE);
    updaterShutDownTimeoutMs =
        verifiableProperties.getIntInRange(UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY, 5 * 1000, 1, Integer.MAX_VALUE);
    backupDir = verifiableProperties.getString(BACKUP_DIRECTORY_KEY, "");
    updateDisabled = verifiableProperties.getBoolean(UPDATE_DISABLED, false);
  }
}

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
 * Configs for MySqlAccountService
 */
public class MySqlAccountServiceConfig extends AccountServiceConfig {
  public static final String MYSQL_ACCOUNT_SERVICE_PREFIX = "mysql.account.service.";
  public static final String DB_INFO = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.info";
  public static final String UPDATER_POLLING_INTERVAL_SECONDS =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.polling.interval.seconds";
  public static final String UPDATER_SHUT_DOWN_TIMEOUT_MINUTES =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.shut.down.timeout.minutes";
  public static final String BACKUP_DIRECTORY_KEY = MYSQL_ACCOUNT_SERVICE_PREFIX + "backup.dir";
  public static final String UPDATE_DISABLED = MYSQL_ACCOUNT_SERVICE_PREFIX + "update.disabled";
  private static final String MAX_BACKUP_FILE_COUNT = MYSQL_ACCOUNT_SERVICE_PREFIX + "max.backup.file.count";

  /**
   * Serialized json array containing the information about all mysql end points.
   * This information should be of the following form:
   * <pre>
   *   [
   *     {
   *       "url":"mysql-host1"
   *       "datacenter":"dc1",
   *       "isWriteable": "true",
   *       "username":"root",
   * 	     "password":"password"
   *     },
   *     {
   *       "url":"mysql-host2"
   *       "datacenter":"dc2",
   *       "isWriteable": "false",
   *       "username":"root",
   * 	     "password":"password"
   *     },
   *     {
   *       "url":"mysql-host3"
   *       "datacenter":"dc3",
   *       "isWriteable": "false",
   *       "username":"root",
   * 	     "password":"password"
   *     }
   *   ]
   * </pre>
   */
  @Config(DB_INFO)
  @Default("")
  public final String dbInfo;

  /**
   * The time interval in seconds between two consecutive account pulling for the background account updater of
   * {@code MySqlAccountService}. Setting to 0 will disable it. Default value is 60 seconds.
   */
  @Config(UPDATER_POLLING_INTERVAL_SECONDS)
  @Default("60")
  public final int updaterPollingIntervalSeconds;

  /**
   * The timeout in minutes to shut down the account updater of {@code MySqlAccountService}. Default value is 2 minutes.
   */
  @Config(UPDATER_SHUT_DOWN_TIMEOUT_MINUTES)
  @Default("2")
  public final int updaterShutDownTimeoutMinutes;

  /**
   * The directory on the local machine where account data backups will be stored before updating accounts.
   * If this string is empty, backups will be disabled.
   */
  @Config(BACKUP_DIRECTORY_KEY)
  @Default("")
  public final String backupDir;

  /**
   * The maximum number of local backup files kept in disk. When account service exceeds this count, every time it creates
   * a new backup file, it will remove the oldest one.
   */
  @Config(MAX_BACKUP_FILE_COUNT)
  @Default("10")
  public final int maxBackupFileCount;

  /**
   * If true, MySqlAccountService would reject all the requests to update accounts.
   */
  @Config(UPDATE_DISABLED)
  @Default("false")
  public final boolean updateDisabled;

  public MySqlAccountServiceConfig(VerifiableProperties verifiableProperties) {
    super(verifiableProperties);
    dbInfo = verifiableProperties.getString(DB_INFO);
    updaterPollingIntervalSeconds =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_SECONDS, 60, 0, Integer.MAX_VALUE);
    updaterShutDownTimeoutMinutes =
        verifiableProperties.getIntInRange(UPDATER_SHUT_DOWN_TIMEOUT_MINUTES, 2, 1, Integer.MAX_VALUE);
    backupDir = verifiableProperties.getString(BACKUP_DIRECTORY_KEY, "");
    updateDisabled = verifiableProperties.getBoolean(UPDATE_DISABLED, false);
    maxBackupFileCount = verifiableProperties.getIntInRange(MAX_BACKUP_FILE_COUNT, 10, 1, Integer.MAX_VALUE);
  }
}

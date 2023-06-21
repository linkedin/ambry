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
  public static final String DB_INFO_NEW = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.info.new";
  public static final String UPDATER_POLLING_INTERVAL_SECONDS =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.polling.interval.seconds";
  public static final String UPDATER_SHUT_DOWN_TIMEOUT_MINUTES =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "updater.shut.down.timeout.minutes";
  public static final String BACKUP_DIRECTORY_KEY = MYSQL_ACCOUNT_SERVICE_PREFIX + "backup.dir";
  public static final String BACKUP_DIRECTORY_KEY_NEW = MYSQL_ACCOUNT_SERVICE_PREFIX + "backup.dir.new";
  public static final String UPDATE_DISABLED = MYSQL_ACCOUNT_SERVICE_PREFIX + "update.disabled";
  private static final String MAX_BACKUP_FILE_COUNT = MYSQL_ACCOUNT_SERVICE_PREFIX + "max.backup.file.count";
  public static final String DB_EXECUTE_BATCH_SIZE = MYSQL_ACCOUNT_SERVICE_PREFIX + "db.execute.batch.size";
  public static final String ZK_CLIENT_CONNECT_STRING_KEY = MYSQL_ACCOUNT_SERVICE_PREFIX + "zk.client.connect.string";
  public static final String WRITE_CACHE_AFTER_UPDATE = MYSQL_ACCOUNT_SERVICE_PREFIX + "write.cache.after.update";
  public static final String MAX_MAJOR_VERSION_FOR_SEMANTIC_SCHEMA_DATASET =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "max.major.version.for.semantic.schema.dataset";
  public static final String ENABLE_NEW_DATABASE_FOR_MIGRATION =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "enable.new.database.for.migration";
  public static final String IGNORE_NEW_DATABASE_UPLOAD_ERROR =
      MYSQL_ACCOUNT_SERVICE_PREFIX + "ignore.new.database.upload.error";
  public static final String ENABLE_GET_FROM_NEW_DB_ONLY = MYSQL_ACCOUNT_SERVICE_PREFIX + "enable.get.from.new.db.only";
  public static final String LIST_DATASETS_MAX_RESULT = MYSQL_ACCOUNT_SERVICE_PREFIX + "list.datasets.max.results";


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

  @Config(DB_INFO_NEW)
  @Default("")
  public final String dbInfoNew;

  /**
   * The time interval in seconds between two consecutive account polling for the background account updater of
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
   * The directory on the local machine where account data backups will be stored before updating accounts.
   * If this string is empty, backups will be disabled.
   */
  @Config(BACKUP_DIRECTORY_KEY_NEW)
  @Default("")
  public final String backupDirNew;

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

  /**
   * The number of mysql insert/update statements that can be batched together for execution.
   */
  @Config(DB_EXECUTE_BATCH_SIZE)
  @Default("50")
  public final int dbExecuteBatchSize;

  /**
   * If true, account service will write changes to the cache immediately after updating the persistent store.
   * If false, the cache will be updated on the subsequent sync call.
   * If written immediately, updated accounts and containers will have out of date snapshot version.
   */
  @Config(WRITE_CACHE_AFTER_UPDATE)
  @Default("true")
  public final boolean writeCacheAfterUpdate;
  /**
   * The ZooKeeper server address for change notifications.  May be null.  If supplied, account service
   * will subscribe to the change topic.
   */
  @Config(ZK_CLIENT_CONNECT_STRING_KEY)
  public final String zkClientConnectString;

  @Config(MAX_MAJOR_VERSION_FOR_SEMANTIC_SCHEMA_DATASET)
  @Default("999")
  public final int maxMajorVersionForSemanticSchemaDataset;

  /**
   * If true, enable upload to new database for migration.
   */
  @Config(ENABLE_NEW_DATABASE_FOR_MIGRATION)
  @Default("false")
  public final boolean enableNewDatabaseForMigration;

  /**
   * If true, ignore the error when uploading data to new database.
   */
  @Config(IGNORE_NEW_DATABASE_UPLOAD_ERROR)
  @Default("true")
  public final boolean ignoreNewDatabaseUploadError;

  /**
   * If true, enable get from new database only.
   */
  @Config(ENABLE_GET_FROM_NEW_DB_ONLY)
  @Default("false")
  public final boolean enableGetFromNewDbOnly;

  /**
   * Max number of dataset names for a list datasets call.
   */
  @Config(LIST_DATASETS_MAX_RESULT)
  @Default("100")
  public final int listDatasetsMaxResult;

  public MySqlAccountServiceConfig(VerifiableProperties verifiableProperties) {
    super(verifiableProperties);
    dbInfo = verifiableProperties.getString(DB_INFO);
    updaterPollingIntervalSeconds =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_SECONDS, 60, 0, Integer.MAX_VALUE);
    updaterShutDownTimeoutMinutes =
        verifiableProperties.getIntInRange(UPDATER_SHUT_DOWN_TIMEOUT_MINUTES, 2, 1, Integer.MAX_VALUE);
    backupDir = verifiableProperties.getString(BACKUP_DIRECTORY_KEY, "");
    updateDisabled = verifiableProperties.getBoolean(UPDATE_DISABLED, false);
    enableNewDatabaseForMigration = verifiableProperties.getBoolean(ENABLE_NEW_DATABASE_FOR_MIGRATION, false);
    ignoreNewDatabaseUploadError = verifiableProperties.getBoolean(IGNORE_NEW_DATABASE_UPLOAD_ERROR, true);
    enableGetFromNewDbOnly = verifiableProperties.getBoolean(ENABLE_GET_FROM_NEW_DB_ONLY, false);
    dbInfoNew = enableNewDatabaseForMigration ? verifiableProperties.getString(DB_INFO_NEW)
        : verifiableProperties.getString(DB_INFO_NEW, "");
    backupDirNew = verifiableProperties.getString(BACKUP_DIRECTORY_KEY_NEW, "");
    maxBackupFileCount = verifiableProperties.getIntInRange(MAX_BACKUP_FILE_COUNT, 10, 1, Integer.MAX_VALUE);
    dbExecuteBatchSize = verifiableProperties.getIntInRange(DB_EXECUTE_BATCH_SIZE, 50, 1, Integer.MAX_VALUE);
    zkClientConnectString = verifiableProperties.getString(ZK_CLIENT_CONNECT_STRING_KEY, null);
    writeCacheAfterUpdate = verifiableProperties.getBoolean(WRITE_CACHE_AFTER_UPDATE, true);
    maxMajorVersionForSemanticSchemaDataset =
        verifiableProperties.getIntInRange(MAX_MAJOR_VERSION_FOR_SEMANTIC_SCHEMA_DATASET, 999, 1, Integer.MAX_VALUE);
    listDatasetsMaxResult = verifiableProperties.getIntInRange(LIST_DATASETS_MAX_RESULT, 100, 1, Integer.MAX_VALUE);
  }
}

/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 * Config for {@link HelixAccountServiceConfig}
 */
public class HelixAccountServiceConfig {
  public static final String HELIX_ACCOUNT_SERVICE_PREFIX = "helix.account.service.";
  public static final String UPDATER_POLLING_INTERVAL_MS_KEY =
      HELIX_ACCOUNT_SERVICE_PREFIX + "updater.polling.interval.ms";
  public static final String UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY =
      HELIX_ACCOUNT_SERVICE_PREFIX + "updater.shut.down.timeout.ms";
  public static final String BACKUP_DIRECTORY_KEY = HELIX_ACCOUNT_SERVICE_PREFIX + "backup.dir";
  public static final String MAX_BACKUP_FILE_COUNT = HELIX_ACCOUNT_SERVICE_PREFIX + "max.backup.file.count";
  public static final String ZK_CLIENT_CONNECT_STRING_KEY = HELIX_ACCOUNT_SERVICE_PREFIX + "zk.client.connect.string";
  public static final String USE_NEW_ZNODE_PATH = HELIX_ACCOUNT_SERVICE_PREFIX + "use.new.znode.path";
  public static final String UPDATE_DISABLED = HELIX_ACCOUNT_SERVICE_PREFIX + "update.disabled";
  public static final String ENABLE_SERVE_FROM_BACKUP = HELIX_ACCOUNT_SERVICE_PREFIX + "enable.serve.from.backup";
  public static final String TOTAL_NUMBER_OF_VERSION_TO_KEEP =
      HELIX_ACCOUNT_SERVICE_PREFIX + "total.number.of.version.to.keep";
  public static final String RETRY_COUNT = HELIX_ACCOUNT_SERVICE_PREFIX + "retry.count";
  public static final String RETRY_DELAY = HELIX_ACCOUNT_SERVICE_PREFIX + "retry.delay";
  /**
   * The ZooKeeper server address. This config is required when using {@code HelixAccountService}.
   */
  @Config(ZK_CLIENT_CONNECT_STRING_KEY)
  public final String zkClientConnectString;

  /**
   * The time interval in second between two consecutive account pulling for the background account updater of
   * {@code HelixAccountService}. Setting to 0 to disable it.
   */
  @Config(UPDATER_POLLING_INTERVAL_MS_KEY)
  @Default("60 * 60 * 1000")
  public final int updaterPollingIntervalMs;

  /**
   * The timeout in ms to shut down the account updater of {@code HelixAccountService}.
   */
  @Config(UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY)
  @Default("60 * 1000")
  public final int updaterShutDownTimeoutMs;

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
  @Default("100")
  public final int maxBackupFileCount;

  /**
   * If true, then use the new znode path to store list of blob ids that point to account metadata content.
   */
  @Config(USE_NEW_ZNODE_PATH)
  @Default("false")
  public final boolean useNewZNodePath;

  /**
   * If true, HelixAccountService would reject all the requests to update accounts.
   */
  @Config(UPDATE_DISABLED)
  @Default("false")
  public final boolean updateDisabled;

  /**
   * If true, HelixAccountService would load the account metadata from local backup file when fetching from helix fails.
   * Set it to false while transitioning, since the old backup files don't have up-to-date account metadata.
   */
  @Config(ENABLE_SERVE_FROM_BACKUP)
  @Default("false")
  public final boolean enableServeFromBackup;

  /**
   * Total number of previous versions of account metadata to keep in the system. Every update account http request would
   * generate a new version. And when the number of versions surpasses this number, HelixAccountService will purge the
   * oldest one to make room for the new one.
   */
  @Config(TOTAL_NUMBER_OF_VERSION_TO_KEEP)
  @Default("100")
  public final int totalNumberOfVersionToKeep;

  /**
   * The number of retry times when the update accounts fails by marking delete_in_progress container status to inactive;
   */
  @Config(RETRY_COUNT)
  @Default("10")
  public final int retryCount;

  /**
   * The sleep time between each retry action when the update accounts fails by marking delete_in_progress container status to inactive;
   */
  @Config(RETRY_DELAY)
  @Default("1000")
  public final long retryDelay;

  public HelixAccountServiceConfig(VerifiableProperties verifiableProperties) {
    zkClientConnectString = verifiableProperties.getString(ZK_CLIENT_CONNECT_STRING_KEY);
    updaterPollingIntervalMs =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_MS_KEY, 60 * 60 * 1000, 0, Integer.MAX_VALUE);
    updaterShutDownTimeoutMs =
        verifiableProperties.getIntInRange(UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY, 60 * 1000, 1, Integer.MAX_VALUE);
    backupDir = verifiableProperties.getString(BACKUP_DIRECTORY_KEY, "");
    maxBackupFileCount = verifiableProperties.getIntInRange(MAX_BACKUP_FILE_COUNT, 100, 1, Integer.MAX_VALUE);
    useNewZNodePath = verifiableProperties.getBoolean(USE_NEW_ZNODE_PATH, false);
    updateDisabled = verifiableProperties.getBoolean(UPDATE_DISABLED, false);
    enableServeFromBackup = verifiableProperties.getBoolean(ENABLE_SERVE_FROM_BACKUP, false);
    totalNumberOfVersionToKeep =
        verifiableProperties.getIntInRange(TOTAL_NUMBER_OF_VERSION_TO_KEEP, 100, 1, Integer.MAX_VALUE);
    retryCount = verifiableProperties.getIntInRange(RETRY_COUNT, 10, 1, 100);
    retryDelay = verifiableProperties.getLongInRange(RETRY_DELAY, 1000, 1, Long.MAX_VALUE);
  }
}

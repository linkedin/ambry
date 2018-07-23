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
  public static final String ZK_CLIENT_CONNECT_STRING_KEY = HELIX_ACCOUNT_SERVICE_PREFIX + "zk.client.connect.string";

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

  public HelixAccountServiceConfig(VerifiableProperties verifiableProperties) {
    zkClientConnectString = verifiableProperties.getString(ZK_CLIENT_CONNECT_STRING_KEY);
    updaterPollingIntervalMs =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_MS_KEY, 60 * 60 * 1000, 0, Integer.MAX_VALUE);
    updaterShutDownTimeoutMs =
        verifiableProperties.getIntInRange(UPDATER_SHUT_DOWN_TIMEOUT_MS_KEY, 60 * 1000, 1, Integer.MAX_VALUE);
    backupDir = verifiableProperties.getString(BACKUP_DIRECTORY_KEY, "");
  }
}

/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
 * Config for {@link JsonAccountService}.
 */
public class JsonAccountConfig {

  /** Prefix used for all configuration options of the JSON account service.*/
  public static final String JSON_ACCOUNT_PREFIX = "json.account.";

  public static final String UPDATER_POLLING_INTERVAL_MS = JSON_ACCOUNT_PREFIX + "updater.polling.interval.ms";

  public static final String FILE_PATH = JSON_ACCOUNT_PREFIX + "file.path";
  public static final String FILE_PATH_DEFAULT = "/tmp/accounts.json";

  @Config(FILE_PATH)
  @Default(FILE_PATH_DEFAULT)
  public final String jsonAccountFilePath;

  /**
   * The time interval in milliseconds between polling the local account JSON file for changes.
   *
   * Set to 0 to disable polling.
   */
  @Config(UPDATER_POLLING_INTERVAL_MS)
  @Default("60 * 1000")
  public final int updaterPollingIntervalMs;

  public JsonAccountConfig(VerifiableProperties verifiableProperties) {
    jsonAccountFilePath = verifiableProperties.getString(FILE_PATH, FILE_PATH_DEFAULT);
    updaterPollingIntervalMs =
        verifiableProperties.getIntInRange(UPDATER_POLLING_INTERVAL_MS, 60 * 1000, 0, Integer.MAX_VALUE);
  }
}

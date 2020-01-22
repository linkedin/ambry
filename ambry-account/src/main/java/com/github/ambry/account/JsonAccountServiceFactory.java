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
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.JsonAccountConfig;
import com.github.ambry.config.VerifiableProperties;

import com.github.ambry.utils.Utils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;


public final class JsonAccountServiceFactory implements AccountServiceFactory {

  /** Prefix of threads spawned by the scheduler observing the local account JSON file for changes.*/
  private static final String LOCAL_JSON_ACCOUNT_UPDATER_PREFIX = "local-json-account-updater";
  /** Location of the JSON account file.*/
  private final Path accountFile;
  private final AccountServiceMetrics accountServiceMetrics;
  private final JsonAccountConfig jsonAccountConfig;

  /**
   * Constructor
   *
   * @param verifiableProperties The {@link VerifiableProperties} used to retrieve configuration properties from.
   * @param metricRegistry The {@link AccountServiceMetrics} to report metrics.
   */
  public JsonAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    jsonAccountConfig = new JsonAccountConfig(verifiableProperties);
    accountServiceMetrics = new AccountServiceMetrics(metricRegistry);

    accountFile = Paths.get(jsonAccountConfig.jsonAccountFilePath);
  }

  @Override
  public AccountService getAccountService() throws InstantiationException {
    ScheduledExecutorService scheduler =
        jsonAccountConfig.updaterPollingIntervalMs > 0 ? Utils.newScheduler(1, LOCAL_JSON_ACCOUNT_UPDATER_PREFIX, true)
            : null;

    JsonAccountService accountService =
        new JsonAccountService(accountFile, accountServiceMetrics, scheduler, jsonAccountConfig);
    accountService.init();

    return accountService;
  }
}

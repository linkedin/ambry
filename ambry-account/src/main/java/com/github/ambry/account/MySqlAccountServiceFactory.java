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
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MySql based implementation of {@link AccountServiceFactory}.
 * <p/>
 * Returns a new instance of {@link MySqlAccountService} on {@link #getAccountService()} call.
 */
public class MySqlAccountServiceFactory implements AccountServiceFactory {
  private static final String MYSQL_ACCOUNT_UPDATER_PREFIX = "mysql-account-updater";
  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountServiceFactory.class);
  protected final MySqlAccountServiceConfig accountServiceConfig;
  protected final AccountServiceMetrics accountServiceMetrics;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link MySqlAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public MySqlAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this(new MySqlAccountServiceConfig(verifiableProperties), new AccountServiceMetrics(metricRegistry));
  }

  /**
   * Constructor.
   * @param accountServiceConfig The {@link MySqlAccountServiceConfig} to use.
   * @param accountServiceMetrics The {@link AccountServiceMetrics} to report metrics.
   */
  protected MySqlAccountServiceFactory(MySqlAccountServiceConfig accountServiceConfig,
      AccountServiceMetrics accountServiceMetrics) {
    this.accountServiceConfig = accountServiceConfig;
    this.accountServiceMetrics = accountServiceMetrics;
  }

  @Override
  public AccountService getAccountService() {
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.info("Starting a MySqlAccountService");
      ScheduledExecutorService scheduler =
          accountServiceConfig.updaterPollingIntervalMs > 0 ? Utils.newScheduler(1, MYSQL_ACCOUNT_UPDATER_PREFIX, false)
              : null;
      MySqlAccountService mySqlAccountService =
          new MySqlAccountService(accountServiceMetrics, accountServiceConfig, scheduler);
      long spentTimeMs = System.currentTimeMillis() - startTimeMs;
      logger.info("MySqlAccountService started, took {} ms", spentTimeMs);
      accountServiceMetrics.startupTimeInMs.update(spentTimeMs);
      return mySqlAccountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate MySqlAccountService", e);
    }
  }
}

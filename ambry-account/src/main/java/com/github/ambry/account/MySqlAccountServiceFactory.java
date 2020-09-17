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
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MySql based implementation of {@link AccountServiceFactory}.
 * <p/>
 * Returns a new instance of {@link MySqlAccountService} on {@link #getAccountService()} call.
 */
public class MySqlAccountServiceFactory implements AccountServiceFactory {
  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountServiceFactory.class);
  protected final MySqlAccountServiceConfig accountServiceConfig;
  protected final AccountServiceMetrics accountServiceMetrics;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link MySqlAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public MySqlAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.accountServiceConfig = new MySqlAccountServiceConfig(verifiableProperties);
    this.accountServiceMetrics = new AccountServiceMetrics(metricRegistry);
  }

  @Override
  public AccountService getAccountService() {
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.info("Starting a MySqlAccountService");

      MySqlAccountStore mySqlAccountStore = null;
      try {
        mySqlAccountStore = new MySqlAccountStore(accountServiceConfig);
      } catch (SQLException e) {
        // Continue account service creation and initialize cache with metadata from local file copy to serve read requests.
        // Connection to MySql DB will be retried by scheduler thread which syncs changes periodically. Until then, write
        // requests will be blocked.
        //TODO: record failure, parse exception to figure out what we did wrong. If it is a non-transient error
        // like credential issue, we should fail creation of MySqlAccountService and return error.
        logger.error("MySQL account store creation failed", e);
      }

      MySqlAccountService mySqlAccountService =
          new MySqlAccountService(accountServiceMetrics, accountServiceConfig, mySqlAccountStore);
      long spentTimeMs = System.currentTimeMillis() - startTimeMs;
      logger.info("MySqlAccountService started, took {} ms", spentTimeMs);
      accountServiceMetrics.startupTimeInMs.update(spentTimeMs);
      return mySqlAccountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate MySqlAccountService", e);
    }
  }
}

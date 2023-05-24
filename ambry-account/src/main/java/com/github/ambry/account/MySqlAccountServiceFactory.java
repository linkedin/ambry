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
import com.github.ambry.account.mysql.MySqlAccountStoreFactory;
import com.github.ambry.commons.HelixNotifier;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
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
  protected final AccountServiceMetricsWrapper accountServiceMetricsWrapper;
  protected final MySqlAccountStoreFactory mySqlAccountStoreFactory;
  protected final Notifier<String> notifier;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link MySqlAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public MySqlAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.accountServiceConfig = new MySqlAccountServiceConfig(verifiableProperties);
    this.accountServiceMetricsWrapper = new AccountServiceMetricsWrapper(metricRegistry);
    this.mySqlAccountStoreFactory = new MySqlAccountStoreFactory(verifiableProperties, metricRegistry);
    this.notifier = accountServiceConfig.zkClientConnectString != null ? new HelixNotifier(
        accountServiceConfig.zkClientConnectString, new HelixPropertyStoreConfig(verifiableProperties)) : null;
  }

  @Override
  public AccountService getAccountService() {
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.info("Starting a MySqlAccountService");
      MySqlAccountService mySqlAccountService =
          new MySqlAccountService(accountServiceMetricsWrapper, accountServiceConfig, mySqlAccountStoreFactory, notifier);
      long spentTimeMs = System.currentTimeMillis() - startTimeMs;
      logger.info("MySqlAccountService started, took {} ms", spentTimeMs);
      accountServiceMetricsWrapper.getAccountServiceMetrics().startupTimeInMs.update(spentTimeMs);
      return mySqlAccountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate MySqlAccountService", e);
    }
  }
}

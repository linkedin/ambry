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
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.CompositeAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Composite account service based implementation of {@link AccountServiceFactory}.
 * <p/>
 * Returns a new instance of {@link CompositeAccountService} on {@link #getAccountService()} call.
 */
public class CompositeAccountServiceFactory implements AccountServiceFactory {
  private static final Logger logger = LoggerFactory.getLogger(CompositeAccountServiceFactory.class);
  protected final AccountServiceMetrics accountServiceMetrics;
  protected final AccountServiceFactory primaryAccountServiceFactory;
  protected final AccountServiceFactory secondaryAccountServiceFactory;
  protected final CompositeAccountServiceConfig compositeAccountServiceConfig;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link CompositeAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public CompositeAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry)
      throws ReflectiveOperationException {
    this.accountServiceMetrics = new AccountServiceMetrics(metricRegistry, false);
    this.compositeAccountServiceConfig = new CompositeAccountServiceConfig(verifiableProperties);
    this.primaryAccountServiceFactory =
        Utils.getObj(compositeAccountServiceConfig.primaryAccountServiceFactory, verifiableProperties, metricRegistry);
    this.secondaryAccountServiceFactory =
        Utils.getObj(compositeAccountServiceConfig.secondaryAccountServiceFactory, verifiableProperties,
            metricRegistry);
  }

  @Override
  public AccountService getAccountService() {
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.info("Starting a CompositeAccountService");
      CompositeAccountService compositeAccountService =
          new CompositeAccountService(primaryAccountServiceFactory.getAccountService(),
              secondaryAccountServiceFactory.getAccountService(), accountServiceMetrics, compositeAccountServiceConfig);
      long spentTimeMs = System.currentTimeMillis() - startTimeMs;
      logger.info("CompositeAccountService started, took {} ms", spentTimeMs);
      accountServiceMetrics.startupTimeInMs.update(spentTimeMs);
      return compositeAccountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate CompositeAccountService", e);
    }
  }
}

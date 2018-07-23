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
import com.github.ambry.config.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link AccountServiceFactory} that returns an {@link InMemoryUnknownAccountService}.
 */
public class InMemoryUnknownAccountServiceFactory implements AccountServiceFactory {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link HelixAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public InMemoryUnknownAccountServiceFactory(VerifiableProperties verifiableProperties,
      MetricRegistry metricRegistry) {
  }

  @Override
  public AccountService getAccountService() {
    logger.info("Starting an InMemoryUnknownAccountService.");
    return new InMemoryUnknownAccountService();
  }
}

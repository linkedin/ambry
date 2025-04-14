/*
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.InMemoryAccountConfig;
import com.github.ambry.config.VerifiableProperties;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link AccountServiceFactory} that returns an {@link InMemoryAccountService}.
 */
public class InMemoryAccountServiceFactory implements AccountServiceFactory {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryAccountServiceFactory.class);
  /** Location of the JSON account file.*/
  private final Path accountFile;
  private final AccountServiceMetrics accountServiceMetrics;
  private final InMemoryAccountConfig inMemoryAccountConfig;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link HelixAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public InMemoryAccountServiceFactory(VerifiableProperties verifiableProperties,
      MetricRegistry metricRegistry) {
    inMemoryAccountConfig = new InMemoryAccountConfig(verifiableProperties);
    accountServiceMetrics = new AccountServiceMetrics(metricRegistry);
    accountFile = Paths.get(inMemoryAccountConfig.inMemoryAccountFilePath);
  }

  @Override
  public AccountService getAccountService() {
    logger.info("Starting an InMemoryAccountService.");
    InMemoryAccountService inMemoryAccountService = new InMemoryAccountService(accountFile, accountServiceMetrics, inMemoryAccountConfig);
    inMemoryAccountService.init();
    return inMemoryAccountService;
  }
}

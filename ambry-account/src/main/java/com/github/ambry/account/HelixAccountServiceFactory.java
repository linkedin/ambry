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
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.commons.HelixNotifier;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code Helix}-based implementation of {@link HelixAccountServiceFactory}.
 * <p/>
 * Returns a new instance of {@link HelixAccountService} on {@link #getAccountService()} call.
 */
public class HelixAccountServiceFactory implements AccountServiceFactory {
  private static final String HELIX_ACCOUNT_UPDATER_PREFIX = "helix-account-updater";
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final HelixPropertyStoreConfig storeConfig;
  protected final HelixAccountServiceConfig accountServiceConfig;
  protected final AccountServiceMetrics accountServiceMetrics;
  protected final Notifier<String> notifier;

  /**
   * Constructor.
   * @param verifiableProperties The properties to get a {@link HelixAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   */
  public HelixAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this(new HelixPropertyStoreConfig(verifiableProperties), new HelixAccountServiceConfig(verifiableProperties),
        new AccountServiceMetrics(metricRegistry),
        new HelixNotifier(new HelixAccountServiceConfig(verifiableProperties).zkClientConnectString,
            new HelixPropertyStoreConfig(verifiableProperties)));
  }

  /**
   * Constructor.
   * @param storeConfig The {@link HelixPropertyStoreConfig} to use.
   * @param accountServiceConfig The {@link HelixAccountServiceConfig} to use.
   * @param accountServiceMetrics The {@link AccountServiceMetrics} to report metrics.
   * @param notifier The {@link Notifier} to start a {@link HelixAccountService}.
   */
  protected HelixAccountServiceFactory(HelixPropertyStoreConfig storeConfig,
      HelixAccountServiceConfig accountServiceConfig, AccountServiceMetrics accountServiceMetrics,
      Notifier<String> notifier) {
    this.storeConfig = storeConfig;
    this.accountServiceConfig = accountServiceConfig;
    this.accountServiceMetrics = accountServiceMetrics;
    this.notifier = notifier;
  }

  @Override
  public AccountService getAccountService() {
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.info("Starting a HelixAccountService");
      HelixPropertyStore<ZNRecord> helixStore =
          CommonUtils.createHelixPropertyStore(accountServiceConfig.zkClientConnectString, storeConfig, null);
      logger.info("HelixPropertyStore started with zkClientConnectString={}, zkClientSessionTimeoutMs={}, "
              + "zkClientConnectionTimeoutMs={}, rootPath={}", accountServiceConfig.zkClientConnectString,
          storeConfig.zkClientSessionTimeoutMs, storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath);
      ScheduledExecutorService scheduler =
          accountServiceConfig.updaterPollingIntervalMs > 0 ? Utils.newScheduler(1, HELIX_ACCOUNT_UPDATER_PREFIX, false)
              : null;
      HelixAccountService helixAccountService =
          new HelixAccountService(helixStore, accountServiceMetrics, notifier, scheduler, accountServiceConfig);
      long spentTimeMs = System.currentTimeMillis() - startTimeMs;
      logger.info("HelixAccountService started, took {} ms", spentTimeMs);
      accountServiceMetrics.startupTimeInMs.update(spentTimeMs);
      return helixAccountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate HelixAccountService", e);
    }
  }
}

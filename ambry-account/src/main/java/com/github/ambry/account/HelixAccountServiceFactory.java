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
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Collections;
import java.util.List;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code Helix}-based implementation of {@link HelixAccountServiceFactory}.
 * <p/>
 * Returns a new instance of {@link HelixAccountService} on {@link #getAccountService()} call.
 */
public class HelixAccountServiceFactory implements AccountServiceFactory {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final HelixPropertyStoreConfig storeConfig;
  private final AccountServiceMetrics accountServiceMetrics;
  private final Notifier<String> notifier;

  /**
   * Constructor
   * @param verifiableProperties The properties to get a {@link HelixAccountService} instance. Cannot be {@code null}.
   * @param metricRegistry The {@link MetricRegistry} for metrics tracking. Cannot be {@code null}.
   * @param notifier The {@link Notifier} used to get a {@link HelixAccountService}. Cannot be {@code null}.
   */
  public HelixAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Notifier<String> notifier) {
    if (verifiableProperties == null || metricRegistry == null || notifier == null) {
      throw new IllegalArgumentException("verifiableProperties or metricRegistry or notifier cannot be null");
    }
    storeConfig = new HelixPropertyStoreConfig(verifiableProperties);
    accountServiceMetrics = new AccountServiceMetrics(metricRegistry);
    this.notifier = notifier;
  }

  @Override
  public AccountService getAccountService() {
    long startTimeMs = System.currentTimeMillis();
    logger.info("Starting a HelixAccountService");
    ZkClient zkClient = new ZkClient(storeConfig.zkClientConnectString, storeConfig.zkClientSessionTimeoutMs,
        storeConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
    List<String> subscribedPaths = Collections.singletonList(storeConfig.rootPath);
    HelixPropertyStore<ZNRecord> helixStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(zkClient), storeConfig.rootPath, subscribedPaths);
    logger.info("HelixPropertyStore started with zkClientConnectString={}, zkClientSessionTimeoutMs={}, "
            + "zkClientConnectionTimeoutMs={}, rootPath={}, subscribedPaths={}", storeConfig.zkClientConnectString,
        storeConfig.zkClientSessionTimeoutMs, storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath,
        subscribedPaths);
    HelixAccountService helixAccountService = new HelixAccountService(helixStore, accountServiceMetrics, notifier);
    long spentTimeMs = System.currentTimeMillis() - startTimeMs;
    logger.info("HelixAccountService started, took {}ms", spentTimeMs);
    this.accountServiceMetrics.startupTimeInMs.update(spentTimeMs);
    return helixAccountService;
  }
}


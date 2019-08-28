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
import com.github.ambry.clustermap.MockHelixPropertyStore;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixAccountServiceConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.helix.ZNRecord;


/**
 * A mock implementation of {@link AccountServiceFactory}. This is only for testing purpose and is not thread safe.
 */
public class MockHelixAccountServiceFactory extends HelixAccountServiceFactory {
  private final String updaterThreadPrefix;
  private final boolean useNewZNodePath;
  private final Map<String, MockHelixPropertyStore<ZNRecord>> storeKeyToMockStoreMap = new HashMap<>();
  private final Router router;

  /**
   * Constructor.
   * @param verifiableProperties The properties to start a {@link HelixAccountService}.
   * @param notifier The {@link Notifier} to start a {@link HelixAccountService}.
   * @param metricRegistry The {@link MetricRegistry} to start a {@link HelixAccountService}.
   * @param router The {@link Router} to add to {@link HelixAccountService}
   */
  public MockHelixAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Notifier<String> notifier, String updaterThreadPrefix, Router router) {
    super(new HelixPropertyStoreConfig(verifiableProperties), new HelixAccountServiceConfig(verifiableProperties),
        new AccountServiceMetrics(metricRegistry), notifier);
    HelixAccountServiceConfig config = new HelixAccountServiceConfig(verifiableProperties);
    this.updaterThreadPrefix = updaterThreadPrefix;
    this.router = router;
    this.useNewZNodePath = config.useNewZNodePath;
  }

  @Override
  public AccountService getAccountService() {
    try {
      ScheduledExecutorService scheduler =
          accountServiceConfig.updaterPollingIntervalMs > 0 ? Utils.newScheduler(1, updaterThreadPrefix, false) : null;
      HelixAccountService accountService =
          new HelixAccountService(getHelixStore(accountServiceConfig.zkClientConnectString, storeConfig),
              accountServiceMetrics, notifier, scheduler, accountServiceConfig);
      if (useNewZNodePath) {
        accountService.setupRouter(router);
      }
      return accountService;
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate HelixAccountService", e);
    }
  }

  /**
   * Gets a {@link MockHelixPropertyStore} for the given {@link HelixPropertyStoreConfig}.
   * @param zkServers the ZooKeeper server address.
   * @param storeConfig A {@link HelixPropertyStoreConfig}.
   * @return A {@link MockHelixPropertyStore} defined by the {@link HelixPropertyStoreConfig}.
   */
  MockHelixPropertyStore<ZNRecord> getHelixStore(String zkServers, HelixPropertyStoreConfig storeConfig) {
    return storeKeyToMockStoreMap.computeIfAbsent(zkServers + storeConfig.rootPath,
        path -> new MockHelixPropertyStore<>());
  }
}

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
import com.github.ambry.commons.MockHelixPropertyStore;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ZNRecord;


/**
 * A mock implementation of {@link AccountServiceFactory}. This is only for testing purpose and is not thread safe.
 */
public class MockHelixAccountServiceFactory extends HelixAccountServiceFactory {
  private final HelixPropertyStoreConfig storeConfig;
  private final AccountServiceMetrics accountServiceMetrics;
  private final Notifier notifier;
  private final Map<String, MockHelixPropertyStore<ZNRecord>> storeKeyToMockStoreMap = new HashMap<>();

  /**
   * Constructor.
   * @param verifiableProperties The properties to start a {@link HelixAccountService}.
   * @param metricRegistry The {@link MetricRegistry} to start a {@link HelixAccountService}.
   * @param notifier The {@link Notifier} to start a {@link HelixAccountService}.
   */
  public MockHelixAccountServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Notifier<String> notifier) {
    super(verifiableProperties, metricRegistry, notifier);
    storeConfig = new HelixPropertyStoreConfig(verifiableProperties);
    accountServiceMetrics = new AccountServiceMetrics(metricRegistry);
    this.notifier = notifier;
  }

  @Override
  public AccountService getAccountService() {
    return new HelixAccountService(getHelixStore(storeConfig), accountServiceMetrics, notifier);
  }

  /**
   * Gets a {@link MockHelixPropertyStore} for the given {@link HelixPropertyStoreConfig}.
   * @param storeConfig A {@link HelixPropertyStoreConfig}.
   * @return A {@link MockHelixPropertyStore} defined by the {@link HelixPropertyStoreConfig}.
   */
  MockHelixPropertyStore<ZNRecord> getHelixStore(HelixPropertyStoreConfig storeConfig) {
    return storeKeyToMockStoreMap.computeIfAbsent(storeConfig.zkClientConnectString + storeConfig.rootPath,
        path -> new MockHelixPropertyStore<>());
  }
}

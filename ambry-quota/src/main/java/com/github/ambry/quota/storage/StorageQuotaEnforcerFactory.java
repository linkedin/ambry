/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;


/**
 * A factory implementation to return {@link StorageQuotaEnforcer}.
 */
public class StorageQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final StorageQuotaEnforcer quotaEnforcer;

  /**
   * Constructor to instantiate a factory.
   * @param quotaConfig the {@link QuotaConfig}.
   * @param quotaSource the {@link QuotaSource}.
   * @param accountStatsStore the {@link AccountStatsStore}.
   * @param quotaMetrics The {@link QuotaMetrics}.
   * @throws Exception
   */
  public StorageQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics) throws Exception {
    quotaEnforcer = new StorageQuotaEnforcer(quotaConfig.storageQuotaConfig, quotaSource, accountStatsStore);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return quotaEnforcer;
  }
}

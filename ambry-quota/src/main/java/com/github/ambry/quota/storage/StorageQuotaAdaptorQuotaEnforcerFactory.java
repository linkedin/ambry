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
import com.github.ambry.quota.AmbryQuotaManager;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaSource;


/**
 * A factory implementation to return {@link StorageQuotaAdaptorQuotaEnforcer}.
 */
public class StorageQuotaAdaptorQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final StorageQuotaAdaptorQuotaEnforcer quotaEnforcer;

  /**
   * Constructor to instantiate a factory.
   * @param quotaConfig the {@link QuotaConfig}.
   * @param quotaSource the {@link QuotaSource}.
   * @throws Exception
   */
  public StorageQuotaAdaptorQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource) throws Exception {
    AccountStatsStore accountStatsStore = AmbryQuotaManager.AmbryQuotaManagerComponents.accountStatsStore;
    quotaEnforcer = new StorageQuotaAdaptorQuotaEnforcer(quotaConfig.storageQuotaConfig, accountStatsStore);
  }

  @Override
  public QuotaEnforcer getRequestQuotaEnforcer() {
    return quotaEnforcer;
  }
}

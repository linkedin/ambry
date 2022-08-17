/*
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
package com.github.ambry.quota;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;


/**
 * Factory to instantiate {@link AmbryQuotaManager} class.
 */
public class AmbryQuotaManagerFactory implements QuotaManagerFactory {
  private final QuotaManager quotaManager;

  /**
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaRecommendationMergePolicy {@link QuotaRecommendationMergePolicy} object.
   * @param accountService {@link AccountService} object.
   * @param accountStatsStore {@link AccountStatsStore} object.
   * @param metricRegistry {@link MetricRegistry} object.
   * @param routerConfig {@link RouterConfig} object.
   * @throws ReflectiveOperationException
   */
  public AmbryQuotaManagerFactory(QuotaConfig quotaConfig,
      QuotaRecommendationMergePolicy quotaRecommendationMergePolicy, AccountService accountService,
      AccountStatsStore accountStatsStore, MetricRegistry metricRegistry, RouterConfig routerConfig)
      throws ReflectiveOperationException {
    quotaManager = new AmbryQuotaManager(quotaConfig, quotaRecommendationMergePolicy, accountService, accountStatsStore,
        new QuotaMetrics(metricRegistry), routerConfig);
  }

  @Override
  public QuotaManager getQuotaManager() {
    return quotaManager;
  }
}

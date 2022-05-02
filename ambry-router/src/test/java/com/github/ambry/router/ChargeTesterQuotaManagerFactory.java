/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.AmbryQuotaManager;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaManagerFactory;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendationMergePolicy;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcer;
import com.github.ambry.rest.RestRequest;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class ChargeTesterQuotaManagerFactory implements QuotaManagerFactory {
  private final QuotaManager quotaManager;
  private final AtomicInteger calledCountListener = new AtomicInteger(0);

  /**
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaRecommendationMergePolicy {@link QuotaRecommendationMergePolicy} object.
   * @param accountService {@link AccountService} object.
   * @param accountStatsStore {@link AccountStatsStore} object.
   * @param metricRegistry {@link MetricRegistry} object.
   * @throws ReflectiveOperationException
   */
  public ChargeTesterQuotaManagerFactory(QuotaConfig quotaConfig,
      QuotaRecommendationMergePolicy quotaRecommendationMergePolicy, AccountService accountService,
      AccountStatsStore accountStatsStore, MetricRegistry metricRegistry) throws ReflectiveOperationException {
    quotaManager = new ChargeTesterQuotaManager(quotaConfig, quotaRecommendationMergePolicy, accountService, accountStatsStore,
        new QuotaMetrics(metricRegistry), new AtomicInteger(0));
  }

  @Override
  public QuotaManager getQuotaManager() {
    return quotaManager;
  }
}
/**
 * {@link AmbryQuotaManager} extension to test behavior with default implementation.
 */
class ChargeTesterQuotaManager extends AmbryQuotaManager {
  private final AtomicInteger chargeCalledCount;

  /**
   * Constructor for {@link ChargeTesterQuotaManagerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaRecommendationMergePolicy {@link QuotaRecommendationMergePolicy} object that makes the overall recommendation.
   * @param accountService {@link AccountService} object to get all the accounts and container information.
   * @param accountStatsStore {@link AccountStatsStore} object to get all the account stats related information.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public ChargeTesterQuotaManager(QuotaConfig quotaConfig,
      QuotaRecommendationMergePolicy quotaRecommendationMergePolicy, AccountService accountService,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics, AtomicInteger chargeCalledCount)
      throws ReflectiveOperationException {
    super(quotaConfig, quotaRecommendationMergePolicy, accountService, accountStatsStore, quotaMetrics);
    this.chargeCalledCount = chargeCalledCount;
  }

  @Override
  public QuotaAction chargeAndRecommend(RestRequest restRequest, Map<QuotaName, Double> requestCostMap,
      boolean shouldCheckIfQuotaExceedAllowed, boolean forceCharge) throws QuotaException {
    chargeCalledCount.incrementAndGet();
    return super.chargeAndRecommend(restRequest, requestCostMap, shouldCheckIfQuotaExceedAllowed, forceCharge);
  }

  /**
   * @return TestCUQuotaSource object from the {@link QuotaEnforcer}s.
   */
  public TestCUQuotaSource getTestCuQuotaSource() {
    for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
      if (quotaEnforcer instanceof AmbryCUQuotaEnforcer && !(quotaEnforcer instanceof RejectingQuotaEnforcer)) {
        return (TestCUQuotaSource) quotaEnforcer.getQuotaSource();
      }
    }
    throw new IllegalStateException("could not find TestCUQuotaSource in QuotaManager.");
  }

  public AtomicInteger getChargeCalledCount() {
    return chargeCalledCount;
  }
}

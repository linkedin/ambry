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
 */
package com.github.ambry.quota;

import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * An {@link AmbryQuotaManager} that makes the quota checking and charging atomic for each {@link QuotaResource}.
 *
 * This {@link QuotaManager} is thread safe and atomic only with respect to {@link QuotaResource}. {@link QuotaEnforcer}s
 * might also perform check-and-charge type non-atomic operations for system resources (like node bandwidth) which are global
 * to all {@link QuotaResource}s. This implementation does not synchronize for such global resources. The max error in
 * accounting for a global resource that is not synchronized is bounded as described below.
 *
 * The degree of parallelism of requests coming to {@link AmbryQuotaManager} is determined by
 * {@link com.github.ambry.config.RouterConfig#routerScalingUnitCount}. This config determines the total number of
 * OperationController threads in the frontend, and is typically in single digits. Within each OperationController, the
 * calls to {@link QuotaManager} are serialized.
 * For the global resources the worst case error due to non-atomic check and charge will be
 * ((routerScalingUnitCount - 1) * (max-chunk-size) / quotaAggregationWindow).
 * e.g, Assuming a routerScalingUnitCount as 8, max chunk size as 4MiB and quota aggregation window as 10s, the max error
 * in accounting for a global resource like node bandwidth will be 2.8MiBps.
 * This implementation of {@link QuotaManager} should only be used in cases where such error is acceptable.
 */
public class AtomicAmbryQuotaManager extends AmbryQuotaManager {
  private final QuotaResourceSynchronizer quotaResourceSynchronizer;

  /**
   * Constructor for {@link AtomicAmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaRecommendationMergePolicy {@link QuotaRecommendationMergePolicy} object which is the policy to merge recommendations.
   * @param accountService {@link AccountService} object.
   * @param accountStatsStore {@link AccountStatsStore} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   * @throws ReflectiveOperationException if {@link AtomicAmbryQuotaManager} could not be created.
   */
  public AtomicAmbryQuotaManager(QuotaConfig quotaConfig, QuotaRecommendationMergePolicy quotaRecommendationMergePolicy,
      AccountService accountService, AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) throws ReflectiveOperationException {
    super(quotaConfig, quotaRecommendationMergePolicy, accountService, accountStatsStore, quotaMetrics, routerConfig);
    quotaResourceSynchronizer = new QuotaResourceSynchronizer(quotaMetrics);
  }

  @Override
  public ThrottlingRecommendation recommend(RestRequest restRequest) throws QuotaException {
    QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);
    try {
      quotaResourceSynchronizer.lock(quotaResource);
      return super.recommend(restRequest);
    } finally {
      quotaResourceSynchronizer.unlock(quotaResource);
    }
  }

  @Override
  public QuotaAction chargeAndRecommend(RestRequest restRequest, Map<QuotaName, Double> requestCostMap,
      boolean shouldCheckIfQuotaExceedAllowed, boolean forceCharge) throws QuotaException {
    QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);
    try {
      quotaResourceSynchronizer.lock(quotaResource);
      return super.chargeAndRecommend(restRequest, requestCostMap, shouldCheckIfQuotaExceedAllowed, forceCharge);
    } finally {
      quotaResourceSynchronizer.unlock(quotaResource);
    }
  }
}

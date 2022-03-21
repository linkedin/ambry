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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.router.RouterErrorCode;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * A {@link QuotaChargeCallback} implementation to be called when quota accounting needs to be done before the chunk for
 * a user request has been processed. In this implementation a chunk request is charged and allowed to go through only if
 * the constraints specified in {@link QuotaManager#chargeAndRecommend} are met.
 *
 * If the {@link QuotaManager} recommends to {@link QuotaAction#REJECT}, this implementation will reject the chunk
 * request with {@link RouterErrorCode#TooManyRequests}. If the {@link QuotaManager} recommends to
 * {@link QuotaAction#DELAY}, then this implementation will return back to caller indicating that a charge was not made.
 */
public class PreProcessQuotaChargeCallback implements QuotaChargeCallback {
  private final QuotaManager quotaManager;
  private final RestRequest restRequest;
  private final RequestQuotaCostPolicy requestCostPolicy;

  /**
   * Constructor for {@link PreProcessQuotaChargeCallback}.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   */
  public PreProcessQuotaChargeCallback(QuotaManager quotaManager, RestRequest restRequest) {
    this.quotaManager = quotaManager;
    requestCostPolicy = new SimpleRequestQuotaCostPolicy(quotaManager.getQuotaConfig());
    this.restRequest = restRequest;
  }

  @Override
  public QuotaAction checkAndCharge(boolean shouldCheckQuotaExceedAllowed, boolean forceCharge, long chunkSize)
      throws QuotaException {
    Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), Map.Entry::getValue));
    QuotaAction quotaAction =
        quotaManager.chargeAndRecommend(restRequest, requestCost, shouldCheckQuotaExceedAllowed, forceCharge);
    // Note that we track quota usages even if QuotaMode is TRACKING, so although we return ALLOW, checkAndCharge still
    // needs to happen
    return quotaManager.getQuotaMode() == QuotaMode.TRACKING ? QuotaAction.ALLOW : quotaAction;
  }

  @Override
  public QuotaAction checkAndCharge(boolean shouldCheckQuotaExceedAllowed, boolean forceCharge) throws QuotaException {
    return checkAndCharge(shouldCheckQuotaExceedAllowed, forceCharge,
        quotaManager.getQuotaConfig().quotaAccountingUnit);
  }

  @Override
  public QuotaResource getQuotaResource() throws QuotaException {
    return QuotaResource.fromRestRequest(restRequest);
  }

  @Override
  public QuotaMethod getQuotaMethod() {
    return QuotaUtils.getQuotaMethod(restRequest);
  }

  @Override
  public QuotaConfig getQuotaConfig() {
    return quotaManager.getQuotaConfig();
  }
}

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
import com.github.ambry.router.RouterException;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link QuotaChargeCallback} implementation to be called when quota accounting needs to be done after chunk for a
 * user request has been processed. Since the accounting is done after the chunk is already processed (uploaded or
 * downloaded from storage nodes), usage is charged irrespective of the recommendation.
 *
 * If {@link QuotaManager} recommends that request be throttled, this implementation will reject the user request with
 * {@link RouterErrorCode#TooManyRequests} if {@link QuotaMode} is set to THROTTLING.
 */
public class PostProcessQuotaChargeCallback implements QuotaChargeCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaChargeCallback.class);
  private final QuotaManager quotaManager;
  private final RestRequest restRequest;
  private final RequestQuotaCostPolicy requestCostPolicy;
  private final boolean isQuotaEnforcedOnRequest;

  /**
   * Constructor for {@link PostProcessQuotaChargeCallback}.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param isQuotaEnforcedOnRequest flag indicating if request quota should be enforced after charging. Requests like
   *                                 update ttl, delete etc are charged, but quota is not enforced on them.
   */
  public PostProcessQuotaChargeCallback(QuotaManager quotaManager, RestRequest restRequest,
      boolean isQuotaEnforcedOnRequest) {
    this.quotaManager = quotaManager;
    requestCostPolicy = new SimpleRequestQuotaCostPolicy(quotaManager.getQuotaConfig());
    this.restRequest = restRequest;
    this.isQuotaEnforcedOnRequest = isQuotaEnforcedOnRequest;
  }

  @Override
  public QuotaAction checkAndCharge(boolean shouldCheckQuotaExceedAllowed, boolean forceCharge, long chunkSize) throws QuotaException {
    QuotaAction quotaAction = QuotaAction.ALLOW;
    try {
      Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), Map.Entry::getValue));
      quotaAction =
          quotaManager.chargeAndRecommend(restRequest, requestCost, shouldCheckQuotaExceedAllowed, forceCharge);
      if (isQuotaEnforcedOnRequest && QuotaUtils.shouldThrottle(quotaAction)) {
        if (quotaManager.getQuotaMode() == QuotaMode.THROTTLING) {
          throw new QuotaException("Exception while charging quota",
              new RouterException("RequestQuotaExceeded", RouterErrorCode.TooManyRequests), false);
        } else {
          LOGGER.debug("Quota exceeded for an in progress request.");
        }
      }
    } catch (Exception ex) {
      if (ex.getCause() instanceof RouterException && ((RouterException) ex.getCause()).getErrorCode()
          .equals(RouterErrorCode.TooManyRequests)) {
        throw ex;
      }
      LOGGER.error("Unexpected exception while charging quota.", ex);
    }
    return quotaAction;
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

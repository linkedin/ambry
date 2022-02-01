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

import com.github.ambry.rest.RestRequest;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link QuotaChargeCallback} implementation that will reject requests that exceed their quota.
 */
public class RejectingQuotaChargeCallback implements QuotaChargeCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaChargeCallback.class);
  private final QuotaManager quotaManager;
  private final RestRequest restRequest;
  private final RequestQuotaCostPolicy requestCostPolicy;
  private final boolean shouldThrottle;

  /**
   * Constructor for {@link RejectingQuotaChargeCallback}.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param shouldThrottle flag indicating if request should be throttled after charging. Requests like updatettl, delete etc need not be throttled.
   */
  public RejectingQuotaChargeCallback(QuotaManager quotaManager, RestRequest restRequest, boolean shouldThrottle) {
    this.quotaManager = quotaManager;
    requestCostPolicy = new SimpleRequestQuotaCostPolicy(quotaManager.getQuotaConfig());
    this.restRequest = restRequest;
    this.shouldThrottle = shouldThrottle;
  }

  @Override
  public void charge(long chunkSize) throws QuotaException {
    try {
      Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), Map.Entry::getValue));
      ThrottlingRecommendation throttlingRecommendation = quotaManager.charge(restRequest, null, requestCost);
      if (throttlingRecommendation != null && throttlingRecommendation.shouldThrottle() && shouldThrottle) {
        if (quotaManager.getQuotaMode() == QuotaMode.THROTTLING
            && quotaManager.getQuotaConfig().throttleInProgressRequests) {
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
  }

  @Override
  public void charge() throws QuotaException {
    charge(quotaManager.getQuotaConfig().quotaAccountingUnit);
  }

  @Override
  public boolean check() {
    return false;
  }

  @Override
  public boolean quotaExceedAllowed() {
    return false;
  }

  @Override
  public QuotaResource getQuotaResource() throws QuotaException {
    return QuotaResource.fromRestRequest(restRequest);
  }

  @Override
  public QuotaMethod getQuotaMethod() {
    return QuotaUtils.getQuotaMethod(restRequest);
  }
}

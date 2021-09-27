/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
 * Callback for charging request cost against quota. Used by {@link QuotaEnforcer}s to charge quota for a request.
 */
public interface QuotaChargeCallback {
  Logger logger = LoggerFactory.getLogger(QuotaChargeCallback.class);

  /**
   * Build {@link QuotaChargeCallback} to handle quota compliance of requests.
   * @param restRequest {@link RestRequest} for which quota is being charged.
   * @param quotaManager {@link QuotaManager} object responsible for charging the quota.
   * @param shouldThrottle flag indicating if request should be throttled after charging. Requests like updatettl, delete etc need not be throttled.
   * @return QuotaChargeCallback object.
   */
  static QuotaChargeCallback buildQuotaChargeCallback(RestRequest restRequest, QuotaManager quotaManager,
      boolean shouldThrottle) {
    RequestCostPolicy requestCostPolicy = new UserQuotaRequestCostPolicy(quotaManager.getQuotaConfig());
    return new QuotaChargeCallback() {
      @Override
      public void chargeQuota(long chunkSize) throws RouterException {
        try {
          Map<QuotaName, Double> requestCost = requestCostPolicy.calculateRequestQuotaCharge(restRequest, chunkSize)
              .entrySet()
              .stream()
              .collect(Collectors.toMap(entry -> QuotaName.valueOf(entry.getKey()), entry -> entry.getValue()));
          ThrottlingRecommendation throttlingRecommendation = quotaManager.charge(restRequest, null, requestCost);
          if (throttlingRecommendation != null && throttlingRecommendation.shouldThrottle() && shouldThrottle) {
            if (quotaManager.getQuotaMode() == QuotaMode.THROTTLING
                && quotaManager.getQuotaConfig().throttleInProgressRequests) {
              throw new RouterException("RequestQuotaExceeded", RouterErrorCode.TooManyRequests);
            } else {
              logger.debug("Quota exceeded for an in progress request.");
            }
          }
        } catch (Exception ex) {
          if (ex instanceof RouterException && ((RouterException) ex).getErrorCode()
              .equals(RouterErrorCode.TooManyRequests)) {
            throw ex;
          }
          logger.error("Unexpected exception while charging quota.", ex);
        }
      }

      @Override
      public void chargeQuota() throws RouterException {
        chargeQuota(quotaManager.getQuotaConfig().quotaAccountingUnit);
      }
    };
  }

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request.
   * @param chunkSize of the chunk.
   * @throws RouterException In case request needs to be throttled.
   */
  void chargeQuota(long chunkSize) throws RouterException;

  /**
   * Callback method that can be used to charge quota usage for a request or part of a request. Call this method
   * when the quota charge doesn't depend on the chunk size.
   * @throws RouterException In case request needs to be throttled.
   */
  void chargeQuota() throws RouterException;
}

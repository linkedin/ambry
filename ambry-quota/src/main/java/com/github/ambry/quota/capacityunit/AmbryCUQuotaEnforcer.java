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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestRequest;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.quota.QuotaUtils.*;


/**
 * Implementation of {@link QuotaEnforcer} for Capacity Unit Quota of Ambry resource.
 */
public class AmbryCUQuotaEnforcer implements QuotaEnforcer {
  private static final Logger logger = LoggerFactory.getLogger(AmbryCUQuotaEnforcer.class);
  private static final long THROTTLE_RETRY_AFTER_MS = 1;
  private static final long NO_THROTTLE_RETRY_AFTER_MS = -1;
  private static final int NO_THROTTLE_HTTP_STATUS = 200;
  private static final int THROTTLE_HTTP_STATUS = 429;

  private final QuotaSource quotaSource;
  private final QuotaConfig quotaConfig;

  /**
   * Constructor for {@link AmbryCUQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   * @param quotaConfig {@link QuotaConfig} object.
   */
  public AmbryCUQuotaEnforcer(QuotaSource quotaSource, QuotaConfig quotaConfig) {
    this.quotaSource = quotaSource;
    this.quotaConfig = quotaConfig;
  }

  @Override
  public void init() throws QuotaException {
    quotaSource.init();
  }

  @Override
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) throws QuotaException {
    try {
      if (requestCostMap.size() == 0) {
        String errorMessage = String.format("Empty cost map provided for %s request for blob %s. Nothing to charge",
            (restRequest == null ? "null" : restRequest.getRestMethod().name()),
            (blobInfo == null ? "null" : blobInfo.getBlobProperties()));
        logger.warn(errorMessage);
        throw new QuotaException(errorMessage, true);
      }
      QuotaName quotaName = getCUQuotaName(restRequest);
      if(!quotaSource.isReady()) {
        // If quota source is not ready we don't have any information to charge or enforce quota.
        logger.warn("Allowing request to go through as CU QuotaSource is not ready.");
        return new QuotaRecommendation(false, 0, quotaName, 200, -1);
      }
      quotaSource.chargeUsage(getQuotaResource(restRequest), quotaName, requestCostMap.get(quotaName));
      quotaSource.chargeSystemResourceUsage(quotaName, requestCostMap.get(quotaName));
      return recommend(restRequest);
    } catch (QuotaException qEx) {
      throw qEx;
    } catch (Exception ex) {
      throw new QuotaException("Unexpected exception during chargeAndRecommend", true);
    }
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) throws QuotaException {
    try {
      QuotaName quotaName = getCUQuotaName(restRequest);
      QuotaResource quotaResource = getQuotaResource(restRequest);
      if(!quotaSource.isReady()) {
        // If quota source is not ready we don't have any information to make recommendation.
        logger.warn("Allowing request to go through as CU QuotaSource is not ready.");
        return new QuotaRecommendation(false, 0, quotaName, 200, -1);
      }
      long limit = (long) (quotaSource.getQuota(quotaResource, quotaName).getQuotaValue());
      float usage = quotaSource.getUsage(quotaResource, quotaName);
      return buildQuotaRecommendation(limit, usage, quotaName);
    } catch (QuotaException qEx) {
      logger.warn("Cannot check resource usage because could not create resourceId.", qEx);
      throw qEx;
    } catch (Exception ex) {
      logger.warn("Unexpected exception while checking usage", ex);
      throw new QuotaException("Unexpected exception while checking usage", true);
    }
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
    try {
      if(!quotaSource.isReady()) {
        // If quota source is not ready we will allow all requests to go through. So this should never happen.
        // If for some reason this happens, we will not allow quota to exceed as we don't have any information to make that decision.
        logger.warn("Requests aren't allowed to exceed quota because CU QuotaSource is not ready yet.");
        return false;
      }
      QuotaName quotaName = getCUQuotaName(restRequest);
      double usage = quotaSource.getSystemResourceUsage(quotaName);
      if(usage == -1) {
        logger.trace("Quota exceed cannot be allowed for the quota {}.", quotaName);
        return false;
      }
      return usage < quotaConfig.maxFrontendCuUsageToAllowExceed;
    } catch (Exception ex) {
      throw new QuotaException("Unexpected exception while checking isQuotaExceedAllowed", true);
    }
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {
    quotaSource.shutdown();
  }

  /**
   * Build the {@link QuotaRecommendation} object from the specified arguments.
   * @param limit the quota limit value.
   * @param usage the quota usage.
   * @param quotaName {@link QuotaName} object.s
   * @return QuotaRecommendation object.
   */
  private QuotaRecommendation buildQuotaRecommendation(long limit, float usage, QuotaName quotaName) {
    boolean shouldThrottle = (usage >= 100);
    return new QuotaRecommendation(shouldThrottle, usage, quotaName,
        shouldThrottle ? THROTTLE_HTTP_STATUS : NO_THROTTLE_HTTP_STATUS,
        shouldThrottle ? THROTTLE_RETRY_AFTER_MS : NO_THROTTLE_RETRY_AFTER_MS);
  }
}

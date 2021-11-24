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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestRequest;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link QuotaEnforcer} for Capacity Units of Ambry resource.
 */
public class AmbryCUQuotaEnforcer implements QuotaEnforcer {
  private static final Logger logger = LoggerFactory.getLogger(AmbryCUQuotaEnforcer.class);

  private final QuotaSource quotaSource;
  private final float maxFrontendCuUsageToAllowExceed;

  /**
   * Constructor for {@link AmbryCUQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCUQuotaEnforcer(QuotaSource quotaSource, float maxFrontendCuUsageToAllowExceed) {
    this.quotaSource = quotaSource;
    this.maxFrontendCuUsageToAllowExceed = maxFrontendCuUsageToAllowExceed;
  }

  @Override
  public void init() {
  }

  @Override
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) {
    if (!quotaSource.isReady()) {
      return null;
    }
    if (requestCostMap.size() == 0) {
      logger.warn("Empty cost map provided for {} request for blob {}. Nothing to charge",
          (restRequest == null ? "null" : restRequest.getRestMethod().name()),
          (blobInfo == null ? "null" : blobInfo.getBlobProperties()));
    }
    quotaSource.charge(restRequest, blobInfo, requestCostMap);
    return quotaSource.checkResourceUsage(restRequest);
  }

  @Override
  public QuotaRecommendation getResourceRecommendation(RestRequest restRequest) {
    if (!quotaSource.isReady()) {
      return null;
    }
    return quotaSource.checkResourceUsage(restRequest);
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) {
    QuotaRecommendation feQuotaRecommendation = quotaSource.checkFrontendUsage(restRequest);
    return feQuotaRecommendation.getQuotaUsagePercentage() < maxFrontendCuUsageToAllowExceed;
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {

  }
}

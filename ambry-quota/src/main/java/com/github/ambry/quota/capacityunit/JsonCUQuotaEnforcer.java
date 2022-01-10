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
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link QuotaEnforcer} for Capacity Units of Ambry resource.
 */
public class JsonCUQuotaEnforcer implements QuotaEnforcer {
  private static final Logger logger = LoggerFactory.getLogger(JsonCUQuotaEnforcer.class);
  private static final long THROTTLE_RETRY_AFTER_MS = 1;
  private static final long NO_THROTTLE_RETRY_AFTER_MS = -1;
  private static final int NO_THROTTLE_HTTP_STATUS = 200;
  private static final int THROTTLE_HTTP_STATUS = 429;

  private final JsonCUQuotaSource quotaSource;

  /**
   * Constructor for {@link JsonCUQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public JsonCUQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = (JsonCUQuotaSource) quotaSource;
  }

  @Override
  public void init() {
  }

  @Override
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) throws QuotaException {
    try {
      if (requestCostMap.size() == 0) {
        String errorMessage = String.format("Empty cost map provided for %s request for blob %s. Nothing to charge", (restRequest == null ? "null" : restRequest.getRestMethod().name()),
            (blobInfo == null ? "null" : blobInfo.getBlobProperties()));
        logger.warn(errorMessage);
        throw new QuotaException(errorMessage, true);
      }
      quotaSource.charge(restRequest, blobInfo, requestCostMap);
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
      QuotaName quotaName = QuotaUtils.getQuotaName(restRequest);
      QuotaResource quotaResource;
      quotaResource = QuotaUtils.getQuotaResource(restRequest);
      long limit = quotaSource.getQuota(quotaResource, QuotaUtils.getQuotaName(restRequest)).getQuotaValue();
      long usage = quotaSource.getUsage(quotaResource, QuotaUtils.getQuotaName(restRequest)).getQuotaValue();
      return buildQuotaRecommendation(limit, usage, quotaName);
    } catch (QuotaException qEx) {
      logger.error("Cannot check resource usage because could not create resourceId due to exception {}",
          qEx.toString());
      throw qEx;
    } catch (Exception ex) {
      throw new QuotaException("Unexpected exception while checking usage", true);
    }
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
    try {
      return quotaSource.isQuotaExceedAllowed(QuotaUtils.getQuotaMethod(restRequest));
    } catch (Exception ex) {
      throw new QuotaException("Unexpected exception while checking isQuotaExceedAllowed", true);
    }
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  public void shutdown() {

  }

  private QuotaRecommendation buildQuotaRecommendation(long limit, long usage, QuotaName quotaName) {
    boolean shouldThrottle = (usage >= limit);
    return new QuotaRecommendation(shouldThrottle, (limit == 0) ? 100 : ((usage * 100) / (float) limit), quotaName,
        shouldThrottle ? THROTTLE_HTTP_STATUS : NO_THROTTLE_HTTP_STATUS,
        shouldThrottle ? THROTTLE_RETRY_AFTER_MS : NO_THROTTLE_RETRY_AFTER_MS);
  }
}

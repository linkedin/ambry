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
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * Implementation of {@link QuotaEnforcer} for Capacity Units of ambry resource.
 */
// TODO: The current implementation allows all requests without any processing. It needs to be replaced by an implementation that enforces quota.
public class AmbryCapacityUnitQuotaEnforcer implements QuotaEnforcer {
  private final QuotaSource quotaSource;
  private final QuotaRecommendation allowReadRecommendation;
  private final QuotaRecommendation allowWriteRecommendation;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCapacityUnitQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
    this.allowReadRecommendation = new QuotaRecommendation(false, 0, QuotaName.READ_CAPACITY_UNIT, 200, 0);
    this.allowWriteRecommendation = new QuotaRecommendation(false, 0, QuotaName.WRITE_CAPACITY_UNIT, 200, 0);
  }

  @Override
  public void init() {
  }

  @Override
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) {
    if (isReadRequest(restRequest)) {
      return allowReadRecommendation;
    } else {
      return allowWriteRecommendation;
    }
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) {
    if (isReadRequest(restRequest)) {
      return allowReadRecommendation;
    } else {
      return allowWriteRecommendation;
    }
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {

  }

  /**
   * Check if the restRequest passed is for a READ request.
   * @param restRequest {@link RestRequest} object.
   * @return true is restRequest is a READ request. False otherwise.
   */
  private boolean isReadRequest(RestRequest restRequest) {
    return (restRequest.getRestMethod() == RestMethod.GET || restRequest.getRestMethod() == RestMethod.HEAD);
  }
}

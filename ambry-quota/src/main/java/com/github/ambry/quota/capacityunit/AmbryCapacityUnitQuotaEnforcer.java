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
import com.github.ambry.quota.EnforcementRecommendation;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.RequestQuotaEnforcer;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;


/**
 * Implementation of {@link RequestQuotaEnforcer} for Capacity Units of ambry resource.
 */
// TODO: The current implementation allows all requests without any processing. It needs to be replaced by an implementation that enforces quota.
public class AmbryCapacityUnitQuotaEnforcer implements RequestQuotaEnforcer {
  private final QuotaSource quotaSource;
  private final EnforcementRecommendation allowReadRecommendation;
  private final EnforcementRecommendation allowWriteRecommendation;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCapacityUnitQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
    this.allowReadRecommendation = new EnforcementRecommendation(false, 0, QuotaName.READ_CAPACITY_UNIT, 200, 0, -1);
    this.allowWriteRecommendation = new EnforcementRecommendation(false, 0, QuotaName.WRITE_CAPACITY_UNIT, 200, 0, -1);
  }

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo) {
    if (isReadRequest(restRequest)) {
      return allowReadRecommendation;
    } else {
      return allowWriteRecommendation;
    }
  }

  @Override
  public EnforcementRecommendation recommend(RestRequest restRequest) {
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

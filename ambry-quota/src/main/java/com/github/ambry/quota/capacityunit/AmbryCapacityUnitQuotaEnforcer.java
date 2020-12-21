/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.quota.AmbryEnforcementRecommendation;
import com.github.ambry.quota.EnforcementRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.RequestCost;
import com.github.ambry.quota.RequestQuotaEnforcer;
import com.github.ambry.rest.RestRequest;


/**
 * Implementation of {@link RequestQuotaEnforcer} for Capacity Units of ambry resource.
 */
// TODO: The current implementation allows all requests without any processing. It needs to be replaced by an implementation that enforces quota.
public class AmbryCapacityUnitQuotaEnforcer implements RequestQuotaEnforcer {
  private final QuotaSource quotaSource;
  private final EnforcementRecommendation allowRecommendation;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCapacityUnitQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
    allowRecommendation =
        new AmbryEnforcementRecommendation(false, 0, AmbryCapacityUnitQuotaEnforcer.class.getSimpleName(), 200,
            new RequestCost(null, null));
  }

  @Override
  public void init() {

  }

  @Override
  public EnforcementRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo) {
    return allowRecommendation;
  }

  @Override
  public EnforcementRecommendation recommend(RestRequest restRequest) {
    return allowRecommendation;
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {

  }
}

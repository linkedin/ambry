/*
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

import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Implementation of {@link QuotaEnforcer} for Capacity Units of ambry resource.
 */
public class AmbryCapacityUnitQuotaEnforcer implements QuotaEnforcer {
  private final static List<QuotaName> SUPPORTED_QUOTA_NAMES =
      Collections.unmodifiableList(Arrays.asList(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT));
  private final QuotaSource quotaSource;
  private final QuotaRecommendation allowReadRecommendation;
  private final QuotaRecommendation allowWriteRecommendation;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCapacityUnitQuotaEnforcer(QuotaSource quotaSource) {
    this.quotaSource = quotaSource;
    this.allowReadRecommendation = new QuotaRecommendation(false, 0, QuotaName.READ_CAPACITY_UNIT, 0);
    this.allowWriteRecommendation = new QuotaRecommendation(false, 0, QuotaName.WRITE_CAPACITY_UNIT, 0);
  }

  @Override
  public void init() {
  }

  @Override
  public QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap) {
    return null;
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
  public boolean isQuotaExceedAllowed(RestRequest restRequest) {
    return false;
  }

  @Override
  public List<QuotaName> supportedQuotaNames() {
    return SUPPORTED_QUOTA_NAMES;
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

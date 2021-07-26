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
package com.github.ambry.quota;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * Interface for class that would do the quota enforcement of a particular quota.
 * A {@link QuotaEnforcer} object would need a {@link QuotaSource} to get and save quota and usage.
 */
public interface QuotaEnforcer {
  /**
   * Method to initialize the {@link QuotaEnforcer}.
   */
  void init() throws Exception;

  /**
   * Makes an {@link QuotaRecommendation} using the information in {@link BlobInfo} and {@link RestRequest}. This
   * method also charges the request cost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @param blobInfo {@link BlobInfo} object representing the blob information involved in the request.
   * @param requestCostMap {@link Map} of {@link QuotaName} to the cost incurred to handle the request.
   * @return QuotaRecommendation object with the recommendation.
   */
  QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap);

  /**
   * Makes an {@link QuotaRecommendation} for the restRequest. This method doesn't know the
   * request details and hence makes the recommendation based on current quota usage.
   * @param restRequest {@link RestRequest} object.
   * @return QuotaRecommendation object with the recommendation.
   */
  QuotaRecommendation recommend(RestRequest restRequest);

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link QuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}

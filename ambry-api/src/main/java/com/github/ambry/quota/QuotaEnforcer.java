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
 * Interface for class that would do the quota enforcement of a particular type of quota.
 * Each enforcer is responsible to make decisions only for its own particular type of quota. The {@link QuotaManager}
 * looks at each of the enforcers' decision and merges them based on {@link ThrottlePolicy}.
 * A {@link QuotaEnforcer} object would need a {@link QuotaSource} to get and save quota and usage.
 */
public interface QuotaEnforcer {

  /**
   * Method to initialize the {@link QuotaEnforcer}.
   * @throws QuotaException in case of any exception.
   */
  void init() throws QuotaException;

  /**
   * Makes an {@link QuotaRecommendation} using the information in {@link BlobInfo} and {@link RestRequest}. This
   * method also charges the request cost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @param blobInfo {@link BlobInfo} object representing the blob information involved in the request.
   * @param requestCostMap {@link Map} of {@link QuotaName} to the cost incurred to handle the request.
   * @return QuotaRecommendation object with the recommendation.
   * @throws QuotaException in case of any exception.
   */
  QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) throws QuotaException;

  /**
   * Makes an {@link QuotaRecommendation} for the restRequest. This method doesn't know the
   * request cost and hence makes the recommendation based on existing quota usage.
   * @param restRequest {@link RestRequest} object.
   * @return QuotaRecommendation object with the recommendation.
   * @throws QuotaException in case of any exception.
   */
  QuotaRecommendation recommend(RestRequest restRequest) throws QuotaException;

  /**
   * @return {@code true} if quota exceed is allowed. {@code false} otherwise.
   * A QuotaResource's request could be allowed to exceed its quota is the system has enough resources to handle the request.
   * @throws QuotaException in case of any exception.
   */
  boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException;

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link QuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}

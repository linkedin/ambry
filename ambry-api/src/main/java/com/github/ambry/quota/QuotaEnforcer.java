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

import com.github.ambry.rest.RestRequest;
import java.util.List;
import java.util.Map;


/**
 * A {@link QuotaEnforcer} is responsible for the enforcement of quota(s) it supports. It would need a {@link QuotaSource}
 * as a source of truth for quota and usage values.
 * A {@link QuotaEnforcer} implementation needs to decide what system resource (if any) it needs to track. This will
 * determine the behavior of {@link QuotaEnforcer#isQuotaExceedAllowed)}.
 * For example, an implementation of storage quota enforcer might track overall cluster storage usage as system resource,
 * and use this information to decide {@link QuotaEnforcer#isQuotaExceedAllowed} behavior. Another storage quota
 * implementation might decide to not track the overall storage usage in a cluster. As a side effect, it will always
 * return false for {@link QuotaEnforcer#isQuotaExceedAllowed}.
 */
public interface QuotaEnforcer {
  /**
   * Method to initialize the {@link QuotaEnforcer}.
   */
  void init() throws Exception;

  /**
   * Charges the request cost against the quota and returns the {@link QuotaRecommendation} after charging.
   * @param restRequest {@link RestRequest} object.
   * @param requestCostMap {@link Map} of {@link QuotaName} to the cost incurred to handle the request.
   * @return QuotaRecommendation object with the recommendation after charging is done.
   */
  QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap)
      throws QuotaException;

  /**
   * Makes a {@link QuotaRecommendation} for the restRequest. This method doesn't know the cost details and hence
   * makes the recommendation based on current quota usage.
   * @param restRequest {@link RestRequest} object.
   * @return QuotaRecommendation object with the recommendation.
   */
  QuotaRecommendation recommend(RestRequest restRequest) throws QuotaException;

  /**
   * @return {@code true} if quota exceed is allowed. {@code false} otherwise.
   */
  boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException;

  /**
   * @return {@link List} of {@link QuotaName}s supported by this enforcer.
   */
  List<QuotaName> supportedQuotaNames();

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link QuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}

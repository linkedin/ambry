/*
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
package com.github.ambry.quota;

/**
 * Interface for class that would do the quota enforcement of a particular quota.
 */
public interface QuotaEnforcer {
  /**
   * Method to initialize the {@link QuotaEnforcer}.
   */
  void init();

  /**
   * Makes an {@link EnforcementRecommendation} for the quotaResource and quotaOperation based on specified requestCost.
   * This method also charges the specified requestCost against the quota.
   * @param requestCost {@link RequestCost} object indicating the cost of the request.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaOperation {@link QuotaOperation} object.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation chargeAndRecommend(RequestCost requestCost, QuotaResource quotaResource,
      QuotaOperation quotaOperation);

  /**
   * Makes an {@link EnforcementRecommendation} for the quotaResource and quotaOperation based on specified requestCost.
   * This method only makes the recommendation and does not charge the cost against the quota.
   * @param requestCost {@link RequestCost} object indicating the cost of the request.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaOperation {@link QuotaOperation} object.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation recommend(RequestCost requestCost, QuotaResource quotaResource,
      QuotaOperation quotaOperation);

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link QuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}

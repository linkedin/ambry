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

import java.util.List;


/**
 * Interface for the class that acts as the manager of all the quotas in Ambry. Implementations of this interface
 * should take care of initializing all the various type of quota enforcements and getting the overall quota
 * recommendation for each request.
 */
public interface QuotaManager {

  /**
   * Method to intialize the {@link QuotaManager}.
   */
  void init();

  /**
   * Computes the overall boolean recommendation to throttle a request or not for all the types of quotas supported
   * based on the specified requestCost, and populates the specified enforcementRecommendations list with the
   * recommendations. This method does not charge the requestCost against the quota.
   * @param requestCost {@link RequestCost} of the request.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaOperation {@link QuotaOperation} object.
   * @param enforcementRecommendations {@link List} of {@link EnforcementRecommendation}s to be populated.
   * @return true if the request should be throttled. false otherwise.
   */
  boolean shouldThrottle(RequestCost requestCost, QuotaResource quotaResource, QuotaOperation quotaOperation,
      List<EnforcementRecommendation> enforcementRecommendations);

  /**
   * Computes the overall boolean recommendation to throttle a request or not for all the types of quotas supported
   * based on the specified requestCost, and populates the specified enforcementRecommendations list with the
   * recommendations. This method charges the requestCost against the quota.
   * @param requestCost {@link RequestCost} of the request.
   * @param quotaResource {@link QuotaResource} object.
   * @param quotaOperation {@link QuotaOperation} object.
   * @param enforcementRecommendations {@link List} of {@link EnforcementRecommendation}s to be populated.
   * @return true if the request should be throttled. false otherwise.
   */
  boolean shouldThrottleAndCharge(RequestCost requestCost, QuotaResource quotaResource, QuotaOperation quotaOperation,
      List<EnforcementRecommendation> enforcementRecommendations);

  /**
   * Method to add {@link QuotaEnforcer} implementation to a {@link QuotaManager}.
   * This method allows to add special enforcers which might be difficult to create within the QuotaManager in a generic
   * way. This method should only be used sparingly for special cases.
   */
  default void addQuotaEnforcer(QuotaEnforcer quotaEnforcer) {
  }

  /**
   * Method to shutdown the {@link QuotaManager} and cleanup if required.
   */
  void shutdown();
}

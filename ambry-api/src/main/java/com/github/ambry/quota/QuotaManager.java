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

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import java.util.List;


/**
 * Interface for the class that acts as the manager of all the quotas in Ambry. Implementations of this interface
 * should take care of initializing all the various type of quota enforcements and getting the overall quota
 * recommendation for each request.
 */
public interface QuotaManager {

  /**
   * Method to initialize the {@link QuotaManager}.
   */
  void init();

  /**
   * Computes the overall boolean recommendation to throttle a request or not, for all the types of quotas that depend on
   * the load on host resources, and populates the specified enforcementRecommendations list with the
   * recommendations. This method should be used for throttling on quota that doesn't depend upon request characteristics.
   * @param enforcementRecommendations {@link List} of {@link EnforcementRecommendation}s to be populated.
   * @return true if the request should be throttled. false otherwise.
   */
  boolean shouldThrottleOnHost(List<EnforcementRecommendation> enforcementRecommendations);

  /**
   * Computes the overall boolean recommendation to throttle a request or not for all the types of request quotas supported.
   * This method does not charge the requestCost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @param enforcementRecommendations {@link List} of {@link EnforcementRecommendation}s to be populated.
   * @return true if the request should be throttled. false otherwise.
   */
  boolean shouldThrottleOnRequest(RestRequest restRequest, List<EnforcementRecommendation> enforcementRecommendations);

  /**
   * Computes the overall boolean recommendation to throttle a request or not for all the types of request quotas supported.
   * This method charges the requestCost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @param blobInfo {@link BlobInfo} object representing the blob characteristics using which request cost can be
   *                                 determined by enforcers.
   * @param enforcementRecommendations {@link List} of {@link EnforcementRecommendation}s to be populated.
   * @return true if the request should be throttled. false otherwise.
   */
  boolean shouldThrottleOnRequestAndCharge(RestRequest restRequest, BlobInfo blobInfo,
      List<EnforcementRecommendation> enforcementRecommendations);

  /**
   * Method to shutdown the {@link QuotaManager} and cleanup if required.
   */
  void shutdown();
}

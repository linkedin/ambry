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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * Interface for the class that acts as the manager of all the quotas in Ambry. Implementations of this interface
 * should take care of initializing all the various type of quota enforcements and getting the overall quota
 * recommendation for each request.
 */
public interface QuotaManager {

  /**
   * Method to initialize the {@link QuotaManager}.
   */
  void init() throws InstantiationException;

  /**
   * Computes the overall boolean recommendation to throttle a request or not for all the types of request quotas supported.
   * This method does not charge the requestCost against the quota.
   * @param restRequest {@link RestRequest} object.
   * @return ThrottlingRecommendation object that captures the overall recommendation.
   */
  ThrottlingRecommendation getThrottleRecommendation(RestRequest restRequest);

  /**
   * Charges the requestCost against the quota for the specified restRequest and blobInfo.
   * @param restRequest {@link RestRequest} object.
   * @param blobInfo {@link BlobInfo} object representing the blob characteristics using which request cost can be
   *                                 determined by enforcers.
   * @param requestCostMap {@link Map} of {@link QuotaName} to the cost incurred to handle the request.
   * @return ThrottlingRecommendation object that captures the overall recommendation.
   */
  ThrottlingRecommendation charge(RestRequest restRequest, BlobInfo blobInfo, Map<QuotaName, Double> requestCostMap);

  /**
   * @return QuotaConfig object.
   */
  QuotaConfig getQuotaConfig();

  /**
   * Set {@link QuotaMode} for {@link QuotaManager}.
   * @param mode The mode to set
   */
  void setQuotaMode(QuotaMode mode);

  /**
   * Use this method to get the {@link QuotaMode} rather than {@link QuotaConfig#throttlingMode} since the {@link QuotaMode}
   * might be updated by {@link #setQuotaMode}.
   * @return the {@link QuotaMode}. By default, it will return the {@link QuotaMode} from {@link QuotaConfig}.
   */
  QuotaMode getQuotaMode();

  /**
   * Method to shutdown the {@link QuotaManager} and cleanup if required.
   */
  void shutdown();
}

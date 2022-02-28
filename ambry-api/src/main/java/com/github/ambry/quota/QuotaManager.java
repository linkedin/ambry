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
import com.github.ambry.rest.RestRequest;
import java.util.Map;


/**
 * Interface that rest of Ambry relies on, for ensuring quota compliance. Defines methods to conditionally charge usage
 * and provide quota compliance recommendations.
 *
 * In order to ensure compliance for each type of Quota, a {@link QuotaManager} relies on {@link QuotaEnforcer}s and
 * corresponding {@link QuotaSource}s. As such, a {@link QuotaManager} implementation must take care of initializing all
 * the {@link QuotaEnforcer}s. {@link QuotaName} specifies all the quota types that Ambry enforces.
 *
 * The recommendations provided by individual {@link QuotaEnforcer}s are merged using
 * {@link QuotaRecommendationMergePolicy} implementation to provide a unified recommendation to the caller.
 */
public interface QuotaManager {

  /**
   * Method to initialize the {@link QuotaManager}.
   * @throws Exception if the initialization fails due to any exception.
   */
  void init() throws Exception;

  /**
   * Computes the overall quota compliance recommendation for all the types of request quotas supported.
   * @param restRequest {@link RestRequest} object.
   * @return ThrottlingRecommendation object that captures the overall recommendation.
   * @throws QuotaException in case of any exception.
   */
  ThrottlingRecommendation recommend(RestRequest restRequest) throws QuotaException;

  /**
   * Checks if the specified request should be throttled, and charges the specified requestCost against the
   * quota for the {@link QuotaResource}. Charging is done if it is determined that the request should not be throttled,
   * or if the argument forceCharge is set to true. The {@link QuotaResource} to be charged is inferred from the
   * specified restRequest.
   *
   * This method also looks at checkQuotaExceedAllowed argument to check if usage is allowed to exceed quota for the
   * {@link QuotaResource} and makes the check and charge decisions accordingly.
   *
   * @param restRequest {@link RestRequest} object.
   * @param requestCostMap {@link Map} of {@link QuotaName} to the cost incurred to handle the request.
   * @param shouldCheckIfQuotaExceedAllowed if set to {@code true} check if {@link QuotaResource}'s request is allowed
   *                                        to exceed its quota.
   * @param forceCharge if set to {@code true} usage is charged without checking for quota compliance.
   * @return QuotaAction object containing the recommendation for handling the restRequest.
   * @throws QuotaException in case of any exception.
   */
  QuotaAction chargeAndRecommend(RestRequest restRequest, Map<QuotaName, Double> requestCostMap,
      boolean shouldCheckIfQuotaExceedAllowed, boolean forceCharge) throws QuotaException;

  /**
   * @return QuotaConfig object.
   */
  QuotaConfig getQuotaConfig();

  /**
   * @return the {@link QuotaMode} object.
   * Unless modified using the setter, {@link QuotaMode} is determined by {@link QuotaConfig#throttlingMode}.
   */
  QuotaMode getQuotaMode();

  /**
   * Set {@link QuotaMode} for {@link QuotaManager}.
   * @param mode The {@link QuotaMode} object to set.
   */
  void setQuotaMode(QuotaMode mode);

  /**
   * Method to shutdown the {@link QuotaManager} and cleanup if required.
   */
  void shutdown();
}

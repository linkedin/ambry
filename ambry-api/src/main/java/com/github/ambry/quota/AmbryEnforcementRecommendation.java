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
package com.github.ambry.quota;

/**
 * An implementation of {@link EnforcementRecommendation} made by Ambry.
 */
public class AmbryEnforcementRecommendation implements EnforcementRecommendation {
  private final boolean shouldThrottle;
  private final float usagePercentage;
  private final String quotaEnforcerName;
  private final int recommendedHttpStatus;
  private final RequestCost requestCost;

  /**
   * Constructor for {@link AmbryEnforcementRecommendation}.
   * @param shouldThrottle boolean flag indicating throttling recommendation.
   * @param usagePercentage percentage of resource usage.
   * @param quotaEnforcerName name of the enforcement that made the recommendation.
   * @param recommendedHttpStatus recommended http status.
   * @param requestCost {@link RequestCost} of the request.
   */
  public AmbryEnforcementRecommendation(boolean shouldThrottle, float usagePercentage, String quotaEnforcerName,
      int recommendedHttpStatus, RequestCost requestCost) {
    this.shouldThrottle = shouldThrottle;
    this.usagePercentage = usagePercentage;
    this.quotaEnforcerName = quotaEnforcerName;
    this.recommendedHttpStatus = recommendedHttpStatus;
    this.requestCost = requestCost;
  }

  @Override
  public boolean shouldThrottle() {
    return shouldThrottle;
  }

  @Override
  public float getQuotaUsagePercentage() {
    return usagePercentage;
  }

  @Override
  public String getQuotaEnforcerName() {
    return quotaEnforcerName;
  }

  @Override
  public int getRecommendedHttpStatus() {
    return recommendedHttpStatus;
  }

  @Override
  public RequestCost getRequestCost() {
    return requestCost;
  }
}

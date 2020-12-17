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

public class AmbryEnforcementRecommendation implements EnforcementRecommendation {
  private final boolean shouldThrottle;
  private final float usagePercentage;
  private final String quotaEnforcerName;
  private final int recommendedHttpStatus;

  public AmbryEnforcementRecommendation(boolean shouldThrottle, float usagePercentage, String quotaEnforcerName,
      int recommendedHttpStatus) {
    this.shouldThrottle = shouldThrottle;
    this.usagePercentage = usagePercentage;
    this.quotaEnforcerName = quotaEnforcerName;
    this.recommendedHttpStatus = recommendedHttpStatus;
  }

  @Override
  public boolean shouldThrottle() {
    return shouldThrottle;
  }

  @Override
  public float quotaUsagePercentage() {
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
}

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
 * Interface representing enforcement recommendation.
 */
public interface EnforcementRecommendation {

  /**
   * Boolean recommendation to throttle or not.
   * @return true if recommendation is to throttle. false otherwise.
   */
  boolean shouldThrottle();

  /**
   * Estimation of percentage of quota in use when the recommendation was made.
   * @return percentage value between 0 and 100.
   */
  float quotaUsagePercentage();

  /**
   * @return name of the enforcer that created this recommendation.
   */
  String getQuotaEnforcerName();

  /**
   * @return http status recommended by enforcer.
   */
  int getRecommendedHttpStatus();
}

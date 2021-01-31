/**
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

import java.util.Map;


public interface ThrottlingRecommendation {

  /**
   * @return true if recommendation is to throttle. false otherwise.
   */
  boolean shouldThrottle();

  /**
   * @return A {@link Map} of quota  name and estimation of percentage of quota in use when the recommendation was made.
   */
  Map<QuotaName, Float> getQuotaUsagePercentage();

  /**
   * @return http status recommended.
   */
  int getRecommendedHttpStatus();

  /**
   * @return A {@link Map} of quota name and cost value for serving the request.
   */
  Map<QuotaName, Double> getRequestCost();

  /**
   * @return the time interval in milliseconds after the request can be retried.
   * If request is not throttled then returns 0.
   */
  long getRetryAfterMs();
}

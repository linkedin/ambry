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
 * Interface representing enforcement recommendation made by a {@link RequestQuotaEnforcer} or {@link HostQuotaEnforcer}
 * implementation. QuotaEnforcer implementations can use this object to provide a boolean recommendation to throttle the
 * request or not, along with usage information like usage percentage, name of the enforcer that made this recommendation,
 * the recommended http status (indicating whether or not throttled request should be retried) and cost to serve the request.
 */
public interface EnforcementRecommendation {

  /**
   * @return true if recommendation is to throttle. false otherwise.
   */
  boolean shouldThrottle();

  /**
   * @return estimation of percentage of quota in use when the recommendation was made.
   */
  float getQuotaUsagePercentage();

  /**
   * @return name of the enforcer that created this recommendation.
   */
  String getQuotaEnforcerName();

  /**
   * @return http status recommended by enforcer.
   */
  int getRecommendedHttpStatus();

  /**
   * @return the {@link RequestCost} for serving the request.
   */
  RequestCost getRequestCost();
}

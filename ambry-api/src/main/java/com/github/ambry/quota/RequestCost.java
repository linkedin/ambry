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

import com.github.ambry.quota.storage.QuotaOperation;
import java.util.HashMap;
import java.util.Map;


/**
 * Class representing cost of a request for a resource.
 */
public class RequestCost {
  private final Map<QuotaMetric, Double> costMap;
  private final QuotaResource quotaResource;
  private final QuotaOperation quotaOperation;

  /**
   * Constructor of {@link RequestCost}.
   * @param quotaResource {@link QuotaResource} specifying the resource for which cost should be applied.
   * @param quotaOperation {@link QuotaOperation} object.
   */
  public RequestCost(QuotaResource quotaResource, QuotaOperation quotaOperation) {
    this.costMap = new HashMap<>();
    this.quotaResource = quotaResource;
    this.quotaOperation = quotaOperation;
  }

  /**
   * @return cost of the request.
   */
  public double getCost(QuotaMetric quotaMetric) {
    return costMap.getOrDefault(quotaMetric, 0.0);
  }

  /**
   * Set the cost of the quota along with its specified metric.
   * @param cost the cost incurred in serving the request.
   * @param quotaMetric {@link QuotaMetric} in which the cost is measured.
   */
  public void setCost(double cost, QuotaMetric quotaMetric) {
    costMap.put(quotaMetric, cost);
  }

  /**
   * @return QuotaResource object.
   */
  public QuotaResource getQuotaResource() {
    return quotaResource;
  }

  /**
   * @return QuotaOperation object.
   */
  public QuotaOperation getQuotaOperation() {
    return quotaOperation;
  }
}

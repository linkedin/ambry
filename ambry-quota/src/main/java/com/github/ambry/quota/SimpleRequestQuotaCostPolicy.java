/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import java.util.HashMap;
import java.util.Map;


/**
 * {@link RequestQuotaCostPolicy} implementation that calculates request cost in terms of metrics tracked by user quota - capacity unit and storage.
 * Capacity unit cost is the as the number of config defined units in each chunk. Storage cost is defined as number of GB of storage used.
 */
public class SimpleRequestQuotaCostPolicy implements RequestQuotaCostPolicy {
  private static final long DEFAULT_MIN_QUOTA_CHARGE = 1;

  private final QuotaConfig quotaConfig;

  public SimpleRequestQuotaCostPolicy(QuotaConfig quotaConfig) {
    this.quotaConfig = quotaConfig;
  }

  @Override
  public Map<String, Double> calculateRequestQuotaCharge(RestRequest restRequest, long size) {
    Map<String, Double> costMap = new HashMap<>();
    double capacityUnitCost =
        Math.max(Math.ceil(size / (double) quotaConfig.quotaAccountingUnit), DEFAULT_MIN_QUOTA_CHARGE);
    costMap.put(QuotaUtils.getCUQuotaName(restRequest).name(), capacityUnitCost);
    costMap.put(QuotaName.STORAGE_IN_GB.name(), QuotaUtils.calculateStorageCost(restRequest, size));
    return costMap;
  }
}

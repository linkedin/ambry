/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.RejectThrottler;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;


/**
 * A class to manage requests based on request context.
 */
public class QuotaManager {
  private final Map<RestMethod, RejectThrottler> quotaMap;

  public QuotaManager(FrontendConfig frontendConfig) {
    JSONObject quota = new JSONObject(frontendConfig.restRequestQuota);
    quotaMap = new HashMap<>();
    for (RestMethod restMethod : RestMethod.values()) {
      quotaMap.put(restMethod, new RejectThrottler(quota.optInt(restMethod.name(), -1)));
    }
  }

  public QuotaManager(Map<RestMethod, RejectThrottler> quotaMap) {
    this.quotaMap = quotaMap;
  }

  /**
   * Return {@code true} if throttling is required. Return {@code false} if no throttler for this rest method or quota
   * is not reached.
   * @param restRequest provides the information.
   */
  public boolean shouldThrottle(RestRequest restRequest) {
    RejectThrottler throttler = quotaMap.get(restRequest.getRestMethod());
    return throttler != null && throttler.shouldThrottle(1);
  }
}

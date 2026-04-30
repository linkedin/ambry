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
package com.github.ambry.commons;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.RejectThrottler;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to decide if a request should be throttled.
 */
public class HostLevelThrottler {
  private static final Logger logger = LoggerFactory.getLogger(HostLevelThrottler.class);
  private final Map<RestMethod, RejectThrottler> quotaMap;
  private final Map<HardwareResource, Criteria> hardwareThresholdMap;
  private final HardwareUsageMeter hardwareUsageMeter;

  public HostLevelThrottler(HostThrottleConfig hostThrottleConfig) {
    this.hardwareUsageMeter =
        new HardwareUsageMeter(hostThrottleConfig.cpuSamplingPeriodMs, hostThrottleConfig.memorySamplingPeriodMs);
    JSONObject quota = new JSONObject(hostThrottleConfig.restRequestQuota);
    quotaMap = new HashMap<>();
    for (RestMethod restMethod : RestMethod.values()) {
      quotaMap.put(restMethod, new RejectThrottler(quota.optInt(restMethod.name(), -1)));
    }
    hardwareThresholdMap = getHardwareThresholdMap(hostThrottleConfig.hardwareThresholds);
    logger.info("Host throttling config: {} {}", quotaMap, hardwareThresholdMap);
  }

  // Test Constructor
  public HostLevelThrottler(Map<RestMethod, RejectThrottler> quotaMap,
      Map<HardwareResource, Criteria> hardwareThresholdMap, HardwareUsageMeter hardwareUsageMeter) {
    this.quotaMap = quotaMap;
    this.hardwareThresholdMap = hardwareThresholdMap;
    this.hardwareUsageMeter = hardwareUsageMeter;
  }

  /**
   * Return {@code true} if throttling is required. Return {@code false} if no throttler for this rest method or quota
   * is not reached.
   * @param restRequest provides the information.
   */
  public boolean shouldThrottle(RestRequest restRequest) {
    boolean shouldThrottle;
    // Check quotaMap
    RejectThrottler throttler = quotaMap.get(restRequest.getRestMethod());
    shouldThrottle = (throttler != null && throttler.shouldThrottle(1));
    // Check hardwareThresholdMap
    for (HardwareResource hardwareResource : HardwareResource.values()) {
      if (!shouldThrottle && hardwareThresholdMap.containsKey(hardwareResource)) {
        shouldThrottle = !hardwareThresholdMap.get(hardwareResource)
            .meetRequirement((long) hardwareUsageMeter.getHardwareResourcePercentage(hardwareResource));
        if (shouldThrottle) {
          logger.warn("Throttle because of {} : {}%", hardwareResource,
              hardwareUsageMeter.getHardwareResourcePercentage(hardwareResource));
        }
      }
    }
    return shouldThrottle;
  }

  /**
   * Return a HardwareThresholdMap by parsing input string which in JsonObject format.
   * @param hardwareThresholdsString hardwareThresholds string.
   */
  static Map<HardwareResource, Criteria> getHardwareThresholdMap(String hardwareThresholdsString) {
    JSONObject thresholds = new JSONObject(hardwareThresholdsString);
    Map<HardwareResource, Criteria> hardwareThresholdMap = new HashMap<>();
    // All criteria should be satisfied.
    // Future work: if multiple throttling rules under "OR" condition needed, a list of thresholds/hardwareThresholdMap can be used.
    for (HardwareResource hardwareResource : HardwareResource.values()) {
      if (thresholds.has(hardwareResource.name())) {
        String jsonString = thresholds.getJSONObject(hardwareResource.name()).toString();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
          Criteria criteria = objectMapper.readValue(jsonString, Criteria.class);
          hardwareThresholdMap.put(hardwareResource, criteria);
        } catch (IOException e) {
          logger.error("Error while parsing {} threshold ", hardwareResource.name(), e);
          continue;
        }
      }
    }
    return hardwareThresholdMap;
  }
}

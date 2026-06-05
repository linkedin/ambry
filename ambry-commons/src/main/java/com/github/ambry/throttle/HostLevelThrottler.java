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
package com.github.ambry.throttle;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.commons.Criteria;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.JsonUtil;
import com.github.ambry.utils.RejectThrottler;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to decide if a request should be host-level throttled. Operating modes are defined by
 * {@link ThrottleMode}.
 *
 * <p>In {@code TRACK} / {@code ENFORCE} two trigger families are checked (first match wins):
 * <ol>
 *   <li>{@code restMethodCap.<RestMethod>}: the configured per-{@link RestMethod} QPS cap in
 *       {@link HostThrottleConfig#restRequestQuota} is exceeded.</li>
 *   <li>{@code hardwareThreshold.<HardwareResource>}: the measured percentage falls outside the configured
 *       {@link Criteria} threshold in {@link HostThrottleConfig#hardwareThresholds} (e.g.
 *       {@code DIRECT_MEMORY > 85%}).</li>
 * </ol>
 */
public class HostLevelThrottler {
  /** Trigger family name for the per-{@link RestMethod} QPS cap branch. */
  static final String TRIGGER_REST_METHOD_CAP = "restMethodCap";
  /** Trigger family name for the per-{@link HardwareResource} threshold branch. */
  static final String TRIGGER_HARDWARE_THRESHOLD = "hardwareThreshold";
  private static final String WOULD_THROTTLE_METRIC_BASE = "wouldThrottle";
  private static final String THROTTLED_METRIC_BASE = "throttled";

  private static final Logger logger = LoggerFactory.getLogger(HostLevelThrottler.class);

  private final ThrottleMode mode;
  private final Map<RestMethod, RejectThrottler> quotaMap;
  private final Map<HardwareResource, Criteria> hardwareThresholdMap;
  private final HardwareUsageMeter hardwareUsageMeter;
  private final Map<String, Meter> wouldThrottleMeters;
  private final Map<String, Meter> throttledMeters;

  /**
   * Production constructor. Reads {@link HostThrottleConfig} and registers per-trigger meters on the
   * provided {@link MetricRegistry}.
   */
  public HostLevelThrottler(HostThrottleConfig hostThrottleConfig, MetricRegistry metricRegistry) {
    this(hostThrottleConfig,
        new HardwareUsageMeter(hostThrottleConfig.cpuSamplingPeriodMs, hostThrottleConfig.memorySamplingPeriodMs),
        buildQuotaMap(hostThrottleConfig.restRequestQuota), metricRegistry);
  }

  /**
   * Package-private test constructor that lets tests inject a mocked {@link HardwareUsageMeter} and a
   * pre-built per-method {@link RejectThrottler} map (e.g. with a {@code MockClock}-backed
   * {@link com.codahale.metrics.Meter}).
   */
  HostLevelThrottler(HostThrottleConfig hostThrottleConfig, HardwareUsageMeter hardwareUsageMeter,
      Map<RestMethod, RejectThrottler> quotaMap, MetricRegistry metricRegistry) {
    this.mode = hostThrottleConfig.mode;
    this.hardwareUsageMeter = hardwareUsageMeter;
    this.quotaMap = quotaMap;
    this.hardwareThresholdMap = getHardwareThresholdMap(hostThrottleConfig.hardwareThresholds);
    this.wouldThrottleMeters = registerTriggerMeters(metricRegistry, WOULD_THROTTLE_METRIC_BASE);
    this.throttledMeters = registerTriggerMeters(metricRegistry, THROTTLED_METRIC_BASE);
    logger.info("HostLevelThrottler initialized: mode={} quotaMap={} hardwareThresholdMap={}", mode, quotaMap,
        hardwareThresholdMap);
  }

  /**
   * @param restRequest the {@link RestRequest} to evaluate.
   * @return {@code true} if the request should be rejected (only possible when mode == {@code ENFORCE}).
   */
  public boolean shouldThrottle(RestRequest restRequest) {
    if (mode == ThrottleMode.OFF) {
      return false;
    }
    String triggerFired = null;
    RestMethod restMethod = restRequest.getRestMethod();
    RejectThrottler perMethodThrottler = quotaMap.get(restMethod);
    if (perMethodThrottler != null && perMethodThrottler.shouldThrottle(1)) {
      triggerFired = TRIGGER_REST_METHOD_CAP + "." + restMethod.name();
    }
    if (triggerFired == null) {
      for (HardwareResource hardwareResource : HardwareResource.values()) {
        Criteria criteria = hardwareThresholdMap.get(hardwareResource);
        if (criteria == null) {
          continue;
        }
        int observed = hardwareUsageMeter.getHardwareResourcePercentage(hardwareResource);
        if (!meetsRequirement(criteria, observed)) {
          triggerFired = TRIGGER_HARDWARE_THRESHOLD + "." + hardwareResource.name();
          logger.warn("HostLevelThrottler drop on {}: observed={}%", hardwareResource, observed);
          break;
        }
      }
    }
    if (triggerFired == null) {
      return false;
    }
    Meter wouldThrottleMeter = wouldThrottleMeters.get(triggerFired);
    if (wouldThrottleMeter != null) {
      wouldThrottleMeter.mark();
    }
    if (mode == ThrottleMode.ENFORCE) {
      Meter throttledMeter = throttledMeters.get(triggerFired);
      if (throttledMeter != null) {
        throttledMeter.mark();
      }
      return true;
    }
    return false;
  }

  /**
   * Parse the per-{@link RestMethod} QPS quota JSON string from {@link HostThrottleConfig#restRequestQuota}
   * into a {@link RejectThrottler} map. Missing methods get a {@code -1} cap (disabled).
   */
  private static Map<RestMethod, RejectThrottler> buildQuotaMap(String restRequestQuotaJson) {
    JSONObject quota = new JSONObject(restRequestQuotaJson);
    Map<RestMethod, RejectThrottler> quotaMap = new HashMap<>();
    for (RestMethod restMethod : RestMethod.values()) {
      quotaMap.put(restMethod, new RejectThrottler(quota.optInt(restMethod.name(), -1)));
    }
    return quotaMap;
  }

  /**
   * Eagerly register per-trigger meters on the given {@link MetricRegistry}, one per
   * {@code (family, sub-key)} pair:
   * <ul>
   *   <li>{@code <base>.restMethodCap.<RestMethod>} for every {@link RestMethod} value.</li>
   *   <li>{@code <base>.hardwareThreshold.<HardwareResource>} for every {@link HardwareResource} value.</li>
   * </ul>
   * Meters are registered up front so subsequent config updates can change which sub-key fires
   * without mutating the metric registry. {@link MetricRegistry#meter(String)} is idempotent by
   * name, so the {@code wouldThrottle} and {@code throttled} families can safely share the same
   * registry.
   */
  private static Map<String, Meter> registerTriggerMeters(MetricRegistry metricRegistry, String metricBase) {
    Map<String, Meter> result = new HashMap<>();
    for (RestMethod method : RestMethod.values()) {
      String key = TRIGGER_REST_METHOD_CAP + "." + method.name();
      result.put(key,
          metricRegistry.meter(MetricRegistry.name(HostLevelThrottler.class, metricBase, TRIGGER_REST_METHOD_CAP,
              method.name())));
    }
    for (HardwareResource resource : HardwareResource.values()) {
      String key = TRIGGER_HARDWARE_THRESHOLD + "." + resource.name();
      result.put(key,
          metricRegistry.meter(MetricRegistry.name(HostLevelThrottler.class, metricBase, TRIGGER_HARDWARE_THRESHOLD,
              resource.name())));
    }
    return result;
  }

  /**
   * Evaluate a {@link Criteria} against an observed percentage. {@link Criteria#meetRequirement(Long)}
   * is package-private to {@code com.github.ambry.commons}; this throttler lives in
   * {@code com.github.ambry.throttle}, so we apply the same predicate via the public getters.
   */
  private static boolean meetsRequirement(Criteria criteria, int observed) {
    long threshold = criteria.getThreshold();
    return criteria.getBoundType() == Criteria.BoundType.UpperBound ? observed <= threshold : observed >= threshold;
  }

  /**
   * Parse the hardware threshold JSON string into a typed {@link Criteria} map. Unparseable
   * per-resource blocks are skipped with a warning so an operator-config typo doesn't disable
   * the rest of the thresholds.
   */
  static Map<HardwareResource, Criteria> getHardwareThresholdMap(String hardwareThresholdsString) {
    JSONObject thresholds = new JSONObject(hardwareThresholdsString);
    Map<HardwareResource, Criteria> hardwareThresholdMap = new EnumMap<>(HardwareResource.class);
    // All criteria must be satisfied. If we ever need OR-of-rules, this should become a list.
    for (HardwareResource hardwareResource : HardwareResource.values()) {
      if (thresholds.has(hardwareResource.name())) {
        String jsonString = thresholds.getJSONObject(hardwareResource.name()).toString();
        ObjectMapper objectMapper = JsonUtil.newObjectMapper();
        try {
          Criteria criteria = objectMapper.readValue(jsonString, Criteria.class);
          hardwareThresholdMap.put(hardwareResource, criteria);
        } catch (IOException e) {
          logger.error("Error while parsing {} threshold", hardwareResource.name(), e);
        }
      }
    }
    return hardwareThresholdMap;
  }
}

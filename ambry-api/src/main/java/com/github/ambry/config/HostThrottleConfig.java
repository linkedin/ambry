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
package com.github.ambry.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.commons.Criteria;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.throttle.HardwareResource;
import com.github.ambry.throttle.ThrottleMode;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;


/**
 * Config for Host Level Throttling.
 */
public class HostThrottleConfig {
  private static final String PREFIX = "host.throttle.";
  public static final String REST_REQUEST_QUOTA_STRING = PREFIX + "rest.request.quota";
  public static final String HARDWARE_THRESHOLDS = PREFIX + "hardware.thresholds";
  public static final String CPU_SAMPLING_PERIOD_MS = PREFIX + "cpu.sampling.period.ms";
  public static final String MEMORY_SAMPLING_PERIOD_MS = PREFIX + "memory.sampling.period.ms";
  public static final String MODE = PREFIX + "mode";

  // Default: empty map => every method is uncapped (consumers use getOrDefault(method, -1L)).
  // Per-method values: cap < 0 = uncapped; cap = 0 = kill switch (drop every request for that method);
  // cap > 0 = normal rate cap with fair-share over-share drop.
  private static final Map<RestMethod, Long> DEFAULT_REST_REQUEST_QUOTA = Collections.emptyMap();

  // Default: every hardware resource has an unreachable UpperBound (101%). When the config property is unset,
  // no hardware trigger fires, so the throttler is effectively no-op under default config.
  private static final Map<HardwareResource, Criteria> DEFAULT_HARDWARE_THRESHOLDS = buildDefaultHardwareThresholds();

  private static Map<HardwareResource, Criteria> buildDefaultHardwareThresholds() {
    Map<HardwareResource, Criteria> result = new EnumMap<>(HardwareResource.class);
    for (HardwareResource r : HardwareResource.values()) {
      result.put(r, new Criteria(101, Criteria.BoundType.UpperBound));
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * Per-{@link RestMethod} QPS cap. Three values per method are meaningful:
   * <ul>
   *   <li>{@code cap < 0} — uncapped (also the default for methods missing from the JSON).</li>
   *   <li>{@code cap = 0} — kill switch: drop every request for that method.</li>
   *   <li>{@code cap > 0} — normal rate cap with per-namespace fair-share over-share drop.</li>
   * </ul>
   * Parsed from a JSON object keyed by RestMethod name (e.g. {@code {"PUT": 100, "DELETE": 0, ...}}).
   */
  @Config(REST_REQUEST_QUOTA_STRING)
  public final Map<RestMethod, Long> restRequestQuota;

  /**
   * Threshold per {@link HardwareResource}; missing resources are unbounded. Parsed from a JSON object
   * with {@code threshold} (int 0–100) and {@code boundType} ({@code UpperBound}|{@code LowerBound}).
   * LowerBound is silently skipped in fair-share throttling.
   */
  @Config(HARDWARE_THRESHOLDS)
  public final Map<HardwareResource, Criteria> hardwareThresholds;

  /**
   * The sampling period for CPU usage query
   */
  @Config(CPU_SAMPLING_PERIOD_MS)
  @Default("100")
  public int cpuSamplingPeriodMs;

  /**
   * The sampling period for memory usage query
   */
  @Config(MEMORY_SAMPLING_PERIOD_MS)
  @Default("100")
  public int memorySamplingPeriodMs;

  /**
   * Mode for HostLevelThrottler. One of {@code OFF}, {@code TRACK}, {@code ENFORCE}.
   * {@code OFF} disables the throttler; every request passes.
   * {@code TRACK} accounts per-(method, account, container) fair-share and emits "wouldThrottle" meters but never throttles.
   * {@code ENFORCE} does the accounting and throttles over-share callers when a trigger fires.
   */
  @Config(MODE)
  @Default("OFF")
  public final ThrottleMode mode;

  public HostThrottleConfig(VerifiableProperties verifiableProperties) {
    String quotaJson = verifiableProperties.getString(REST_REQUEST_QUOTA_STRING, null);
    restRequestQuota = (quotaJson == null) ? DEFAULT_REST_REQUEST_QUOTA
        : parseEnumMap(quotaJson, REST_REQUEST_QUOTA_STRING, new TypeReference<EnumMap<RestMethod, Long>>() { });
    String thresholdsJson = verifiableProperties.getString(HARDWARE_THRESHOLDS, null);
    hardwareThresholds = (thresholdsJson == null) ? DEFAULT_HARDWARE_THRESHOLDS
        : parseEnumMap(thresholdsJson, HARDWARE_THRESHOLDS,
            new TypeReference<EnumMap<HardwareResource, Criteria>>() { });
    cpuSamplingPeriodMs = verifiableProperties.getInt(CPU_SAMPLING_PERIOD_MS, 100);
    memorySamplingPeriodMs = verifiableProperties.getInt(MEMORY_SAMPLING_PERIOD_MS, 100);
    mode = ThrottleMode.valueOf(verifiableProperties.getString(MODE, ThrottleMode.OFF.name()));
  }

  /**
   * Parse a JSON object keyed by an enum into an unmodifiable EnumMap. Throws {@link IllegalArgumentException}
   * (fail-fast at config load) on malformed JSON or unknown enum keys.
   */
  private static <K extends Enum<K>, V> Map<K, V> parseEnumMap(String json, String configKey,
      TypeReference<EnumMap<K, V>> typeRef) {
    try {
      return Collections.unmodifiableMap(new ObjectMapper().readValue(json, typeRef));
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse " + configKey + ": " + json, e);
    }
  }
}

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

import com.github.ambry.throttle.ThrottleMode;
import org.json.JSONObject;


/**
 * Config for Host Level Throttling.
 */
public class HostThrottleConfig {
  private static final String PREFIX = "host.throttle.";
  public static final String MODE = PREFIX + "mode";
  public static final String REST_REQUEST_QUOTA_STRING = PREFIX + "rest.request.quota";
  public static final String HARDWARE_THRESHOLDS = PREFIX + "hardware.thresholds";
  public static final String CPU_SAMPLING_PERIOD_MS = PREFIX + "cpu.sampling.period.ms";
  public static final String MEMORY_SAMPLING_PERIOD_MS = PREFIX + "memory.sampling.period.ms";

  private static final String DEFAULT_REST_REQUEST_QUOTA_STRING = new JSONObject().put("PUT", -1)
      .put("PUT", -1)
      .put("GET", -1)
      .put("POST", -1)
      .put("HEAD", -1)
      .put("OPTIONS", -1)
      .put("DELETE", -1)
      .put("UNKNOWN", -1)
      .toString();
  /**
   * A better looking sample:
   *   {
   *   "HEAP_MEMORY": {
   *     "threshold": 90,
   *     "boundType": "UpperBound"
   *   },
   *   {
   *   "DIRECT_MEMORY": {
   *     "threshold": 90,
   *     "boundType": "UpperBound"
   *   },
   *   "CPU": {
   *     "threshold": 95,
   *     "boundType": "UpperBound"
   *   }
   * }
   */
  private static final String DEFAULT_HARDWARE_THRESHOLDS_STRING =
      new JSONObject().put("HEAP_MEMORY", new JSONObject().put("threshold", 101).put("boundType", "UpperBound"))
          .put("DIRECT_MEMORY", new JSONObject().put("threshold", 101).put("boundType", "UpperBound"))
          .put("CPU", new JSONObject().put("threshold", 101).put("boundType", "UpperBound"))
          .toString();

  /**
   * Operating mode for the host-level throttler. Default {@code OFF} keeps the throttler silently
   * disabled out-of-the-box; operators opt in per-fabric via config.
   */
  @Config(MODE)
  @Default("OFF")
  public final ThrottleMode mode;

  /**
   * Quotas for rest requests, in JSON string.
   * Default value: DEFAULT_REST_REQUEST_QUOTA_STRING
   */
  @Config(REST_REQUEST_QUOTA_STRING)
  public final String restRequestQuota;

  /**
   * Threshold for hardware resource usage, in JSON string.
   * Default value: DEFAULT_REST_REQUEST_QUOTA_STRING
   */
  @Config(HARDWARE_THRESHOLDS)
  public final String hardwareThresholds;

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

  public HostThrottleConfig(VerifiableProperties verifiableProperties) {
    // Fail-fast on invalid mode strings via the IllegalArgumentException thrown by Enum.valueOf —
    // a typo in the config property should surface at startup, not silently degrade to OFF.
    mode = ThrottleMode.valueOf(verifiableProperties.getString(MODE, ThrottleMode.OFF.name()));
    restRequestQuota = verifiableProperties.getString(REST_REQUEST_QUOTA_STRING, DEFAULT_REST_REQUEST_QUOTA_STRING);
    hardwareThresholds = verifiableProperties.getString(HARDWARE_THRESHOLDS, DEFAULT_HARDWARE_THRESHOLDS_STRING);
    cpuSamplingPeriodMs = verifiableProperties.getInt(CPU_SAMPLING_PERIOD_MS, 100);
    memorySamplingPeriodMs = verifiableProperties.getInt(MEMORY_SAMPLING_PERIOD_MS, 100);
  }
}

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
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.utils.MockClock;
import com.github.ambry.utils.RejectThrottler;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests for {@link HostLevelThrottler} after the {@link ThrottleMode} knob and per-trigger metric
 * scaffolding were introduced. The drop algorithm itself is unchanged from the legacy binary
 * throttler — these tests cover mode wrapping and meter emission.
 */
public class HostLevelThrottlerTest {
  private static final String METRIC_PREFIX = "com.github.ambry.throttle.HostLevelThrottler.";
  private static final String WOULD_THROTTLE_METHOD_CAP_GET =
      METRIC_PREFIX + "wouldThrottle.restMethodCap." + RestMethod.GET.name();
  private static final String THROTTLED_METHOD_CAP_GET =
      METRIC_PREFIX + "throttled.restMethodCap." + RestMethod.GET.name();
  private static final String WOULD_THROTTLE_DIRECT_MEMORY =
      METRIC_PREFIX + "wouldThrottle.hardwareThreshold." + HardwareResource.DIRECT_MEMORY.name();
  private static final String THROTTLED_DIRECT_MEMORY =
      METRIC_PREFIX + "throttled.hardwareThreshold." + HardwareResource.DIRECT_MEMORY.name();

  @Test
  public void defaultModeIsOff() {
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(new Properties()));
    Assert.assertEquals("Default mode should be OFF", ThrottleMode.OFF, config.mode);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidModeFailsFastAtConfigLoad() {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, "BOGUS");
    new HostThrottleConfig(new VerifiableProperties(props));
  }

  @Test
  public void offModeAdmitsAllTrafficEvenWhenAlgorithmWouldDrop() throws Exception {
    // Per-method cap = 1 (effectively always-drop after a brief burst) AND HW threshold = 10%
    // (mock returns 99%). With mode=OFF the throttler must short-circuit and return false.
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.OFF.name());
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING,
        new JSONObject().put("GET", 1).put("PUT", 1).put("POST", 1).put("HEAD", 1).put("DELETE", 1).toString());
    props.setProperty(HostThrottleConfig.HARDWARE_THRESHOLDS, hardwareThresholdsJson(10));
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HardwareUsageMeter mockMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(mockMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(99);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, mockMeter, buildQuotaMap(config.restRequestQuota, new MockClock()),
            metricRegistry);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("OFF mode must never throttle",
          throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    }
    // Mocked HW meter must not even have been queried.
    Mockito.verify(mockMeter, Mockito.never()).getHardwareResourcePercentage(Mockito.any());
  }

  @Test
  public void trackModeMarksWouldThrottleButReturnsFalse() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.TRACK.name());
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING,
        new JSONObject().put("GET", 20).toString());
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    MockClock clock = new MockClock();
    HostLevelThrottler throttler = new HostLevelThrottler(config, Mockito.mock(HardwareUsageMeter.class),
        buildQuotaMap(config.restRequestQuota, clock), metricRegistry);

    // Issue 100 GETs at "tick=0" — Meter.getOneMinuteRate() is 0, so nothing fires yet.
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("TRACK mode never returns true",
          throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    }
    // Advance the clock 5s so the meter's one-minute rate climbs above the cap of 20.
    clock.tick(5);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("TRACK mode never returns true",
          throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    }
    Assert.assertTrue("TRACK mode must mark wouldThrottle.restMethodCap.GET on drop conditions",
        metricRegistry.meter(WOULD_THROTTLE_METHOD_CAP_GET).getCount() > 0);
    Assert.assertEquals("TRACK mode must not mark throttled.* on drop conditions", 0,
        metricRegistry.meter(THROTTLED_METHOD_CAP_GET).getCount());
  }

  @Test
  public void enforceModeMarksBothMetersAndReturnsTrueOnMethodCap() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.ENFORCE.name());
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING,
        new JSONObject().put("GET", 20).toString());
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    MockClock clock = new MockClock();
    HostLevelThrottler throttler = new HostLevelThrottler(config, Mockito.mock(HardwareUsageMeter.class),
        buildQuotaMap(config.restRequestQuota, clock), metricRegistry);

    // Tick=0: rate is 0, nothing fires.
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("Should not throttle yet", throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    }
    clock.tick(5);
    int rejections = 0;
    for (int i = 0; i < 50; i++) {
      if (throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/"))) {
        rejections++;
      }
    }
    Assert.assertTrue("ENFORCE mode must drop some requests once rate exceeds cap", rejections > 0);
    Assert.assertEquals("wouldThrottle.restMethodCap.GET count must match rejection count in ENFORCE", rejections,
        metricRegistry.meter(WOULD_THROTTLE_METHOD_CAP_GET).getCount());
    Assert.assertEquals("throttled.restMethodCap.GET count must match rejection count in ENFORCE", rejections,
        metricRegistry.meter(THROTTLED_METHOD_CAP_GET).getCount());
  }

  @Test
  public void enforceModeReturnsTrueOnHardwareThresholdBreach() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.ENFORCE.name());
    props.setProperty(HostThrottleConfig.HARDWARE_THRESHOLDS, hardwareThresholdsJson(10));
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HardwareUsageMeter mockMeter = Mockito.mock(HardwareUsageMeter.class);
    // CPU and HEAP under threshold, DIRECT_MEMORY over.
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(5);
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(5);
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(99);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, mockMeter, buildQuotaMap(config.restRequestQuota, new MockClock()),
            metricRegistry);

    Assert.assertTrue("ENFORCE mode must drop on DIRECT_MEMORY breach",
        throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    Assert.assertEquals("wouldThrottle.hardwareThreshold.DIRECT_MEMORY must be marked", 1,
        metricRegistry.meter(WOULD_THROTTLE_DIRECT_MEMORY).getCount());
    Assert.assertEquals("throttled.hardwareThreshold.DIRECT_MEMORY must be marked in ENFORCE", 1,
        metricRegistry.meter(THROTTLED_DIRECT_MEMORY).getCount());
  }

  @Test
  public void trackModeMarksWouldThrottleOnHardwareBreachButReturnsFalse() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.TRACK.name());
    props.setProperty(HostThrottleConfig.HARDWARE_THRESHOLDS, hardwareThresholdsJson(10));
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HardwareUsageMeter mockMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(5);
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(5);
    Mockito.when(mockMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(99);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, mockMeter, buildQuotaMap(config.restRequestQuota, new MockClock()),
            metricRegistry);

    Assert.assertFalse("TRACK mode must never return true",
        throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    Assert.assertEquals("wouldThrottle.hardwareThreshold.DIRECT_MEMORY must be marked in TRACK", 1,
        metricRegistry.meter(WOULD_THROTTLE_DIRECT_MEMORY).getCount());
    Assert.assertEquals("throttled.hardwareThreshold.DIRECT_MEMORY must NOT be marked in TRACK", 0,
        metricRegistry.meter(THROTTLED_DIRECT_MEMORY).getCount());
  }

  @Test
  public void enforceModeAdmitsTrafficWhenAlgorithmDoesNotFire() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, ThrottleMode.ENFORCE.name());
    // No caps configured, no hardware thresholds either (DEFAULT thresholds are 101% UpperBound).
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    MetricRegistry metricRegistry = new MetricRegistry();
    HardwareUsageMeter mockMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(mockMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(50);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, mockMeter, buildQuotaMap(config.restRequestQuota, new MockClock()),
            metricRegistry);

    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("ENFORCE mode must admit when no trigger fires",
          throttler.shouldThrottle(createRestRequest(RestMethod.GET, "/")));
    }
  }

  /** Helper: build a per-{@link RestMethod} {@link RejectThrottler} map using a controllable clock. */
  private static Map<RestMethod, RejectThrottler> buildQuotaMap(String restRequestQuotaJson, MockClock clock) {
    JSONObject quota = new JSONObject(restRequestQuotaJson);
    Map<RestMethod, RejectThrottler> quotaMap = new HashMap<>();
    for (RestMethod restMethod : RestMethod.values()) {
      int cap = quota.optInt(restMethod.name(), -1);
      quotaMap.put(restMethod, new RejectThrottler(cap, new Meter(clock)));
    }
    return quotaMap;
  }

  /** Helper: build a hardware thresholds JSON string where every resource has the given UpperBound. */
  private static String hardwareThresholdsJson(int upperBoundPercent) {
    return new JSONObject().put(HardwareResource.HEAP_MEMORY.name(),
            new JSONObject().put("threshold", upperBoundPercent).put("boundType", "UpperBound"))
        .put(HardwareResource.DIRECT_MEMORY.name(),
            new JSONObject().put("threshold", upperBoundPercent).put("boundType", "UpperBound"))
        .put(HardwareResource.CPU.name(),
            new JSONObject().put("threshold", upperBoundPercent).put("boundType", "UpperBound"))
        .toString();
  }

  /** Helper: build a {@link MockRestRequest} for the given method + URI. */
  static RestRequest createRestRequest(RestMethod restMethod, String uri)
      throws UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, uri);
    return new MockRestRequest(request, null);
  }
}

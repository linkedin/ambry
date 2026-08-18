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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.commons.Criteria;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.MockClock;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Tests for {@link HostLevelThrottler} after the hardware threshold branch was replaced with per-namespace
 * sustainabilityFactor-scaled fair-share dropping (max-wins composition with the per-method cap branch),
 * the caller-visible response was flipped from 429 to 503, and the {@code updateConfig} runtime hook was
 * added for runtime overrides.
 */
public class HostLevelThrottlerTest {

  /**
   * Default {@code host.throttle.mode} must be {@code OFF} so the throttler ships disabled-by-default and
   * operators opt in per-fabric via config.
   */
  @Test
  public void defaultModeIsOff() {
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(new Properties()));
    Assert.assertEquals("Default host.throttle.mode must be OFF.", ThrottleMode.OFF, config.mode);
  }

  /**
   * Invalid {@code host.throttle.mode} must fail at config construction (via {@code Enum.valueOf}) so a
   * typo in the config property is caught at startup rather than silently disabling the throttler.
   */
  @Test(expected = IllegalArgumentException.class)
  public void invalidModeFailsFastAtConfigLoad() {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, "BOGUS");
    new HostThrottleConfig(new VerifiableProperties(props));
  }

  /**
   * In {@code OFF} mode every request is admitted regardless of configured triggers. We set a tight
   * per-method cap and a low hardware threshold so both triggers would fire under TRACK/ENFORCE, then
   * confirm OFF still passes everything — operators flipping a fabric to OFF must get truly no throttling.
   */
  @Test
  public void offModeAdmitsAllTraffic() throws Exception {
    String capsJson = new JSONObject().put("GET", 1).toString();
    HostThrottleConfig config = buildConfig("OFF", capsJson, hardwareThresholds(10, 10, 10));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(99);
    MockClock mockClock = new MockClock();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, new MetricRegistry(), mockClock);
    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 500; i++) {
      Assert.assertFalse("OFF mode must admit every request.", throttler.shouldThrottle(req));
    }
    mockClock.tick(60);
    for (int i = 0; i < 500; i++) {
      Assert.assertFalse("OFF mode must admit every request even after rate builds.", throttler.shouldThrottle(req));
    }
    // OFF mode must short-circuit before ever touching the hardware meter.
    Mockito.verify(hwMeter, Mockito.never()).getHardwareResourcePercentage(Mockito.any());
  }

  /**
   * TRACK mode runs the per-method fair-share algorithm and marks {@code wouldThrottle.restMethodCap.GET} when
   * an over-share namespace would be dropped, but never returns {@code shouldThrottle=true}.
   */
  @Test
  public void trackModeMarksWouldThrottleButReturnsFalse() throws Exception {
    // Cap=10 with one noisy namespace: rate (~16.7/s) > cap (10), fairShare = 10/1 = 10, dropProb ≈ 0.4.
    String capsJson = new JSONObject().put("GET", 10).toString();
    HostThrottleConfig config = buildConfig("TRACK", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      Assert.assertFalse("TRACK mode must not throttle.", throttler.shouldThrottle(req));
    }
    mockClock.tick(60);
    for (int i = 0; i < 1000; i++) {
      Assert.assertFalse("TRACK mode must not throttle even after rate exceeds cap.", throttler.shouldThrottle(req));
    }
    Meter wouldThrottle = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "wouldThrottle", "restMethodCap", RestMethod.GET.name()));
    Meter throttled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Assert.assertTrue("TRACK mode must mark wouldThrottle.restMethodCap.GET when fair-share trigger fires; count was "
        + wouldThrottle.getCount(), wouldThrottle.getCount() > 0);
    Assert.assertEquals("TRACK mode must NEVER mark throttled — that's the ENFORCE-only metric.",
        0, throttled.getCount());
  }

  /**
   * In ENFORCE mode with the restMethodCap branch firing (cap > 0, not kill switch), an over-share namespace
   * for that method must eventually be throttled while an under-share namespace is admitted. Both
   * namespaces share the same RestMethod so {@code activeKeysForMethod} = 2 and fairShare = cap/2.
   */
  @Test
  public void perMethodFairShareDropsOverShareNamespace() throws Exception {
    String capsJson = new JSONObject().put("GET", 10).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest noisyReq = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    RestRequest quietReq = newRequest(RestMethod.GET, ACCOUNT_B, CONTAINER_Y);
    // Build lopsided rates: aggregate (~16.7/s) > cap=10; fairShare = 10/2 = 5; noisy (~16.7/s) > 5,
    // quiet (~0.17/s) << 5.
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(noisyReq);
    }
    for (int i = 0; i < 10; i++) {
      throttler.shouldThrottle(quietReq);
    }
    mockClock.tick(60);

    int noisyDropped = 0;
    int quietDropped = 0;
    int quietAdmitted = 0;
    for (int i = 0; i < 500; i++) {
      if (throttler.shouldThrottle(noisyReq)) {
        noisyDropped++;
      }
      if (throttler.shouldThrottle(quietReq)) {
        quietDropped++;
      } else {
        quietAdmitted++;
      }
    }
    Assert.assertTrue("Noisy namespace should be dropped at least once; got " + noisyDropped, noisyDropped > 0);
    Assert.assertTrue("Quiet namespace should be admitted; got admitted=" + quietAdmitted, quietAdmitted > 0);
    // Quantitative asymmetry: the noisier namespace bears the drop weight, not random/uniform across namespaces.
    Assert.assertTrue("Noisy must be dropped at >=10x the rate of quiet; noisy=" + noisyDropped
        + " quiet=" + quietDropped, noisyDropped >= 10 * (quietDropped + 1));
    Meter throttled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Assert.assertEquals("ENFORCE mode must mark throttled.restMethodCap.GET exactly once per actually-rejected request.",
        noisyDropped + quietDropped, throttled.getCount());
  }

  /**
   * In ENFORCE mode, {@code restRequestQuota = {"GET": 0}} must drop every GET request (kill switch).
   * Matches legacy {@code RejectThrottler} semantics: cap = 0 means "block this method entirely." A typo of
   * 0 instead of -1 (uncapped) would otherwise admit traffic the operator meant to block.
   */
  @Test
  public void perMethodKillSwitchDropsAll() throws Exception {
    String capsJson = new JSONObject().put("GET", 0).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, new MockClock());

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    int dropped = 0;
    for (int i = 0; i < 200; i++) {
      if (throttler.shouldThrottle(req)) {
        dropped++;
      }
    }
    Assert.assertEquals("cap=0 must drop every request.", 200, dropped);
    Meter throttled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Meter wouldThrottle = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "wouldThrottle", "restMethodCap", RestMethod.GET.name()));
    Assert.assertEquals("Every dropped request must mark throttled.restMethodCap.GET.", 200, throttled.getCount());
    Assert.assertEquals("Every dropped request must also mark wouldThrottle.restMethodCap.GET (same drop value drives both).",
        200, wouldThrottle.getCount());
  }

  /**
   * Per-(method, namespace) meters must be separated: a PUT cap=0 kill switch on a namespace must not
   * affect GET decisions for the same namespace. Verifies {@code perMethodNamespaceMeters} maintains
   * independent Caffeine caches per RestMethod, and the cap=0 kill switch is method-scoped.
   */
  @Test
  public void perMethodCapZeroOnOneMethodLeavesOthersAlone() throws Exception {
    // Kill switch on PUT only; GET is not in the JSON ⇒ uncapped.
    String capsJson = new JSONObject().put("PUT", 0).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), new MockClock());

    RestRequest putReq = newRequest(RestMethod.PUT, ACCOUNT_A, CONTAINER_X);
    RestRequest getReq = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    int putDropped = 0;
    for (int i = 0; i < 100; i++) {
      if (throttler.shouldThrottle(putReq)) {
        putDropped++;
      }
    }
    Assert.assertEquals("cap=0 on PUT must drop every PUT.", 100, putDropped);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("GET (uncapped) must pass even when PUT kill switch is active.",
          throttler.shouldThrottle(getReq));
    }
  }

  /**
   * Per-method caches must not bleed across methods. A PUT rate cap with a noisy namespace must drop only
   * PUTs from that namespace — GET on the same namespace must remain admitted.
   */
  @Test
  public void perMethodNamespaceMetersAreSeparated() throws Exception {
    // Cap PUT (not GET). Same namespace does both. PUT rate exceeds cap; GET has no cap, must pass.
    String capsJson = new JSONObject().put("PUT", 10).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MockClock mockClock = new MockClock();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, new MetricRegistry(), mockClock);

    RestRequest putReq = newRequest(RestMethod.PUT, ACCOUNT_A, CONTAINER_X);
    RestRequest getReq = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(putReq);
      throttler.shouldThrottle(getReq);
    }
    mockClock.tick(60);

    int putDropped = 0;
    int getDropped = 0;
    for (int i = 0; i < 500; i++) {
      if (throttler.shouldThrottle(putReq)) {
        putDropped++;
      }
      if (throttler.shouldThrottle(getReq)) {
        getDropped++;
      }
    }
    Assert.assertTrue("PUT must be throttled (over cap); got " + putDropped, putDropped > 0);
    Assert.assertEquals("GET must NOT be throttled — separate per-method meter ⇒ PUT's cap does not bleed into GET.",
        0, getDropped);
  }

  /**
   * Admin/operational endpoints with no resolvable (account, container) — e.g. {@code /accounts},
   * {@code /peers} — must be exempt from fair-share dropping. The throttler short-circuits on UNKNOWN
   * before any accounting marks, so admin traffic doesn't poison the per-method aggregate either.
   */
  @Test
  public void extractNamespaceFallsBackToUnknownForMissingArgs() throws Exception {
    String capsJson = new JSONObject().put("GET", 10).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    // Prime a real-namespace caller so the per-method aggregate sees rate (would otherwise be a no-op).
    RestRequest namespacedReq = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(namespacedReq);
    }
    mockClock.tick(60);

    Meter wouldThrottle = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "wouldThrottle", "restMethodCap", RestMethod.GET.name()));
    long countBefore = wouldThrottle.getCount();

    RestRequest unknownReq = createRestRequest(RestMethod.GET, "https://linkedin.com/accounts");
    // Don't set TARGET_ACCOUNT_KEY / TARGET_CONTAINER_KEY — extractNamespace must return UNKNOWN.
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("UNKNOWN namespace must be exempt from fair-share drop.",
          throttler.shouldThrottle(unknownReq));
    }
    Assert.assertEquals("UNKNOWN traffic must not drive any wouldThrottle accounting — short-circuit precedes marks.",
        countBefore, wouldThrottle.getCount());
    Meter throttled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Assert.assertEquals("UNKNOWN traffic must never be marked as throttled.", 0, throttled.getCount());
  }

  /**
   * Hardware threshold boundary: when {@code observedPercent == threshold}, the branch must NOT fire. The
   * guard at the hardware branch is {@code if (observedPercent <= threshold) continue;} — strict
   * greater-than for firing. Pins the off-by-one against a future refactor.
   */
  @Test
  public void hardwareBoundaryExactlyAtThresholdDoesNotFire() throws Exception {
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 50));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(10);
    // Exactly at threshold — must NOT fire.
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(50);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), new MockClock());

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("observed == threshold must not fire the hardware branch.",
          throttler.shouldThrottle(req));
    }
  }

  /**
   * When no trigger is configured (no caps + thresholds at 101) the throttler must pass every request even
   * in ENFORCE mode. Guards the proportional-drop branches behind the trigger conditions.
   */
  @Test
  public void enforceModeWithNoTriggerNeverThrottles() throws Exception {
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MockClock mockClock = new MockClock();
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), mockClock);
    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(req);
    }
    mockClock.tick(60);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("No trigger configured ⇒ ENFORCE must pass.", throttler.shouldThrottle(req));
    }
  }

  // ---------- Hardware fair-share branch (new in this commit) ----------

  /**
   * Hardware fair-share: when DIRECT_MEMORY is over threshold, sustainabilityFactor = threshold/observed
   * scales the host-wide rate down to the sustainable target. Per-namespace fair-share = (hostTotal *
   * sustainabilityFactor) / activeNamespaces. The over-share namespace bears the drop weight while
   * under-share namespaces are admitted.
   */
  @Test
  public void hardwareFairShareDropsOverShareNamespace() throws Exception {
    // observed=95, threshold=80 ⇒ sustainabilityFactor ≈ 0.842. With one noisy + two quiet namespaces,
    // the noisy one dominates hostTotal and ends up over its fair share; the two quiet ones stay under.
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 80));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(95);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest noisyReq = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    RestRequest quietReqB = newRequest(RestMethod.GET, ACCOUNT_B, CONTAINER_Y);
    RestRequest quietReqC = newRequest(RestMethod.GET, (short) 300, (short) 30);
    // Build lopsided rates: 1000 noisy vs 10 each on quiet ⇒ noisy is the clear over-share owner.
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(noisyReq);
    }
    for (int i = 0; i < 10; i++) {
      throttler.shouldThrottle(quietReqB);
      throttler.shouldThrottle(quietReqC);
    }
    mockClock.tick(60);

    int noisyDropped = 0;
    int quietBDropped = 0;
    int quietCDropped = 0;
    for (int i = 0; i < 500; i++) {
      if (throttler.shouldThrottle(noisyReq)) {
        noisyDropped++;
      }
      if (throttler.shouldThrottle(quietReqB)) {
        quietBDropped++;
      }
      if (throttler.shouldThrottle(quietReqC)) {
        quietCDropped++;
      }
    }
    Assert.assertTrue("Noisy namespace must be dropped under hardware pressure; got " + noisyDropped, noisyDropped > 0);
    Assert.assertTrue("Noisy must be dropped at >=10x the rate of either quiet namespace; noisy=" + noisyDropped
        + " quietB=" + quietBDropped + " quietC=" + quietCDropped,
        noisyDropped >= 10 * (quietBDropped + 1) && noisyDropped >= 10 * (quietCDropped + 1));
    Meter throttled = registry.meter(MetricRegistry.name(HostLevelThrottler.class, "throttled", "hardwareThreshold",
        HardwareResource.DIRECT_MEMORY.name()));
    Assert.assertTrue("Drops from hardware fair-share must be attributed to the hardwareThreshold.DIRECT_MEMORY trigger; count="
        + throttled.getCount(), throttled.getCount() > 0);
  }

  /**
   * Hardware fair-share with uniform load: every namespace drives equal rate and the sustainabilityFactor
   * pulls fair share below the per-namespace rate, so all three see (roughly equal) non-zero drop. Pins the
   * proportional behaviour — no namespace gets a free pass under sustained overload.
   */
  @Test
  public void hardwareFairShareUniformOverloadDropsEveryone() throws Exception {
    // observed=95, threshold=50 ⇒ sustainabilityFactor ≈ 0.526. With 3 namespaces at equal rate the
    // fair share is well below each namespace's rate, so all three drop at the same ratio.
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 50));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(95);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest reqA = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    RestRequest reqB = newRequest(RestMethod.GET, ACCOUNT_B, CONTAINER_Y);
    RestRequest reqC = newRequest(RestMethod.GET, (short) 300, (short) 30);
    for (int i = 0; i < 500; i++) {
      throttler.shouldThrottle(reqA);
      throttler.shouldThrottle(reqB);
      throttler.shouldThrottle(reqC);
    }
    mockClock.tick(60);

    int droppedA = 0;
    int droppedB = 0;
    int droppedC = 0;
    int samples = 500;
    for (int i = 0; i < samples; i++) {
      if (throttler.shouldThrottle(reqA)) {
        droppedA++;
      }
      if (throttler.shouldThrottle(reqB)) {
        droppedB++;
      }
      if (throttler.shouldThrottle(reqC)) {
        droppedC++;
      }
    }
    Assert.assertTrue("All three namespaces must drop under uniform hardware overload; A=" + droppedA + " B="
        + droppedB + " C=" + droppedC, droppedA > 0 && droppedB > 0 && droppedC > 0);
    // Sustainability fraction ~0.526 ⇒ expected drop ratio ~0.47. Allow a generous band for sampling noise.
    int min = Math.min(droppedA, Math.min(droppedB, droppedC));
    int max = Math.max(droppedA, Math.max(droppedB, droppedC));
    Assert.assertTrue("Drop ratios across three equally-noisy namespaces must be roughly equal; A=" + droppedA
        + " B=" + droppedB + " C=" + droppedC, max <= 2 * min);
  }

  /**
   * Max-wins composition between per-method cap branch and hardware fair-share branch: when both fire on
   * the same request, the larger dropProb (and its trigger label) wins. We pick a hardware threshold so far
   * below observed that sustainabilityFactor dominates over a mild per-method cap fair-share — the trigger
   * meter for {@code hardwareThreshold.DIRECT_MEMORY} must accumulate while {@code restMethodCap.GET}'s does
   * not, when the HW dropProb wins.
   */
  @Test
  public void hardwareFairShareMaxWinsWithPerMethodCap() throws Exception {
    // Configure a modest per-method cap and a HW threshold low enough that HW fair-share is much tighter.
    // 1 namespace, GET cap=100, rate ~16.7/s ⇒ aggregate (~16.7) < cap (100): per-method branch doesn't
    // even fire. HW threshold=20 vs observed=99 ⇒ sustainabilityFactor ≈ 0.202, fair share ≈ 3.4/s ⇒ HW
    // branch drops with dropProb ≈ 0.8. So the trigger marked is unambiguously DIRECT_MEMORY.
    String capsJson = new JSONObject().put("GET", 100).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 20));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(99);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(req);
    }
    mockClock.tick(60);
    for (int i = 0; i < 500; i++) {
      throttler.shouldThrottle(req);
    }

    Meter hwThrottled = registry.meter(MetricRegistry.name(HostLevelThrottler.class, "throttled",
        "hardwareThreshold", HardwareResource.DIRECT_MEMORY.name()));
    Meter restMethodCapThrottled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Assert.assertTrue("hardwareThreshold.DIRECT_MEMORY trigger must own the throttle attribution; count="
        + hwThrottled.getCount(), hwThrottled.getCount() > 0);
    Assert.assertEquals("restMethodCap.GET must not be the attributed trigger when HW dropProb wins.", 0,
        restMethodCapThrottled.getCount());
  }

  /**
   * LowerBound criteria are silently skipped in {@code shouldThrottle}'s hardware branch — the
   * sustainabilityFactor math is only sensible for an UpperBound (observed > threshold). Even with observed
   * way above threshold, a LowerBound criterion must not drop any request.
   */
  @Test
  public void lowerBoundCriteriaSkipped() throws Exception {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, "ENFORCE");
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING, caps());
    // DIRECT_MEMORY is LowerBound at 50 ⇒ observed=99 satisfies "above" but the branch must skip it.
    String thresholdsJson = new JSONObject()
        .put("HEAP_MEMORY", new JSONObject().put("threshold", 101).put("boundType", "UpperBound"))
        .put("CPU", new JSONObject().put("threshold", 101).put("boundType", "UpperBound"))
        .put("DIRECT_MEMORY", new JSONObject().put("threshold", 50).put("boundType", "LowerBound"))
        .toString();
    props.setProperty(HostThrottleConfig.HARDWARE_THRESHOLDS, thresholdsJson);
    HostThrottleConfig config = new HostThrottleConfig(new VerifiableProperties(props));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(99);
    MockClock mockClock = new MockClock();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, new MetricRegistry(), mockClock);

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(req);
    }
    mockClock.tick(60);
    for (int i = 0; i < 200; i++) {
      Assert.assertFalse("LowerBound criteria must be silently skipped — no drops attributable to them.",
          throttler.shouldThrottle(req));
    }
  }

  /**
   * Trigger meters must be pre-registered for every {@link RestMethod} cap and every {@link HardwareResource}
   * threshold at construction so {@link HostLevelThrottler#updateConfig} can later expand the trigger set
   * without mutating the {@link MetricRegistry}. Construct with an empty config and assert every expected
   * meter is registered.
   */
  @Test
  public void triggerMetersPreRegisteredForAllResources() {
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MetricRegistry registry = new MetricRegistry();
    new HostLevelThrottler(config, hwMeter, registry, new MockClock());

    for (String base : new String[]{"wouldThrottle", "throttled"}) {
      for (RestMethod method : RestMethod.values()) {
        String name = MetricRegistry.name(HostLevelThrottler.class, base, "restMethodCap", method.name());
        Assert.assertTrue(name + " must be pre-registered.", registry.getMeters().containsKey(name));
      }
      for (HardwareResource resource : HardwareResource.values()) {
        String name = MetricRegistry.name(HostLevelThrottler.class, base, "hardwareThreshold", resource.name());
        Assert.assertTrue(name + " must be pre-registered.", registry.getMeters().containsKey(name));
      }
    }
  }

  // ---------- updateConfig runtime hook (new in this commit) ----------

  /**
   * {@link HostLevelThrottler#updateConfig} must swap the active mode atomically: an ENFORCE throttler that
   * was dropping traffic must immediately stop dropping after the caller pushes OFF.
   */
  @Test
  public void updateConfigSwapsModeAndIsObservedByNextRequest() throws Exception {
    // Start in ENFORCE with a kill switch on GET so every request drops.
    String capsJson = new JSONObject().put("GET", 0).toString();
    HostThrottleConfig config = buildConfig("ENFORCE", capsJson, hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), new MockClock());

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue("ENFORCE+killSwitch must drop.", throttler.shouldThrottle(req));
    }

    // Flip mode to OFF — must instantly admit everything regardless of the killSwitch quota.
    throttler.updateConfig(ThrottleMode.OFF, killSwitchGetQuota(), unboundedThresholds());
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("OFF after updateConfig must admit every request.", throttler.shouldThrottle(req));
    }
  }

  /**
   * {@link HostLevelThrottler#updateConfig} must swap the {@code restRequestQuota} map atomically: a
   * throttler that was uncapped must immediately start enforcing a fresh per-method cap after the push.
   */
  @Test
  public void updateConfigSwapsQuotaAtomically() throws Exception {
    // Start in ENFORCE with no caps (uncapped).
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, new MockClock());

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 100; i++) {
      Assert.assertFalse("No caps configured ⇒ ENFORCE must pass.", throttler.shouldThrottle(req));
    }

    // Push a kill switch on GET via updateConfig — pre-registered restMethodCap.GET meter must accumulate.
    throttler.updateConfig(ThrottleMode.ENFORCE, killSwitchGetQuota(), unboundedThresholds());
    int dropped = 0;
    for (int i = 0; i < 100; i++) {
      if (throttler.shouldThrottle(req)) {
        dropped++;
      }
    }
    Assert.assertEquals("Fresh cap=0 must drop every request after updateConfig.", 100, dropped);
    Meter throttled = registry.meter(
        MetricRegistry.name(HostLevelThrottler.class, "throttled", "restMethodCap", RestMethod.GET.name()));
    Assert.assertEquals("Drops from the pushed cap must be attributed to restMethodCap.GET.", 100,
        throttled.getCount());
  }

  /**
   * {@link HostLevelThrottler#updateConfig} must swap the {@code hardwareThresholds} map atomically: with
   * the same observed HW reading, flipping the threshold low enough must start firing the HW branch.
   */
  @Test
  public void updateConfigSwapsThresholdsAtomically() throws Exception {
    // Start ENFORCE with all HW thresholds unreachable (101) — HW branch is inert.
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.DIRECT_MEMORY)).thenReturn(95);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.HEAP_MEMORY)).thenReturn(10);
    Mockito.when(hwMeter.getHardwareResourcePercentage(HardwareResource.CPU)).thenReturn(10);
    MockClock mockClock = new MockClock();
    MetricRegistry registry = new MetricRegistry();
    HostLevelThrottler throttler = new HostLevelThrottler(config, hwMeter, registry, mockClock);

    RestRequest req = newRequest(RestMethod.GET, ACCOUNT_A, CONTAINER_X);
    for (int i = 0; i < 1000; i++) {
      throttler.shouldThrottle(req);
    }
    mockClock.tick(60);

    // Push a new threshold for DIRECT_MEMORY that is below observed (95 > 50) ⇒ HW branch should fire.
    Map<HardwareResource, Criteria> newThresholds = new EnumMap<>(HardwareResource.class);
    newThresholds.put(HardwareResource.HEAP_MEMORY, new Criteria(101, Criteria.BoundType.UpperBound));
    newThresholds.put(HardwareResource.CPU, new Criteria(101, Criteria.BoundType.UpperBound));
    newThresholds.put(HardwareResource.DIRECT_MEMORY, new Criteria(50, Criteria.BoundType.UpperBound));
    throttler.updateConfig(ThrottleMode.ENFORCE, Collections.emptyMap(), newThresholds);

    int dropped = 0;
    for (int i = 0; i < 500; i++) {
      if (throttler.shouldThrottle(req)) {
        dropped++;
      }
    }
    Assert.assertTrue("HW fair-share must fire after threshold pushed below observed; dropped=" + dropped,
        dropped > 0);
    Meter throttled = registry.meter(MetricRegistry.name(HostLevelThrottler.class, "throttled", "hardwareThreshold",
        HardwareResource.DIRECT_MEMORY.name()));
    Assert.assertTrue("Pushed-threshold drops must be attributed to hardwareThreshold.DIRECT_MEMORY; count="
        + throttled.getCount(), throttled.getCount() > 0);
  }

  /**
   * Null arguments to {@link HostLevelThrottler#updateConfig} must throw {@link NullPointerException}. The
   * caller is responsible for validating the push contents; rejecting null arg is the throttler's
   * defensive line.
   */
  @Test
  public void updateConfigRejectsNullArgsViaNPE() {
    HostThrottleConfig config = buildConfig("OFF", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), new MockClock());
    Map<RestMethod, Long> quota = Collections.emptyMap();
    Map<HardwareResource, Criteria> thresholds = Collections.emptyMap();
    try {
      throttler.updateConfig(null, quota, thresholds);
      Assert.fail("null mode must throw NPE.");
    } catch (NullPointerException expected) {
    }
    try {
      throttler.updateConfig(ThrottleMode.OFF, null, thresholds);
      Assert.fail("null quota must throw NPE.");
    } catch (NullPointerException expected) {
    }
    try {
      throttler.updateConfig(ThrottleMode.OFF, quota, null);
      Assert.fail("null thresholds must throw NPE.");
    } catch (NullPointerException expected) {
    }
  }

  /**
   * Concurrent readers and a writer pushing alternating {@code updateConfig} snapshots must never see a
   * torn triple. The volatile snapshot guarantees that each {@code shouldThrottle} call observes a
   * consistent (mode, quota, thresholds). Hard timeout guards against deadlocks.
   */
  @Test(timeout = 30_000)
  public void concurrentReadsUnderUpdateConfigSeeConsistentSnapshot() throws Exception {
    HostThrottleConfig config = buildConfig("ENFORCE", caps(), hardwareThresholds(101, 101, 101));
    HardwareUsageMeter hwMeter = Mockito.mock(HardwareUsageMeter.class);
    Mockito.when(hwMeter.getHardwareResourcePercentage(Mockito.any(HardwareResource.class))).thenReturn(10);
    HostLevelThrottler throttler =
        new HostLevelThrottler(config, hwMeter, new MetricRegistry(), new MockClock());

    int readers = 4;
    int readsPerThread = 1000;
    Map<Integer, RestRequest> requestsByThread = new HashMap<>();
    for (int t = 0; t < readers; t++) {
      requestsByThread.put(t, newRequest(RestMethod.GET, (short) (1000 + t), (short) 1));
    }
    ExecutorService executor = Executors.newFixedThreadPool(readers + 1);
    CountDownLatch start = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch finished = new CountDownLatch(readers + 1);

    for (int t = 0; t < readers; t++) {
      final RestRequest req = requestsByThread.get(t);
      executor.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < readsPerThread; i++) {
            // Just call — we don't care about the bool result, only that it doesn't throw.
            throttler.shouldThrottle(req);
          }
        } catch (Throwable e) {
          error.set(e);
        } finally {
          finished.countDown();
        }
      });
    }

    // One writer thread alternates the snapshot.
    Map<RestMethod, Long> capA = killSwitchGetQuota();
    Map<RestMethod, Long> capB = Collections.emptyMap();
    Map<HardwareResource, Criteria> thresholds = unboundedThresholds();
    executor.submit(() -> {
      try {
        start.await();
        for (int i = 0; i < 100; i++) {
          throttler.updateConfig(i % 2 == 0 ? ThrottleMode.ENFORCE : ThrottleMode.OFF,
              i % 2 == 0 ? capA : capB, thresholds);
        }
      } catch (Throwable e) {
        error.set(e);
      } finally {
        finished.countDown();
      }
    });

    start.countDown();
    Assert.assertTrue("Workers did not finish within 20s.", finished.await(20, TimeUnit.SECONDS));
    executor.shutdown();
    Assert.assertNull("No exception expected under concurrent updateConfig: " + error.get(), error.get());
  }

  // ---------- Helpers ----------

  private static final short ACCOUNT_A = 100;
  private static final short ACCOUNT_B = 200;
  private static final short CONTAINER_X = 10;
  private static final short CONTAINER_Y = 20;

  /** Build an in-memory {@link HostThrottleConfig} from mode + method-caps JSON + thresholds JSON. */
  private static HostThrottleConfig buildConfig(String mode, String capsJson, String thresholdsJson) {
    Properties props = new Properties();
    props.setProperty(HostThrottleConfig.MODE, mode);
    props.setProperty(HostThrottleConfig.REST_REQUEST_QUOTA_STRING, capsJson);
    props.setProperty(HostThrottleConfig.HARDWARE_THRESHOLDS, thresholdsJson);
    return new HostThrottleConfig(new VerifiableProperties(props));
  }

  /** Default uncapped caps (all -1). */
  private static String caps() {
    JSONObject quota = new JSONObject();
    for (RestMethod m : RestMethod.values()) {
      quota.put(m.name(), -1);
    }
    return quota.toString();
  }

  /** A quota map with {@code GET=0} (kill switch). Used by updateConfig tests. */
  private static Map<RestMethod, Long> killSwitchGetQuota() {
    Map<RestMethod, Long> quota = new EnumMap<>(RestMethod.class);
    quota.put(RestMethod.GET, 0L);
    return quota;
  }

  /** Hardware thresholds map with every resource UpperBound at an unreachable 101%. */
  private static Map<HardwareResource, Criteria> unboundedThresholds() {
    Map<HardwareResource, Criteria> map = new EnumMap<>(HardwareResource.class);
    for (HardwareResource r : HardwareResource.values()) {
      map.put(r, new Criteria(101, Criteria.BoundType.UpperBound));
    }
    return map;
  }

  /** Build a hardware-thresholds JSON: heap/cpu/direct UpperBound at the given percents. */
  private static String hardwareThresholds(int heap, int cpu, int direct) {
    return new JSONObject()
        .put("HEAP_MEMORY", new JSONObject().put("threshold", heap).put("boundType", "UpperBound"))
        .put("CPU", new JSONObject().put("threshold", cpu).put("boundType", "UpperBound"))
        .put("DIRECT_MEMORY", new JSONObject().put("threshold", direct).put("boundType", "UpperBound"))
        .toString();
  }

  /** Build a MockRestRequest with target Account/Container set in InternalKeys. */
  private static RestRequest newRequest(RestMethod method, short accountId, short containerId) throws Exception {
    RestRequest req = createRestRequest(method, "https://linkedin.com/" + accountId + "/" + containerId);
    Account account = new AccountBuilder(accountId, "acct" + accountId, Account.AccountStatus.ACTIVE).build();
    Container container =
        new ContainerBuilder(containerId, "ctr" + containerId, Container.ContainerStatus.ACTIVE, "test", accountId)
            .build();
    req.setArg(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY, account);
    req.setArg(RestUtils.InternalKeys.TARGET_CONTAINER_KEY, container);
    return req;
  }

  /** Build a {@link MockRestRequest} for the given method + URI (no namespace set). */
  static RestRequest createRestRequest(RestMethod restMethod, String uri)
      throws UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, uri);
    return new MockRestRequest(request, null);
  }
}

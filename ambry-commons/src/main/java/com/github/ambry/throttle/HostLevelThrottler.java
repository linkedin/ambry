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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.commons.Criteria;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to decide if a request should be throttled. Operating modes are defined by {@link ThrottleMode}.
 *
 * <p>In {@code TRACK} / {@code ENFORCE} two trigger families fire independently and the largest drop probability
 * across all triggers wins:
 * <ol>
 *   <li>{@code restMethodCap.<RestMethod>}: the configured per-RestMethod QPS cap is exceeded; the over-share
 *       caller for that method receives {@code dropProb = (rate - fairShare) / rate}, where
 *       {@code fairShare = cap / activeKeysForMethod}.</li>
 *   <li>{@code hardwareThreshold.<HardwareResource>}: the observed percentage for a configured hardware
 *       resource (e.g. {@code DIRECT_MEMORY}) exceeds the configured UpperBound — drops everyone (binary).
 *       Fair-share for the hardware branch lands in a follow-up commit.</li>
 * </ol>
 *
 * <p>Requests without a resolvable (account, container) — admin/operational endpoints such as
 * {@code /accounts}, {@code /peers}, {@code /getClusterMapSnapshot}, {@code /statsReport} — are never
 * throttled in {@code TRACK}/{@code ENFORCE}, regardless of trigger state. The throttler is scoped to
 * defending against noisy namespaced tenants (e.g. one account+container hammering a host); admin traffic
 * is low-volume internal traffic and not in scope. Short-circuiting on UNKNOWN also avoids polluting the
 * per-method aggregate rate that the fair-share branch reads.
 */
public class HostLevelThrottler {
  private static final Logger logger = LoggerFactory.getLogger(HostLevelThrottler.class);

  /** Trigger family name for the per-RestMethod QPS-cap branch. */
  private static final String TRIGGER_REST_METHOD_CAP = "restMethodCap";
  /** Trigger family name for the per-HardwareResource threshold branch. */
  private static final String TRIGGER_HARDWARE_THRESHOLD = "hardwareThreshold";

  // Caffeine idle-expiry for per-namespace meter entries. Hardcoded to match Codahale Meter's 1-minute EWMA
  // window (Meter.getOneMinuteRate, which we call below): once a namespace stops emitting, its rate decays
  // to near-zero within ~60s, so dropping it from the cache after 60s of inactivity loses no operational
  // signal. This is also what keeps perMethodNamespaceMeters.estimatedSize() honest as the fair-share
  // denominator — without expiry, every namespace ever seen would count forever and active callers would
  // eat ever-tighter fair shares. Promote to config only if the algorithm ever moves off the 1-minute window.
  private static final long METER_TTL_SECONDS = 60;

  // Sentinel used when the request has no resolvable namespace (admin/operational endpoints — see class Javadoc).
  private static final Pair<Short, Short> UNKNOWN_NAMESPACE =
      new Pair<>(Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);

  private final ThrottleMode mode;
  // Parsed at config load — see HostThrottleConfig.
  private final Map<HardwareResource, Criteria> hardwareThresholds;
  private final HardwareUsageMeter hardwareUsageMeter;
  // Per-method QPS cap from config. cap < 0 = uncapped; cap = 0 = kill switch; cap > 0 = rate cap with fair-share.
  // Methods not present are treated as uncapped (consumer uses getOrDefault(method, -1L)).
  private final Map<RestMethod, Long> restRequestQuota;
  // For each method: cache of per-namespace Meters. estimatedSize() approximates the active-key count for that method.
  private final EnumMap<RestMethod, Cache<Pair<Short, Short>, Meter>> perMethodNamespaceMeters;
  // Aggregate rate per method (sum of all namespaces). Compared against restRequestQuota.
  private final EnumMap<RestMethod, Meter> perMethodAggregateMeters;
  // Externally visible "would throttle" meters, keyed by trigger name. Marked on every drop decision
  // regardless of mode — preserves rate continuity across TRACK→ENFORCE flips (the drop bit is the same).
  private final Map<String, Meter> wouldThrottleMeters;
  // Externally visible "throttled" meters, keyed by trigger name. Marked only when the request was actually
  // rejected (mode == ENFORCE && drop). Lets operators alert on real impact independent of mode.
  private final Map<String, Meter> throttledMeters;
  // Clock used to back every per-namespace Meter we lazily allocate.
  private final Clock clock;

  public HostLevelThrottler(HostThrottleConfig hostThrottleConfig, MetricRegistry metricRegistry) {
    this(hostThrottleConfig,
        new HardwareUsageMeter(hostThrottleConfig.cpuSamplingPeriodMs, hostThrottleConfig.memorySamplingPeriodMs),
        metricRegistry, Clock.defaultClock());
  }

  /**
   * Package-private test entry point. Lets tests inject a mock {@link HardwareUsageMeter} and a
   * {@link Clock} that backs every internal {@link Meter}.
   */
  HostLevelThrottler(HostThrottleConfig hostThrottleConfig, HardwareUsageMeter hardwareUsageMeter,
      MetricRegistry metricRegistry, Clock clock) {
    this.hardwareUsageMeter = hardwareUsageMeter;
    this.restRequestQuota = hostThrottleConfig.restRequestQuota;
    this.hardwareThresholds = hostThrottleConfig.hardwareThresholds;
    this.mode = hostThrottleConfig.mode;

    this.perMethodNamespaceMeters = new EnumMap<>(RestMethod.class);
    this.perMethodAggregateMeters = new EnumMap<>(RestMethod.class);
    for (RestMethod restMethod : RestMethod.values()) {
      this.perMethodNamespaceMeters.put(restMethod,
          Caffeine.newBuilder().expireAfterAccess(METER_TTL_SECONDS, TimeUnit.SECONDS).build());
      this.perMethodAggregateMeters.put(restMethod, new Meter(clock));
    }
    this.clock = clock;
    this.wouldThrottleMeters = registerTriggerMeters(metricRegistry, "wouldThrottle");
    this.throttledMeters = registerTriggerMeters(metricRegistry, "throttled");

    logger.info("Host throttling config: mode={} restRequestQuota={} hardwareThresholds={}", this.mode,
        this.restRequestQuota, this.hardwareThresholds);
  }

  /**
   * Decide whether to throttle this request.
   *
   * <p>{@code OFF} short-circuits with no accounting. {@code TRACK} runs the algorithm and marks
   * {@code wouldThrottle.*} on each drop decision, but always returns {@code false}; {@code ENFORCE}
   * returns the drop decision.
   *
   * @param restRequest the request to check.
   * @return {@code true} if the caller should reject this request, {@code false} otherwise.
   */
  public boolean shouldThrottle(RestRequest restRequest) {
    if (mode == ThrottleMode.OFF) {
      return false;
    }

    RestMethod method = restRequest.getRestMethod();
    Pair<Short, Short> namespace = extractNamespace(restRequest);
    // Admin/operational endpoints (no resolvable account+container) bypass throttling — see the class
    // Javadoc for scope. Returning early also skips the accounting marks below, so their rate doesn't
    // poison the per-method aggregate.
    if (namespace.equals(UNKNOWN_NAMESPACE)) {
      return false;
    }

    // Emit-on-write: every active aggregate gets a single mark. Each call is O(1).
    Meter perMethodNamespaceMeter =
        perMethodNamespaceMeters.get(method).get(namespace, key -> new Meter(clock));
    perMethodNamespaceMeter.mark();
    Meter perMethodAggregate = perMethodAggregateMeters.get(method);
    perMethodAggregate.mark();

    double dropProb = 0.0;
    String triggerFired = null;

    // Per-method cap branch. Missing methods are uncapped (-1) — the config map only contains explicit entries.
    // Matches legacy RejectThrottler semantics: cap < 0 = uncapped, cap = 0 = drop everything (kill switch),
    // cap > 0 = normal rate cap with fair-share over-share drop.
    long restMethodCap = restRequestQuota.getOrDefault(method, -1L);
    if (restMethodCap == 0) {
      dropProb = 1.0;
      triggerFired = TRIGGER_REST_METHOD_CAP + "." + method.name();
    } else if (restMethodCap > 0 && perMethodAggregate.getOneMinuteRate() > restMethodCap) {
      double activeKeys = perMethodNamespaceMeters.get(method).estimatedSize();
      // Guard against async Caffeine eviction races: a concurrent eviction between our get() and
      // estimatedSize() can leave activeKeys at 0 even though we just inserted. Skip the branch in that
      // (rare) case rather than divide by zero.
      if (activeKeys > 0.0) {
        double fairShare = (double) restMethodCap / activeKeys;
        double thisRate = perMethodNamespaceMeter.getOneMinuteRate();
        if (thisRate > fairShare) {
          double newDrop = (thisRate - fairShare) / thisRate;
          if (newDrop > dropProb) {
            dropProb = newDrop;
            triggerFired = TRIGGER_REST_METHOD_CAP + "." + method.name();
          }
        }
      }
    }

    // Hardware threshold branch — still BINARY in this commit. Any breach drops everyone (dropProb = 1.0).
    // Per-resource fair-share lands in the next commit.
    for (Map.Entry<HardwareResource, Criteria> entry : hardwareThresholds.entrySet()) {
      Criteria criteria = entry.getValue();
      // Only UpperBound is meaningful for "observed exceeds threshold" semantics. LowerBound criteria are
      // silently skipped — they will become meaningful when fair-share replaces the binary check.
      if (criteria.getBoundType() != Criteria.BoundType.UpperBound) {
        continue;
      }
      int observedPercent = hardwareUsageMeter.getHardwareResourcePercentage(entry.getKey());
      if (observedPercent <= criteria.getThreshold()) {
        continue;
      }
      if (1.0 > dropProb) {
        dropProb = 1.0;
        triggerFired = TRIGGER_HARDWARE_THRESHOLD + "." + entry.getKey().name();
      }
    }

    // One drop decision shared across TRACK and ENFORCE — guarantees TRACK-mode wouldThrottle rate
    // equals the rate that ENFORCE would have produced for the same traffic.
    boolean drop = dropProb > 0.0 && ThreadLocalRandom.current().nextDouble() < dropProb;
    boolean enforce = (mode == ThrottleMode.ENFORCE) && drop;
    if (drop) {
      // dropProb > 0 implies triggerFired was set above (every dropProb assignment co-occurs with one).
      // Always mark wouldThrottle on drop (regardless of mode) so dashboards see continuous rate across
      // TRACK→ENFORCE flips. Additionally mark throttled when the request was actually rejected — that's
      // the metric operators alert on for real impact.
      mark(wouldThrottleMeters, triggerFired);
      if (enforce) {
        mark(throttledMeters, triggerFired);
      }
    }

    if (enforce && logger.isDebugEnabled()) {
      // Logged at debug because under sustained ENFORCE this fires on every dropped request — operators
      // should watch the throttled.* meters for aggregate visibility instead.
      logger.debug("Throttling request method={} namespace={} dropProb={} trigger={}", method, namespace, dropProb,
          triggerFired);
    }
    return enforce;
  }

  private static void mark(Map<String, Meter> meters, String trigger) {
    Meter meter = meters.get(trigger);
    if (meter != null) {
      meter.mark();
    }
  }

  /**
   * Best-effort namespace extraction. Reads the {@link Account} / {@link Container} that
   * {@code AccountAndContainerInjector} placed under {@link RestUtils.InternalKeys#TARGET_ACCOUNT_KEY} /
   * {@code TARGET_CONTAINER_KEY}. Falls back to {@link #UNKNOWN_NAMESPACE} for requests with no resolvable
   * namespace (admin/operational endpoints — see class Javadoc) — those all share a single bucket.
   */
  private static Pair<Short, Short> extractNamespace(RestRequest restRequest) {
    Object accountObj = restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY);
    Object containerObj = restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY);
    if (accountObj instanceof Account && containerObj instanceof Container) {
      return new Pair<>(((Account) accountObj).getId(), ((Container) containerObj).getId());
    }
    return UNKNOWN_NAMESPACE;
  }

  /**
   * Eagerly register every {@code <metricBase>.<family>.<sub-key>} meter the throttler can ever mark:
   * <ul>
   *   <li>{@code restMethodCap.<RestMethod>} for every {@link RestMethod} value.</li>
   *   <li>{@code hardwareThreshold.<HardwareResource>} for every {@link HardwareResource} value.</li>
   * </ul>
   * Registered up front (independent of the current config) so runtime config flips can change which
   * sub-key actually fires without mutating the metric registry. Per-namespace meters are NOT registered —
   * that would blow up cardinality.
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
}

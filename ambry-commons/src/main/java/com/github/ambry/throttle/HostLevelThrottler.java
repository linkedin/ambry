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
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to decide if a request should be host-level throttled. Operating modes are defined by
 * {@link ThrottleMode}.
 *
 * <p>In {@code TRACK} / {@code ENFORCE} two trigger families fire independently and the largest drop probability
 * across all triggers wins:
 * <ol>
 *   <li>{@code restMethodCap.<RestMethod>}: the configured per-RestMethod QPS cap is exceeded; the over-share
 *       caller for that method receives {@code dropProb = (rate - fairShare) / rate}, where
 *       {@code fairShare = cap / activeKeysForMethod}.</li>
 *   <li>{@code hardwareThreshold.<HardwareResource>}: same {@code dropProb} formula, with
 *       {@code fairShare = hostTotal.rate() × sustainabilityFactor / activeKeys} and
 *       {@code sustainabilityFactor = threshold / observedPercent} ramping the accepted rate down
 *       proportionally to overload.</li>
 * </ol>
 *
 * <p>Requests without a resolvable (account, container) — admin/operational endpoints such as
 * {@code /accounts}, {@code /peers}, {@code /getClusterMapSnapshot}, {@code /statsReport} — are never
 * throttled in {@code TRACK}/{@code ENFORCE}, regardless of trigger state. The throttler is scoped to
 * defending against noisy namespaced tenants (e.g. one account+container hammering a host); admin traffic
 * is low-volume internal traffic and not in scope.
 *
 * <p>The {@code (mode, restRequestQuota, hardwareThresholds)} triple is mutable at runtime via
 * {@link #updateConfig(ThrottleMode, Map, Map)} to support runtime overrides without restart.
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
  // signal. This is also what keeps perMethodNamespaceMeters.estimatedSize() / perNamespaceMeters.estimatedSize()
  // honest as fair-share denominators — without expiry, every namespace ever seen would count forever and
  // active callers would eat ever-tighter fair shares. Promote to config only if the algorithm ever moves
  // off the 1-minute window.
  private static final long METER_TTL_SECONDS = 60;

  // Sentinel used when the request has no resolvable namespace (admin/operational endpoints — see class Javadoc).
  private static final Pair<Short, Short> UNKNOWN_NAMESPACE =
      new Pair<>(Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID);

  /**
   * Immutable snapshot of the three runtime-mutable knobs. Read once per {@link #shouldThrottle(RestRequest)}
   * call so all three fields come from the same {@link #updateConfig(ThrottleMode, Map, Map)} push — no torn
   * reads across the (mode, quota, thresholds) triple.
   */
  private static final class ConfigSnapshot {
    final ThrottleMode mode;
    final Map<RestMethod, Long> restRequestQuota;
    final Map<HardwareResource, Criteria> hardwareThresholds;

    ConfigSnapshot(ThrottleMode mode, Map<RestMethod, Long> restRequestQuota,
        Map<HardwareResource, Criteria> hardwareThresholds) {
      this.mode = mode;
      this.restRequestQuota = restRequestQuota;
      this.hardwareThresholds = hardwareThresholds;
    }
  }

  // Single volatile read per shouldThrottle call yields a consistent (mode, quota, thresholds) triple.
  private volatile ConfigSnapshot snapshot;

  private final HardwareUsageMeter hardwareUsageMeter;
  // For each method: cache of per-namespace Meters. estimatedSize() approximates the active-key count for that method.
  private final EnumMap<RestMethod, Cache<Pair<Short, Short>, Meter>> perMethodNamespaceMeters;
  // Aggregate rate per method (sum of all namespaces). Compared against restRequestQuota.
  private final EnumMap<RestMethod, Meter> perMethodAggregateMeters;
  // Aggregate rate per namespace (sum across methods). Used by the hardware fair-share branch — hardware
  // pressure is method-agnostic, so a noisy PUT on namespace A counts against A's hardware share even when
  // A's GET traffic is quiet.
  private final Cache<Pair<Short, Short>, Meter> perNamespaceMeters;
  // Host-wide aggregate rate. Used by the hardware branch as the basis for sustainable target rate.
  private final Meter hostTotalMeter;
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
    this.snapshot = new ConfigSnapshot(hostThrottleConfig.mode, hostThrottleConfig.restRequestQuota,
        hostThrottleConfig.hardwareThresholds);

    this.perMethodNamespaceMeters = new EnumMap<>(RestMethod.class);
    this.perMethodAggregateMeters = new EnumMap<>(RestMethod.class);
    for (RestMethod restMethod : RestMethod.values()) {
      this.perMethodNamespaceMeters.put(restMethod,
          Caffeine.newBuilder().expireAfterAccess(METER_TTL_SECONDS, TimeUnit.SECONDS).build());
      this.perMethodAggregateMeters.put(restMethod, new Meter(clock));
    }
    this.perNamespaceMeters = Caffeine.newBuilder().expireAfterAccess(METER_TTL_SECONDS, TimeUnit.SECONDS).build();
    this.hostTotalMeter = new Meter(clock);
    this.clock = clock;
    this.wouldThrottleMeters = registerTriggerMeters(metricRegistry, "wouldThrottle");
    this.throttledMeters = registerTriggerMeters(metricRegistry, "throttled");

    logger.info("Host throttling config: mode={} restRequestQuota={} hardwareThresholds={}", snapshot.mode,
        snapshot.restRequestQuota, snapshot.hardwareThresholds);
  }

  /**
   * Atomically replace the {@code (mode, restRequestQuota, hardwareThresholds)} triple. Subsequent
   * {@link #shouldThrottle(RestRequest)} calls see the new triple in one go via a single volatile read — no
   * torn cross-field reads. Callers may be any runtime configuration updater pushing live overrides;
   * the throttler itself never mutates the snapshot.
   *
   * <p>The caller is responsible for validating the inputs. Bad pushes (malformed JSON, invalid enum, etc.)
   * should be caught in the caller and dropped — keep the throttler running on the last known-good snapshot
   * rather than poisoning it.
   *
   * @param mode new operating mode; must be non-null.
   * @param restRequestQuota new per-method cap map; must be non-null (may be empty).
   * @param hardwareThresholds new per-resource threshold map; must be non-null (may be empty).
   */
  public void updateConfig(ThrottleMode mode, Map<RestMethod, Long> restRequestQuota,
      Map<HardwareResource, Criteria> hardwareThresholds) {
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(restRequestQuota, "restRequestQuota");
    Objects.requireNonNull(hardwareThresholds, "hardwareThresholds");
    this.snapshot = new ConfigSnapshot(mode, restRequestQuota, hardwareThresholds);
    logger.info("HostLevelThrottler updateConfig: mode={} restRequestQuota={} hardwareThresholds={}", mode,
        restRequestQuota, hardwareThresholds);
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
    // Single volatile read — guarantees this call sees a triple-consistent (mode, quota, thresholds).
    ConfigSnapshot cfg = snapshot;
    if (cfg.mode == ThrottleMode.OFF) {
      return false;
    }

    RestMethod method = restRequest.getRestMethod();
    Pair<Short, Short> namespace = extractNamespace(restRequest);
    // Admin/operational endpoints (no resolvable account+container) bypass throttling — see the class
    // Javadoc for scope. Returning early also skips the accounting marks below, so their rate doesn't
    // poison the per-method aggregate or the host-wide aggregate.
    if (namespace.equals(UNKNOWN_NAMESPACE)) {
      return false;
    }

    // Emit-on-write: every active aggregate gets a single mark. Each call is O(1).
    Meter perMethodNamespaceMeter =
        perMethodNamespaceMeters.get(method).get(namespace, key -> new Meter(clock));
    perMethodNamespaceMeter.mark();
    Meter perMethodAggregate = perMethodAggregateMeters.get(method);
    perMethodAggregate.mark();
    Meter perNamespaceMeter = perNamespaceMeters.get(namespace, key -> new Meter(clock));
    perNamespaceMeter.mark();
    hostTotalMeter.mark();

    double dropProb = 0.0;
    String triggerFired = null;

    // Per-method cap branch. Missing methods are uncapped (-1) — the config map only contains explicit entries.
    // Matches legacy RejectThrottler semantics: cap < 0 = uncapped, cap = 0 = drop everything (kill switch),
    // cap > 0 = normal rate cap with fair-share over-share drop.
    long restMethodCap = cfg.restRequestQuota.getOrDefault(method, -1L);
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

    // Hardware threshold branch — one per configured resource. Per-resource trigger name so incident
    // debugging can identify which resource caused the throttle. Max-wins composition with the per-method
    // branch above: the more-stressed signal dominates.
    for (Map.Entry<HardwareResource, Criteria> entry : cfg.hardwareThresholds.entrySet()) {
      HardwareResource resource = entry.getKey();
      Criteria criteria = entry.getValue();
      // Only UpperBound is meaningful for the sustainabilityFactor math (we scale the rate DOWN when
      // observed > threshold). LowerBound criteria are silently skipped — the math doesn't apply.
      if (criteria.getBoundType() != Criteria.BoundType.UpperBound) {
        continue;
      }
      int observedPercent = hardwareUsageMeter.getHardwareResourcePercentage(resource);
      long threshold = criteria.getThreshold();
      if (observedPercent <= threshold) {
        continue;
      }
      // Cast to double — getHardwareResourcePercentage returns int and integer division would collapse the ratio.
      double sustainabilityFactor = (double) threshold / observedPercent;
      double activeKeys = perNamespaceMeters.estimatedSize();
      if (activeKeys <= 0.0) {
        // Defensive — should be > 0 since we just marked perNamespaceMeter. Same Caffeine eviction-race
        // guard as the per-method branch above.
        continue;
      }
      double fairShare = hostTotalMeter.getOneMinuteRate() * sustainabilityFactor / activeKeys;
      double thisRate = perNamespaceMeter.getOneMinuteRate();
      if (thisRate > fairShare) {
        double newDrop = (thisRate - fairShare) / thisRate;
        if (newDrop > dropProb) {
          dropProb = newDrop;
          triggerFired = TRIGGER_HARDWARE_THRESHOLD + "." + resource.name();
        }
      }
    }

    // One drop decision shared across TRACK and ENFORCE — guarantees TRACK-mode wouldThrottle rate
    // equals the rate that ENFORCE would have produced for the same traffic.
    boolean drop = dropProb > 0.0 && ThreadLocalRandom.current().nextDouble() < dropProb;
    boolean enforce = (cfg.mode == ThrottleMode.ENFORCE) && drop;
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
   * Register the externally visible {@code <metricBase>.<family>.<sub-key>} meters with the host metric
   * registry, eagerly for every possible trigger: one per {@link RestMethod} cap and one per
   * {@link HardwareResource} threshold. Eager pre-registration means
   * {@link #updateConfig(ThrottleMode, Map, Map)} can expand the trigger set at runtime (e.g., enable a
   * previously-unconfigured HW resource or per-method cap) without touching the {@link MetricRegistry}. The
   * cost is fixed and small — one meter per (base, family, sub-key) tuple regardless of the initial config.
   * Per-namespace meters are NOT registered here — that would blow up cardinality.
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

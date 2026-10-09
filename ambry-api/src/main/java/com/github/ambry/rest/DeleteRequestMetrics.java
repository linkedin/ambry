/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.rest;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.EnumMap;


/**
 * Additive request TTFB and rate cohorts, classified by DELETE attempts observed before request finalization.
 */
public class DeleteRequestMetrics {
  private enum Path {
    NO_REMOTE_ATTEMPT("NoRemoteAttempt"),
    REMOTE_ATTEMPT("RemoteAttempt"),
    ON_DEMAND_REPAIR("OnDemandRepair");

    private final String suffix;

    Path(String suffix) {
      this.suffix = suffix;
    }
  }

  private final EnumMap<Path, Histogram> timeToFirstByte = new EnumMap<>(Path.class);
  private final EnumMap<Path, Meter> rate = new EnumMap<>(Path.class);

  public DeleteRequestMetrics(Class<?> ownerClass, String requestType, MetricRegistry registry) {
    for (Path path : Path.values()) {
      timeToFirstByte.put(path, registry.histogram(
          MetricRegistry.name(ownerClass, requestType + path.suffix + RestRequestMetrics.NIO_TIME_TO_FIRST_BYTE_SUFFIX)));
      rate.put(path, registry.meter(
          MetricRegistry.name(ownerClass, requestType + path.suffix + RestRequestMetrics.OPERATION_RATE_SUFFIX)));
    }
  }

  /**
   * Shared by all synchronous deletes for one frontend request. Repair takes precedence over remote DELETE attempts.
   * Finalization freezes classification so work continuing after a timeout cannot move an emitted sample.
   */
  public static class Tracker {
    private final DeleteRequestMetrics metrics;
    private Path path = Path.NO_REMOTE_ATTEMPT;
    private boolean recorded;

    public Tracker(DeleteRequestMetrics metrics) {
      this.metrics = metrics;
    }

    public synchronized void markRemoteAttempt() {
      if (!recorded && path == Path.NO_REMOTE_ATTEMPT) {
        path = Path.REMOTE_ATTEMPT;
      }
    }

    public synchronized void markOnDemandRepair() {
      if (!recorded) {
        path = Path.ON_DEMAND_REPAIR;
      }
    }

    synchronized void recordMetrics(long timeToFirstByteInMs) {
      if (!recorded) {
        recorded = true;
        metrics.timeToFirstByte.get(path).update(timeToFirstByteInMs);
        metrics.rate.get(path).mark();
      }
    }
  }
}

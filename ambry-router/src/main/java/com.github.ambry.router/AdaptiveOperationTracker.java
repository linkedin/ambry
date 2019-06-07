/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * An implementation of {@link OperationTracker}. It internally maintains the status of a corresponding operation, and
 * returns information that decides if the operation should continue or terminate.
 *
 * This differs from {@link SimpleOperationTracker} in its interpretation of parallelism. A request does not count
 * towards parallelism if it has been outstanding for more than a configurable cutoff latency (via quantiles) that is
 * obtained from a {@link Histogram} with latencies of all requests of the same class. In this way it "adapts" to
 * perceived latencies.
 */
class AdaptiveOperationTracker extends SimpleOperationTracker {
  private final RouterConfig routerConfig;
  private final NonBlockingRouterMetrics routerMetrics;
  private final Time time;
  private final double quantile;
  private final Histogram localDcHistogram;
  private final Histogram crossDcHistogram;
  private final Counter pastDueCounter;
  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;
  private Map<PartitionId, Histogram> localDcPartitionToHistogram;
  private Map<PartitionId, Histogram> crossDcPartitionToHistogram;

  // The value contains a pair - the boolean indicates whether the request to the corresponding replicaId has been
  // determined as expired (but not yet removed). The long is the time at which the request was sent.
  private final LinkedHashMap<ReplicaId, Pair<Boolean, Long>> unexpiredRequestSendTimes = new LinkedHashMap<>();
  private final Map<ReplicaId, Long> expiredRequestSendTimes = new HashMap<>();

  private ReplicaId lastReturned = null;

  /**
   * Constructs an {@link AdaptiveOperationTracker}
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} that contains histograms used by this operation tracker.
   * @param routerOperation The {@link RouterOperation} which {@link AdaptiveOperationTracker} is associated with.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName name of originating DC whose replicas should be tried first.
   * @param time the {@link Time} instance to use.
   */
  AdaptiveOperationTracker(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      RouterOperation routerOperation, PartitionId partitionId, String originatingDcName, Time time) {
    super(routerConfig, routerOperation, partitionId, originatingDcName, true);
    this.routerConfig = routerConfig;
    this.routerMetrics = routerMetrics;
    this.time = time;
    this.localDcHistogram = getWholeDcTracker(routerOperation, true);
    this.crossDcHistogram = getWholeDcTracker(routerOperation, false);
    this.pastDueCounter = getWholeDcPastDueCounter(routerOperation);
    this.quantile = routerConfig.routerLatencyToleranceQuantile;
    this.otIterator = new OpTrackerIterator();
    if (routerConfig.routerOperationTrackerMetricScope == OperationTrackerScope.Partition) {
      localDcPartitionToHistogram = getPartitionToLatencyMap(routerOperation, true);
      crossDcPartitionToHistogram = getPartitionToLatencyMap(routerOperation, false);
    }
  }

  @Override
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    long elapsedTime;
    if (unexpiredRequestSendTimes.containsKey(replicaId)) {
      elapsedTime = time.milliseconds() - unexpiredRequestSendTimes.remove(replicaId).getSecond();
    } else {
      elapsedTime = time.milliseconds() - expiredRequestSendTimes.remove(replicaId);
    }
    if (trackedRequestFinalState != TrackedRequestFinalState.TIMED_OUT) {
      getLatencyHistogram(replicaId).update(elapsedTime);
      if (routerConfig.routerOperationTrackerMetricScope != OperationTrackerScope.Datacenter) {
        // This is only used to report whole datacenter histogram for monitoring purpose
        Histogram histogram =
            replicaId.getDataNodeId().getDatacenterName().equals(datacenterName) ? localDcHistogram : crossDcHistogram;
        histogram.update(elapsedTime);
      }
    }
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  /**
   * Gets the {@link Histogram} that tracks request latencies to the class of replicas (intra or inter DC) that
   * {@code replicaId} belongs to.
   * @param replicaId the {@link ReplicaId} whose request latency is going to be tracked.
   * @return the {@link Histogram} associated with this replica.
   */
  Histogram getLatencyHistogram(ReplicaId replicaId) {
    boolean isLocalReplica = replicaId.getDataNodeId().getDatacenterName().equals(datacenterName);
    Histogram histogramToReturn;
    // TODO add support for replica-level/disk-level/node-level histogram based on the config
    switch (routerConfig.routerOperationTrackerMetricScope) {
      case Datacenter:
        histogramToReturn = isLocalReplica ? localDcHistogram : crossDcHistogram;
        break;
      case Partition:
        PartitionId partitionId = replicaId.getPartitionId();
        histogramToReturn = isLocalReplica ? localDcPartitionToHistogram.get(partitionId)
            : crossDcPartitionToHistogram.get(partitionId);
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation tracker metric scope.");
    }
    return histogramToReturn;
  }

  /**
   * Get certain whole DC latency histogram based on given arguments.
   * @param routerOperation the {@link RouterOperation} that uses this tracker.
   * @param isLocal {@code true} if local DC latency histogram should be returned. {@code false} otherwise.
   * @return whole DC latency histogram.
   */
  private Histogram getWholeDcTracker(RouterOperation routerOperation, boolean isLocal) {
    Histogram trackerToReturn;
    switch (routerOperation) {
      case GetBlobInfoOperation:
        trackerToReturn =
            isLocal ? routerMetrics.getBlobInfoLocalDcLatencyMs : routerMetrics.getBlobInfoCrossDcLatencyMs;
        break;
      case GetBlobOperation:
        trackerToReturn = isLocal ? routerMetrics.getBlobLocalDcLatencyMs : routerMetrics.getBlobCrossDcLatencyMs;
        break;
      default:
        throw new IllegalArgumentException("Unsupported router operation when getting whole DC latency tracker.");
    }
    return trackerToReturn;
  }

  /**
   * Get certain whole DC past due counter based on given arguments.
   * @param routerOperation the {@link RouterOperation} that uses this tracker.
   * @return whole DC past due counter.
   */
  private Counter getWholeDcPastDueCounter(RouterOperation routerOperation) {
    Counter pastDueCounter;
    switch (routerOperation) {
      case GetBlobInfoOperation:
        pastDueCounter = routerMetrics.getBlobInfoPastDueCount;
        break;
      case GetBlobOperation:
        pastDueCounter = routerMetrics.getBlobPastDueCount;
        break;
      default:
        throw new IllegalArgumentException("Unsupported router operation when getting whole DC past due counter.");
    }
    return pastDueCounter;
  }

  /**
   * Get certain partition-level histograms based on given arguments.
   * @param routerOperation the {@link RouterOperation} that uses this tracker.
   * @param isLocal {@code true} if local partition-level histograms should be returned. {@code false} otherwise.
   * @return partition-to-histogram map.
   */
  Map<PartitionId, Histogram> getPartitionToLatencyMap(RouterOperation routerOperation, boolean isLocal) {
    Map<PartitionId, Histogram> partitionToHistogramMap;
    switch (routerOperation) {
      case GetBlobInfoOperation:
        partitionToHistogramMap = isLocal ? routerMetrics.getBlobInfoLocalDcPartitionToLatency
            : routerMetrics.getBlobInfoCrossDcPartitionToLatency;
        break;
      case GetBlobOperation:
        partitionToHistogramMap =
            isLocal ? routerMetrics.getBlobLocalDcPartitionToLatency : routerMetrics.getBlobCrossDcPartitionToLatency;
        break;
      default:
        throw new IllegalArgumentException("Unsupported router operation when getting partition-to-latency map");
    }
    return partitionToHistogramMap;
  }

  /**
   * An iterator to fetch replicas to send requests to. Respects parallelism and discounts requests that have been
   * outstanding for a certain amount of time from parallelism.
   */
  private class OpTrackerIterator implements Iterator<ReplicaId> {

    @Override
    public boolean hasNext() {
      return replicaIterator.hasNext() && (inflightCount < parallelism || isOldestRequestPastDue());
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      if (inflightCount >= parallelism && unexpiredRequestSendTimes.size() > 0) {
        // we are here because oldest request is past due
        Map.Entry<ReplicaId, Pair<Boolean, Long>> oldestEntry = unexpiredRequestSendTimes.entrySet().iterator().next();
        expiredRequestSendTimes.put(oldestEntry.getKey(), oldestEntry.getValue().getSecond());
        unexpiredRequestSendTimes.remove(oldestEntry.getKey());
        pastDueCounter.inc();
      }
      unexpiredRequestSendTimes.put(lastReturned, new Pair<>(false, time.milliseconds()));
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastReturned = replicaIterator.next();
      return lastReturned;
    }

    /**
     * @return {@code true} if the oldest request that was sent has been outstanding for more than the cutoff latency.
     */
    private boolean isOldestRequestPastDue() {
      boolean isPastDue = true;
      if (unexpiredRequestSendTimes.size() > 0) {
        Map.Entry<ReplicaId, Pair<Boolean, Long>> oldestEntry = unexpiredRequestSendTimes.entrySet().iterator().next();
        if (!oldestEntry.getValue().getFirst()) {
          Histogram latencyTracker = getLatencyHistogram(oldestEntry.getKey());
          isPastDue = (latencyTracker.getCount() >= routerConfig.routerOperationTrackerMinDataPointsRequired) && (
              time.milliseconds() - oldestEntry.getValue().getSecond() >= latencyTracker.getSnapshot()
                  .getValue(quantile));
          if (isPastDue) {
            // indicate that the request has been processed and declared expired.
            oldestEntry.setValue(new Pair<>(true, oldestEntry.getValue().getSecond()));
          }
        }
      }
      return isPastDue;
    }
  }
}

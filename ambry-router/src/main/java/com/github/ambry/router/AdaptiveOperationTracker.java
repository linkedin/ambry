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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.Resource;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.utils.CachedHistogram;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.ambry.router.NonBlockingRouterMetrics.*;


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
  private final CachedHistogram localDcHistogram;
  private final CachedHistogram crossDcHistogram;
  private final Counter pastDueCounter;
  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;

  private Map<Resource, CachedHistogram> localDcResourceToHistogram;
  private Map<Resource, CachedHistogram> crossDcResourceToHistogram;

  // The value contains a pair - the boolean indicates whether the request to the corresponding replicaId has been
  // determined as expired (but not yet removed). The long is the time at which the request was sent.
  private final LinkedHashMap<ReplicaId, Pair<Boolean, Long>> unexpiredRequestSendTimes = new LinkedHashMap<>();
  private final Map<ReplicaId, Long> expiredRequestSendTimes = new HashMap<>();

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
    this.otIterator = new OpTrackerIterator();
    if (routerConfig.routerOperationTrackerMetricScope != OperationTrackerScope.Datacenter) {
      localDcResourceToHistogram = getResourceToLatencyMap(routerOperation, true);
      crossDcResourceToHistogram = getResourceToLatencyMap(routerOperation, false);
    }
    if (diskParallelism > routerConfig.routerOperationTrackerMaxInflightRequests) {
      throw new IllegalArgumentException(String.format(
          "Operation tracker disk replica parallelism (%s) is larger than adaptive tracker max inflight number (%s)",
          diskParallelism, routerConfig.routerOperationTrackerMaxInflightRequests));
    }
    if (cloudParallelism > routerConfig.routerOperationTrackerMaxInflightRequests) {
      throw new IllegalArgumentException(String.format(
          "Operation tracker cloud replica parallelism (%s) is larger than adaptive tracker max inflight number (%s)",
          diskParallelism, routerConfig.routerOperationTrackerMaxInflightRequests));
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
    if (trackedRequestFinalState != TrackedRequestFinalState.TIMED_OUT
        || !routerConfig.routerOperationTrackerExcludeTimeoutEnabled) {
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
   * @return the {@link CachedHistogram} associated with this replica.
   */
  CachedHistogram getLatencyHistogram(ReplicaId replicaId) {
    boolean isLocalReplica = replicaId.getDataNodeId().getDatacenterName().equals(datacenterName);
    CachedHistogram histogramToReturn;
    switch (routerConfig.routerOperationTrackerMetricScope) {
      case Datacenter:
        histogramToReturn = isLocalReplica ? localDcHistogram : crossDcHistogram;
        break;
      case Partition:
        PartitionId partitionId = replicaId.getPartitionId();
        histogramToReturn = isLocalReplica ? localDcResourceToHistogram.computeIfAbsent(partitionId,
            k -> createHistogram(routerConfig, false))
            : crossDcResourceToHistogram.computeIfAbsent(partitionId, k -> createHistogram(routerConfig, false));
        break;
      case DataNode:
        DataNodeId dataNodeId = replicaId.getDataNodeId();
        histogramToReturn = isLocalReplica ? localDcResourceToHistogram.computeIfAbsent(dataNodeId,
            k -> createHistogram(routerConfig, false))
            : crossDcResourceToHistogram.computeIfAbsent(dataNodeId, k -> createHistogram(routerConfig, false));
        break;
      case Disk:
        DiskId diskId = replicaId.getDiskId();
        histogramToReturn = isLocalReplica ? localDcResourceToHistogram.computeIfAbsent(diskId,
            k -> createHistogram(routerConfig, false))
            : crossDcResourceToHistogram.computeIfAbsent(diskId, k -> createHistogram(routerConfig, false));
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
  private CachedHistogram getWholeDcTracker(RouterOperation routerOperation, boolean isLocal) {
    CachedHistogram trackerToReturn;
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
   * Get certain resource-level histograms based on given arguments.
   * @param routerOperation the {@link RouterOperation} that uses this tracker.
   * @param isLocal {@code true} if local resource-level histograms should be returned. {@code false} otherwise.
   * @return resource-to-histogram map.
   */
  Map<Resource, CachedHistogram> getResourceToLatencyMap(RouterOperation routerOperation, boolean isLocal) {
    Map<Resource, CachedHistogram> resourceToHistogramMap;
    switch (routerOperation) {
      case GetBlobInfoOperation:
        resourceToHistogramMap = isLocal ? routerMetrics.getBlobInfoLocalDcResourceToLatency
            : routerMetrics.getBlobInfoCrossDcResourceToLatency;
        break;
      case GetBlobOperation:
        resourceToHistogramMap =
            isLocal ? routerMetrics.getBlobLocalDcResourceToLatency : routerMetrics.getBlobCrossDcResourceToLatency;
        break;
      default:
        throw new IllegalArgumentException("Unsupported router operation when getting resource-to-latency map");
    }
    return resourceToHistogramMap;
  }

  /**
   * An iterator to fetch replicas to send requests to. Respects parallelism and discounts requests that have been
   * outstanding for a certain amount of time from parallelism.
   */
  private class OpTrackerIterator implements Iterator<ReplicaId> {

    @Override
    public boolean hasNext() {
      if (replicaIterator.hasNext()) {
        if (inflightCount < getCurrentParallelism()) {
          return true;
        }
        if (inflightCount < routerConfig.routerOperationTrackerMaxInflightRequests && isOldestRequestPastDue()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      if (inflightCount >= getCurrentParallelism() && unexpiredRequestSendTimes.size() > 0) {
        // we are here because oldest request is past due
        Map.Entry<ReplicaId, Pair<Boolean, Long>> oldestEntry = unexpiredRequestSendTimes.entrySet().iterator().next();
        expiredRequestSendTimes.put(oldestEntry.getKey(), oldestEntry.getValue().getSecond());
        unexpiredRequestSendTimes.remove(oldestEntry.getKey());
        pastDueCounter.inc();
      }
      unexpiredRequestSendTimes.put(lastReturnedByIterator, new Pair<>(false, time.milliseconds()));
      inFlightReplicaType = lastReturnedByIterator.getReplicaType();
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastReturnedByIterator = replicaIterator.next();
      return lastReturnedByIterator;
    }

    /**
     * @return {@code true} if the oldest request that was sent has been outstanding for more than the cutoff latency.
     */
    private boolean isOldestRequestPastDue() {
      boolean isPastDue = true;
      if (unexpiredRequestSendTimes.size() > 0) {
        Map.Entry<ReplicaId, Pair<Boolean, Long>> oldestEntry = unexpiredRequestSendTimes.entrySet().iterator().next();
        if (!oldestEntry.getValue().getFirst()) {
          CachedHistogram latencyTracker = getLatencyHistogram(oldestEntry.getKey());
          isPastDue = (latencyTracker.getCount() >= routerConfig.routerOperationTrackerMinDataPointsRequired) && (
              time.milliseconds() - oldestEntry.getValue().getSecond() >= latencyTracker.getCachedValue());
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

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
  static final long MIN_DATA_POINTS_REQUIRED = 1000;

  private final Time time;
  private final String datacenterName;
  private final double quantile;
  private final Histogram localColoTracker;
  private final Histogram crossColoTracker;
  private final Counter pastDueCounter;
  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;
  // The value contains a pair - the boolean indicates whether the request to the corresponding replicaId has been
  // determined as expired (but not yet removed). The long is the time at which the request was sent.
  private final LinkedHashMap<ReplicaId, Pair<Boolean, Long>> unexpiredRequestSendTimes = new LinkedHashMap<>();
  private final Map<ReplicaId, Long> expiredRequestSendTimes = new HashMap<>();

  private ReplicaId lastReturned = null;

  /**
   * Constructs an {@link AdaptiveOperationTracker}
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param crossColoEnabled {@code true} if requests can be sent to remote replicas, {@code false}
   *                                otherwise.
   * @param originatingDcName name of the cross colo DC whose replicas should be tried first.
   * @param includeNonOriginatingDcReplicas if take the option to include remote non originating DC replicas.
   * @param replicasRequired The number of replicas required for the operation.
   * @param successTarget The number of successful responses required to succeed the operation.
   * @param parallelism The maximum number of inflight requests at any point of time.
   * @param time the {@link Time} instance to use.
   * @param localColoTracker the {@link Histogram} that tracks intra datacenter latencies for this class of requests.
   * @param crossColoTracker the {@link Histogram} that tracks inter datacenter latencies for this class of requests.
   * @param pastDueCounter the {@link Counter} that tracks the number of times a request is past due.
   * @param quantile the quantile cutoff to use for when evaluating requests against the trackers.
   */
  AdaptiveOperationTracker(String datacenterName, PartitionId partitionId, boolean crossColoEnabled,
      String originatingDcName, boolean includeNonOriginatingDcReplicas, int replicasRequired, int successTarget,
      int parallelism, Time time, Histogram localColoTracker, Histogram crossColoTracker, Counter pastDueCounter,
      double quantile) {
    super(datacenterName, partitionId, crossColoEnabled, originatingDcName, includeNonOriginatingDcReplicas,
        replicasRequired, successTarget, parallelism, true);
    this.datacenterName = datacenterName;
    this.time = time;
    this.localColoTracker = localColoTracker;
    this.crossColoTracker = crossColoTracker;
    this.pastDueCounter = pastDueCounter;
    this.quantile = quantile;
    this.otIterator = new OpTrackerIterator();
  }

  @Override
  public void onResponse(ReplicaId replicaId, boolean isSuccessFul) {
    super.onResponse(replicaId, isSuccessFul);
    long elapsedTime;
    if (unexpiredRequestSendTimes.containsKey(replicaId)) {
      elapsedTime = time.milliseconds() - unexpiredRequestSendTimes.remove(replicaId).getSecond();
    } else {
      elapsedTime = time.milliseconds() - expiredRequestSendTimes.remove(replicaId);
    }
    getLatencyHistogram(replicaId).update(elapsedTime);
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
   * @return the {@link Histogram} that tracks requests to the class of replicas (intra or inter DC) that
   * {@code replicaId} belongs to.
   */
  private Histogram getLatencyHistogram(ReplicaId replicaId) {
    if (replicaId.getDataNodeId().getDatacenterName().equals(datacenterName)) {
      return localColoTracker;
    }
    return crossColoTracker;
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
          isPastDue = (latencyTracker.getCount() >= MIN_DATA_POINTS_REQUIRED) && (
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

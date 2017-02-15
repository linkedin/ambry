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

import com.codahale.metrics.Histogram;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
class AdaptiveOperationTracker implements OperationTracker {
  static final long MIN_DATA_POINTS_REQUIRED = 1000;

  private final Time time;
  private final String datacenterName;
  private final int successTarget;
  private final int parallelism;
  private final double quantile;
  private final Histogram localColoTracker;
  private final Histogram crossColoTracker;
  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;
  private final LinkedList<ReplicaId> replicaPool = new LinkedList<ReplicaId>();
  private final Map<ReplicaId, Long> replicaRequestSendTimes = new HashMap<>();

  private int totalReplicaCount = 0;
  private int inflightCount = 0;
  private int succeededCount = 0;
  private int failedCount = 0;

  private ReplicaId lastReturned = null;
  private ReplicaId mostRecentlyTracked = null;

  /**
   * Constructs an {@link AdaptiveOperationTracker}
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param crossColoEnabled {@code true} if requests can be sent to remote replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses required to succeed the operation.
   * @param parallelism The maximum number of inflight requests at any point of time.
   * @param time the {@link Time} instance to use.
   * @param localColoTracker the {@link Histogram} that tracks intra datacenter latencies for this class of requests
   * @param crossColoTracker the {@link Histogram} that tracks inter datacenter latencies for this class of requests
   * @param quantile the quantile cutoff to use for when evaluating requests against the trackers.
   */
  AdaptiveOperationTracker(String datacenterName, PartitionId partitionId, boolean crossColoEnabled, int successTarget,
      int parallelism, Time time, Histogram localColoTracker, Histogram crossColoTracker, double quantile) {
    this.datacenterName = datacenterName;
    this.time = time;
    this.successTarget = successTarget;
    this.parallelism = parallelism;
    this.localColoTracker = localColoTracker;
    this.crossColoTracker = crossColoTracker;
    this.quantile = quantile;

    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    List<ReplicaId> replicas = partitionId.getReplicaIds();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    Collections.shuffle(replicas);

    for (ReplicaId replicaId : replicas) {
      String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
      if (!replicaId.isDown()) {
        if (replicaDcName.equals(datacenterName)) {
          replicaPool.addFirst(replicaId);
        } else if (crossColoEnabled) {
          replicaPool.addLast(replicaId);
        }
      } else {
        if (replicaDcName.equals(datacenterName)) {
          downReplicas.addFirst(replicaId);
        } else if (crossColoEnabled) {
          downReplicas.addLast(replicaId);
        }
      }
    }
    replicaPool.addAll(downReplicas);
    totalReplicaCount = replicaPool.size();
    if (totalReplicaCount < successTarget) {
      throw new IllegalArgumentException(
          "Total Replica count " + totalReplicaCount + " is less than success target " + successTarget);
    }
    this.otIterator = new OpTrackerIterator();
  }

  @Override
  public boolean hasSucceeded() {
    return succeededCount >= successTarget;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, boolean isSuccessFul) {
    inflightCount--;
    if (isSuccessFul) {
      succeededCount++;
    } else {
      failedCount++;
    }
    getTracker(replicaId).update(time.milliseconds() - replicaRequestSendTimes.remove(replicaId));
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  /**
   * @return {@code true} if the operation has failed.
   */
  private boolean hasFailed() {
    return (totalReplicaCount - failedCount) < successTarget;
  }

  /**
   * Gets the {@link Histogram} that tracks requests to the class of replicas (intra or inter DC) that {@code replicaId}
   * belongs to.
   * @param replicaId the {@link ReplicaId} whose request latency is going to be tracked.
   * @return the {@link Histogram} that tracks requests to the class of replicas (intra or inter DC) that
   * {@code replicaId} belongs to.
   */
  private Histogram getTracker(ReplicaId replicaId) {
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
      return replicaIterator.hasNext() && (inflightCount < parallelism || isMostRecentRequestPastDue());
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      replicaRequestSendTimes.put(lastReturned, time.milliseconds());
      mostRecentlyTracked = lastReturned;
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
     * @return {@code true} if the most recent request that was sent has been outstanding for more than the cutoff
     * latency or if a response has already been received for it.
     * @throws IllegalStateException if no requests have been sent yet.
     */
    private boolean isMostRecentRequestPastDue() {
      if (replicaRequestSendTimes.size() == 0) {
        throw new IllegalStateException("There have been no requests");
      }
      if (!replicaRequestSendTimes.containsKey(mostRecentlyTracked)) {
        // if the most recently tracked request has already had a response, it is considered past due since
        // the next most recent request is probably due.
        return true;
      }
      long requestSendTime = replicaRequestSendTimes.get(mostRecentlyTracked);
      Histogram latencyTracker = getTracker(mostRecentlyTracked);
      return (latencyTracker.getCount() >= MIN_DATA_POINTS_REQUIRED) && (time.milliseconds() - requestSendTime
          >= latencyTracker.getSnapshot().getValue(quantile));
    }
  }
}

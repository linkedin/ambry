/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * A implementation of {@link OperationTracker}. It internally maintains the status of a
 * corresponding operation, and returns information that decides if the operation should
 * continue or terminate.
 *
 * This implementation simplifies such that it unifies parallelism. That is, a single parallelism
 * parameter controls the maximum number of total allowed in-flight requests to both local and remote
 * replicas. This simplification is valid for PUT operation, yet a mature implementation will take
 * a more sophisticated control of parallelism in the future.
 *
 * This class assumes a request will be {@code succeeded, failed, or timedout} (which means failed).
 * So a deterministic response will be received in a definite time, and no request will pend forever.
 * When a request is timed out, it is considered as failed.
 *
 * A typical usage of an {@code SimpleOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   SimpleOperationTracker operationTracker = new SimpleOperationTracker(datacenterName,
 *            partitionId, crossColoEnabled, successTarget, parallelism);
 *   //...
 *   Iterator<ReplicaId> itr = operationTracker.getReplicaIterator();
 *   while (itr.hasNext()) {
 *     ReplicaId nextReplica = itr.next();
 *     //determine request can be sent to the replica, i.e., connection available.
 *     if(true) {
 *       itr.remove();
 *     }
 *   }
 *}
 *</pre>
 *
 */
class SimpleOperationTracker implements OperationTracker {
  protected final int successTarget;
  protected final int parallelism;
  protected final LinkedList<ReplicaId> replicaPool = new LinkedList<ReplicaId>();

  protected int totalReplicaCount = 0;
  protected int inflightCount = 0;
  protected int succeededCount = 0;
  protected int failedCount = 0;

  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;

  /**
   * Constructor for an {@code SimpleOperationTracker}.
   *
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param crossColoEnabled {@code true} if requests can be sent to remote replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses required to succeed the operation.
   * @param parallelism The maximum number of inflight requests at any point of time.
   * @param shuffleReplicas Indicates if the replicas need to be shuffled.
   */
  SimpleOperationTracker(String datacenterName, PartitionId partitionId, boolean crossColoEnabled, int successTarget,
      int parallelism, boolean shuffleReplicas) {
    if (parallelism < 1) {
      throw new IllegalArgumentException("Parallelism has to be > 0. Configured to be " + parallelism);
    }
    this.successTarget = successTarget;
    this.parallelism = parallelism;
    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    List<? extends ReplicaId> replicas = partitionId.getReplicaIds();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    if (shuffleReplicas) {
      Collections.shuffle(replicas);
    }
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

  /**
   * Constructor for an {@code SimpleOperationTracker}, which shuffles replicas.
   *
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param crossColoEnabled {@code true} if requests can be sent to remote replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses required to succeed the operation.
   * @param parallelism The maximum number of inflight requests at any point of time.
   */
  SimpleOperationTracker(String datacenterName, PartitionId partitionId, boolean crossColoEnabled, int successTarget,
      int parallelism) {
    this(datacenterName, partitionId, crossColoEnabled, successTarget, parallelism, true);
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
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  private class OpTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      return inflightCount < parallelism && replicaIterator.hasNext();
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return replicaIterator.next();
    }
  }

  private boolean hasFailed() {
    return (totalReplicaCount - failedCount) < successTarget;
  }
}

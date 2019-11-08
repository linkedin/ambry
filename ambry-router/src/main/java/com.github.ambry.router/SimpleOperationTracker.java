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
import com.github.ambry.config.RouterConfig;
import java.util.ArrayList;
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
  protected final String datacenterName;
  protected final String originatingDcName;
  protected final int successTarget;
  protected final int parallelism;
  protected final LinkedList<ReplicaId> replicaPool = new LinkedList<>();

  protected int totalReplicaCount = 0;
  protected int inflightCount = 0;
  protected int succeededCount = 0;
  protected int failedCount = 0;

  // How many NotFound responses from originating dc will terminate the operation.
  // It's decided by the success target of each mutation operations, including put, delete, update ttl etc.
  protected int originatingDcNotFoundFailureThreshold = 0;
  protected int originatingDcNotFoundCount = 0;

  private final OpTrackerIterator otIterator;
  private Iterator<ReplicaId> replicaIterator;

  /**
   * Constructor for an {@code SimpleOperationTracker}.
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param routerOperation The {@link RouterOperation} which {@link SimpleOperationTracker} is associated with.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName The original DC where blob was put.
   * @param shuffleReplicas Indicates if the replicas need to be shuffled.
   */
  SimpleOperationTracker(RouterConfig routerConfig, RouterOperation routerOperation, PartitionId partitionId,
      String originatingDcName, boolean shuffleReplicas) {
    // populate tracker parameters based on operation type
    boolean crossColoEnabled = false;
    boolean includeNonOriginatingDcReplicas = true;
    int numOfReplicasRequired = Integer.MAX_VALUE;
    switch (routerOperation) {
      case GetBlobOperation:
      case GetBlobInfoOperation:
        successTarget = routerConfig.routerGetSuccessTarget;
        parallelism = routerConfig.routerGetRequestParallelism;
        crossColoEnabled = routerConfig.routerGetCrossDcEnabled;
        includeNonOriginatingDcReplicas = routerConfig.routerGetIncludeNonOriginatingDcReplicas;
        numOfReplicasRequired = routerConfig.routerGetReplicasRequired;
        break;
      case PutOperation:
        successTarget = routerConfig.routerPutSuccessTarget;
        parallelism = routerConfig.routerPutRequestParallelism;
        break;
      case DeleteOperation:
        successTarget = routerConfig.routerDeleteSuccessTarget;
        parallelism = routerConfig.routerDeleteRequestParallelism;
        crossColoEnabled = true;
        break;
      case TtlUpdateOperation:
        successTarget = routerConfig.routerTtlUpdateSuccessTarget;
        parallelism = routerConfig.routerTtlUpdateRequestParallelism;
        crossColoEnabled = true;
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + routerOperation);
    }
    if (parallelism < 1) {
      throw new IllegalArgumentException("Parallelism has to be > 0. Configured to be " + parallelism);
    }
    datacenterName = routerConfig.routerDatacenterName;
    this.originatingDcName = originatingDcName;

    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    List<? extends ReplicaId> replicas = partitionId.getReplicaIds();
    LinkedList<ReplicaId> backupReplicas = new LinkedList<>();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    if (shuffleReplicas) {
      Collections.shuffle(replicas);
    }
    // While iterating through the replica list, count the number of replicas from the originating DC. And subtract
    // the success target of each mutation operation to get the not found failure threshold.
    int numReplicasInOriginatingDc = 0;

    // The priority here is local dc replicas, originating dc replicas, other dc replicas, down replicas.
    // To improve read-after-write performance across DC, we prefer to take local and originating replicas only,
    // which can be done by setting includeNonOriginatingDcReplicas False.
    List<ReplicaId> examinedReplicas = new ArrayList<>();

    for (ReplicaId replicaId : replicas) {
      examinedReplicas.add(replicaId);
      String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
      if (replicaDcName.equals(originatingDcName)) {
        numReplicasInOriginatingDc++;
      }
      if (!replicaId.isDown()) {
        if (replicaDcName.equals(datacenterName)) {
          replicaPool.addFirst(replicaId);
        } else if (crossColoEnabled && replicaDcName.equals(originatingDcName)) {
          replicaPool.addLast(replicaId);
        } else if (crossColoEnabled) {
          backupReplicas.addFirst(replicaId);
        }
      } else {
        if (replicaDcName.equals(datacenterName)) {
          downReplicas.addFirst(replicaId);
        } else if (crossColoEnabled) {
          downReplicas.addLast(replicaId);
        }
      }
    }
    List<ReplicaId> backupReplicasToCheck = new ArrayList<>(backupReplicas);
    List<ReplicaId> downReplicasToCheck = new ArrayList<>(downReplicas);
    if (includeNonOriginatingDcReplicas || originatingDcName == null) {
      replicaPool.addAll(backupReplicas);
      replicaPool.addAll(downReplicas);
    } else {
      // This is for get request only. Take replicasRequired copy of replicas to do the request
      // Please note replicasRequired is 6 because total number of local and originating replicas is always <= 6.
      // This may no longer be true with partition classes and flexible replication.
      // Don't do this if originatingDcName is unknown.
      while (replicaPool.size() < numOfReplicasRequired && backupReplicas.size() > 0) {
        replicaPool.add(backupReplicas.pollFirst());
      }
      while (replicaPool.size() < numOfReplicasRequired && downReplicas.size() > 0) {
        replicaPool.add(downReplicas.pollFirst());
      }
    }
    totalReplicaCount = replicaPool.size();
    if (totalReplicaCount < successTarget) {
      // {@link MockPartitionId#getReplicaIds} is returning a shared reference which may cause race condition.
      // Please report the test failure if you run into this exception.
      throw new IllegalArgumentException(
          generateErrorMessage(partitionId, examinedReplicas, replicaPool, backupReplicasToCheck, downReplicasToCheck));
    }
    if (routerConfig.routerOperationTrackerTerminateOnNotFoundEnabled && numReplicasInOriginatingDc > 0) {
      this.originatingDcNotFoundFailureThreshold =
          Math.max(numReplicasInOriginatingDc - routerConfig.routerPutSuccessTarget + 1, 0);
    }
    this.otIterator = new OpTrackerIterator();
  }

  @Override
  public boolean hasSucceeded() {
    return succeededCount >= successTarget;
  }

  @Override
  public boolean hasFailedOnNotFound() {
    return originatingDcNotFoundFailureThreshold > 0
        && originatingDcNotFoundCount >= originatingDcNotFoundFailureThreshold;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    inflightCount--;
    if (trackedRequestFinalState == TrackedRequestFinalState.SUCCESS) {
      succeededCount++;
    } else {
      failedCount++;
      // NOT_FOUND is a special error. When tracker sees more than 2 NOT_FOUND from the originating DC, we can
      // be sure the operation will end up with a NOT_FOUND error.
      if (trackedRequestFinalState == TrackedRequestFinalState.NOT_FOUND && replicaId.getDataNodeId()
          .getDatacenterName()
          .equals(originatingDcName)) {
        originatingDcNotFoundCount++;
      }
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
    return (totalReplicaCount - failedCount) < successTarget || hasFailedOnNotFound();
  }

  /**
   * @return the success target number of this operation tracker.
   */
  public int getSuccessTarget() {
    return successTarget;
  }

  /**
   * Helper function to catch a potential race condition in {@link SimpleOperationTracker#SimpleOperationTracker(RouterConfig, RouterOperation, PartitionId, String, boolean)}.
   *
   * @param partitionId The partition on which the operation is performed.
   * @param examinedReplicas All replicas examined.
   * @param replicaPool Replicas added to replicaPool.
   * @param backupReplicas Replicas added to backupReplicas.
   * @param downReplicas Replicas added to downReplicas.
   */
  static private String generateErrorMessage(PartitionId partitionId, List<ReplicaId> examinedReplicas,
      List<ReplicaId> replicaPool, List<ReplicaId> backupReplicas, List<ReplicaId> downReplicas) {
    StringBuilder errMsg = new StringBuilder("Total Replica count ").append(replicaPool.size())
        .append(" is less than success target. ")
        .append("Partition is ")
        .append(partitionId)
        .append(" and partition class is ")
        .append(partitionId.getPartitionClass())
        .append(". examinedReplicas: ");
    for (ReplicaId replicaId : examinedReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("replicaPool: ");
    for (ReplicaId replicaId : replicaPool) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("backupReplicas: ");
    for (ReplicaId replicaId : backupReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("downReplicas: ");
    for (ReplicaId replicaId : downReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    return errMsg.toString();
  }
}

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
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.RouterConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  protected int disabledCount = 0;

  // How many NotFound responses from originating dc will terminate the operation.
  // It's decided by the success target of each mutation operations, including put, delete, update ttl etc.
  protected int originatingDcNotFoundFailureThreshold = 0;
  protected int originatingDcNotFoundCount = 0;

  private final OpTrackerIterator otIterator;
  private final RouterOperation routerOperation;
  private final RouterConfig routerConfig;
  private Iterator<ReplicaId> replicaIterator;
  private static final Logger logger = LoggerFactory.getLogger(SimpleOperationTracker.class);

  /**
   * Constructor for an {@code SimpleOperationTracker}. In constructor, there is a config allowing operation tracker to
   * use eligible replicas to populate replica pool. ("eligible" replicas are those in required states for specific
   * operation)
   * Following are different types of operation and their eligible replica states:
   *  ---------------------------------------------------------
   * |  Operation Type  |        Eligible Replica State        |
   *  ---------------------------------------------------------
   * |      PUT         | STANDBY, LEADER                      |
   * |      GET         | STANDBY, LEADER, BOOTSTRAP, INACTIVE |
   * |    DELETE        | STANDBY, LEADER, BOOTSTRAP           |
   * |   TTLUpdate      | STANDBY, LEADER, BOOTSTRAP           |
   *  ---------------------------------------------------------
   * Following are dynamic configs when replica state is taken into consideration: (N is number of eligible replicas)
   *  -----------------------------------------------------------------------
   * |  Operation Type  |        Parallelism              |  Success Target  |
   *  -----------------------------------------------------------------------
   * |     GET          | 1~2 decided by adaptive tracker |         1        |
   * |     PUT          |           N                     |       N - 1      |
   * |    DELETE        |          3~N                    |         2        |
   * |   TTLUpdate      |          3~N                    |         2        |
   *  -----------------------------------------------------------------------
   *  Note: for now, we still use 3 as parallelism for DELETE/TTLUpdate even though there are N eligible replicas, this
   *        can be adjusted to any number between 3 and N (inclusive)
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
    this.routerConfig = routerConfig;
    this.routerOperation = routerOperation;
    datacenterName = routerConfig.routerDatacenterName;
    List<ReplicaId> eligibleReplicas;
    switch (routerOperation) {
      case GetBlobOperation:
      case GetBlobInfoOperation:
        successTarget = routerConfig.routerGetSuccessTarget;
        parallelism = routerConfig.routerGetRequestParallelism;
        crossColoEnabled = routerConfig.routerGetCrossDcEnabled;
        includeNonOriginatingDcReplicas = routerConfig.routerGetIncludeNonOriginatingDcReplicas;
        numOfReplicasRequired = routerConfig.routerGetReplicasRequired;
        eligibleReplicas = getEligibleReplicas(partitionId, null,
            EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.INACTIVE));
        break;
      case PutOperation:
        eligibleReplicas =
            getEligibleReplicas(partitionId, datacenterName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
        successTarget = routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.max(eligibleReplicas.size() - 1,
            routerConfig.routerPutSuccessTarget) : routerConfig.routerPutSuccessTarget;
        parallelism = routerConfig.routerGetEligibleReplicasByStateEnabled ? eligibleReplicas.size()
            : routerConfig.routerPutRequestParallelism;
        break;
      case DeleteOperation:
        successTarget = routerConfig.routerDeleteSuccessTarget;
        parallelism = routerConfig.routerDeleteRequestParallelism;
        crossColoEnabled = true;
        eligibleReplicas = getEligibleReplicas(partitionId, null,
            EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
        break;
      case TtlUpdateOperation:
        successTarget = routerConfig.routerTtlUpdateSuccessTarget;
        parallelism = routerConfig.routerTtlUpdateRequestParallelism;
        crossColoEnabled = true;
        eligibleReplicas = getEligibleReplicas(partitionId, null,
            EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + routerOperation);
    }
    if (parallelism < 1) {
      throw new IllegalArgumentException("Parallelism has to be > 0. Configured to be " + parallelism);
    }
    this.originatingDcName = originatingDcName;

    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    List<? extends ReplicaId> replicas =
        routerConfig.routerGetEligibleReplicasByStateEnabled ? eligibleReplicas : partitionId.getReplicaIds();
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
      // we relax this condition to account for intermediate state of moving replicas (there could be 6 replicas in
      // originating dc temporarily)
      this.originatingDcNotFoundFailureThreshold = Math.max(numReplicasInOriginatingDc - 1, 0);
    }
    this.otIterator = new OpTrackerIterator();
    logger.debug(
        "Router operation type: {}, successTarget = {}, parallelism = {}, originatingDcNotFoundFailureThreshold = {}",
        routerOperation, successTarget, parallelism, originatingDcNotFoundFailureThreshold);
  }

  @Override
  public boolean hasSucceeded() {
    boolean hasSucceeded;
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      hasSucceeded =
          succeededCount >= Math.max(totalReplicaCount - disabledCount - 1, routerConfig.routerPutSuccessTarget);
    } else {
      hasSucceeded = succeededCount >= successTarget;
    }
    return hasSucceeded;
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
    switch (trackedRequestFinalState) {
      case SUCCESS:
        succeededCount++;
        break;
      case REQUEST_DISABLED:
        disabledCount++;
        break;
      default:
        failedCount++;
        // NOT_FOUND is a special error. When tracker sees >= numReplicasInOriginatingDc - 1 "NOT_FOUND" from the
        // originating DC, we can be sure the operation will end up with a NOT_FOUND error.
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

  /**
   * Get eligible replicas by states for given partition from specified data center. If dcName is null, it gets all eligible
   * replicas from all data centers.
   * @param partitionId the {@link PartitionId} that replicas belong to.
   * @param dcName the name of data center from which the replicas should come from. This can be {@code null}.
   * @param states a set of {@link ReplicaState}(s) that replicas should match.
   * @return a list of eligible replicas that are in specified states.
   */
  private List<ReplicaId> getEligibleReplicas(PartitionId partitionId, String dcName, EnumSet<ReplicaState> states) {
    Set<ReplicaId> eligibleReplicas = new HashSet<>();
    states.forEach(state -> eligibleReplicas.addAll(partitionId.getReplicaIdsByState(state, dcName)));
    return new ArrayList<>(eligibleReplicas);
  }

  private boolean hasFailed() {
    boolean hasFailed;
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      hasFailed = totalReplicaCount - disabledCount - failedCount < Math.max(totalReplicaCount - disabledCount - 1,
          routerConfig.routerPutSuccessTarget);
    } else {
      hasFailed = (totalReplicaCount - failedCount) < successTarget || hasFailedOnNotFound();
    }
    return hasFailed;
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

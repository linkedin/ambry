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
package com.github.ambry.coordinator;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;


/**
 * An OperationPolicy controls parallelism of an operation (how many requests in flight at a time), probing policy
 * (order in which to send requests to replicas), and whether an operation isComplete or not.
 */
public interface OperationPolicy {
  /**
   * Determines if more requests should be sent.
   *
   * @param requestsInFlight replica IDs to which a request is currently in flight.
   * @return true iff one or more additional requests should be in flight
   */
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight);

  /**
   * Determines if an operation is now successfully complete.
   *
   * @return true iff the operation is successfully complete.
   */
  public boolean isComplete();

  /**
   * Determines if an operation may have failed because of cluster wide corrupt state (blob, blob properties, user
   * metadata, on-the-wire, or on-the-disk corruption issues, as well as serde issues).
   *
   * @return true if the operation is failed and all responses fail due to some form of corruption.
   */
  public boolean isCorrupt();

  /**
   * Determines if an operation may complete in the future.
   *
   * @return true iff the operation may complete in the future, false if the operation can never complete in the
   *         future.
   */
  public boolean mayComplete();

  /**
   * Accounts for successful request-response pairs. Operation must invoke this method so that sendMoreRequests and
   * isComplete has necessary information.
   *
   * @param replicaId ReplicaId that successfully handled request.
   */
  public void onSuccessfulResponse(ReplicaId replicaId);

  /**
   * Accounts for failed request-response pairs not due to corruption. Operation must invoke this method so that
   * sendMoreRequests and isComplete has necessary information. A failed request-response pair is any request-response
   * pair that experienced an exception or that returned a response with an error (modulo corruption).
   *
   * @param replicaId ReplicaId that did not successfully handle request.
   */
  public void onFailedResponse(ReplicaId replicaId);

  /**
   * Accounts for failed request-response pairs due to corrupt replica or corrupt response. Operation must invoke this
   * method so that sendMoreRequests and isComplete has necessary information.
   *
   * @param replicaId ReplicaId that has corrupt replica or returned corrupt response.
   */
  public void onCorruptResponse(ReplicaId replicaId);

  /**
   * Determines the next replica to which to send a request. This method embodies the "probing" policy for the
   * operation. I.e., which replicas are sent requests when.
   *
   * @return ReplicaId of next replica to send to.
   */
  public ReplicaId getNextReplicaIdForSend();

  /**
   * Returns the count of replica ids in the partition to which the specified blob id belongs.
   *
   * @return count of replica Ids in partition.
   */
  public int getReplicaIdCount();

  /**
   * Returns whether the operation has proxied to remote colo or not
   *
   * @return true if proxied to remote colo, false otherwise
   */
  public boolean hasProxied();
}

/**
 * Implements a local datacenter first probing policy. I.e., replicas for the local datacenter are sent requests before
 * replicas in remote datacenters. Also implements basic request accounting of failed and successful requests.
 */
abstract class ProbeLocalFirstOperationPolicy implements OperationPolicy {
  int replicaIdCount;
  Queue<ReplicaId> orderedReplicaIds;
  protected boolean proxied = false;

  List<ReplicaId> corruptRequests;
  List<ReplicaId> failedRequests;
  List<ReplicaId> successfulRequests;

  protected Logger logger = LoggerFactory.getLogger(getClass());

  ProbeLocalFirstOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    this.orderedReplicaIds = orderReplicaIds(datacenterName, partitionId.getReplicaIds(), crossDCProxyCallEnabled);
    this.replicaIdCount = this.orderedReplicaIds.size();
    if (replicaIdCount < 1) {
      CoordinatorException e =
          new CoordinatorException("Partition has invalid configuration.", CoordinatorError.UnexpectedInternalError);
      logger.error("PartitionId {} has invalid number of replicas {}: {}", partitionId, replicaIdCount, e);
      throw e;
    }
    this.corruptRequests = new ArrayList<ReplicaId>(replicaIdCount);
    this.failedRequests = new ArrayList<ReplicaId>(replicaIdCount);
    this.successfulRequests = new ArrayList<ReplicaId>(replicaIdCount);
  }

  Queue<ReplicaId> orderReplicaIds(String datacenterName, List<ReplicaId> replicaIds, boolean crossDCProxyCallEnabled) {
    Queue<ReplicaId> orderedReplicaIds = new ArrayDeque<ReplicaId>(replicaIdCount);

    List<ReplicaId> localReplicaIds = new ArrayList<ReplicaId>(replicaIdCount);
    List<ReplicaId> remoteReplicaIds = new ArrayList<ReplicaId>(replicaIdCount);
    for (ReplicaId replicaId : replicaIds) {
      if (!replicaId.isDown()) {
        if (replicaId.getDataNodeId().getDatacenterName().equals(datacenterName)) {
          localReplicaIds.add(replicaId);
        } else if (crossDCProxyCallEnabled) {
          remoteReplicaIds.add(replicaId);
        }
      }
    }

    Collections.shuffle(localReplicaIds);
    orderedReplicaIds.addAll(localReplicaIds);
    Collections.shuffle(remoteReplicaIds);
    orderedReplicaIds.addAll(remoteReplicaIds);

    return orderedReplicaIds;
  }

  @Override
  public abstract boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight);

  @Override
  public abstract boolean isComplete();

  @Override
  public boolean isCorrupt() {
    if (!mayComplete()) {
      return (corruptRequests.size() == failedRequests.size());
    }
    return false;
  }

  @Override
  public void onSuccessfulResponse(ReplicaId replicaId) {
    successfulRequests.add(replicaId);
  }

  @Override
  public void onCorruptResponse(ReplicaId replicaId) {
    corruptRequests.add(replicaId);
    failedRequests.add(replicaId);
  }

  @Override
  public void onFailedResponse(ReplicaId replicaId) {
    failedRequests.add(replicaId);
  }

  @Override
  public ReplicaId getNextReplicaIdForSend() {
    return orderedReplicaIds.remove();
  }

  @Override
  public int getReplicaIdCount() {
    return replicaIdCount;
  }

  @Override
  public boolean hasProxied() {
    return this.proxied;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("OperationPolicyState[");
    sb.append(" orderedReplicaIds=");
    for (ReplicaId replicaId : orderedReplicaIds) {
      sb.append(replicaId).append(", ");
    }
    sb.append("; successfulRequests=");
    for (ReplicaId replicaId : successfulRequests) {
      sb.append(replicaId).append(", ");
    }
    sb.append("; failedRequests=");
    for (ReplicaId replicaId : failedRequests) {
      sb.append(replicaId).append(", ");
    }
    sb.append("; corruptRequests=");
    for (ReplicaId replicaId : corruptRequests) {
      sb.append(replicaId).append(", ");
    }
    sb.append("]");
    return sb.toString();
  }
}

/**
 * Serially probes data nodes until blob is retrieved.
 */
class SerialOperationPolicy extends ProbeLocalFirstOperationPolicy {
  public SerialOperationPolicy(String datacenterName, PartitionId partitionId, OperationContext oc)
      throws CoordinatorException {
    super(datacenterName, partitionId, oc.isCrossDCProxyCallEnabled());
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    return !orderedReplicaIds.isEmpty() && requestsInFlight.size() < 1;
  }

  @Override
  public boolean isComplete() {
    return successfulRequests.size() >= 1;
  }

  @Override
  public boolean mayComplete() {
    return failedRequests.size() != replicaIdCount;
  }
}

/**
 * Sends some number of requests in parallel and waits for a threshold number of successes.
 * <p/>
 * If "additional" parallelism is initially requested (i.e., more than success threshold), then best effort to keep
 * "additional" request outstanding in the face of failed requests. If parallelism is less than success threshold, then
 * no "additional" requests are kept in flight.
 */
abstract class ParallelOperationPolicy extends ProbeLocalFirstOperationPolicy {
  int successTarget;
  int requestParallelism;

  ParallelOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    super(datacenterName, partitionId, crossDCProxyCallEnabled);
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    if (orderedReplicaIds.isEmpty()) {
      return false;
    }

    int inFlightTarget;
    if (requestParallelism >= successTarget) {
      inFlightTarget = requestParallelism - successfulRequests.size();
    } else {
      inFlightTarget = min(requestParallelism, successTarget - successfulRequests.size());
    }
    return (requestsInFlight.size() < inFlightTarget);
  }

  @Override
  public boolean isComplete() {
    return successfulRequests.size() >= successTarget;
  }

  @Override
  public boolean mayComplete() {
    return (replicaIdCount - failedRequests.size()) >= successTarget;
  }
}

/**
 * Sends get requests to all replicas in local data centre honoring parallelism
 * If no conclusion is reached based on local responses, more requests are sent to remote replicas
 * honoring parallelism
 */
class GetCrossColoParallelOperationPolicy extends ParallelOperationPolicy {
  private boolean isLocalDone = false;
  private ReplicaId nextReplicaId = null;
  private int remoteDataCenterCount = 0;
  private int remainingReplicaCount;
  //contains the replica List for each datacenter including local
  private Map<String, List<ReplicaId>> replicaListPerDatacenter;
  //contains the replicas in flight for remote datacenters
  private Map<String, List<ReplicaId>> replicasInFlightPerDatacenter;
  private final String localDataCenterName;
  private final CoordinatorMetrics coordinatorMetrics;

  public GetCrossColoParallelOperationPolicy(String datacenterName, PartitionId partitionId, int parallelism,
      OperationContext oc)
      throws CoordinatorException {
    super(datacenterName, partitionId, true);
    super.successTarget = 1;
    super.requestParallelism = parallelism;
    this.localDataCenterName = datacenterName;
    this.coordinatorMetrics = oc.getCoordinatorMetrics();
    this.remainingReplicaCount = replicaIdCount;
    replicaListPerDatacenter = new HashMap<String, List<ReplicaId>>();
    replicasInFlightPerDatacenter = new HashMap<String, List<ReplicaId>>();
    populateReplicaListPerDatacenter(partitionId.getReplicaIds());
    remoteDataCenterCount = replicaListPerDatacenter.size() - 1;
    shuffleAndPopulate();
  }

  /**
   * Adds replicas to replicasPerDatacenter (Map of datacenter to List of replicas)
   * @param replicaIds ReplicaIds which are to be added to the interested data structure
   */
  private void populateReplicaListPerDatacenter(List<ReplicaId> replicaIds) {
    for (ReplicaId replicaId : replicaIds) {
      if (!replicaId.isDown()) {
        String dataCenterName = replicaId.getDataNodeId().getDatacenterName();
        if (!replicaListPerDatacenter.containsKey(dataCenterName)) {
          replicaListPerDatacenter.put(dataCenterName, new ArrayList<ReplicaId>());
        }
        replicaListPerDatacenter.get(dataCenterName).add(replicaId);
      }
    }
  }

  /**
   * To shuffle the replicas for each datacenter
   */
  private void shuffleAndPopulate() {
    for (String dataCenter : replicaListPerDatacenter.keySet()) {
      Collections.shuffle(replicaListPerDatacenter.get(dataCenter));
      replicasInFlightPerDatacenter.put(dataCenter, new ArrayList<ReplicaId>());
    }
    logger.trace("ReplicaListPerDatacenter " + replicaListPerDatacenter);
  }

  @Override
  public void onSuccessfulResponse(ReplicaId replicaId) {
    super.onSuccessfulResponse(replicaId);
    if (proxied) {
      coordinatorMetrics.successfulCrossColoProxyCallCount.inc();
      logger.trace("Operation succeeded after going cross colo");
    }
    logger.trace("Successful response from " + replicaId);
    onReplicaResponse(replicaId);
  }

  @Override
  public void onCorruptResponse(ReplicaId replicaId) {
    super.onCorruptResponse(replicaId);
    logger.trace("Corrupt response from " + replicaId);
    onReplicaResponse(replicaId);
  }

  @Override
  public void onFailedResponse(ReplicaId replicaId) {
    super.onFailedResponse(replicaId);
    logger.trace("Failed response from " + replicaId);
    onReplicaResponse(replicaId);
  }

  /**
   * Updates the replicasInFlight on response from a replica
   * @param replicaId ReplicaId for which the response was received
   */
  private void onReplicaResponse(ReplicaId replicaId) {
    String dataCenter = replicaId.getDataNodeId().getDatacenterName();
    List<ReplicaId> replicasInFlight = replicasInFlightPerDatacenter.get(dataCenter);
    if (replicasInFlight.contains(replicaId)) {
      replicasInFlight.remove(replicaId);
      if (dataCenter.equals(localDataCenterName)) {
        if (replicaListPerDatacenter.get(localDataCenterName).size() == 0 && replicasInFlight.size() == 0) {
          isLocalDone = true;
          requestParallelism = requestParallelism * remoteDataCenterCount;
          logger.trace("All local replicas exhausted. RequestParallelism changed to " + requestParallelism);
        }
      }
    } else {
      logger.error("Found response for which no request was sent " + replicaId);
      coordinatorMetrics.unknownReplicaResponseError.inc();
    }
  }

  @Override
  public ReplicaId getNextReplicaIdForSend() {
    return nextReplicaId;
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    if (remainingReplicaCount == 0) {
      return false;
    }
    int inFlightTarget = requestParallelism;
    int replicasInFlight = getReplicasInFlightCount();
    if (replicasInFlight < inFlightTarget) {
      setNextReplicaToSend(isLocalDone);
    } else {
      nextReplicaId = null;
    }
    logger.trace("NextReplicaId to send within sendMoreRequests " + nextReplicaId);
    return (nextReplicaId != null);
  }

  /**
   * Fetches the replicas in flight count (either local or remote)
   * @return total number of replicas in flight count
   */
  private int getReplicasInFlightCount() {
    int replicasInFlight = 0;
    for (List<ReplicaId> replicaIdList : replicasInFlightPerDatacenter.values()) {
      replicasInFlight += replicaIdList.size();
    }
    return replicasInFlight;
  }

  /**
   * Sets the next replica for which the request to be sent to a remote data centre
   */
  private void setNextReplicaToSend(boolean forRemoteDatacenter) {
    logger.trace("setNextReplicaToSend called for remote Datacenter " + forRemoteDatacenter);
    String nextDataCenterToSend = localDataCenterName;
    if (forRemoteDatacenter) {
      int requestParallelismPerDatacenter = requestParallelism / remoteDataCenterCount;
      for (String dataCenter : replicasInFlightPerDatacenter.keySet()) {
        if (!dataCenter.equals(localDataCenterName)) {
          if (replicasInFlightPerDatacenter.get(dataCenter).size() < requestParallelismPerDatacenter) {
            if (replicaListPerDatacenter.get(dataCenter).size() > 0) {
              nextDataCenterToSend = dataCenter;
              break;
            }
          }
        }
      }
      logger.trace("NextDataCenter to send " + nextDataCenterToSend);
      if (getReplicasInFlightCount() == 0) {
        proxied = true;
        logger.trace("Operation going cross colo after exhausting all local replicas");
        coordinatorMetrics.totalCrossColoProxyCallCount.inc();
      }
    }
    ReplicaId nextReplicaToSend = null;
    if (replicaListPerDatacenter.get(nextDataCenterToSend).size() > 0) {
      logger.trace(
          "List of pending replicas in next datacenter to send " + replicaListPerDatacenter.get(nextDataCenterToSend));
      nextReplicaToSend = replicaListPerDatacenter.get(nextDataCenterToSend).remove(0);
      replicasInFlightPerDatacenter.get(nextDataCenterToSend).add(nextReplicaToSend);
      remainingReplicaCount--;
    }
    nextReplicaId = nextReplicaToSend;
    logger.trace("Setting next replica to send " + nextReplicaId);
  }
}

/**
 * Sends get requests in parallel. Has up to two in flight to mask single server latency events.
 */
class GetTwoInParallelOperationPolicy extends ParallelOperationPolicy {
  public GetTwoInParallelOperationPolicy(String datacenterName, PartitionId partitionId,
      boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    super(datacenterName, partitionId, crossDCProxyCallEnabled);
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    } else {
      super.successTarget = 1;
      super.requestParallelism = 2;
    }
  }
}

/**
 * Sends requests in parallel --- threshold number for durability plus one for good luck. Durability threshold is 2 so
 * long as there are more than 2 replicas in the partition.
 */
class PutParallelOperationPolicy extends ParallelOperationPolicy {
  /*
   There are many possibilities for extending the put policy. Some ideas that have been discussed include the following:

   (1) Try a new partition ("slipping the partition") before trying remote replicas for initial partition. This
   may be faster than requiring WAN put before succeeding.

   (2) sending additional put requests (increasing the requestParallelism) after a short timeout.
  */
  public PutParallelOperationPolicy(String datacenterName, PartitionId partitionId, OperationContext oc)
      throws CoordinatorException {
    super(datacenterName, partitionId, oc.isCrossDCProxyCallEnabled());
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    } else if (replicaIdCount <= 2) {
      super.successTarget = 1;
      super.requestParallelism = 2;
    } else {
      super.successTarget = 2;
      super.requestParallelism = 3;
    }
  }

  public PutParallelOperationPolicy(String datacenterName, PartitionId partitionId, OperationContext oc,
      int successTarget, int requestParallelism)
      throws CoordinatorException {
    super(datacenterName, partitionId, oc.isCrossDCProxyCallEnabled());
    super.successTarget = successTarget;
    super.requestParallelism = requestParallelism;
  }
}

/**
 * Sends requests in parallel to all replicas. Durability threshold is 2, so  long as there are more than 2 replicas in
 * the partition. Policy is used for delete
 */
class AllInParallelOperationPolicy extends ParallelOperationPolicy {
  public AllInParallelOperationPolicy(String datacenterName, PartitionId partitionId, OperationContext oc)
      throws CoordinatorException {
    super(datacenterName, partitionId, oc.isCrossDCProxyCallEnabled());
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    } else if (replicaIdCount <= 2) {
      super.successTarget = 1;
      super.requestParallelism = 2;
    } else {
      super.successTarget = 2;
      super.requestParallelism = replicaIdCount;
    }
  }
}

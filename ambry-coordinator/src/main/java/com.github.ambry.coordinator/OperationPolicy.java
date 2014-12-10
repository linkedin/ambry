package com.github.ambry.coordinator;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

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
}

/**
 * Implements a local datacenter first probing policy. I.e., replicas for the local datacenter are sent requests before
 * replicas in remote datacenters. Also implements basic request accounting of failed and successful requests.
 */
abstract class ProbeLocalFirstOperationPolicy implements OperationPolicy {
  int replicaIdCount;
  int dataCenterCount;
  int localReplicaCount;
  String localDataCenterName;
  Queue<ReplicaId> orderedReplicaIds;

  List<ReplicaId> corruptRequests;
  List<ReplicaId> failedRequests;
  List<ReplicaId> successfulRequests;
  List<ReplicaId> localRequests;
  //contains the replica List for each datacenter including local
  Map<String, List<ReplicaId>> replicaListPerDatacenter;
  //contains the replicas in Flight for local datacenter
  List<ReplicaId> localReplicasInFlight;
  //contains the replicas in flight for remote datacenters
  Map<String, List<ReplicaId>> replicasInFlightPerDatacenter;

  protected Logger logger = LoggerFactory.getLogger(getClass());

  ProbeLocalFirstOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    this.localRequests = new ArrayList<ReplicaId>();
    this.dataCenterCount = 0;
    this.localDataCenterName = datacenterName;
    this.replicasInFlightPerDatacenter = new HashMap<String, List<ReplicaId>>();
    this.replicaListPerDatacenter = new HashMap<String, List<ReplicaId>>();
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
    localReplicasInFlight = new ArrayList<ReplicaId>();
  }

  Queue<ReplicaId> orderReplicaIds(String datacenterName, List<ReplicaId> replicaIds, boolean crossDCProxyCallEnabled) {
    Queue<ReplicaId> orderedReplicaIds = new ArrayDeque<ReplicaId>(replicaIdCount);

    Map<String, Deque<ReplicaId>> replicasPerDatacenter = new HashMap<String, Deque<ReplicaId>>();
    Map<String, Integer> availableReplicaCountPerDatacenter = new HashMap<String, Integer>();

    //Initialize replicasPerDatacenter and availableReplicaCountPerDatacenter
    for (ReplicaId replicaId : replicaIds) {
      String dataCenter = replicaId.getDataNodeId().getDatacenterName();
      if (!replicasPerDatacenter.containsKey(dataCenter)) {
        replicasPerDatacenter.put(dataCenter, new LinkedList<ReplicaId>());
        availableReplicaCountPerDatacenter.put(dataCenter, 0);
      }
    }

    //Populate replicasPerDatacenter and availableReplicaCountPerDatacenter
    for (ReplicaId replicaId : replicaIds) {
      String dcName = replicaId.getDataNodeId().getDatacenterName();
      if (dcName.equals(datacenterName)) {
        addReplicaToDataCenter(replicaId, dcName, replicasPerDatacenter, availableReplicaCountPerDatacenter);
      } else if (crossDCProxyCallEnabled) {
        addReplicaToDataCenter(replicaId, dcName, replicasPerDatacenter, availableReplicaCountPerDatacenter);
      }
    }

    dataCenterCount += replicasPerDatacenter.size() - 1;
    shuffleAndPopulate(replicasPerDatacenter, availableReplicaCountPerDatacenter, orderedReplicaIds);
    return orderedReplicaIds;
  }

  private void shuffleAndPopulate(Map<String, Deque<ReplicaId>> replicasPerDatacenter,
      Map<String, Integer> availableReplicaCountPerDatacenter, Queue<ReplicaId> orderedReplicaIds) {

    List<ReplicaId> replicaIdList =
        shuffleReplicaListForDataCenter(localDataCenterName, replicasPerDatacenter, availableReplicaCountPerDatacenter);
    localRequests = replicaIdList;
    localReplicaCount = replicaIdList.size();
    orderedReplicaIds.addAll(localRequests);
    replicaListPerDatacenter.put(localDataCenterName, replicaIdList);

    for (String dataCenter : replicasPerDatacenter.keySet()) {
      if (!dataCenter.equalsIgnoreCase(localDataCenterName)) {
        List<ReplicaId> replicaList =
            shuffleReplicaListForDataCenter(dataCenter, replicasPerDatacenter, availableReplicaCountPerDatacenter);
        orderedReplicaIds.addAll(replicaList);
        replicaListPerDatacenter.put(dataCenter, replicaList);
        replicasInFlightPerDatacenter.put(dataCenter, new ArrayList<ReplicaId>());
      }
    }
  }

  private List<ReplicaId> shuffleReplicaListForDataCenter(String dataCenter,
      Map<String, Deque<ReplicaId>> replicasPerDatacenter, Map<String, Integer> availableReplicaCountPerDatacenter) {
    List<ReplicaId> replicaList = new ArrayList<ReplicaId>();
    replicaList.addAll(replicasPerDatacenter.get(dataCenter));
    List<ReplicaId> availableReplicas = replicaList.subList(0, availableReplicaCountPerDatacenter.get(dataCenter));
    Collections.shuffle(availableReplicas);
    List<ReplicaId> downReplicas =
        replicaList.subList(availableReplicaCountPerDatacenter.get(dataCenter), replicaList.size());
    Collections.shuffle(downReplicas);
    return replicaList;
  }

  private void addReplicaToDataCenter(ReplicaId replicaId, String dataCenterName,
      Map<String, Deque<ReplicaId>> replicasPerDatacenter, Map<String, Integer> availableReplicaCountPerDatacenter) {
    if (replicaId.isDown()) {
      replicasPerDatacenter.get(dataCenterName).addLast(replicaId);
    } else {
      replicasPerDatacenter.get(dataCenterName).add(replicaId);
      availableReplicaCountPerDatacenter
          .put(dataCenterName, availableReplicaCountPerDatacenter.get(dataCenterName) + 1);
    }
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
  public SerialOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    super(datacenterName, partitionId, crossDCProxyCallEnabled);
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
  boolean isLocalDone = false;
  ReplicaId nextReplicaId = null;
  List<ReplicaId> localReplicasInFlight;

  public GetCrossColoParallelOperationPolicy(String datacenterName, PartitionId partitionId)
      throws CoordinatorException {
    this(datacenterName, partitionId, 2);
  }

  public GetCrossColoParallelOperationPolicy(String datacenterName, PartitionId partitionId, int parallelism)
      throws CoordinatorException {
    super(datacenterName, partitionId, true);
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    } else {
      super.successTarget = 1;
    }

    super.requestParallelism = parallelism;
    localReplicasInFlight = new ArrayList<ReplicaId>();
  }

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
    onReplicaResponse(replicaId);
  }

  @Override
  public void onCorruptResponse(ReplicaId replicaId) {
    corruptRequests.add(replicaId);
    failedRequests.add(replicaId);
    onReplicaResponse(replicaId);
  }

  @Override
  public void onFailedResponse(ReplicaId replicaId) {
    failedRequests.add(replicaId);
    onReplicaResponse(replicaId);
  }

  /**
   * Updates the replicasInFlight on response from a replica
   * @param replicaId
   */
  private void onReplicaResponse(ReplicaId replicaId) {
    boolean isReplicaFound = false;
    if (localReplicasInFlight.contains(replicaId)) {
      isReplicaFound = true;
      localReplicasInFlight.remove(replicaId);
      if (replicaListPerDatacenter.get(localDataCenterName).size() == 0 && localReplicasInFlight.size() == 0) {
        isLocalDone = true;
        requestParallelism = requestParallelism * dataCenterCount;
        replicaListPerDatacenter.remove(localDataCenterName);
      }
    } else {
      for (String dataCenter : replicasInFlightPerDatacenter.keySet()) {
        List<ReplicaId> replicasInFlight = replicasInFlightPerDatacenter.get(dataCenter);
        if (replicasInFlight.contains(replicaId)) {
          replicasInFlight.remove(replicaId);
          isReplicaFound = true;
          replicasInFlightPerDatacenter.get(dataCenter).clear();
          replicasInFlightPerDatacenter.get(dataCenter).addAll(replicasInFlight);
          break;
        }
      }
    }
    if (!isReplicaFound) {
      logger.error("Found response for which no request was sent ",
          new CoordinatorException("Found response from " + replicaId + "replica for which no request was sent ",
              CoordinatorError.UnexpectedInternalError));
    }
  }

  @Override
  public ReplicaId getNextReplicaIdForSend() {
    return nextReplicaId;
  }

  /**
   * Fetches the next replica for which the request to be sent from a remote data centre
   * @return ReplicaId for whcih the request has to be sent
   */
  private ReplicaId getNextRemoteReplicaToSend() {
    ReplicaId nextRemoteReplica = null;
    String nextDataCenter = null;
    for (String dataCenter : replicasInFlightPerDatacenter.keySet()) {
      List<ReplicaId> replicaIdList = replicasInFlightPerDatacenter.get(dataCenter);
      if (nextDataCenter == null) {
        nextDataCenter = dataCenter;
      }
      if (replicaIdList.size() < requestParallelism) {
        nextDataCenter = dataCenter;
        break;
      }
    }
    nextRemoteReplica = replicaListPerDatacenter.get(nextDataCenter).remove(0);
    if (replicaListPerDatacenter.get(nextDataCenter).size() == 0) {
      replicaListPerDatacenter.remove(nextDataCenter);
    }
    replicasInFlightPerDatacenter.get(nextDataCenter).add(nextRemoteReplica);
    return nextRemoteReplica;
  }

  public int getReplicasInFlightCount() {
    int replicasInFlight = 0;
    if (isLocalDone) {
      for (String dataCenter : replicasInFlightPerDatacenter.keySet()) {
        replicasInFlight += replicasInFlightPerDatacenter.get(dataCenter).size();
      }
    } else {
      replicasInFlight = localReplicasInFlight.size();
    }
    return replicasInFlight;
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    if (replicaListPerDatacenter.isEmpty()) {
      return false;
    }
    boolean toReturn = false;
    int inFlightTarget = requestParallelism;

    int replicasInFlight = getReplicasInFlightCount();
    if (isLocalDone) {
      if (replicasInFlight < inFlightTarget) {
        nextReplicaId = getNextRemoteReplicaToSend();
        toReturn = true;
      } else {
        nextReplicaId = null;
      }
    } else {
      if (replicasInFlight < inFlightTarget) {
        List<ReplicaId> replicaIdList = replicaListPerDatacenter.get(localDataCenterName);
        if (replicaIdList.size() == 0) {
          toReturn = false;
          nextReplicaId = null;
        } else {
          nextReplicaId = replicaIdList.remove(0);
          localReplicasInFlight.add(nextReplicaId);
          toReturn = true;
        }
      }
    }
    return toReturn;
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
  public PutParallelOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    super(datacenterName, partitionId, crossDCProxyCallEnabled);
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
}

/**
 * Sends requests in parallel to all replicas. Durability threshold is 2, so  long as there are more than 2 replicas in
 * the partition. Policy is used for delete
 */
class AllInParallelOperationPolicy extends ParallelOperationPolicy {
  public AllInParallelOperationPolicy(String datacenterName, PartitionId partitionId, boolean crossDCProxyCallEnabled)
      throws CoordinatorException {
    super(datacenterName, partitionId, crossDCProxyCallEnabled);
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

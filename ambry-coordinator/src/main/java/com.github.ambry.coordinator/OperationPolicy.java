package com.github.ambry.coordinator;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static java.lang.Math.min;

/**
 * An OperationPolicy controls parallelism of an operation (how many requests in flight at a time), probing policy
 * (order in which to send requests to replicas), and whether an operation isDone or not.
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
   * Determines if an operation is complete or not.
   *
   * @return true iff the operation is complete.
   * @throws CoordinatorException if operation must end in an exceptional manner.
   */
  public boolean isDone() throws CoordinatorException;

  /**
   * Accounts for successful request-response pairs. Operation must invoke this method so that sendMoreRequests and
   * isDone has necessary information.
   *
   * @param replicaId ReplicaId that successfully handled request.
   */
  public void successfulResponse(ReplicaId replicaId);

  /**
   * Accounts for failed request-response pairs. Operation must invoke this method so that sendMoreRequests and isDone
   * has necessary information. A failed request-response pair is any request-response pair that experienced an
   * exception or that returned are response with an error.
   *
   * @param replicaId ReplicaId that did not successfully handle request.
   */
  public void failedResponse(ReplicaId replicaId);

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
 * Implements a local datacenter first probing policy. Implements basic request accounting too.
 */
abstract class ProbeLocalFirstOperationPolicy implements OperationPolicy {
  protected int replicaIdCount;
  protected Queue<ReplicaId> orderedReplicaIds;

  protected List<ReplicaId> failedRequests;
  protected List<ReplicaId> successfulRequests;

  protected ProbeLocalFirstOperationPolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    this.replicaIdCount = partitionId.getReplicaIds().size();
    if (replicaIdCount < 1) {
      throw new CoordinatorException("Insufficient replica ids in specified partition.",
                                     CoordinatorError.UnexpectedInternalError);
    }
    this.orderedReplicaIds = orderReplicaIds(datacenterName, partitionId.getReplicaIds());

    this.failedRequests = new ArrayList<ReplicaId>(replicaIdCount);
    this.successfulRequests = new ArrayList<ReplicaId>(replicaIdCount);
  }

  protected Queue<ReplicaId> orderReplicaIds(String datacenterName, List<ReplicaId> replicaIds) {
    Queue<ReplicaId> orderedReplicaIds = new ArrayDeque<ReplicaId>(replicaIdCount);

    List<ReplicaId> localReplicaIds = new ArrayList<ReplicaId>(replicaIdCount);
    List<ReplicaId> remoteReplicaIds = new ArrayList<ReplicaId>(replicaIdCount);
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.getDataNodeId().getDatacenterName().equals(datacenterName)) {
        localReplicaIds.add(replicaId);
      }
      else {
        remoteReplicaIds.add(replicaId);
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
  public abstract boolean isDone() throws CoordinatorException;

  @Override
  public void successfulResponse(ReplicaId replicaId) {
    successfulRequests.add(replicaId);
  }

  @Override
  public void failedResponse(ReplicaId replicaId) {
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
}

/**
 * Serially probes data nodes until blob is retrieved.
 */
class GetPolicy extends ProbeLocalFirstOperationPolicy {
  public GetPolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    super(datacenterName, partitionId);
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    return !orderedReplicaIds.isEmpty() && requestsInFlight.size() < 1;
  }

  @Override
  public boolean isDone() throws CoordinatorException {
    if (successfulRequests.size() >= 1) {
      return true;
    }
    else if (failedRequests.size() == replicaIdCount) {
      throw new CoordinatorException("Insufficient DataNodes replied to complete operation",
                                     CoordinatorError.AmbryUnavailable);
    }
    return false;
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
  protected int successTarget;
  protected int requestParallelism;

  protected ParallelOperationPolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    super(datacenterName, partitionId);
  }

  @Override
  public boolean sendMoreRequests(Collection<ReplicaId> requestsInFlight) {
    if (orderedReplicaIds.isEmpty()) {
      return false;
    }

    int inFlightTarget;
    if (requestParallelism >= successTarget) {
      inFlightTarget = requestParallelism - successfulRequests.size();
    }
    else {
      inFlightTarget = min(requestParallelism, successTarget - successfulRequests.size());
    }
    return (requestsInFlight.size() < inFlightTarget);
  }

  @Override
  public boolean isDone() throws CoordinatorException {
    if (successfulRequests.size() >= successTarget) {
      return true;
    }
    else if ((replicaIdCount - failedRequests.size()) < successTarget) {
      throw new CoordinatorException("Insufficient DataNodes replied to complete Operation.",
                                     CoordinatorError.AmbryUnavailable);
    }
    return false;
  }
}

class PutPolicy extends ParallelOperationPolicy {
  /*
   There are many possibilities for extending the put policy. Some ideas that have been discussed include the following:

   (1) Try a new partition ("slipping the partition") before trying remote replicas for initial partition. This
   may be faster than requiring WAN put before succeeding.

   (2) sending additional put requests (increasing the requestParallelism) after a short timeout.
  */
  public PutPolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    super(datacenterName, partitionId);
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    }
    else if (replicaIdCount <= 2) {
      super.successTarget = 1;
      super.requestParallelism = 2;
    }
    else {
      super.successTarget = 2;
      super.requestParallelism = 3;
    }
  }
}

class CancelTTLPolicy extends ParallelOperationPolicy {
  public CancelTTLPolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    super(datacenterName, partitionId);
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    }
    else if (replicaIdCount <= 2) {
      super.successTarget = 1;
      super.requestParallelism = 2;
    }
    else {
      super.successTarget = 2;
      super.requestParallelism = replicaIdCount;
    }
  }
}

class DeletePolicy extends ParallelOperationPolicy {
  public DeletePolicy(String datacenterName, PartitionId partitionId) throws CoordinatorException {
    super(datacenterName, partitionId);
    if (replicaIdCount == 1) {
      super.successTarget = 1;
      super.requestParallelism = 1;
    }
    else if (replicaIdCount <= 2) {
      super.successTarget = 1;
      super.requestParallelism = 2;
    }
    else {
      super.successTarget = 2;
      super.requestParallelism = replicaIdCount;
    }
  }
}

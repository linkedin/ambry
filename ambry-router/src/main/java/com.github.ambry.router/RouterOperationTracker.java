package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import java.util.*;


/**
 * An implementation of {@code OperationTracker} used by non-blocking router. It internally maintains
 * the status of the corresponding operation, and returns information to progress or terminate the
 * operation.
 *
 * This class assumes a request will be {@code succeeded, failed, or timed out} (which means failed).
 * No request will pend forever.
 *
 * This class is not meant to be thread safe. Please apply appropriate mechanisms at the caller.
 *
 * A typical usage of an {@code RouterOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   RouterOperationTracker operationTracker = new RouterOperationTracker(datacenterName,
 *            partitionId, operationType, localOnly, successTarget, localParallelism);
 *   //...
 *   while (operationTracker.shouldSendMoreRequests()) {
 *     nextReplica = operationTracker.getNextReplicaIdForSend();
 *     //send request to nextReplica.
 *     operationTracker.onSend(nextReplica);
 *   }
 *
 *}
 *</pre>
 *
 */
public class RouterOperationTracker implements OperationTracker {
  final OperationType operationType;
  final boolean localDcOnly;
  final int successTarget;
  final int localParallelism;

  int numLocalReplica = 0;
  int numLocalUnsent = 0;
  int numTotalRemoteUnsent = 0;
  int numTotalRemoteInflight = 0;
  int numTotalRemoteSucceeded = 0;
  int numTotalRemoteFailed = 0;
  final String localDcName;
  final PartitionId partitionId;
  ReplicaId nextLocalToSend = null;
  ReplicaId nextRemoteToSend = null;

  final LinkedList<ReplicaId> localUnsentQueue = new LinkedList<ReplicaId>();
  final LinkedList<ReplicaId> localInflightQueue = new LinkedList<ReplicaId>();
  final LinkedList<ReplicaId> localSucceededQueue = new LinkedList<ReplicaId>();
  final LinkedList<ReplicaId> localFailedQueue = new LinkedList<ReplicaId>();
  final HashSet<ReplicaId> totalReplicaSet = new HashSet<ReplicaId>();
  final HashMap<String, LinkedList<ReplicaId>> remoteUnsentQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteInflightQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteSucceededQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteFailedQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();

  /**
   * Constructor for an {@code OperationTracker}.
   *
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param operationType The type of operation, which can be one of {@code PUT, GET, DELETE}.
   * @param localDcOnly {@code true} if requests only can be sent to local replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses received to succeed the operation.
   * @param localParallelism The maximum number of inflight requests sent to local replicas.
   */
  public RouterOperationTracker(String datacenterName, PartitionId partitionId, OperationType operationType,
      boolean localDcOnly, int successTarget, int localParallelism) {
    this.operationType = operationType;
    this.localDcOnly = localDcOnly;
    this.successTarget = successTarget;
    this.localParallelism = localParallelism;
    this.localDcName = datacenterName;
    this.partitionId = partitionId;
    List<ReplicaId> replicas = partitionId.getReplicaIds();

    for (ReplicaId replicaId : replicas) {
      if (!replicaId.isDown()) {
        String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
        if (replicaDcName.equals(localDcName)) {
          totalReplicaSet.add(replicaId);
          localUnsentQueue.add(replicaId);
          numLocalReplica++;
          numLocalUnsent++;
        } else if (!localDcOnly) {
          if (!remoteUnsentQueuePerDc.containsKey(replicaDcName)) {
            remoteUnsentQueuePerDc.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteInflightQueuePerDc.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteSucceededQueuePerDc.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteFailedQueuePerDc.put(replicaDcName, new LinkedList<ReplicaId>());
          }
          totalReplicaSet.add(replicaId);
          remoteUnsentQueuePerDc.get(replicaDcName).add(replicaId);
          numTotalRemoteUnsent++;
        }
      }
    }
  }

  @Override
  public boolean shouldSendMoreRequests() {
    return canSendMoreLocal() || canSendMoreRemote();
  }

  /**
   * Determine if requests can be sent to more replicas. A request can be sent to a local replica
   * only when an operation is NOT completed.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  private boolean canSendMoreLocal() {
    if (isComplete()) {
      return false;
    } else if (nextLocalToSend != null) {
      return true;
    } else if (localUnsentQueue.size() > 0 && localInflightQueue.size() < localParallelism) {
      nextLocalToSend = localUnsentQueue.poll();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Determine if there are more remote replicas to send request. A request can be sent to a remote replica
   * only when an operation is NOT completed.
   * 
   * <p>
   * If {@code localOnly} is set {@code true}, no request can be sent to remote replicas.
   *
   * <p>
   * If an operation is {@code GET or DELETE}, a request can be sent to only when responses from all local
   * replicas have been received.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  private boolean canSendMoreRemote() {
    if (nextRemoteToSend != null) {
      return true;
    } else if (localDcOnly || nextLocalToSend!=null || isSucceeded() || isFailed()
        || localSucceededQueue.size() + localFailedQueue.size() < numLocalReplica) {
      return false;
    } else {
      for (Map.Entry<String, LinkedList<ReplicaId>> entry : remoteUnsentQueuePerDc.entrySet()) {
        String remoteDcName = entry.getKey();
        if (entry.getValue().size() > 0 && remoteInflightQueuePerDc.get(remoteDcName).size() == 0) {
          nextRemoteToSend = entry.getValue().poll();
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean isSucceeded() {
    return getTotalSucceeded() >= successTarget;
  }

  @Override
  public boolean isComplete() {
    return isSucceeded() || isFailed();
  }

  @Override
  public boolean isFailed() {
    return totalReplicaSet.size() - getTotalFailed() < successTarget;
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    if (!totalReplicaSet.contains(replicaId)) {
      throw new IllegalStateException("Responding replica does not belong to the Partition.");
    }
    String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
    if (localDcName.equals(replicaDcName)) {
      localInflightQueue.remove(replicaId);
      if (e != null) {
        localFailedQueue.add(replicaId);
      } else {
        localSucceededQueue.add(replicaId);
      }
    } else {
      remoteInflightQueuePerDc.get(replicaDcName).remove(replicaId);
      numTotalRemoteInflight--;
      if (e != null) {
        remoteFailedQueuePerDc.get(replicaDcName).add(replicaId);
        numTotalRemoteFailed++;
      } else {
        remoteSucceededQueuePerDc.get(replicaDcName).add(replicaId);
        numTotalRemoteSucceeded++;
      }
    }
  }

  @Override
  public void onSend(ReplicaId replicaId) {
    if (replicaId == null) {
      throw new IllegalStateException("Cannot onsend a null replica.");
    } else if (replicaId != nextLocalToSend && replicaId != nextRemoteToSend) {
      throw new IllegalStateException("This replica is not selected by operation tracker.");
    }
    String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
    if (localDcName.equals(replicaDcName)) {
      localInflightQueue.add(replicaId);
      numLocalUnsent--;
    } else {
      remoteInflightQueuePerDc.get(replicaDcName).add(replicaId);
      numTotalRemoteUnsent--;
      numTotalRemoteInflight++;
    }
    if (replicaId == nextLocalToSend) {
      nextLocalToSend = null;
    } else {
      nextRemoteToSend = null;
    }
  }

  @Override
  public ReplicaId getNextReplicaIdForSend() {
    if (nextLocalToSend != null) {
      return nextLocalToSend;
    } else {
      return nextRemoteToSend;
    }
  }

  private int getTotalSucceeded() {
    return localSucceededQueue.size() + numTotalRemoteSucceeded;
  }

  private int getTotalFailed() {
    return localFailedQueue.size() + numTotalRemoteFailed;
  }
}

package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import java.util.*;


/**
 * An implementation of OperationTracker. It internally maintains the status of the corresponding operation,
 * and makes decision how to progress the operation.
 *
 * An operation tracker is configured through {@code AmbryPolicyConfig}, and depends on the operation type,
 * which can be one of {@code PUT, GET, DELETE}.
 *
 * A typical usage of an operation policy would be:
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
  OperationType operationType;
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
  final HashSet<ReplicaId> totalToSendReplicaSet = new HashSet<ReplicaId>();
  final HashMap<String, LinkedList<ReplicaId>> remoteUnsentQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteInflightQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteSucceededQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteFailedQueuePerDc = new HashMap<String, LinkedList<ReplicaId>>();

  /**
   * Constructor for {@code AmbryOperationPolicy}.
   *
   * @param datacenterName The datacenter where the operation is performed.
   * @param partitionId The partition on which the operation is performed.
   * @param operationType The type of operation that helps determine the corresponding policy.
   * @param localDcOnly
   * @param successTarget
   * @param localParallelism
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
          totalToSendReplicaSet.add(replicaId);
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
          totalToSendReplicaSet.add(replicaId);
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
   * Determine if there are more local replicas to send request. When a request can be sent to a replica,
   * the operation can be neither in {@code succeeded} nor {@code failed} status. This decision is subject
   * to {@code localParameterFactor}.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  public boolean canSendMoreLocal() {
    if (nextLocalToSend != null) {
      return true;
    } else if (isSucceeded() || isFailed()) {
      return false;
    } else if (localUnsentQueue.size() > 0 && localInflightQueue.size() < localParallelism) {
      nextLocalToSend = localUnsentQueue.poll();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Determine if there are more remote replicas to send request. When a request can be sent to a replica,
   * the operation can be neither in {@code succeeded} nor {@code failed} status.
   *
   * If {@code localOnly} is set {@code true}, no request can be sent to remote replicas.
   *
   * If {@code localBarrier} is enabled, a request can be sent to only when responses from all local replicas
   * have been rerceived.
   *
   * The decision is subject to both {@code remoteParallelFactorPerDc} and {@code totalRemoteParallelFactor}.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  public boolean canSendMoreRemote() {
    if (nextRemoteToSend != null) {
      return true;
    } else if (localDcOnly || isSucceeded() || isFailed()
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
    return totalToSendReplicaSet.size() - getTotalFailed() < successTarget;
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    if (!totalToSendReplicaSet.contains(replicaId)) {
      throw new IllegalStateException("Responding replica does not belong to the Partition.");
    }
    String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
    if (localDcName.equals(replicaDcName)) {
      if (e != null) {
        localFailedQueue.add(replicaId);
      } else {
        localSucceededQueue.add(replicaId);
      }
    } else {
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
      throw new IllegalStateException("This replica is not selected by policy.");
    }
    String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
    if (localDcName.equals(replicaDcName)) {
      localInflightQueue.add(replicaId);
      numLocalUnsent--;
    } else {
      remoteInflightQueuePerDc.get(replicaDcName).add(replicaId);
      numTotalRemoteUnsent--;
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

  private int getTotalInflight() {
    return localInflightQueue.size() + numTotalRemoteInflight;
  }

  private int getTotalRemoteInflight() {
    return numTotalRemoteInflight;
  }

  private int getTotalUnsent() {
    return numLocalUnsent + numTotalRemoteUnsent;
  }
}

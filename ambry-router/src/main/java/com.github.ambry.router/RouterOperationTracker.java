package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import java.util.*;


/**
 * An implementation of {@link OperationTracker} used by non-blocking router. It internally maintains
 * the status of the corresponding operation, and returns information to progress or terminate the
 * operation.
 *
 * This class assumes a request will be {@code succeeded, failed, or timed out} (which means failed).
 * No request will pend forever.
 *
 * This class is not meant to be thread safe. Please apply appropriate mechanisms at the caller.
 *
 * A typical usage of an {@link RouterOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   RouterOperationTracker operationTracker = new RouterOperationTracker(datacenterName,
 *            partitionId, operationType, localOnly, successTarget, localParallelism);
 *   //...
 *       ReplicaId nextReplica = operationTracker.getNextReplica();
 *       while (nextReplica != null) {
 *         //send a request to the replica.
 *         nextReplica = operationTracker.getNextReplica();
 *       }
 *}
 *</pre>
 *
 */
public class RouterOperationTracker implements OperationTracker {
  final private OperationType operationType;
  final private boolean localDcOnly;
  final private int successTarget;
  final private int localParallelism;

  private int numLocalReplica = 0;
  private int numTotalRemoteSucceeded = 0;
  private int numTotalRemoteFailed = 0;
  private final String localDcName;
  private final PartitionId partitionId;
  private ReplicaId nextLocalToSend = null;
  private ReplicaId nextRemoteToSend = null;

  private final LinkedList<ReplicaId> localUnsentQueue = new LinkedList<ReplicaId>();
  private int numLocalInflight = 0;
  private int numLocalSucceeded = 0;
  private int numLocalFailed = 0;
  private final HashSet<ReplicaId> totalReplicaSet = new HashSet<ReplicaId>();
  private final HashMap<String, LinkedList<ReplicaId>> remoteUnsentQueuePerDc =
      new HashMap<String, LinkedList<ReplicaId>>();
  private final HashMap<String, Integer> remoteInflightQueuePerDc = new HashMap<String, Integer>();

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
        } else if (!localDcOnly) {
          if (!remoteUnsentQueuePerDc.containsKey(replicaDcName)) {
            remoteUnsentQueuePerDc.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteInflightQueuePerDc.put(replicaDcName, 0);
          }
          totalReplicaSet.add(replicaId);
          remoteUnsentQueuePerDc.get(replicaDcName).add(replicaId);
        }
      }
    }
    numLocalReplica = localUnsentQueue.size();
  }

  private boolean shouldSendMoreRequests() {
    return canSendMoreLocal() || canSendMoreRemote();
  }

  /**
   * Determine if requests can be sent to more replicas. A request can be sent to a local replica
   * only when an operation is NOT completed.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  private boolean canSendMoreLocal() {
    if (nextLocalToSend != null) {
      return true;
    } else if (hasSucceeded() || hasFailed()) {
      return false;
    } else if (localUnsentQueue.size() > 0 && numLocalInflight < localParallelism) {
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
   * <p/>
   * If {@code localOnly} is set {@code true}, no request can be sent to remote replicas.
   *
   * <p/>
   * If an operation is {@code GET or DELETE}, a request can be sent to only when responses from all local
   * replicas have been received.
   *
   * @return {@code true} if there is at least one more local replica to send request.
   */
  private boolean canSendMoreRemote() {
    if (nextRemoteToSend != null) {
      return true;
    } else if (localDcOnly || nextLocalToSend != null || hasSucceeded() || hasFailed()
        || numLocalSucceeded + numLocalFailed < numLocalReplica) {
      return false;
    } else {
      for (Map.Entry<String, LinkedList<ReplicaId>> entry : remoteUnsentQueuePerDc.entrySet()) {
        String remoteDcName = entry.getKey();
        if (entry.getValue().size() > 0 && remoteInflightQueuePerDc.get(remoteDcName) == 0) {
          nextRemoteToSend = entry.getValue().poll();
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean hasSucceeded() {
    return getTotalSucceeded() >= successTarget;
  }

  @Override
  public boolean hasFailed() {
    return totalReplicaSet.size() - getTotalFailed() < successTarget;
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    if (!totalReplicaSet.contains(replicaId)) {
      throw new IllegalStateException("Responding replica does not belong to the Partition.");
    }
    String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
    if (localDcName.equals(replicaDcName)) {
      numLocalInflight--;
      if (e != null) {
        numLocalFailed++;
      } else {
        numLocalSucceeded++;
      }
    } else {
      remoteInflightQueuePerDc.put(replicaDcName, remoteInflightQueuePerDc.get(replicaDcName) - 1);
      if (e != null) {
        numTotalRemoteFailed++;
      } else {
        numTotalRemoteSucceeded++;
      }
    }
  }

  @Override
  public ReplicaId getNextReplica() {
    if (!shouldSendMoreRequests()) {
      return null;
    }
    ReplicaId nextToReturn = null;
    if (nextLocalToSend != null) {
      nextToReturn = nextLocalToSend;
      numLocalInflight++;
      nextLocalToSend = null;
    } else {
      nextToReturn = nextRemoteToSend;
      String remoteDc = nextRemoteToSend.getDataNodeId().getDatacenterName();
      remoteInflightQueuePerDc.put(remoteDc, remoteInflightQueuePerDc.get(remoteDc) + 1);
      nextRemoteToSend = null;
    }
    return nextToReturn;
  }

  private int getTotalSucceeded() {
    return numLocalSucceeded + numTotalRemoteSucceeded;
  }

  private int getTotalFailed() {
    return numLocalFailed + numTotalRemoteFailed;
  }
}

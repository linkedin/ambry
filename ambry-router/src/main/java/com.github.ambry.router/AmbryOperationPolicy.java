package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import com.github.ambry.config.AmbryPolicyConfig;
import java.util.*;


/**
 * An implementation of OperationPolicy. It internally maintains the status of the corresponding operation,
 * and makes decision how to progress the operation.
 *
 * An operation policy is configured through {@code AmbryPolicyConfig}, and depends on the operation type,
 * which can be one of {@code PUT, GET, DELETE}.
 *
 * A typical usage of an operation policy would be:
 *<pre>
 *{@code
 *
 *   AmbryOperationPolicy ambryOperationPolicy = new AmbryOperationPolicy(datacenterName,
 *            partitionId, operationType, ambryPolicyConfig);
 *   //...
 *   while (operationPolicy.shouldSendMoreRequests()) {
 *     nextReplica = operationPolicy.getNextReplicaIdForSend();
 *     //send request to nextReplica.
 *     operationPolicy.onSend(nextReplica);
 *   }
 *
 *}
 *</pre>
 *
 */
public class AmbryOperationPolicy implements OperationPolicy {
  OperationType operationType;
  final boolean localDcOnly;
  final boolean localBarrier;
  final int successTarget;
  final int localParallelFactor;
  int remoteParallelFactorPerDc;
  int totalRemoteParallelFactor;

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
  final HashSet<ReplicaId> totalValidReplicaSet = new HashSet<ReplicaId>();
  final HashMap<String, LinkedList<ReplicaId>> remoteDcUnsentQueue = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteDcInflightQueue = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteDcSucceededQueue = new HashMap<String, LinkedList<ReplicaId>>();
  final HashMap<String, LinkedList<ReplicaId>> remoteDcFailedQueue = new HashMap<String, LinkedList<ReplicaId>>();

  /**
   * Constructor for {@code AmbryOperationPolicy}.
   *
   * @param datacenterName The datacenter where the operation is performed.
   * @param partitionId The partition on which the operation is performed.
   * @param operationType The type of operation that helps determine the corresponding policy.
   * @param ambryPolicyConfig Configuration parameters for the policy.
   */
  public AmbryOperationPolicy(String datacenterName, PartitionId partitionId, OperationType operationType,
      AmbryPolicyConfig ambryPolicyConfig) {
    this.operationType = operationType;
    if (operationType == OperationType.GET) {
      localDcOnly = ambryPolicyConfig.routerGetPolicyLocalOnly;
      localBarrier = ambryPolicyConfig.routerGetPolicyLocalBarrier;
      successTarget = ambryPolicyConfig.routerGetPolicySuccessTarget;
      localParallelFactor = ambryPolicyConfig.routerGetPolicyLocalParallelFactor;
      remoteParallelFactorPerDc = ambryPolicyConfig.routerGetPolicyRemoteParallelFactorPerDc;
      totalRemoteParallelFactor = ambryPolicyConfig.routerGetPolicyTotalRemoteParallelFactor;
    } else if (operationType == OperationType.PUT) {
      localDcOnly = ambryPolicyConfig.routerPutPolicyLocalOnly;
      localBarrier = ambryPolicyConfig.routerPutPolicyLocalBarrier;
      successTarget = ambryPolicyConfig.routerPutPolicySuccessTarget;
      localParallelFactor = ambryPolicyConfig.routerPutPolicyLocalParallelFactor;
      remoteParallelFactorPerDc = ambryPolicyConfig.routerPutPolicyRemoteParallelFactorPerDc;
      totalRemoteParallelFactor = ambryPolicyConfig.routerPutPolicyTotalRemoteParallelFactor;
    } else if (operationType == OperationType.DELETE) {
      localDcOnly = ambryPolicyConfig.routerDeletePolicyLocalOnly;
      localBarrier = ambryPolicyConfig.routerDeletePolicyLocalBarrier;
      successTarget = ambryPolicyConfig.routerDeletePolicySuccessTarget;
      localParallelFactor = ambryPolicyConfig.routerDeletePolicyLocalParallelFactor;
      remoteParallelFactorPerDc = ambryPolicyConfig.routerDeletePolicyRemoteParallelFactorPerDc;
      totalRemoteParallelFactor = ambryPolicyConfig.routerDeletePolicyTotalRemoteParallelFactor;
    } else {
      throw new IllegalArgumentException("Wrong operation type.");
    }

    this.localDcName = datacenterName;
    this.partitionId = partitionId;
    List<ReplicaId> replicas = partitionId.getReplicaIds();
    if (localDcOnly) {
      remoteParallelFactorPerDc = 0;
      totalRemoteParallelFactor = 0;
    }
    for (ReplicaId replicaId : replicas) {
      if (!replicaId.isDown()) {
        String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
        if (replicaDcName.equals(localDcName)) {
          totalValidReplicaSet.add(replicaId);
          localUnsentQueue.add(replicaId);
          numLocalReplica++;
          numLocalUnsent++;
        } else if (!localDcOnly) {
          if (!remoteDcUnsentQueue.containsKey(replicaDcName)) {
            remoteDcUnsentQueue.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteDcInflightQueue.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteDcSucceededQueue.put(replicaDcName, new LinkedList<ReplicaId>());
            remoteDcFailedQueue.put(replicaDcName, new LinkedList<ReplicaId>());
          }
          totalValidReplicaSet.add(replicaId);
          remoteDcUnsentQueue.get(replicaDcName).add(replicaId);
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
    if (isSucceeded() || isFailed()) {
      return false;
    } else if (nextLocalToSend != null) {
      return true;
    } else if (localUnsentQueue.size() > 0 && localInflightQueue.size() < localParallelFactor) {
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
    if (isSucceeded() || isFailed() || (localBarrier && (localSucceededQueue.size() + localFailedQueue.size()
        < numLocalReplica))) {
      return false;
    } else if (nextRemoteToSend != null) {
      return true;
    } else {
      for (Map.Entry<String, LinkedList<ReplicaId>> entry : remoteDcUnsentQueue.entrySet()) {
        String remoteDcName = entry.getKey();
        if (entry.getValue().size() > 0 && remoteDcInflightQueue.get(remoteDcName).size() < remoteParallelFactorPerDc
            && getTotalRemoteInflight() < totalRemoteParallelFactor) {
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
    return totalValidReplicaSet.size() - getTotalFailed() < successTarget;
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    if (!totalValidReplicaSet.contains(replicaId)) {
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
        remoteDcFailedQueue.get(replicaDcName).add(replicaId);
        numTotalRemoteFailed++;
      } else {
        remoteDcSucceededQueue.get(replicaDcName).add(replicaId);
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
      remoteDcInflightQueue.get(replicaDcName).add(replicaId);
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

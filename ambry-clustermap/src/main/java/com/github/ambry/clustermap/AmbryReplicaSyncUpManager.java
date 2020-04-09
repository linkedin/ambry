/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * An implementation of {@link ReplicaSyncUpManager} that helps track replica catchup state.
 *
 * To track state of replica that is catching up or being commissioned, this class leverages a {@link java.util.concurrent.CountDownLatch}.
 * Every time {@link ReplicaSyncUpManager#initiateBootstrap(ReplicaId)} is called, a new latch (with initial value = 1)
 * is created associated with given replica. Any caller that invokes {@link ReplicaSyncUpManager#waitBootstrapCompleted(String)}
 * is blocked and wait until corresponding latch counts to zero. External component (i.e. replication manager) is able to
 * call {@link ReplicaSyncUpManager#onBootstrapComplete(ReplicaId)} or {@link ReplicaSyncUpManager#onBootstrapError(ReplicaId)}
 * to mark sync-up success or failure by counting down the latch. This will unblock caller waiting fot this latch and
 * proceed with subsequent actions.
 */
public class AmbryReplicaSyncUpManager implements ReplicaSyncUpManager {
  private final ConcurrentHashMap<String, CountDownLatch> partitionToBootstrapLatch = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CountDownLatch> partitionToDeactivationLatch = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CountDownLatch> partitionToDisconnectionLatch = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> partitionToBootstrapSuccess = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> partitionToDeactivationSuccess = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Boolean> partitionToDisconnectionSuccess = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ReplicaId, LocalReplicaLagInfos> replicaToLagInfos = new ConcurrentHashMap<>();
  private final ClusterMapConfig clusterMapConfig;

  private static final Logger logger = LoggerFactory.getLogger(AmbryReplicaSyncUpManager.class);

  public AmbryReplicaSyncUpManager(ClusterMapConfig clusterMapConfig) {
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public void initiateBootstrap(ReplicaId replicaId) {
    partitionToBootstrapLatch.put(replicaId.getPartitionId().toPathString(), new CountDownLatch(1));
    partitionToBootstrapSuccess.put(replicaId.getPartitionId().toPathString(), false);
    replicaToLagInfos.put(replicaId,
        new LocalReplicaLagInfos(replicaId, clusterMapConfig.clustermapReplicaCatchupAcceptableLagBytes,
            ReplicaState.BOOTSTRAP));
  }

  @Override
  public void initiateDeactivation(ReplicaId replicaId) {
    partitionToDeactivationLatch.put(replicaId.getPartitionId().toPathString(), new CountDownLatch(1));
    partitionToDeactivationSuccess.put(replicaId.getPartitionId().toPathString(), false);
    // once deactivation is initiated, local replica won't receive new PUTs. All remote replicas should be able to
    // eventually catch with last PUT in local store. Hence, we set acceptable lag threshold to 0.
    replicaToLagInfos.put(replicaId, new LocalReplicaLagInfos(replicaId, 0, ReplicaState.INACTIVE));
  }

  /**
   * {@inheritDoc}
   * The method is blocked on a {@link java.util.concurrent.CountDownLatch} until the bootstrap is complete
   */
  @Override
  public void waitBootstrapCompleted(String partitionName) throws InterruptedException {
    CountDownLatch latch = partitionToBootstrapLatch.get(partitionName);
    if (latch == null) {
      logger.info("Skipping bootstrap for existing partition {}", partitionName);
    } else {
      logger.info("Waiting for new partition {} to complete bootstrap", partitionName);
      latch.await();
      partitionToBootstrapLatch.remove(partitionName);
      if (!partitionToBootstrapSuccess.remove(partitionName)) {
        throw new StateTransitionException("Partition " + partitionName + " failed to bootstrap.", BootstrapFailure);
      }
      logger.info("Bootstrap is complete on partition {}", partitionName);
    }
  }

  @Override
  public void waitDeactivationCompleted(String partitionName) throws InterruptedException {
    CountDownLatch latch = partitionToDeactivationLatch.get(partitionName);
    if (latch == null) {
      logger.error("Partition {} is not found for deactivation", partitionName);
      throw new StateTransitionException("No deactivation latch is found for partition " + partitionName,
          ReplicaNotFound);
    } else {
      logger.info("Waiting for partition {} to be deactivated", partitionName);
      latch.await();
      partitionToDeactivationLatch.remove(partitionName);
      // throw exception to put replica into ERROR state， this happens when disk crashes during deactivation
      if (!partitionToDeactivationSuccess.remove(partitionName)) {
        throw new StateTransitionException("Deactivation failed on partition " + partitionName, DeactivationFailure);
      }
      logger.info("Deactivation is complete on partition {}", partitionName);
    }
  }

  @Override
  public boolean updateLagBetweenReplicas(ReplicaId localReplica, ReplicaId peerReplica, long lagInBytes) {
    boolean updated = false;
    LocalReplicaLagInfos lagInfos = replicaToLagInfos.get(localReplica);
    if (lagInfos != null) {
      lagInfos.updateLagInfo(peerReplica, lagInBytes);
      if (logger.isDebugEnabled()) {
        logger.debug(lagInfos.toString());
      }
      updated = true;
    }
    return updated;
  }

  @Override
  public boolean isSyncUpComplete(ReplicaId replicaId) {
    LocalReplicaLagInfos lagInfos = replicaToLagInfos.get(replicaId);
    if (lagInfos == null) {
      throw new IllegalStateException(
          "Replica " + replicaId.getPartitionId().toPathString() + " is not found in AmbryReplicaSyncUpManager!");
    }
    return lagInfos.hasSyncedUpWithEnoughPeers();
  }

  /**
   * {@inheritDoc}
   * This method will count down the latch associated with given replica and
   * unblock external service waiting on this latch.
   */
  @Override
  public void onBootstrapComplete(ReplicaId replicaId) {
    partitionToBootstrapSuccess.put(replicaId.getPartitionId().toPathString(), true);
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToBootstrapLatch, replicaId.getPartitionId().toPathString());
  }

  @Override
  public void onDeactivationComplete(ReplicaId replicaId) {
    partitionToDeactivationSuccess.put(replicaId.getPartitionId().toPathString(), true);
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToDeactivationLatch, replicaId.getPartitionId().toPathString());
  }

  /**
   * {@inheritDoc}
   * This method will count down latch and terminates bootstrap.
   */
  @Override
  public void onBootstrapError(ReplicaId replicaId) {
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToBootstrapLatch, replicaId.getPartitionId().toPathString());
  }

  @Override
  public void onDeactivationError(ReplicaId replicaId) {
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToDeactivationLatch, replicaId.getPartitionId().toPathString());
  }

  @Override
  public void initiateDisconnection(ReplicaId replicaId) {
    partitionToDisconnectionLatch.put(replicaId.getPartitionId().toPathString(), new CountDownLatch(1));
    partitionToDisconnectionSuccess.put(replicaId.getPartitionId().toPathString(), false);
    // once disconnection is initiated, local replica won't receive any PUT/DELETE/TTLUpdate. All remote replicas should
    // be able to eventually catch with local replica. Hence, we set acceptable lag threshold to 0.
    replicaToLagInfos.put(replicaId, new LocalReplicaLagInfos(replicaId, 0, ReplicaState.OFFLINE));
  }

  @Override
  public void waitDisconnectionCompleted(String partitionName) throws InterruptedException {
    CountDownLatch latch = partitionToDisconnectionLatch.get(partitionName);
    if (latch == null) {
      logger.error("Partition {} is not found for disconnection", partitionName);
      throw new StateTransitionException("No disconnection latch is found for partition " + partitionName,
          ReplicaNotFound);
    } else {
      logger.info("Waiting for partition {} to be disconnected", partitionName);
      latch.await();
      partitionToDisconnectionLatch.remove(partitionName);
      // throw exception to put replica into ERROR state， this happens when disk crashes during disconnection
      if (!partitionToDisconnectionSuccess.remove(partitionName)) {
        throw new StateTransitionException("Disconnection failed on partition " + partitionName, DisconnectionFailure);
      }
      logger.info("Disconnection is complete on partition {}", partitionName);
    }
  }

  @Override
  public void onDisconnectionComplete(ReplicaId replicaId) {
    partitionToDisconnectionSuccess.put(replicaId.getPartitionId().toPathString(), true);
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToDisconnectionLatch, replicaId.getPartitionId().toPathString());
  }

  @Override
  public void onDisconnectionError(ReplicaId replicaId) {
    replicaToLagInfos.remove(replicaId);
    countDownLatch(partitionToDisconnectionLatch, replicaId.getPartitionId().toPathString());
  }

  /**
   * @return the map whose key is partition name and the value is corresponding deactivation latch.
   */
  public ConcurrentHashMap<String, CountDownLatch> getPartitionToDeactivationLatch() {
    return partitionToDeactivationLatch;
  }

  /**
   * @return the map whose key is partition name and the value is corresponding disconnection latch.
   */
  public ConcurrentHashMap<String, CountDownLatch> getPartitionToDisconnectionLatch() {
    return partitionToDisconnectionLatch;
  }

  /**
   * clean up in-mem maps
   */
  void reset() {
    partitionToBootstrapLatch.clear();
    partitionToBootstrapSuccess.clear();
    partitionToDeactivationLatch.clear();
    partitionToDeactivationSuccess.clear();
    partitionToDisconnectionLatch.clear();
    partitionToDisconnectionSuccess.clear();
    replicaToLagInfos.clear();
  }

  /**
   * Count down the latch associated with given partition
   * @param countDownLatchMap the map in which the latch is specified for given partition.
   * @param partitionName the partition whose corresponding latch needs to count down.
   */
  private void countDownLatch(Map<String, CountDownLatch> countDownLatchMap, String partitionName) {
    CountDownLatch latch = countDownLatchMap.get(partitionName);
    if (latch == null) {
      throw new IllegalStateException("No countdown latch is found for partition " + partitionName);
    } else {
      latch.countDown();
    }
  }

  /**
   * A class helps to (1) record local replica's lag from peer replicas; (2) track number of peers that local replica has
   * caught up with; (3) determine if local replica's sync-up is complete.
   */
  private class LocalReplicaLagInfos {
    // keep lag map here for tracking progress
    private final ConcurrentHashMap<ReplicaId, Long> localDcPeerReplicaAndLag = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ReplicaId, Long> remoteDcPeerReplicaAndLag = new ConcurrentHashMap<>();
    // refer to those replicas that current replica has caught up with
    private final Set<ReplicaId> localDcCaughtUpReplicas = ConcurrentHashMap.newKeySet();
    private final Set<ReplicaId> remoteDcCaughtUpReplicas = ConcurrentHashMap.newKeySet();
    private final String localDcName;
    private final long acceptableThreshold;
    private final int catchupTarget;
    private final ReplicaId replicaOnCurrentNode;
    private final ReplicaState currentState;

    /**
     * Constructor for {@link LocalReplicaLagInfos}.
     * @param localReplica {@link ReplicaId} on current node.
     * @param acceptableThreshold acceptable threshold in byte to determine if replica has caught up (or has been caught
     *                            up with by peer replicas)
     * @param currentState the current {@link ReplicaState} of local replica.
     */
    LocalReplicaLagInfos(ReplicaId localReplica, long acceptableThreshold, ReplicaState currentState) {
      this.acceptableThreshold = acceptableThreshold;
      this.currentState = currentState;
      Set<ReplicaId> peerReplicas = new HashSet<>();
      // new replica only needs to catch up with STANDBY or LEADER replicas
      for (ReplicaState state : EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)) {
        peerReplicas.addAll(localReplica.getPartitionId().getReplicaIdsByState(state, null));
      }
      replicaOnCurrentNode = localReplica;
      localDcName = localReplica.getDataNodeId().getDatacenterName();
      for (ReplicaId peerReplica : peerReplicas) {
        if (peerReplica == replicaOnCurrentNode) {
          // This may happen when local replica transits from STANDBY to INACTIVE because Helix still shows local replica
          // is in STANDBY. We skip here if peer replica == replica on current node.
          continue;
        }
        // put peer replicas into local/remote DC maps (initial value is Long.MAX_VALUE)
        if (peerReplica.getDataNodeId().getDatacenterName().equals(localDcName)) {
          localDcPeerReplicaAndLag.put(peerReplica, Long.MAX_VALUE);
        } else {
          remoteDcPeerReplicaAndLag.put(peerReplica, Long.MAX_VALUE);
        }
      }
      catchupTarget = clusterMapConfig.clustermapReplicaCatchupTarget == 0 ? localDcPeerReplicaAndLag.size()
          : clusterMapConfig.clustermapReplicaCatchupTarget;
    }

    /**
     * Update replication lag in bytes between current replica and specified peer.
     * @param peerReplica the peer replica that the current replica is lagging behind
     * @param lagInBytes the bytes of current replica's lag from peer replica
     */
    void updateLagInfo(ReplicaId peerReplica, long lagInBytes) {
      if (peerReplica.getDataNodeId().getDatacenterName().equals(localDcName)) {
        localDcPeerReplicaAndLag.put(peerReplica, lagInBytes);
        if (lagInBytes <= acceptableThreshold) {
          localDcCaughtUpReplicas.add(peerReplica);
        } else {
          localDcCaughtUpReplicas.remove(peerReplica);
        }
      } else {
        remoteDcPeerReplicaAndLag.put(peerReplica, lagInBytes);
        if (lagInBytes <= acceptableThreshold) {
          remoteDcCaughtUpReplicas.add(peerReplica);
        } else {
          remoteDcCaughtUpReplicas.remove(peerReplica);
        }
      }
    }

    /**
     * @return whether current replica has caught up with enough peers
     */
    boolean hasSyncedUpWithEnoughPeers() {
      // We don't need to check if replicas, which have been caught up with, are up or down currently. As long as, the
      // peer replica has been put into catchup set, this means it has been caught up sometime before it went down.
      return localDcCaughtUpReplicas.size() + remoteDcCaughtUpReplicas.size() >= catchupTarget;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Replica(")
          .append(replicaOnCurrentNode.getReplicaPath())
          .append("), current state: ")
          .append(currentState)
          .append(", lag infos: ");
      sb.append("Local DC peer replicas lag: {");
      for (Map.Entry<ReplicaId, Long> replicaAndLag : localDcPeerReplicaAndLag.entrySet()) {
        sb.append(" [")
            .append(replicaAndLag.getKey().getDataNodeId().getHostname())
            .append(" lag = ")
            .append(replicaAndLag.getValue())
            .append(" bytes ]");
      }
      sb.append(" }  Remote DC peer replicas lag: {");
      for (Map.Entry<ReplicaId, Long> replicaAndLag : remoteDcPeerReplicaAndLag.entrySet()) {
        sb.append(" [")
            .append(replicaAndLag.getKey().getDataNodeId().getHostname())
            .append(" lag = ")
            .append(replicaAndLag.getValue())
            .append(" bytes ]");
      }
      sb.append(" }");
      return sb.toString();
    }
  }
}

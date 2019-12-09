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


public class AmbryReplicaSyncUpService implements ReplicaSyncUpService {
  private ConcurrentHashMap<String, CountDownLatch> partitionToBootstrapLatch = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Boolean> partitionToBootstrapSuccess = new ConcurrentHashMap<>();
  private ConcurrentHashMap<ReplicaId, RemoteReplicaLagInfos> replicaToLagInfos = new ConcurrentHashMap<>();
  private final ClusterMapConfig clusterMapConfig;

  private static final Logger logger = LoggerFactory.getLogger(AmbryReplicaSyncUpService.class);

  AmbryReplicaSyncUpService(ClusterMapConfig clusterMapConfig) {
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public void initiateBootstrap(ReplicaId replicaId) {
    partitionToBootstrapLatch.put(replicaId.getPartitionId().toPathString(), new CountDownLatch(1));
    partitionToBootstrapSuccess.put(replicaId.getPartitionId().toPathString(), false);
    replicaToLagInfos.put(replicaId,
        new RemoteReplicaLagInfos(replicaId, clusterMapConfig.clustermapReplicaCatchupAcceptableLagBytes));
  }

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
        throw new IllegalStateException("Partition " + partitionName + " failed on bootstrap.");
      }
      logger.info("Bootstrap is complete on partition {}", partitionName);
    }
  }

  @Override
  public boolean updateLagBetweenReplicas(ReplicaId chaser, ReplicaId precursor, long lagInBytes) {
    boolean updated = false;
    if (replicaToLagInfos.containsKey(chaser)) {
      replicaToLagInfos.get(chaser).updateLagInfo(precursor, lagInBytes);
      logger.debug(replicaToLagInfos.get(chaser).toString());
      updated = true;
    }
    return updated;
  }

  @Override
  public boolean isSyncUpComplete(ReplicaId replicaId) {
    RemoteReplicaLagInfos lagInfos = replicaToLagInfos.get(replicaId);
    if (lagInfos == null) {
      throw new IllegalStateException(
          "Replica " + replicaId.getPartitionId().toPathString() + " is not found in AmbryReplicaSyncUpService!");
    }
    return lagInfos.hasSyncedUpWithEnoughPeers();
  }

  @Override
  public void onBootstrapComplete(String partitionName) {
    partitionToBootstrapSuccess.put(partitionName, true);
    countDownLatch(partitionName);
  }

  @Override
  public void onBootstrapError(String partitionName) {
    countDownLatch(partitionName);
  }

  /**
   * Count down the latch associated with given partition
   * @param partitionName the partition whose corresponding latch needs to count down.
   */
  private void countDownLatch(String partitionName) {
    CountDownLatch latch = partitionToBootstrapLatch.get(partitionName);
    if (latch == null) {
      throw new IllegalStateException("No bootstrap latch is found for partition " + partitionName);
    } else {
      latch.countDown();
    }
  }

  private class RemoteReplicaLagInfos {
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

    RemoteReplicaLagInfos(ReplicaId localReplica, long acceptableThreshold) {
      this.acceptableThreshold = acceptableThreshold;
      Set<ReplicaId> peerReplicas = new HashSet<>();
      // new replica only needs to catch up with STANDBY or LEADER replicas
      for (ReplicaState state : EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)) {
        peerReplicas.addAll(localReplica.getPartitionId().getReplicaIdsByState(state, null));
      }
      replicaOnCurrentNode = localReplica;
      localDcName = localReplica.getDataNodeId().getDatacenterName();
      for (ReplicaId peerReplica : peerReplicas) {
        // put peer replicas into local/remote DC maps (initial value is Long.MAX_VALUE)
        if (peerReplica.getDataNodeId().getDatacenterName().equals(localDcName)) {
          localDcPeerReplicaAndLag.put(peerReplica, Long.MAX_VALUE);
        } else {
          remoteDcPeerReplicaAndLag.put(peerReplica, Long.MAX_VALUE);
        }
      }
      catchupTarget = localDcPeerReplicaAndLag.size();
    }

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

    boolean hasSyncedUpWithEnoughPeers() {
      // We don't need to check if replicas, which have been caught up with, are up or down currently. As long as, the
      // peer replica has been put into catchup set, this means it has been caught up sometime before it went down.
      return localDcCaughtUpReplicas.size() + remoteDcCaughtUpReplicas.size() >= catchupTarget;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Replica(").append(replicaOnCurrentNode.getReplicaPath()).append(") lag infos: ");
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

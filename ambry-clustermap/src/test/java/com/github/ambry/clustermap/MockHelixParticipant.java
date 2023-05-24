/*
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static org.mockito.Mockito.*;


public class MockHelixParticipant extends HelixParticipant {
  public static MetricRegistry metricRegistry = new MetricRegistry();
  static HelixFactory mockHelixFactory = new MockHelixManagerFactory();
  public Boolean updateNodeInfoReturnVal = null;
  public Boolean setStoppedStateReturnVal = null;
  public PartitionStateChangeListener mockStatsManagerListener = null;
  public boolean overrideDisableReplicaMethod = true;
  public boolean resetPartitionVal = true;
  CountDownLatch listenerLatch = null;
  ReplicaState replicaState = ReplicaState.OFFLINE;
  ReplicaId currentReplica = null;
  ReplicaSyncUpManager replicaSyncUpService = null;
  private Set<ReplicaId> sealedReplicas = new HashSet<>();
  private Set<ReplicaId> partiallySealedReplicas = new HashSet<>();
  private Set<ReplicaId> stoppedReplicas = new HashSet<>();
  private Set<ReplicaId> disabledReplicas = new HashSet<>();
  private PartitionStateChangeListener mockReplicationManagerListener;

  public MockHelixParticipant(ClusterMapConfig clusterMapConfig) {
    this(clusterMapConfig, mockHelixFactory);
    // create mock state change listener for ReplicationManager
    mockReplicationManagerListener = Mockito.mock(PartitionStateChangeListener.class);
    // mock Bootstrap-To-Standby change
    doAnswer(invocation -> {
      replicaState = ReplicaState.BOOTSTRAP;
      if (replicaSyncUpService != null && currentReplica != null) {
        replicaSyncUpService.initiateBootstrap(currentReplica);
      }
      if (listenerLatch != null) {
        listenerLatch.countDown();
      }
      return null;
    }).when(mockReplicationManagerListener).onPartitionBecomeStandbyFromBootstrap(any(String.class));
    // mock Standby-To-Inactive change
    doAnswer(invocation -> {
      replicaState = ReplicaState.INACTIVE;
      if (replicaSyncUpService != null && currentReplica != null) {
        replicaSyncUpService.initiateDeactivation(currentReplica);
      }
      if (listenerLatch != null) {
        listenerLatch.countDown();
      }
      return null;
    }).when(mockReplicationManagerListener).onPartitionBecomeInactiveFromStandby(any(String.class));
    // mock Inactive-To-Offline change
    doAnswer(invocation -> {
      replicaState = ReplicaState.OFFLINE;
      if (replicaSyncUpService != null && currentReplica != null) {
        replicaSyncUpService.initiateDisconnection(currentReplica);
      }
      if (listenerLatch != null) {
        listenerLatch.countDown();
      }
      return null;
    }).when(mockReplicationManagerListener).onPartitionBecomeOfflineFromInactive(any(String.class));
  }

  public MockHelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) {
    super(clusterMapConfig, helixFactory, metricRegistry,
        parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings).get(
            clusterMapConfig.clusterMapDatacenterName).getZkConnectStrs().get(0), true);
  }

  @Override
  public void participate(List<AmbryStatsReport> ambryStatsReports, AccountStatsStore accountStatsStore,
      Callback<AggregatedAccountStorageStats> callback) throws IOException {
    // no op
  }

  @Override
  public boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus) {
    switch (replicaSealStatus) {
      case SEALED:
        sealedReplicas.add(replicaId);
        partiallySealedReplicas.remove(replicaId);
        break;
      case PARTIALLY_SEALED:
        partiallySealedReplicas.add(replicaId);
        sealedReplicas.remove(replicaId);
        break;
      case NOT_SEALED:
        partiallySealedReplicas.remove(replicaId);
        sealedReplicas.remove(replicaId);
        break;
    }
    return true;
  }

  @Override
  public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
    if (setStoppedStateReturnVal != null) {
      return setStoppedStateReturnVal;
    }
    if (markStop) {
      stoppedReplicas.addAll(replicaIds);
    } else {
      stoppedReplicas.removeAll(replicaIds);
    }
    return true;
  }

  @Override
  public void setReplicaDisabledState(ReplicaId replicaId, boolean disable) {
    if (overrideDisableReplicaMethod) {
      if (disable) {
        disabledReplicas.add(replicaId);
      } else {
        disabledReplicas.remove(replicaId);
      }
    } else {
      super.setReplicaDisabledState(replicaId, disable);
    }
  }

  @Override
  public boolean resetPartitionState(String partitionName) {
    return resetPartitionVal;
  }

  @Override
  public List<String> getSealedReplicas() {
    return sealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public List<String> getPartiallySealedReplicas() {
    return partiallySealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public List<String> getStoppedReplicas() {
    return stoppedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public List<String> getDisabledReplicas() {
    return disabledReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist) {
    return updateNodeInfoReturnVal == null ? super.updateDataNodeInfoInCluster(replicaId, shouldExist)
        : updateNodeInfoReturnVal;
  }

  @Override
  public void close() {
    // no op
  }

  @Override
  public void setPartitionDisabledState(String partitionName, boolean disable) {
    super.setPartitionDisabledState(partitionName, disable);
  }

  // Override this method so we don't rely on the HelixParticipant to create a distributed lock.
  @Override
  public DistributedLock getDistributedLock(String resource, String message) {
    return new DistributedLockLocalImpl();
  }

  /**
   * @return the {@link HelixParticipantMetrics} associated with this participant.
   */
  HelixParticipantMetrics getHelixParticipantMetrics() {
    return participantMetrics;
  }

  /**
   * Re-register state change listeners in {@link HelixParticipant} to replace original one with mock state change
   * listener. This is to help with special test cases.
   */
  void registerMockStateChangeListeners() {
    registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
        mockReplicationManagerListener);
    if (mockStatsManagerListener != null) {
      registerPartitionStateChangeListener(StateModelListenerType.StatsManagerListener, mockStatsManagerListener);
    }
  }
}

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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;


public class MockHelixParticipant extends HelixParticipant {
  CountDownLatch listenerLatch = null;
  ReplicaState replicaState = ReplicaState.OFFLINE;
  ReplicaId currentReplica = null;
  ReplicaSyncUpManager replicaSyncUpService = null;
  private Set<ReplicaId> sealedReplicas = new HashSet<>();
  private Set<ReplicaId> stoppedReplicas = new HashSet<>();
  private PartitionStateChangeListener mockReplicationManagerListener;

  public MockHelixParticipant(ClusterMapConfig clusterMapConfig) throws IOException {
    super(clusterMapConfig, new MockHelixManagerFactory(), new MetricRegistry());
    // create mock state change listener for ReplicationManager
    mockReplicationManagerListener = Mockito.mock(PartitionStateChangeListener.class);
    // mock Bootstrap-To-Standby change
    doAnswer((Answer) invocation -> {
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
    doAnswer((Answer) invocation -> {
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
    doAnswer((Answer) invocation -> {
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

  @Override
  public void participate(List<AmbryHealthReport> ambryHealthReports) throws IOException {
    // no op
  }

  @Override
  public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
    if (isSealed) {
      sealedReplicas.add(replicaId);
    } else {
      sealedReplicas.remove(replicaId);
    }
    return true;
  }

  @Override
  public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
    if (markStop) {
      stoppedReplicas.addAll(replicaIds);
    } else {
      stoppedReplicas.removeAll(replicaIds);
    }
    return true;
  }

  @Override
  public List<String> getSealedReplicas() {
    return sealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public List<String> getStoppedReplicas() {
    return stoppedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public void close() {
    // no op
  }

  /**
   * @return a snapshot of current state change listeners.
   */
  public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
    return Collections.unmodifiableMap(partitionStateChangeListeners);
  }

  /**
   * Re-register state change listeners in {@link HelixParticipant} to replace original one with mock state change
   * listener. This is to help with special test cases.
   */
  void registerMockStateChangeListeners() {
    registerPartitionStateChangeListener(StateModelListenerType.ReplicationManagerListener,
        mockReplicationManagerListener);
  }
}

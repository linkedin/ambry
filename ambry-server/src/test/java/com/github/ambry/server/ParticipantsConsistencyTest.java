/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test class to verify participant consistency is correctly tracked by {@link ParticipantsConsistencyChecker} if enabled.
 */
public class ParticipantsConsistencyTest {
  private Properties props = new Properties();
  private Time time;
  private NotificationSystem notificationSystem;
  private MockClusterAgentsFactory clusterAgentsFactory;

  public ParticipantsConsistencyTest() throws Exception {
    clusterAgentsFactory = new MockClusterAgentsFactory(false, false, 1, 1, 1);
    MockClusterMap mockClusterMap = clusterAgentsFactory.getClusterMap();
    props.setProperty("clustermap.cluster.name", "test");
    props.setProperty("clustermap.datacenter.name", "DC1");
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("server.participants.consistency.checker.period.sec", Long.toString(1L));
    props.setProperty("host.name", mockClusterMap.getDataNodes().get(0).getHostname());
    props.setProperty("port", Integer.toString(mockClusterMap.getDataNodes().get(0).getPort()));
    time = SystemTime.getInstance();
    notificationSystem = new LoggingNotificationSystem();
  }

  /**
   * Test that consistency checker should be disabled in some scenarios.
   */
  @Test
  public void consistencyCheckerDisabledTest() throws Exception {
    // 1. only one participant, consistency checker should be disabled
    AmbryServer server =
        new AmbryServer(new VerifiableProperties(props), clusterAgentsFactory, notificationSystem, time);
    server.startup();
    assertNull("The mismatch metric should not be created", server.getServerMetrics().stoppedReplicasMismatchCount);
    server.shutdown();
    // 2. there are two participants but period of checker is zero, consistency checker should be disabled.
    props.setProperty("server.participants.consistency.checker.period.sec", Long.toString(0L));
    List<ClusterParticipant> participants = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      participants.add(
          new MockClusterParticipant(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null,
              null, null));
    }
    clusterAgentsFactory.setClusterParticipants(participants);
    server = new AmbryServer(new VerifiableProperties(props), clusterAgentsFactory, notificationSystem, time);
    server.startup();
    assertNull("The mismatch metric should not be created", server.getServerMetrics().stoppedReplicasMismatchCount);
    server.shutdown();
  }

  /**
   * Test that two participants are consistent in terms of sealed/stopped replicas.
   * @throws Exception
   */
  @Test
  public void participantsWithNoMismatchTest() throws Exception {
    List<String> sealedReplicas = new ArrayList<>(Arrays.asList("10", "1", "4"));
    List<String> stoppedReplicas = new ArrayList<>();
    List<String> partiallySealedReplicas = new ArrayList<>();
    List<ClusterParticipant> participants = new ArrayList<>();
    // create a latch with init value = 2 to ensure both getSealedReplicas and getStoppedReplicas get called at least once
    CountDownLatch invocationLatch = new CountDownLatch(2);
    MockClusterParticipant participant1 =
        new MockClusterParticipant(sealedReplicas, partiallySealedReplicas, stoppedReplicas, invocationLatch, null,
            null);
    MockClusterParticipant participant2 =
        new MockClusterParticipant(sealedReplicas, partiallySealedReplicas, stoppedReplicas, null, null, null);
    participants.add(participant1);
    participants.add(participant2);
    clusterAgentsFactory.setClusterParticipants(participants);
    AmbryServer server =
        new AmbryServer(new VerifiableProperties(props), clusterAgentsFactory, notificationSystem, time);
    server.startup();
    assertTrue("The latch didn't count to zero within 5 secs", invocationLatch.await(5, TimeUnit.SECONDS));
    // verify that: 1. checker is instantiated 2.no mismatch event is emitted.
    assertNotNull("The mismatch metric should be created",
        server.getServerMetrics().sealedReplicasMismatchCount);
    assertNotNull("The partial seal mismatch metric should be created",
        server.getServerMetrics().partiallySealedReplicasMismatchCount);
    assertEquals("Sealed replicas mismatch count should be 0", 0,
        server.getServerMetrics().sealedReplicasMismatchCount.getCount());
    assertEquals("Partially sealed replicas mismatch count should be 0", 0,
        server.getServerMetrics().partiallySealedReplicasMismatchCount.getCount());
    assertEquals("Stopped replicas mismatch count should be 0", 0,
        server.getServerMetrics().stoppedReplicasMismatchCount.getCount());
    server.shutdown();
  }

  /**
   * Test that there is a mismatch between two participants.
   * @throws Exception
   */
  @Test
  public void participantsWithMismatchTest() throws Exception {
    List<ClusterParticipant> participants = new ArrayList<>();
    // create a latch with init value = 2 and add it to second participant. This latch will count down under certain condition
    CountDownLatch invocationLatch = new CountDownLatch(3);
    MockClusterParticipant participant1 =
        new MockClusterParticipant(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null, null, null);
    MockClusterParticipant participant2 =
        new MockClusterParticipant(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null, null,
            invocationLatch);
    participants.add(participant1);
    participants.add(participant2);
    clusterAgentsFactory.setClusterParticipants(participants);
    AmbryServer server =
        new AmbryServer(new VerifiableProperties(props), clusterAgentsFactory, notificationSystem, time);
    server.startup();
    // initially, two participants have consistent sealed/stopped replicas. Verify that:
    // 1. checker is instantiated 2. no mismatch event is emitted.
    assertNotNull("The mismatch metric should be created",
        server.getServerMetrics().sealedReplicasMismatchCount);
    assertNotNull("The mismatch metric should be created",
        server.getServerMetrics().partiallySealedReplicasMismatchCount);
    assertEquals("Sealed replicas mismatch count should be 0", 0,
        server.getServerMetrics().sealedReplicasMismatchCount.getCount());
    assertEquals("Sealed replicas mismatch count should be 0", 0,
        server.getServerMetrics().partiallySealedReplicasMismatchCount.getCount());
    assertEquals("Stopped replicas mismatch count should be 0", 0,
        server.getServerMetrics().stoppedReplicasMismatchCount.getCount());
    // induce mismatch for sealed and stopped replica list
    // add 1 sealed replica to participant1
    ReplicaId mockReplica1 = Mockito.mock(ReplicaId.class);
    when(mockReplica1.getReplicaPath()).thenReturn("12");
    participant1.setReplicaSealedState(mockReplica1, ReplicaSealStatus.SEALED);
    // add 1 stopped replica to participant2
    ReplicaId mockReplica2 = Mockito.mock(ReplicaId.class);
    when(mockReplica2.getReplicaPath()).thenReturn("4");
    participant2.setReplicaStoppedState(Collections.singletonList(mockReplica2), true);
    // add 1 partially sealed replica to participant2
    ReplicaId mockReplica3 = Mockito.mock(ReplicaId.class);
    when(mockReplica2.getReplicaPath()).thenReturn("5");
    participant2.setReplicaSealedState(mockReplica3, ReplicaSealStatus.PARTIALLY_SEALED);
    assertTrue("The latch didn't count to zero within 5 secs", invocationLatch.await(5, TimeUnit.SECONDS));
    assertTrue("Sealed replicas mismatch count should be non-zero",
        server.getServerMetrics().sealedReplicasMismatchCount.getCount() > 0);
    assertTrue("Partially sealed replicas mismatch count should be non-zero",
        server.getServerMetrics().partiallySealedReplicasMismatchCount.getCount() > 0);
    assertTrue("Stopped replicas mismatch count should be non-zero",
        server.getServerMetrics().stoppedReplicasMismatchCount.getCount() > 0);
    server.shutdown();
  }

  /**
   * An implementation of {@link ClusterParticipant} that helps check consistency between participants.
   */
  private static class MockClusterParticipant implements ClusterParticipant {
    List<String> sealedReplicas;
    List<String> partiallySealedReplicas;
    List<String> stoppedReplicas;
    CountDownLatch getSealedReplicaLatch;
    CountDownLatch getPartiallySealedReplicaLatch;
    CountDownLatch getStoppedReplicaLatch;

    public MockClusterParticipant(List<String> sealedReplicas, List<String> partiallySealedReplicas,
        List<String> stoppedReplicas, CountDownLatch getSealedReplicaLatch,
        CountDownLatch getPartiallySealedReplicaLatch, CountDownLatch getStoppedReplicaLatch) {
      this.sealedReplicas = sealedReplicas;
      this.partiallySealedReplicas = partiallySealedReplicas;
      this.stoppedReplicas = stoppedReplicas;
      this.getSealedReplicaLatch = getSealedReplicaLatch;
      this.getPartiallySealedReplicaLatch = getPartiallySealedReplicaLatch;
      this.getStoppedReplicaLatch = getStoppedReplicaLatch;
    }

    @Override
    public void participate(List<AmbryStatsReport> ambryStatsReports, AccountStatsStore accountStatsStore,
        Callback<AggregatedAccountStorageStats> callback) {
    }

    @Override
    public boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus) {
      String replicaPath = replicaId.getReplicaPath();
      switch (replicaSealStatus) {
        case SEALED:
          sealedReplicas.add(replicaPath);
          partiallySealedReplicas.remove(replicaPath);
          break;
        case PARTIALLY_SEALED:
          partiallySealedReplicas.add(replicaPath);
          sealedReplicas.remove(replicaPath);
          break;
        case NOT_SEALED:
          partiallySealedReplicas.remove(replicaPath);
          sealedReplicas.remove(replicaPath);
      }
      return true;
    }

    @Override
    public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
      if (markStop) {
        replicaIds.forEach(r -> stoppedReplicas.add(r.getReplicaPath()));
      }
      return true;
    }

    @Override
    public List<String> getSealedReplicas() {
      if (getSealedReplicaLatch != null) {
        getSealedReplicaLatch.countDown();
      }
      return sealedReplicas;
    }

    @Override
    public List<String> getPartiallySealedReplicas() {
      if (getPartiallySealedReplicaLatch != null) {
        getPartiallySealedReplicaLatch.countDown();
      }
      return partiallySealedReplicas;
    }

    @Override
    public List<String> getStoppedReplicas() {
      if (getStoppedReplicaLatch != null && !stoppedReplicas.isEmpty()) {
        getStoppedReplicaLatch.countDown();
      }
      return stoppedReplicas;
    }

    @Override
    public void registerPartitionStateChangeListener(StateModelListenerType listenerType,
        PartitionStateChangeListener partitionStateChangeListener) {
    }

    @Override
    public ReplicaSyncUpManager getReplicaSyncUpManager() {
      return null;
    }

    @Override
    public boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist) {
      return false;
    }

    @Override
    public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
      return Collections.emptyMap();
    }

    @Override
    public void close() {
    }
  }
}

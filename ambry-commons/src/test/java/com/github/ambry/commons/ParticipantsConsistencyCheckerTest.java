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
package com.github.ambry.commons;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryHealthReport;
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
 * Test class to verify participant consistency checker.
 */
public class ParticipantsConsistencyCheckerTest {
  private Properties props = new Properties();
  private ServerConfig serverConfig;

  public ParticipantsConsistencyCheckerTest() {
    props.setProperty("server.participants.consistency.checker.period.sec", Long.toString(1L));
    serverConfig = new ServerConfig(new VerifiableProperties(props));
  }

  /**
   * Test that consistency checker should be disabled in some scenarios.
   */
  @Test
  public void consistencyCheckDisabledTest() {
    // 1. participants = null, checker should be disabled
    ServerMetrics metrics =
        new ServerMetrics(new MetricRegistry(), ServerMetrics.class, ServerMetrics.class, null, serverConfig);
    assertNull("The mismatch metric should not be created", metrics.sealedReplicasMismatchCount);
    // 2. only one participant exists, checker should be disabled
    List<ClusterParticipant> participants = new ArrayList<>();
    MockClusterParticipant participant1 =
        new MockClusterParticipant(Collections.emptyList(), Collections.emptyList(), null, null);
    participants.add(participant1);
    metrics =
        new ServerMetrics(new MetricRegistry(), ServerMetrics.class, ServerMetrics.class, participants, serverConfig);
    assertNull("The mismatch metric should not be created", metrics.stoppedReplicasMismatchCount);
    // 3. there are two participants but period of checker is zero, checker should be disabled.
    MockClusterParticipant participant2 =
        new MockClusterParticipant(Collections.emptyList(), Collections.emptyList(), null, null);
    participants.add(participant2);
    props.setProperty("server.participants.consistency.checker.period.sec", Long.toString(0L));
    serverConfig = new ServerConfig(new VerifiableProperties(props));
    metrics =
        new ServerMetrics(new MetricRegistry(), ServerMetrics.class, ServerMetrics.class, participants, serverConfig);
    assertNull("The mismatch metric should not be created", metrics.stoppedReplicasMismatchCount);
  }

  /**
   * Test that two participants are consistent in terms of sealed/stopped replicas.
   * @throws Exception
   */
  @Test
  public void consistencyCheckerNoMismatchTest() throws Exception {
    List<String> sealedReplicas = new ArrayList<>(Arrays.asList("10", "1", "4"));
    List<String> stoppedReplicas = new ArrayList<>();
    List<ClusterParticipant> participants = new ArrayList<>();
    // create a latch with init value = 2 to ensure both getSealedReplicas and getStoppedReplicas get called at least once
    CountDownLatch invocationLatch = new CountDownLatch(2);
    MockClusterParticipant participant1 =
        new MockClusterParticipant(sealedReplicas, stoppedReplicas, invocationLatch, null);
    MockClusterParticipant participant2 = new MockClusterParticipant(sealedReplicas, stoppedReplicas, null, null);
    participants.add(participant1);
    participants.add(participant2);
    ServerMetrics metrics =
        new ServerMetrics(new MetricRegistry(), ServerMetrics.class, ServerMetrics.class, participants, serverConfig);
    assertTrue("The latch didn't count to zero within 5 secs", invocationLatch.await(5, TimeUnit.SECONDS));
    // verify that: 1. checker is instantiated 2.no mismatch event is emitted.
    assertNotNull("The mismatch metric should be created", metrics.sealedReplicasMismatchCount);
    assertEquals("Sealed replicas mismatch count should be 0", 0, metrics.sealedReplicasMismatchCount.getCount());
    assertEquals("Stopped replicas mismatch count should be 0", 0, metrics.stoppedReplicasMismatchCount.getCount());
    metrics.shutdownConsistencyChecker();
  }

  /**
   * Test that there is a mismatch between two participants.
   * @throws Exception
   */
  @Test
  public void consistencyCheckerWithMismatchTest() throws Exception {
    List<ClusterParticipant> participants = new ArrayList<>();
    // create a latch with init value = 2 and add it to second participant. This latch will count down under certain condition
    CountDownLatch invocationLatch = new CountDownLatch(2);
    MockClusterParticipant participant1 = new MockClusterParticipant(new ArrayList<>(), new ArrayList<>(), null, null);
    MockClusterParticipant participant2 =
        new MockClusterParticipant(new ArrayList<>(), new ArrayList<>(), null, invocationLatch);
    participants.add(participant1);
    participants.add(participant2);
    // initially, two participants have consistent sealed/stopped replicas
    ServerMetrics metrics =
        new ServerMetrics(new MetricRegistry(), ServerMetrics.class, ServerMetrics.class, participants, serverConfig);
    // verify that: 1. checker is instantiated 2.no mismatch event is emitted.
    assertNotNull("The mismatch metric should be created", metrics.sealedReplicasMismatchCount);
    assertEquals("Sealed replicas mismatch count should be 0", 0, metrics.sealedReplicasMismatchCount.getCount());
    assertEquals("Stopped replicas mismatch count should be 0", 0, metrics.stoppedReplicasMismatchCount.getCount());
    // induce mismatch for sealed and stopped replica list
    // add 1 sealed replica to participant1
    ReplicaId mockReplica1 = Mockito.mock(ReplicaId.class);
    when(mockReplica1.getReplicaPath()).thenReturn("12");
    participant1.setReplicaSealedState(mockReplica1, true);
    // add 1 stopped replica to participant2
    ReplicaId mockReplica2 = Mockito.mock(ReplicaId.class);
    when(mockReplica2.getReplicaPath()).thenReturn("4");
    participant2.setReplicaStoppedState(Collections.singletonList(mockReplica2), true);
    assertTrue("The latch didn't count to zero within 5 secs", invocationLatch.await(5, TimeUnit.SECONDS));
    assertTrue("Sealed replicas mismatch count should be non-zero", metrics.sealedReplicasMismatchCount.getCount() > 0);
    assertTrue("Stopped replicas mismatch count should be non-zero",
        metrics.stoppedReplicasMismatchCount.getCount() > 0);
    metrics.shutdownConsistencyChecker();
  }

  /**
   * An implementation of {@link ClusterParticipant} that helps check consistency between participants.
   */
  private static class MockClusterParticipant implements ClusterParticipant {
    List<String> sealedReplicas;
    List<String> stoppedReplicas;
    CountDownLatch getSealedReplicaLatch;
    CountDownLatch getStoppedReplicaLatch;

    public MockClusterParticipant(List<String> sealedReplicas, List<String> stoppedReplicas,
        CountDownLatch getSealedReplicaLatch, CountDownLatch getStoppedReplicaLatch) {
      this.sealedReplicas = sealedReplicas;
      this.stoppedReplicas = stoppedReplicas;
      this.getSealedReplicaLatch = getSealedReplicaLatch;
      this.getStoppedReplicaLatch = getStoppedReplicaLatch;
    }

    @Override
    public void participate(List<AmbryHealthReport> ambryHealthReports) {
    }

    @Override
    public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
      if (isSealed) {
        sealedReplicas.add(replicaId.getReplicaPath());
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

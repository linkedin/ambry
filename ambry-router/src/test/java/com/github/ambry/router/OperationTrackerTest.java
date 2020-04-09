/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Unit test for the following operation trackers
 * 1. {@link SimpleOperationTracker}
 * 2. {@link AdaptiveOperationTracker}
 *
 * The status of an operation is represented as in the following format:
 *
 * local unsent count-local inflight count-local succeeded count-local failed count;
 * remote unsent count-remote inflight count-remote succeeded count-remote failed count
 *
 * For example: 3-0-0-0; 9-0-0-0
 *
 * The number of replicas for most tests is 12.
 */
@RunWith(Parameterized.class)
public class OperationTrackerTest {
  private static final int PORT = 6666;
  private static final String SIMPLE_OP_TRACKER = "Simple";
  private static final String ADAPTIVE_OP_TRACKER = "Adaptive";
  private static final double QUANTILE = 0.9;

  private final String operationTrackerType;
  private final boolean replicasStateEnabled;

  private List<MockDataNodeId> datanodes;
  private MockPartitionId mockPartition;
  private String localDcName;
  private String originatingDcName;
  private MockClusterMap mockClusterMap;
  private final LinkedList<ReplicaId> inflightReplicas = new LinkedList<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();

  // for AdaptiveOperationTracker
  private final Time time = new MockTime();

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker}
   * @return an array with both {@link #SIMPLE_OP_TRACKER} and {@link #ADAPTIVE_OP_TRACKER}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{SIMPLE_OP_TRACKER, false}, {ADAPTIVE_OP_TRACKER, false}, {SIMPLE_OP_TRACKER, true},
            {ADAPTIVE_OP_TRACKER, true}});
  }

  /**
   * @param operationTrackerType the type of {@link OperationTracker} that needs to be used in tests
   */
  public OperationTrackerTest(String operationTrackerType, boolean replicasStateEnabled) {
    this.operationTrackerType = operationTrackerType;
    this.replicasStateEnabled = replicasStateEnabled;
  }

  /**
   * crossColoEnabled = false, successTarget = 2, parallelism = 3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 2 replicas succeeds.
   * 3. Operation succeeds.
   * 4. 1 local fails.
   * 5. Operation remains succeeded.
   */
  @Test
  public void localSucceedTest() {
    initialize();
    OperationTracker ot =
        getOperationTracker(false, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    // 3-0-0-0; 9-0-0-0
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 3, false);
    // 0-3-0-0; 9-0-0-0
    assertFalse("Operation should not have been done.", ot.isDone());
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    // 0-1-2-0; 9-0-0-0
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());

    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // 0-0-2-1; 9-0-0-0
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * crossColoEnabled = false, successTarget = 2, parallelism = 3.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 1 local replicas succeeded, 2 failed.
   * 3. Operation fails.
   */
  @Test
  public void localFailTest() {
    initialize();
    OperationTracker ot =
        getOperationTracker(false, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    // 3-0-0-0; 9-0-0-0
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 3, false);
    // 0-3-0-0; 9-0-0-0
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should not have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    // 0-1-0-2; 9-0-0-0
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    // 0-0-1-2; 9-0-0-0
    assertFalse("Operation should not have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test put operation with both dynamic success target and get replica by state enabled.
   * Test cases:
   * Case 1: 1 LEADER and 2 STANDBY replicas (regular case)
   * Case 2: 1 LEADER and 3 STANDBY replicas. One of them returns REQUEST_DISABLED error code. There are two combinations
   *         for remaining 3 replicas:
   *         (1) two success and one failure (whole operation should succeed)
   *         (2) one success and two failure (whole operation should fail)
   * Case 3: 1 LEADER and 5 STANDBY replicas. Several combinations to discuss:
   *         (1) 3 REQUEST_DISABLED, 2 success and 1 failure (operation should succeed)
   *         (2) 2 REQUEST_DISABLED, 2 failure (operation should fail no matter what results are from remaining replicas)
   *         (3) 1 REQUEST_DISABLED, 1 failure, 4 success (operation should succeed)
   *         (4) 0 REQUEST_DISABLED, 2 failure, 4 success (operation should fail)
   * Case 4: 1 LEADER, 4 STANDBY, 1 INACTIVE  (this is to mock one replica has completed STANDBY -> INACTIVE)
   *         (1) 2 REQUEST_DISABLED, 1 failure and 2 success (operation should succeed)
   *         (2) 3 REQUEST_DISABLED, 1 failure (operation should fail no matter what result is from remaining replica)
   */
  @Test
  public void putOperationWithDynamicTargetTest() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER) && replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    // Case 1
    populateReplicaList(1, ReplicaState.LEADER);
    populateReplicaList(2, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    OperationTracker ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 2.1
    populateReplicaList(1, ReplicaState.STANDBY);
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 4, false);
    // make 1 replica return REQUEST_DISABLED, then 1 failure and 2 success
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 2.2
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 4, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should fail", ot.hasSucceeded());
    // Case 3.1
    populateReplicaList(2, ReplicaState.STANDBY);
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    for (int i = 0; i < 3; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    }
    assertFalse("Operation should not be done yet", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done yet", ot.isDone());
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 3.2
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should fail", ot.hasSucceeded());
    // Case 3.3
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 4; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 3.4
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    for (int i = 0; i < 6; ++i) {
      if (i < 2) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      } else {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertFalse("Operation should fail", ot.hasSucceeded());
    // Case 4
    // re-populate replica list
    mockPartition.cleanUp();
    repetitionTracker.clear();
    populateReplicaList(1, ReplicaState.LEADER);
    populateReplicaList(4, ReplicaState.STANDBY);
    populateReplicaList(1, ReplicaState.INACTIVE);
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 5, false);
    // Case 4.1
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    }
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done yet", ot.isDone());
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 4.2
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, true);
    sendRequests(ot, 5, false);
    for (int i = 0; i < 3; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    }
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should not be done", ot.isDone());
    assertFalse("Operation should fail", ot.hasSucceeded());
  }

  /**
   * Test put operation in local dc when replicasStateEnabled is enabled/disabled.
   * Test steps:
   * Case1: Only 2 STANDBY replicas in local dc;
   *        Make 1 succeed and the other fail (both current and replicaState enabled tracker should fail)
   * Case2: 2 STANDBY, 1 INACTIVE replicas in local dc
   *        Make 1 fail and 2 succeed (replicaState enabled operation tracker should fail)
   * Case3: 1 LEADER, 4 STANDBY and 1 INACTIVE in local dc
   *        Make 3 succeed and 2 fail (replicaState enabled operation tracker should fail)
   */
  @Test
  public void localPutWithReplicaStateTest() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    // test that if there are only 2 eligible replicas, the success target should use routerConfig.routerPutSuccessTarget
    populateReplicaList(2, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    OperationTracker ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 2, false);
    // make one requests succeed, the other fail
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should fail", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());

    // add one more replica in INACTIVE state, now we have 2 STANDBY and 1 INACTIVE replicas
    populateReplicaList(1, ReplicaState.INACTIVE);
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, false);
    // issue PUT request
    sendRequests(ot, replicasStateEnabled ? 2 : 3, false);
    // make first request fail and rest requests succeed
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    while (!inflightReplicas.isEmpty()) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    if (replicasStateEnabled) {
      assertFalse("Operation should fail because only 2 replicas eligible and 1 has failed", ot.hasSucceeded());
    } else {
      assertTrue("Operation should succeed when there are 2 success", ot.hasSucceeded());
    }

    // add three more replicas: one in LEADER state, the other two in STANDBY state
    populateReplicaList(1, ReplicaState.LEADER);
    populateReplicaList(2, ReplicaState.STANDBY);
    // now we have 6 replicas: 1 LEADER, 4 STANDBY and 1 INACTIVE. Number of eligible replicas = 1 + 4 = 5
    repetitionTracker.clear();
    ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.PutOperation, false);
    // issue PUT request, parallelism should be 5 when replicaState is enabled.
    sendRequests(ot, replicasStateEnabled ? 5 : 3, false);
    // remaining test is for replicaState enabled operation tracker
    if (replicasStateEnabled) {
      // make first 3 requests succeed
      for (int i = 0; i < 3; i++) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
        // success target should be 4 when replicaState is enabled for operation tracker, so operation is not done yet
        // even though it has succeeded on 3 replicas.
        assertFalse("Operation should not be done", ot.isDone());
      }
      // make last 2 requests fail, then operation should be done and result should be failure
      for (int i = 0; i < 2; i++) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      }
      assertFalse("Operation should fail", ot.hasSucceeded());
      assertTrue("Operation should be done", ot.isDone());
    }
  }

  /**
   * Test delete/ttlUpdate operation when replicasStateEnabled is enabled/disabled.
   * local dc: 2 STANDBY and 1 INACTIVE; remote dc: 2 STANDBY and 1 INACTIVE
   * 1. Issue 3 requests in parallel
   * 2. Make 2 requests fail
   * 3. Issue 1 requests (replicaState enabled tracker only has 4 eligible replicas)
   * 4. Make 1 succeed and 1 fail (replicaState enabled tracker should fail)
   * 5. Make remaining requests succeed, this only applies for tracker with replicaState disabled and operation should succeed.
   */
  @Test
  public void deleteTtlUpdateWithReplicaStateTest() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    // set up one node per data center for testing
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1")));
    mockPartition = new MockPartitionId();
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // put two STANDBY replicas in each data center (note that "populateReplicaList" method alternatively distributes
    // the replica, so here we set 4 for two dc in total)
    populateReplicaList(4, ReplicaState.STANDBY);
    // put one INACTIVE in each data center
    populateReplicaList(2, ReplicaState.INACTIVE);

    // test both delete and Ttl Update cases
    for (RouterOperation operation : EnumSet.of(RouterOperation.DeleteOperation, RouterOperation.TtlUpdateOperation)) {
      repetitionTracker.clear();
      OperationTracker ot = getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, operation, false);
      // issue delete/ttlUpdate requests to 2 local replica and 1 remote replica
      sendRequests(ot, 3, false);

      // make 2 requests fail and send requests again
      for (int i = 0; i < 2; ++i) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      }
      // for replicaState enabled operation tracker, only 1 eligible replica left, so numRequestsExpected = 1
      sendRequests(ot, replicasStateEnabled ? 1 : 2, false);
      // make 1 requests fail and 1 request succeed then replicaState enabled operation tracker should fail
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      if (replicasStateEnabled) {
        assertFalse("Operation should fail", ot.hasSucceeded());
        assertTrue("Operation should be done", ot.isDone());
      } else {
        // if replicasStateEnabled = false, operation tracker is able to succeed after 1 more request succeed
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
        assertTrue("Operation should fail", ot.hasSucceeded());
        assertTrue("Operation should be done", ot.isDone());
      }
    }
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.
   * <p/>
   * 1. Get 2 local replicas to send request (and send requests);
   * 2. 1 fails， 1 pending.
   * 3. Get 1 more local replicas to send request (and send requests);
   * 4. 1 succeeds.
   * 5. Operation succeeds.
   */
  @Test
  public void localSucceedWithDifferentParameterTest() {
    initialize();
    OperationTracker ot =
        getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    // 3-0-0-0; 9-0-0-0
    sendRequests(ot, 2, false);
    // 1-2-0-0; 9-0-0-0

    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // 1-1-0-1; 9-0-0-0
    assertFalse("Operation should not have been done.", ot.isDone());

    sendRequests(ot, 1, false);
    // 0-2-0-1; 9-0-0-0
    assertFalse("Operation should not be done", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    // 0-1-1-1; 9-0-0-0
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.
   * <p/>
   * 1. Get 2 local replicas to send request (and send requests);
   * 2. 1 local replica fails, 1 pending.
   * 3. Get 1 more local replicas to send request (and send requests);
   * 4. 2 local replica fails.
   * 5. Get 1 remote replica from each Dc to send request (and send requests);
   * 6. All fails.
   * 7. Get 1 remote replica from each DC to send request (and send requests);
   * 8. 1 fails, 2 pending.
   * 9. Get 1 remote replica from each DC to send request (and send requests);
   * 10. 2 fails.
   * 11. 1 succeeds.
   * 12. Operation succeeds.
   */
  @Test
  public void remoteReplicaTest() {
    initialize();
    OperationTracker ot =
        getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    // 3-0-0-0; 9-0-0-0
    sendRequests(ot, 2, false);
    // 1-2-0-0; 9-0-0-0
    ReplicaId id = inflightReplicas.poll();
    assertEquals("First request should have been to local DC", localDcName, id.getDataNodeId().getDatacenterName());
    ot.onResponse(id, TrackedRequestFinalState.FAILURE);
    // 1-1-0-1; 9-0-0-0
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 1, false);
    // 0-2-0-1; 9-0-0-0
    id = inflightReplicas.poll();
    assertEquals("Second request should have been to local DC", localDcName, id.getDataNodeId().getDatacenterName());
    ot.onResponse(id, TrackedRequestFinalState.FAILURE);
    id = inflightReplicas.poll();
    assertEquals("Third request should have been to local DC", localDcName, id.getDataNodeId().getDatacenterName());
    ot.onResponse(id, TrackedRequestFinalState.FAILURE);
    // 0-0-0-3; 9-0-0-0
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 2, false);
    // 0-0-0-3; 7-2-0-0
    assertFalse("Operation should not be done", ot.isDone());
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    // 0-0-0-3; 7-0-0-2
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 2, false);
    // 0-0-0-3; 5-2-0-2
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done", ot.isDone());
    // 0-0-0-3; 5-1-0-3
    sendRequests(ot, 1, false);
    // 0-0-0-3; 4-1-0-3
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 12, parallelism = 3.
   *
   * This test may be meaningful for DELETE operation.
   *
   * <p/>
   * 1. Get 3 local replicas to send request (and send requests);
   * 2. 3 succeeded.
   * 3. Operation succeeded.
   */
  @Test
  public void fullSuccessTargetTest() {
    initialize();
    OperationTracker ot =
        getOperationTracker(true, 12, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3, false);
      for (int i = 0; i < 3; i++) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * crossColoEnabled = true, successTarget = 1, parallelism = 2.
   * Only 4 local replicas
   *
   * 1. Get 1st local replica to send request (and sent);
   * 2. Get 2nd local replica to send request (and failed to send);
   * 3. Get 3rd local replica to send request (and sent);
   * 4. Receive 2 failed responses from the 1st and 3rd replicas;
   * 5. Get again 2nd local replica to send request (and sent);
   * 6. Get 4th local replica to send request (and failed to send);
   * 7. Receive 1 failed responses from the 2nd replicas;
   * 8. Get again 4th local replica to send request (and sent);
   * 9. Receive 1 successful response from the 4th replica;
   * 10. Operation succeeds.
   */
  @Test
  public void useReplicaNotSucceededSendTest() {
    int replicaCount = 4;
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(replicaCount, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    OperationTracker ot =
        getOperationTracker(true, 1, 2, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 2, true);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 1, true);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 1, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test to ensure that replicas that are down are also returned by the operation tracker, but they are
   * ordered after the healthy replicas.
   */
  @Test
  public void downReplicasOrderingTest() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>();
    datanodes.add(new MockDataNodeId(portList, mountPaths, "dc-0"));
    datanodes.add(new MockDataNodeId(portList, mountPaths, "dc-1"));
    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition),
        datanodes.get(0).getDatacenterName());
    int replicaCount = 6;
    populateReplicaList(replicaCount, ReplicaState.STANDBY);
    // Test scenarios with various number of replicas down
    for (int i = 0; i < replicaCount; i++) {
      testReplicaDown(replicaCount, i);
    }
  }

  /**
   * Test to ensure all replicas are candidates if originatingDcName is unknown.
   */
  @Test
  public void replicasOrderingTestOriginatingUnknown() {
    initialize();
    originatingDcName = null;
    OperationTracker ot = getOperationTracker(true, 3, 3, false, 6, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.FAILURE);
    }
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.FAILURE);
    }
    assertEquals("Should have 0 replica in flight.", 0, inflightReplicas.size());
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not been done", ot.isDone());
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.SUCCESS);
    }
    assertEquals("Should have 0 replica in flight.", 0, inflightReplicas.size());
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test to ensure that replicas in originating DC are first priority when originating DC is local DC.
   */
  @Test
  public void replicasOrderingTestOriginatingIsLocal() {
    initialize();
    originatingDcName = localDcName;
    OperationTracker ot = getOperationTracker(true, 3, 3, false, 6, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.SUCCESS);
      assertEquals("Should be originating DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertEquals("Should have 0 replica in flight.", 0, inflightReplicas.size());
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test to ensure that replicas in originating DC are right after local DC replicas.
   */
  @Test
  public void replicasOrderingTestOriginatingNotLocal() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    OperationTracker ot = getOperationTracker(true, 3, 6, false, 6, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 6, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local
      ot.onResponse(replica, TrackedRequestFinalState.FAILURE);
      assertEquals("Should be local DC", localDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());

    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.SUCCESS);
      assertEquals("Should be originating DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertEquals("Should have 0 replica in flight.", 0, inflightReplicas.size());
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test to ensure that replicas are exactly 6 when includeNonOriginatingDcReplicas is false.
   * 3 local replicas in the beginning and 3 originating replicas afterwards.
   */
  @Test
  public void replicasOrderTestOriginatingDcOnly() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    OperationTracker ot = getOperationTracker(true, 3, 9, false, 6, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 6, false);
    assertEquals("Should have 6 replicas", 6, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.FAILURE);
      assertEquals("Should be local DC", localDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());

    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.SUCCESS);
      assertEquals("Should be originating DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertEquals("Should have 0 replica in flight.", 0, inflightReplicas.size());
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test the case when NotFound Error should be disabled since the cross colo is disabled.
   */
  @Test
  public void blobNotFoundInOriginDcAndCrossColoDisabledTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    OperationTracker ot =
        getOperationTracker(false, 1, 3, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      assertEquals("Should be local DC", localDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test the case where operation has failed on not found when origin DC is unknown and number of NotFound hits threshold.
   */
  @Test
  public void originDcNotFoundUnknownOriginDcTest() {
    initialize();
    originatingDcName = null;
    OperationTracker ot =
        getOperationTracker(true, 1, 12, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 12, false);
    assertEquals("Should have 12 replicas", 12, inflightReplicas.size());
    for (int i = 0; i < 12; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test the case when NotFound Error triggered.
   */
  @Test
  public void originDcNotFoundTriggeredTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    OperationTracker ot =
        getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should be done", ot.isDone());

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    // Only send two not found response, it will terminate the operation.
    for (int i = 0; i < 2; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      assertEquals("Should be originatingDcName DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  @Test
  public void deleteOperationWithDiskDownAndNotFoundTest() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    initialize();
    originatingDcName = localDcName;
    OperationTracker ot =
        getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.DeleteOperation, false);
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    // set up test case in originating dc where 1st replica returns Disk_Unavailable, 2nd returns Deleted, 3rd returns Not_Found
    ReplicaId replica = inflightReplicas.poll();
    ot.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
    replica = inflightReplicas.poll();
    ot.onResponse(replica, TrackedRequestFinalState.SUCCESS);
    replica = inflightReplicas.poll();
    ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation is not yet complete", ot.isDone());
    // make remaining replicas return Not_Found
    for (int i = 0; i < 3; ++i) {
      sendRequests(ot, 3, false);
      for (int j = 0; j < 3; ++j) {
        replica = inflightReplicas.poll();
        ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      }
    }
    assertTrue("Operation should fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  @Test
  public void getOperationWithDiskDownAndNotFoundTest() {
    initialize();
    originatingDcName = localDcName;
    OperationTracker ot =
        getOperationTracker(true, 1, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    // set up test case in originating dc where 1st and 2nd replica return Disk_Unavailable, 3rd returns Not_Found
    ReplicaId replica;
    for (int i = 0; i < 2; ++i) {
      replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
      assertEquals("Should be originatingDcName DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    replica = inflightReplicas.poll();
    ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation is not yet complete", ot.isDone());
    // make remaining replicas return either Disk_Unavailable or Not_Found
    for (int i = 0; i < 2; ++i) {
      sendRequests(ot, 3, false);
      for (int j = 0; j < 3; ++j) {
        replica = inflightReplicas.poll();
        ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      }
    }
    sendRequests(ot, 3, false);
    for (int i = 0; i < 2; ++i) {
      replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.DISK_DOWN);
    }
    // although now we have diskDownCount + totalNotFoundCount = 11, GET operation should not fail at this point of time
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    // after last replica return NotFound, the operation has indeed failed on NotFound
    replica = inflightReplicas.poll();
    ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Tests the case when the success target > number of replicas.
   */
  @Test
  public void notEnoughReplicasToMeetTargetTest() {
    initialize();
    try {
      getOperationTracker(true, 13, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
      fail("Should have failed to construct tracker because success target > replica count");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests the case when parallelism < 1
   */
  @Test
  public void incorrectParallelismTest() {
    initialize();
    for (int parallelism : Arrays.asList(0, -1)) {
      try {
        getOperationTracker(true, 13, 0, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
        fail("Should have failed to construct tracker because parallelism is " + parallelism);
      } catch (IllegalArgumentException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Test that operation tracker can correctly populate parameters(i.e. successTarget) based on input {@link RouterOperation}.
   */
  @Test
  public void operationClassTest() {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "dc-0");
    props.setProperty("router.get.success.target", "1");
    props.setProperty("router.put.success.target", "2");
    props.setProperty("router.delete.success.target", "2");
    props.setProperty("router.ttl.update.success.target", "2");
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    initialize();
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    Map<RouterOperation, Integer> operationAndSuccessTarget = new HashMap<>();
    operationAndSuccessTarget.put(RouterOperation.GetBlobOperation, 1);
    operationAndSuccessTarget.put(RouterOperation.GetBlobInfoOperation, 1);
    operationAndSuccessTarget.put(RouterOperation.PutOperation, 2);
    operationAndSuccessTarget.put(RouterOperation.DeleteOperation, 2);
    operationAndSuccessTarget.put(RouterOperation.TtlUpdateOperation, 2);
    for (Map.Entry<RouterOperation, Integer> entry : operationAndSuccessTarget.entrySet()) {
      SimpleOperationTracker operationTracker = null;
      switch (operationTrackerType) {
        case SIMPLE_OP_TRACKER:
          operationTracker =
              new SimpleOperationTracker(routerConfig, entry.getKey(), mockPartition, originatingDcName, true);
          break;
        case ADAPTIVE_OP_TRACKER:
          try {
            operationTracker = new AdaptiveOperationTracker(routerConfig, routerMetrics, entry.getKey(), mockPartition,
                originatingDcName, time);
          } catch (IllegalArgumentException e) {
            assertTrue("Get operation shouldn't throw any exception in adaptive tracker",
                entry.getKey() != RouterOperation.GetBlobOperation
                    && entry.getKey() != RouterOperation.GetBlobInfoOperation);
          }
          break;
      }
      // ensure the success target matches the number specified for each type of operaiton
      if (operationTracker != null) {
        assertEquals("The suggest target doesn't match", (long) entry.getValue(),
            operationTracker.getSuccessTarget(ReplicaType.DISK_BACKED));
      }
    }
  }

  /**
   * Test cases with cloud replicas in the local datacenter and disk replicas in the originating datacenter.
   */
  @Test
  public void localDcCloudOriginatingDcDiskTest() {
    initializeWithCloudDcs(true);
    originatingDcName = getDatacenters(ReplicaType.DISK_BACKED, localDcName).iterator().next();

    // test success in cloud dc
    OperationTracker ot =
        getOperationTracker(true, 2, 3, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    // parallelism of 1 for cloud replicas, so only one request should be sent
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    // success target of 1 with cloud replicas.
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());

    // test failure in cloud dc with fallback to disk DC.
    repetitionTracker.clear();
    ot = getOperationTracker(true, 2, 3, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // parallelism should switch back to the default of 3.
    sendRequests(ot, 3, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    // 1 failure, 2 successes should meet disk success target.
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test cases with cloud replicas in the local datacenter and cloud replicas in the originating datacenter.
   */
  @Test
  public void localDcCloudOriginatingDcCloudTest() {
    initializeWithCloudDcs(true);
    // test failure in cloud dc with fallback to cloud DC.
    originatingDcName = getDatacenters(ReplicaType.CLOUD_BACKED, localDcName).iterator().next();
    OperationTracker ot =
        getOperationTracker(true, 2, 3, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // parallelism should still be 1.
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());

    // test failure in all cloud DCs, fall back to non originating DCs, fail there too
    repetitionTracker.clear();
    ot = getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // parallelism should still be 1.
    sendRequests(ot, 1, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done", ot.isDone());
    // only disk replicas should remain, so parallelism of three will be used from now on
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should not be done", ot.isDone());
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not be done", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // after the second failure in the last DC, the operation should have failed
    // (hasFailed logic should know that only disk replicas are left, which have a success target of 2)
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have succeeded", ot.hasSucceeded());
  }

  /**
   * Test cases with disk replicas in the local datacenter and cloud replicas in the originating datacenter.
   */
  @Test
  public void localDcDiskOriginatingDcCloudTest() {
    initializeWithCloudDcs(false);
    // test failure in disk dc with fallback to cloud DC
    originatingDcName = getDatacenters(ReplicaType.CLOUD_BACKED, localDcName).iterator().next();
    OperationTracker ot =
        getOperationTracker(true, 2, 3, false, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    // parallelism of 3 for disk replicas (local dc).
    sendRequests(ot, 3, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    // one response frees up a spot to send a cloud request
    sendRequests(ot, 1, false);
    for (int i = 0; i < 2; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      // parallelism should have changed to 1 after cloud request was sent
      sendRequests(ot, 0, false);
    }
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test cases with disk replicas in the local datacenter and disk replicas in the originating datacenter. This helps
   * to ensure that there are no regressions when cloud replicas are added to the clustermap.
   */
  @Test
  public void localDcDiskOriginatingDcDiskTest() {
    initializeWithCloudDcs(false);
    // test failure in disk dc with fallback to disk DC
    originatingDcName = getDatacenters(ReplicaType.DISK_BACKED, localDcName).iterator().next();
    OperationTracker ot =
        getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    // parallelism of 3 for disk replicas (local dc).
    sendRequests(ot, 3, false);
    assertFalse("Operation should not have been done.", ot.isDone());
    for (int i = 0; i < 3; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      // parallelism should still be 3 after disk request returned a failure
      sendRequests(ot, 1, false);
    }
    assertFalse("Operation should not have been done.", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());

    // test failure in all disk DCs, fall back to non originating cloud DCs, fail there too
    repetitionTracker.clear();
    ot = getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should not have been done.", ot.isDone());
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should not have been done.", ot.isDone());
    // now we are running against cloud DCs
    sendRequests(ot, 1, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertFalse("Operation should not have been done.", ot.isDone());
    // hasFailed logic should know that only cloud replicas are left, which have a success target of 1,
    // so all replicas must be tried)
    sendRequests(ot, 1, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have succeeded", ot.hasSucceeded());
  }

  /**
   * Initialize 4 DCs, each DC has 1 data node, which has 3 replicas.
   */
  private void initialize() {
    int replicaCount = 12;
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
        new MockDataNodeId(portList, mountPaths, "dc-3")));
    mockPartition = new MockPartitionId();
    populateReplicaList(replicaCount, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
  }

  /**
   * Initialize 4 DCs, 2 disk datacenters, 2 cloud datacenters. Each disk datacenter has 3 replicas, and each cloud
   * datacenter has 1 replica.
   * @param makeCloudDcLocal {@code true} to make the local datacenter one of the cloud datacenters.
   */
  private void initializeWithCloudDcs(boolean makeCloudDcLocal) {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    mockPartition = new MockPartitionId();
    List<MockDataNodeId> diskNodes = Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1"));
    populateReplicaList(3 * diskNodes.size(), ReplicaState.STANDBY, diskNodes);
    List<MockDataNodeId> cloudNodes = Arrays.asList(new MockDataNodeId(portList, Collections.emptyList(), "cloud-dc-0"),
        new MockDataNodeId(portList, Collections.emptyList(), "cloud-dc-1"));
    // only one cloud replica per cloud dc.
    populateReplicaList(cloudNodes.size(), ReplicaState.STANDBY, cloudNodes);
    datanodes = new ArrayList<>();
    datanodes.addAll(diskNodes);
    datanodes.addAll(cloudNodes);
    localDcName = (makeCloudDcLocal ? cloudNodes : diskNodes).get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
  }

  /**
   * Populate replicas for a partition.
   * @param replicaCount The number of replicas to populate.
   * @param replicaState The {@link ReplicaState} associated with these replicas.
   */
  private void populateReplicaList(int replicaCount, ReplicaState replicaState) {
    populateReplicaList(replicaCount, replicaState, datanodes);
  }

  /**
   * Populate replicas for a partition.
   * @param replicaCount The number of replicas to populate.
   * @param replicaState The {@link ReplicaState} associated with these replicas.
   * @param datanodes the datanodes to populate with replicas
   */
  private void populateReplicaList(int replicaCount, ReplicaState replicaState, List<MockDataNodeId> datanodes) {
    for (int i = 0; i < replicaCount; i++) {
      ReplicaId replicaId = new MockReplicaId(PORT, mockPartition, datanodes.get(i % datanodes.size()), 0);
      mockPartition.replicaIds.add(replicaId);
      mockPartition.replicaAndState.put(replicaId, replicaState);
    }
  }

  /**
   * @param replicaType the type of replica to filter by.
   * @param excludedDcs datacenter names to exclude in the returned set.
   * @return the datacenter names with replicas of type {@code replicaType}.
   */
  private Set<String> getDatacenters(ReplicaType replicaType, String... excludedDcs) {
    Set<String> excludedDcsSet = Arrays.stream(excludedDcs).collect(Collectors.toSet());
    return mockPartition.getReplicaIds()
        .stream()
        .filter(r -> r.getReplicaType() == replicaType)
        .map(r -> r.getDataNodeId().getDatacenterName())
        .filter(dc -> !excludedDcsSet.contains(dc))
        .collect(Collectors.toSet());
  }

  /**
   * Returns the right {@link OperationTracker} based on {@link #operationTrackerType}.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTargetForGet the number of successful responses required for GET operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param includeNonOriginatingDcReplicas if take the option to include remote non originating DC replicas.
   * @param replicasRequired The number of replicas required for the operation.
   * @param routerOperation the {@link RouterOperation} associate with this request.
   * @param useDynamicTargetForPut whether to enable dynamic success target for PUT operation.
   * @return the right {@link OperationTracker} based on {@link #operationTrackerType}.
   */
  private OperationTracker getOperationTracker(boolean crossColoEnabled, int successTargetForGet, int parallelism,
      boolean includeNonOriginatingDcReplicas, int replicasRequired, RouterOperation routerOperation,
      boolean useDynamicTargetForPut) {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", localDcName);
    props.setProperty("router.get.cross.dc.enabled", Boolean.toString(crossColoEnabled));
    props.setProperty("router.get.success.target", Integer.toString(successTargetForGet));
    props.setProperty("router.get.request.parallelism", Integer.toString(parallelism));
    props.setProperty("router.get.include.non.originating.dc.replicas",
        Boolean.toString(includeNonOriginatingDcReplicas));
    props.setProperty("router.get.replicas.required", Integer.toString(replicasRequired));
    props.setProperty("router.latency.tolerance.quantile", Double.toString(QUANTILE));
    props.setProperty("router.operation.tracker.max.inflight.requests", Integer.toString(parallelism));
    props.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
    props.setProperty("router.get.eligible.replicas.by.state.enabled", Boolean.toString(replicasStateEnabled));
    props.setProperty(RouterConfig.ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET, Boolean.toString(useDynamicTargetForPut));
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    OperationTracker tracker;
    switch (operationTrackerType) {
      case SIMPLE_OP_TRACKER:
        tracker = new SimpleOperationTracker(routerConfig, routerOperation, mockPartition, originatingDcName, true);
        break;
      case ADAPTIVE_OP_TRACKER:
        // for now adaptive operation tracker only applies to GetOperation.
        if (!EnumSet.of(RouterOperation.GetBlobInfoOperation, RouterOperation.GetBlobOperation)
            .contains(routerOperation)) {
          throw new IllegalArgumentException("Adaptive tracker currently is not used for " + routerOperation);
        }
        tracker =
            new AdaptiveOperationTracker(routerConfig, routerMetrics, routerOperation, mockPartition, originatingDcName,
                time);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized operation tracker type - " + operationTrackerType);
    }
    return tracker;
  }

  /**
   * Send requests to all replicas provided by the {@link OperationTracker#getReplicaIterator()}
   * @param operationTracker the {@link OperationTracker} that provides replicas.
   * @param numRequestsExpected the number of requests expected to be sent out.
   * @param skipAlternate {@code true} if alternate {@link ReplicaId} instances from the iterator have to be skipped
   *                                  (i.e. no requests sent).
   */
  private void sendRequests(OperationTracker operationTracker, int numRequestsExpected, boolean skipAlternate) {
    int counter = 0;
    int sent = 0;
    Iterator<ReplicaId> replicaIdIterator = operationTracker.getReplicaIterator();
    while (replicaIdIterator.hasNext()) {
      ReplicaId nextReplica = replicaIdIterator.next();
      assertNotNull("There should be a replica to send a request to", nextReplica);
      assertFalse("Replica that was used for a request returned by iterator again: " + nextReplica,
          repetitionTracker.contains(nextReplica));
      if (!skipAlternate || counter % 2 == 0) {
        inflightReplicas.offer(nextReplica);
        replicaIdIterator.remove();
        repetitionTracker.add(nextReplica);
        sent++;
      }
      counter++;
    }
    assertEquals("Did not send expected number of requests: " + inflightReplicas, numRequestsExpected, sent);
  }

  /**
   * Test replica down scenario
   * @param totalReplicaCount total replicas for the partition.
   * @param downReplicaCount partitions to be marked down.
   */
  private void testReplicaDown(int totalReplicaCount, int downReplicaCount) {
    List<Boolean> downStatus = new ArrayList<>(totalReplicaCount);
    for (int i = 0; i < downReplicaCount; i++) {
      downStatus.add(true);
    }
    for (int i = downReplicaCount; i < totalReplicaCount; i++) {
      downStatus.add(false);
    }
    Collections.shuffle(downStatus);
    List<ReplicaId> mockReplicaIds = mockPartition.getReplicaIds();
    for (int i = 0; i < totalReplicaCount; i++) {
      ((MockReplicaId) mockReplicaIds.get(i)).markReplicaDownStatus(downStatus.get(i));
    }
    localDcName = datanodes.get(0).getDatacenterName();
    OperationTracker ot =
        getOperationTracker(true, 2, 3, true, Integer.MAX_VALUE, RouterOperation.GetBlobOperation, false);
    // The iterator should return all replicas, with the first half being the up replicas
    // and the last half being the down replicas.
    Iterator<ReplicaId> itr = ot.getReplicaIterator();
    int count = 0;
    while (itr.hasNext()) {
      ReplicaId nextReplica = itr.next();
      if (count < totalReplicaCount - downReplicaCount) {
        assertFalse(nextReplica.isDown());
      } else {
        assertTrue(nextReplica.isDown());
      }
      count++;
    }
    assertEquals("Total replica count did not match expected", totalReplicaCount, count);
  }
}

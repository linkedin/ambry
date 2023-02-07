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
import com.github.ambry.utils.Pair;
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
import org.junit.Ignore;
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
 * local unsent count - local inflight count - local succeeded count - local failed count;
 * remote unsent count - remote inflight count - remote succeeded count - remote failed count
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
  private final boolean routerUnavailableDueToOfflineReplicas;
  private final LinkedList<ReplicaId> inflightReplicas = new LinkedList<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();
  // for AdaptiveOperationTracker
  private final Time time = new MockTime();
  private List<MockDataNodeId> datanodes;
  private MockPartitionId mockPartition;
  private String localDcName;
  private String originatingDcName;
  private boolean chooseDcWithMostReplicas = false;
  private boolean useDynamicTargetForPut = false;
  private MockClusterMap mockClusterMap;

  /**
   * @param operationTrackerType the type of {@link OperationTracker} that needs to be used in tests
   */
  public OperationTrackerTest(String operationTrackerType, boolean replicasStateEnabled,
      boolean routerUnavailableDueToOfflineReplicas) {
    this.operationTrackerType = operationTrackerType;
    this.replicasStateEnabled = replicasStateEnabled;
    this.routerUnavailableDueToOfflineReplicas = routerUnavailableDueToOfflineReplicas;
  }

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker}
   * @return an array with both {@link #SIMPLE_OP_TRACKER} and {@link #ADAPTIVE_OP_TRACKER}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{SIMPLE_OP_TRACKER, false, false}, {SIMPLE_OP_TRACKER, false, true},
        {ADAPTIVE_OP_TRACKER, false, false}, {ADAPTIVE_OP_TRACKER, false, true}, {SIMPLE_OP_TRACKER, true, false},
        {SIMPLE_OP_TRACKER, true, true}, {ADAPTIVE_OP_TRACKER, true, false}, {ADAPTIVE_OP_TRACKER, true, true}});
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
    OperationTracker ot = getOperationTrackerForGetOrPut(false, 2, 3, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(false, 2, 3, RouterOperation.GetBlobOperation, true);
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
    useDynamicTargetForPut = true;
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    // Case 1
    populateReplicaList(1, ReplicaState.LEADER);
    populateReplicaList(2, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 3, RouterOperation.PutOperation, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 2.1
    populateReplicaList(1, ReplicaState.STANDBY);
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 4, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 4, RouterOperation.PutOperation, true);
    sendRequests(ot, 4, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should fail", ot.hasSucceeded());
    // Case 3.1
    populateReplicaList(2, ReplicaState.STANDBY);
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 6, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 6, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    for (int i = 0; i < 2; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    }
    assertFalse("Operation should fail", ot.hasSucceeded());
    // Case 3.3
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 6, RouterOperation.PutOperation, true);
    sendRequests(ot, 6, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    for (int i = 0; i < 4; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    }
    assertTrue("Operation should succeed", ot.hasSucceeded());
    // Case 3.4
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 6, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 5, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 5, RouterOperation.PutOperation, true);
    sendRequests(ot, 5, false);
    for (int i = 0; i < 3; ++i) {
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.REQUEST_DISABLED);
    }
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should be done", ot.isDone());
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 3, RouterOperation.PutOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 1, 5, RouterOperation.PutOperation, true);
    // issue PUT request, parallelism should be 5 when replicaState is enabled.
    sendRequests(ot, 5, false);
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
   * Test GET operation is able to try on OFFLINE replicas if routerOperationTrackerIncludeDownReplicas is true.
   */
  @Test
  public void getOperationWithReplicaStateTest() {
    assumeTrue(replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1")));
    mockPartition = new MockPartitionId();
    for (ReplicaState state : EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER,
        ReplicaState.INACTIVE, ReplicaState.OFFLINE)) {
      populateReplicaList(1, state);
    }
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // 1. include down replicas (OFFLINE replicas are eligible for GET)
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, true);
    // make sure 4 requests fails and last one succeeds. (This is to verify operation tracker adds offline replica into replica pool as well)
    ReplicaId inflightReplica;
    for (int i = 0; i < 4; ++i) {
      sendRequests(ot, 1, false);
      inflightReplica = inflightReplicas.poll();
      // verify that the first 4 replicas are not OFFLINE replica. (OFFLINE replica should be added to the end of queue)
      assertNotSame("Replica state should not be OFFLINE ", mockPartition.replicaAndState.get(inflightReplica),
          ReplicaState.OFFLINE);
      ot.onResponse(inflightReplica, TrackedRequestFinalState.FAILURE);
      assertFalse("Operation should not complete", ot.isDone());
    }
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    assertEquals("The last replica should be OFFLINE", ReplicaState.OFFLINE,
        mockPartition.replicaAndState.get(inflightReplica));
    ot.onResponse(inflightReplica, TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should be done", ot.isDone());

    // 2. exclude down replicas
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, false);
    for (int i = 0; i < 4; ++i) {
      sendRequests(ot, 1, false);
      inflightReplica = inflightReplicas.poll();
      // verify that none of these replicas is OFFLINE replica.
      assertNotSame("Replica state should not be OFFLINE ", mockPartition.replicaAndState.get(inflightReplica),
          ReplicaState.OFFLINE);
      ot.onResponse(inflightReplica, TrackedRequestFinalState.FAILURE);
      if (i < 3) {
        assertFalse("Operation should not complete", ot.isDone());
      } else {
        assertTrue("Operation should complete", ot.isDone());
      }
    }
  }

  /**
   * Duplicate the GCN issue:
   * Among the 9 replicas, some are ERROR(in the GCN case, these are not listed in the Helix external view.
   * All the eligible replica returns either DiskDown or NotFound.
   * Originating Colo only has 1 replica in the eligible replica, so we return Ambry Unavailable instead of Not_Found.
   */
  @Test
  public void getOperationFailDueToNotFoundWithOriginatingTwoDown() {
    assumeTrue(replicasStateEnabled);
    assumeTrue(routerUnavailableDueToOfflineReplicas);

    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    MockDataNodeId localDcNode = new MockDataNodeId(portList, mountPaths, "dc-0");
    datanodes = new ArrayList<>(Collections.singletonList(localDcNode));
    mockPartition = new MockPartitionId();
    populateReplicaList(1, ReplicaState.LEADER, Collections.singletonList(localDcNode));
    populateReplicaList(2, ReplicaState.OFFLINE, Collections.singletonList(localDcNode));
    localDcName = datanodes.get(0).getDatacenterName();
    originatingDcName = localDcName;
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // 1. include down replicas (OFFLINE replicas are eligible for GET)
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, true);
    ReplicaId inflightReplica;
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    assertNotSame("Replica state should not be OFFLINE ", mockPartition.replicaAndState.get(inflightReplica),
        ReplicaState.OFFLINE);
    ot.onResponse(inflightReplica, TrackedRequestFinalState.DISK_DOWN);
    assertFalse("Operation should not complete", ot.isDone());

    for (int i = 0; i < 2; ++i) {
      sendRequests(ot, 1, false);
      inflightReplica = inflightReplicas.poll();
      ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
      if (i == 1) {
        // the last one
        assertTrue("Operation should complete", ot.isDone());
      } else {
        assertFalse("Operation should not complete", ot.isDone());
      }
    }
    assertFalse(ot.hasFailedOnNotFound());

    // 2. exclude down replicas
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, false);
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should be done", ot.isDone());
    assertFalse(ot.hasFailedOnNotFound());
  }

  /**
   * Duplicate the GCN issue:
   * Among the 9 replicas, some are ERROR(in the GCN case, these are not listed in the Helix external view.
   * All the eligible replica returns either DiskDown or NotFound.
   * Originating Colo has 2 replica in the eligible replica, so we return NOT_FOUND.
   */
  @Test
  @Ignore
  public void getOperationFailDueToNotFoundWithOriginatingTwoOnline() {
    assumeTrue(replicasStateEnabled);
    assumeTrue(routerUnavailableDueToOfflineReplicas);

    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    MockDataNodeId localDcNode = new MockDataNodeId(portList, mountPaths, "dc-0");
    MockDataNodeId remoteNode1 = new MockDataNodeId(portList, mountPaths, "dc-1");
    MockDataNodeId remoteNode2 = new MockDataNodeId(portList, mountPaths, "dc-2");
    datanodes = new ArrayList<>(Arrays.asList(localDcNode, remoteNode1, remoteNode2));
    mockPartition = new MockPartitionId();
    populateReplicaList(2, ReplicaState.STANDBY, Collections.singletonList(localDcNode));
    populateReplicaList(1, ReplicaState.OFFLINE, Collections.singletonList(localDcNode));
    populateReplicaList(3, ReplicaState.ERROR, Collections.singletonList(remoteNode1));
    populateReplicaList(3, ReplicaState.OFFLINE, Collections.singletonList(remoteNode2));

    localDcName = datanodes.get(0).getDatacenterName();
    originatingDcName = localDcName;
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // 1. include down replicas (OFFLINE replicas are eligible for GET)
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, true);
    ReplicaId inflightReplica;
    // two originating returns DISK_DOWN or NOT_FOUND
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not complete", ot.isDone());
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not complete", ot.isDone());
    //ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED is enabled in the test.
    //Although there are 6 replicas, it'll go through the originating replica only.
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should complete", ot.isDone());
    assertTrue(ot.hasFailedOnNotFound());

    // 2. exclude down replicas
    repetitionTracker.clear();
    ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, false);
    // two originating returns DISK_DOWN
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.DISK_DOWN);
    assertFalse("Operation should not complete", ot.isDone());
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should not complete", ot.isDone());
    assertTrue(ot.hasFailedOnNotFound());
  }

  @Test
  public void noReplicaStateTtlUpdateTest() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    // set up one node per data center for testing
    MockDataNodeId origintingDcNode = new MockDataNodeId(portList, mountPaths, "dc-0");
    MockDataNodeId remoteDcNode = new MockDataNodeId(portList, mountPaths, "dc-1");
    datanodes = new ArrayList<>(Arrays.asList(origintingDcNode, remoteDcNode));
    mockPartition = new MockPartitionId();
    localDcName = datanodes.get(0).getDatacenterName();
    originatingDcName = localDcName;
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // One replica down, one with no-state, one is ok
    populateReplicaList(3, ReplicaState.STANDBY, Collections.singletonList(origintingDcNode));
    populateReplicaList(2, ReplicaState.OFFLINE, Collections.singletonList(origintingDcNode));
    populateReplicaList(1, null, Collections.singletonList(origintingDcNode));

    // test both delete and Ttl Update cases
    for (RouterOperation operation : EnumSet.of(RouterOperation.DeleteOperation, RouterOperation.TtlUpdateOperation)) {
      repetitionTracker.clear();
      OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, operation, true);
      // issue delete/ttlUpdate requests to 2 local replica and 1 remote replica
      sendRequests(ot, 3, false);

      for (int i = 0; i < 2; ++i) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
      }
      // for replicaState enabled operation tracker, only 1 eligible replica left, so numRequestsExpected = 1
      sendRequests(ot, replicasStateEnabled ? 0 : 2, false);
      // make 1 requests fail and 1 request succeed then replicaState enabled operation tracker should fail
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
      ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      if (replicasStateEnabled) {
        assertFalse("Operation should fail", ot.hasSucceeded());
      } else {
        // if replicasStateEnabled = false, operation tracker is able to succeed after 1 more request succeed
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
        assertTrue("Operation should succeed", ot.hasSucceeded());
      }
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
      OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, operation, true);
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
      } else {
        // if replicasStateEnabled = false, operation tracker is able to succeed after 1 more request succeed
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
        assertTrue("Operation should succeed", ot.hasSucceeded());
      }
      assertTrue("Operation should be done", ot.isDone());
    }
  }

  /**
   * Test the case where originating dc name is null and operation tracker will choose dc with most replicas to send
   * cross-colo request.
   * local dc: one replica;
   * remote dc1: three replicas;
   * remote dc2: one replica;
   */
  @Test
  public void sendCrossColoRequestToDcWithMostReplicasTest() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    // set up one node per data center for testing
    MockDataNodeId localDcNode = new MockDataNodeId(portList, mountPaths, "dc-0");
    MockDataNodeId remoteDc1Node = new MockDataNodeId(portList, mountPaths, "dc-1");
    MockDataNodeId remoteDc2Node = new MockDataNodeId(portList, mountPaths, "dc-2");
    mockPartition = new MockPartitionId();
    localDcName = localDcNode.getDatacenterName();
    originatingDcName = null;
    chooseDcWithMostReplicas = true;
    mockClusterMap = new MockClusterMap(false, Arrays.asList(localDcNode, remoteDc1Node, remoteDc2Node), 1,
        Collections.singletonList(mockPartition), localDcName);

    populateReplicaList(1, ReplicaState.STANDBY, Collections.singletonList(localDcNode));
    populateReplicaList(3, ReplicaState.STANDBY, Collections.singletonList(remoteDc1Node));
    populateReplicaList(1, ReplicaState.STANDBY, Collections.singletonList(remoteDc2Node));
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 1, RouterOperation.GetBlobOperation, true);
    // make local replica return Not_Found
    sendRequests(ot, 1, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ReplicaId inflightReplica;
    // next, operation tracker should send request to dc-1 as it has most replicas
    for (int i = 0; i < 3; ++i) {
      sendRequests(ot, 1, false);
      inflightReplica = inflightReplicas.poll();
      assertEquals("The request should be sent to dc-1 with most replicas", "dc-1",
          inflightReplica.getDataNodeId().getDatacenterName());
      // we deliberately make all replicas in dc-1 return Not_Found to verify that operation won't terminate on not found
      ot.onResponse(inflightReplica, TrackedRequestFinalState.NOT_FOUND);
      assertFalse("Operation should not be done yet", ot.isDone());
    }
    // the last request should go to dc-2 and this time we make it succeed
    sendRequests(ot, 1, false);
    inflightReplica = inflightReplicas.poll();
    assertEquals("The request should be sent to dc-2", "dc-2", inflightReplica.getDataNodeId().getDatacenterName());
    ot.onResponse(inflightReplica, TrackedRequestFinalState.SUCCESS);
    assertTrue("Operation should succeed", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 12, 3, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 2, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 3, 3, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 3, 3, RouterOperation.GetBlobOperation, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 3, 6, RouterOperation.GetBlobOperation, true);
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
   * Test that hasFailedOnNotFound() returns false when the local dc is not same as originating dc and cross colo is
   * disabled.
   */
  @Test
  public void blobNotFoundInOriginDcAndCrossColoDisabledTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    OperationTracker ot = getOperationTrackerForGetOrPut(false, 1, 3, RouterOperation.GetBlobOperation, true);
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
   * Test that hasFailedOnNotFound() returns false when origin DC is unknown.
   */
  @Test
  public void originDcNotFoundUnknownOriginDcTest() {
    initialize();
    originatingDcName = null;
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 1, 12, RouterOperation.GetBlobOperation, true);
    sendRequests(ot, 12, false);
    assertEquals("Should have 12 replicas", 12, inflightReplicas.size());
    for (int i = 0; i < 12; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test that hasFailedOnNotFound() returns false when there is one success response.
   */
  @Test
  public void originDcNotFoundWithOneSuccessResponse() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(3, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    originatingDcName = localDcName;
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 2, 3, RouterOperation.GetBlobOperation, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should not have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test that hasFailedOnNotFound() cases when all originating DC replicas are up
   */
  @Test
  public void originatingDcNotFoundWithAllReplicaInStandBy() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(3, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    originatingDcName = localDcName;
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(1));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(3));

    // Case 2.1: Check all originating dc replicas.
    OperationTracker ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());

    // Case 2: Check minimum needed originating dc replicas
    repetitionTracker.clear();
    inflightReplicas.clear();
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertTrue("Operation should fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should not be done", ot.isDone());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test hasFailedOnFound() when 1 originating replica is in bootstrap stage.
   */
  @Test
  public void originatingDcNotFoundWithOneReplicaInBootstrap() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(1, ReplicaState.BOOTSTRAP);
    ReplicaId bootstrapReplica = mockPartition.getReplicaIds().get(0);
    populateReplicaList(2, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    originatingDcName = localDcName;
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(1));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(3));

    // Case 1: When we are searching in all replicas
    OperationTracker ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should have some unavailability", ot.hasSomeUnavailability());
    assertTrue("Operation should be done", ot.isDone());

    // Case 2.1: Search minimum needed replicas - first 2 not found responses are from standby replicas.
    repetitionTracker.clear();
    inflightReplicas.clear();
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    LinkedList<ReplicaId> bootStrapReplicaList = new LinkedList<>();
    LinkedList<ReplicaId> standbyReplicaList = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaId replicaId = inflightReplicas.poll();
      if (replicaId.equals(bootstrapReplica)) {
        bootStrapReplicaList.add(replicaId);
      } else {
        standbyReplicaList.add(replicaId);
      }
    }
    ot.onResponse(standbyReplicaList.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(standbyReplicaList.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertTrue("Operation should fail with NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should have been done", ot.isDone());

    // Case 2.2: Search minimum needed replicas - receive 1 response from bootstrap first and standby later.
    repetitionTracker.clear();
    inflightReplicas.clear();
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    for (int i = 0; i < 3; i++) {
      ReplicaId replicaId = inflightReplicas.poll();
      if (replicaId.equals(bootstrapReplica)) {
        bootStrapReplicaList.add(replicaId);
      } else {
        standbyReplicaList.add(replicaId);
      }
    }
    // Send 1 response from bootstrap replica and 1 from standby. Operation should not complete since first NOT_FOUND is
    // from a bootstrap replica
    ot.onResponse(bootStrapReplicaList.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(standbyReplicaList.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should not fail with NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not have been done", ot.isDone());
    // After receiving 3 response, operation would complete with 404.
    ot.onResponse(standbyReplicaList.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should fail with NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test hasFailedOnNotFound() case when we 2 replicas in originating DC are in bootstrap (not up).
   */
  @Test
  public void originatingDcNotFoundWithTwoReplicasInBootstrap() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(2, ReplicaState.BOOTSTRAP);
    populateReplicaList(1, ReplicaState.LEADER);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    originatingDcName = localDcName;
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(1));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(3));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    OperationTracker ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should not fail on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should have some unavailability", ot.hasSomeUnavailability());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test the case when NotFound Error triggered.
   */
  @Test
  public void originDcNotFoundTerminateEarlyTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();

    // Case 1: Check all replicas in originating DC before returning that blob is not found.
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(2));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(3));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(true));
    OperationTracker ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    // Send three not found response from originating dc, it will terminate the operation.
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to originating dc replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      assertEquals("Should be originatingDcName DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
      if (i < 2) {
        assertFalse("Operation should not have failed on NOT_FOUND", ot.hasFailedOnNotFound());
      }
    }
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());

    // Case 2: Check minimum needed number of replicas in originating DC before returning that blob is not found
    repetitionTracker.clear();
    inflightReplicas.clear();
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    // Send two not found response from originating dc, it will terminate the operation.
    for (int i = 0; i < 2; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 2 requests to originating dc replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      assertEquals("Should be originatingDcName DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
      if (i < 1) {
        assertFalse("Operation should not have failed on NOT_FOUND", ot.hasFailedOnNotFound());
      }
    }
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  /**
   * Test the case when terminate on not found is disabled, operation is completed only after receiving responses from all
   * colos.
   */
  @Test
  public void originDcNotFoundNoTerminateEarlyTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();

    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(1));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(3));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED, Boolean.toString(false));
    OperationTracker ot = getOperationTracker(props, true, 3, RouterOperation.GetBlobOperation, true, false);

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to local replicas
      assertEquals("Should be local DC name", localDcName, replica.getDataNodeId().getDatacenterName());
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    // Send three not found response from originating dc, it should not terminate the operation.
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      // fail first 3 requests to originating dc replicas
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
      assertEquals("Should be originatingDcName DC", originatingDcName, replica.getDataNodeId().getDatacenterName());
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());

    // fail next 6 requests to non local/originating dc replicas
    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should not have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertFalse("Operation should not be done", ot.isDone());

    sendRequests(ot, 3, false);
    assertEquals("Should have 3 replicas", 3, inflightReplicas.size());
    for (int i = 0; i < 3; i++) {
      ReplicaId replica = inflightReplicas.poll();
      ot.onResponse(replica, TrackedRequestFinalState.NOT_FOUND);
    }

    // Operation should be done now and hasFailedOnNotFound() should have returned true.
    assertFalse("Operation should have not succeeded", ot.hasSucceeded());
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
    assertTrue("Operation should be done", ot.isDone());
  }

  @Test
  public void originatingDcNotFoundWithMoreThanThreeReplicas() {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER) && replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = Collections.singletonList(new MockDataNodeId(portList, mountPaths, "dc-0"));
    mockPartition = new MockPartitionId();
    populateReplicaList(6, ReplicaState.STANDBY);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    originatingDcName = localDcName;

    // Case 1: With config to search all originating dc replicas
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(1));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(6));
    OperationTracker ot = getOperationTracker(props, true, 6, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 6, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());

    // Case 2: With config to search minimum originating dc replicas
    repetitionTracker.clear();
    inflightReplicas.clear();
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_CHECK_ALL_ORIGINATING_REPLICAS_FOR_NOT_FOUND,
        Boolean.toString(false));
    ot = getOperationTracker(props, true, 6, RouterOperation.GetBlobOperation, true, true);
    sendRequests(ot, 6, false);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertFalse("Operation should not succeed", ot.hasSucceeded());
    assertFalse("Operation should have not failed on NOT_FOUND", ot.hasFailedOnNotFound());
    ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
    assertTrue("Operation should have failed on NOT_FOUND", ot.hasFailedOnNotFound());
  }

  /**
   * Tests the case when the success target > number of replicas.
   */
  @Test
  public void notEnoughReplicasToMeetTargetTest() {
    initialize();
    try {
      getOperationTrackerForGetOrPut(true, 13, 3, RouterOperation.GetBlobOperation, true);
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
        getOperationTrackerForGetOrPut(true, 13, 0, RouterOperation.GetBlobOperation, true);
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
              new SimpleOperationTracker(routerConfig, entry.getKey(), mockPartition, originatingDcName, true,
                  routerMetrics);
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
        assertEquals("The suggest target doesn't match", (long) entry.getValue(), operationTracker.getSuccessTarget());
      }
    }
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 2, 3, RouterOperation.GetBlobOperation, true);
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
    ot = getOperationTrackerForGetOrPut(true, 2, 3, RouterOperation.GetBlobOperation, true);
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
  }

  /**
   * Test cases where some replicas have changed their state in the middle of constructing operation tracker. The code
   * should be able to get correct number of replicas even with concurrent state transition. The most common case is
   * replica changes from OFFLINE to BOOTSTRAP/STANDBY while the replica pool is being populated in the tracker.
   */
  @Test
  public void buildOperationTrackerWithStateTransitionTest() {
    assumeTrue(replicasStateEnabled);
    initialize();
    originatingDcName = localDcName;
    // pick one replica from originating dc and set it to OFFLINE initially
    ReplicaId originatingReplica = mockPartition.replicaIds.stream()
        .filter(r -> r.getDataNodeId().getDatacenterName().equals(localDcName))
        .findAny()
        .get();
    // pick another replica from remote dc and set it to OFFLINE initially
    ReplicaId remoteReplica = mockPartition.replicaIds.stream()
        .filter(r -> !r.getDataNodeId().getDatacenterName().equals(localDcName))
        .findAny()
        .get();
    mockPartition.replicaAndState.put(remoteReplica, ReplicaState.OFFLINE);
    mockPartition.replicaAndState.put(originatingReplica, ReplicaState.OFFLINE);
    // induce concurrent state transition between "getEligibleReplicas()" and "routerOperationTrackerIncludeDownReplicas"
    mockPartition.resetReplicaStateCount = 5;
    mockPartition.resetAllReplicasToStandbyState = true;
    SimpleOperationTracker ot =
        (SimpleOperationTracker) getOperationTrackerForGetOrPut(true, 2, 3, RouterOperation.GetBlobOperation, true);
    assertEquals("Mismatch in replica count in the pool", 12, ot.getReplicaPoolSize());
    Iterator<ReplicaId> iterator = ot.getReplicaIterator();
    List<ReplicaId> orderedReplicas = new ArrayList<>();
    while (iterator.hasNext()) {
      orderedReplicas.add(iterator.next());
    }
    assertEquals("Mismatch in last replica", remoteReplica, orderedReplicas.get(orderedReplicas.size() - 1));
    assertEquals("Mismatch in last but one replica", originatingReplica,
        orderedReplicas.get(orderedReplicas.size() - 2));
    mockPartition.resetAllReplicasToStandbyState = false;
  }

  @Test
  @Ignore
  public void failedDueToOfflineReplicaTest() {
    failedDueToOfflineReplicaTest(RouterOperation.TtlUpdateOperation);
    failedDueToOfflineReplicaTest(RouterOperation.DeleteOperation);
  }

  @Test
  public void getReplicasByStateTest() {
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    int totalReplicaCount = 9;
    for (int bootStrapCount = 0; bootStrapCount < totalReplicaCount; bootStrapCount++) {
      int remainingCount = totalReplicaCount - bootStrapCount;
      for (int standByCount = 0; standByCount < remainingCount; standByCount++) {
        remainingCount = totalReplicaCount - standByCount - bootStrapCount;
        for (int leaderCount = 0; leaderCount < remainingCount; leaderCount++) {
          remainingCount = totalReplicaCount - leaderCount - standByCount - bootStrapCount;
          for (int inactiveCount = 0; inactiveCount < remainingCount; inactiveCount++) {
            remainingCount = totalReplicaCount - leaderCount - standByCount - bootStrapCount - inactiveCount;
            for (int offlineCount = 0; offlineCount < remainingCount; offlineCount++) {
              List<Pair<Integer, ReplicaState>> replicaStateCountPairList =
                  fillMapWithReplicaState(bootStrapCount, standByCount, leaderCount, inactiveCount, offlineCount);
              populateReplicaList(replicaStateCountPairList);
              datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
                  new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
                  new MockDataNodeId(portList, mountPaths, "dc-3")));
              localDcName = datanodes.get(0).getDatacenterName();
              mockPartition = new MockPartitionId();
              mockClusterMap =
                  new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
              SimpleOperationTracker ot = null;
              try {
                ot = (SimpleOperationTracker) getOperationTrackerForGetOrPut(true, 1, 3, RouterOperation.PutOperation,
                    true);
              } catch (IllegalArgumentException exception) {
                continue;
              }
              assertEquals(bootStrapCount, ot.getReplicasByState(null, EnumSet.of(ReplicaState.BOOTSTRAP))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(standByCount, ot.getReplicasByState(null, EnumSet.of(ReplicaState.STANDBY))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(leaderCount, ot.getReplicasByState(null, EnumSet.of(ReplicaState.LEADER))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(inactiveCount, ot.getReplicasByState(null, EnumSet.of(ReplicaState.INACTIVE))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(offlineCount, ot.getReplicasByState(null, EnumSet.of(ReplicaState.OFFLINE))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(bootStrapCount + standByCount,
                  ot.getReplicasByState(null, EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY))
                      .values()
                      .stream()
                      .mapToInt(l -> l.size())
                      .sum());
              assertEquals(bootStrapCount + standByCount + leaderCount, ot.getReplicasByState(null,
                  EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(bootStrapCount + standByCount + leaderCount + inactiveCount, ot.getReplicasByState(null,
                  EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.INACTIVE))
                  .values()
                  .stream()
                  .mapToInt(l -> l.size())
                  .sum());
              assertEquals(bootStrapCount + standByCount + leaderCount + inactiveCount + offlineCount,
                  ot.getReplicasByState(null,
                      EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER,
                          ReplicaState.INACTIVE, ReplicaState.OFFLINE))
                      .values()
                      .stream()
                      .mapToInt(l -> l.size())
                      .sum());
            }
          }
        }
      }
    }
  }

  /**
   * Create a {@link List} of {@link Pair} of {@link ReplicaState} with their counts, as specified by counts in the
   * parameters.
   * @param bootstrapCount count of replicas in {@link ReplicaState#BOOTSTRAP}.
   * @param standbyCount count of replicas in {@link ReplicaState#STANDBY}.
   * @param leaderCount count of replicas in {@link ReplicaState#LEADER}.
   * @param inactiveCount count of replicas in {@link ReplicaState#INACTIVE}.
   * @param offlineCount count of replicas in {@link ReplicaState#OFFLINE}.
   * @return
   */
  private List<Pair<Integer, ReplicaState>> fillMapWithReplicaState(int bootstrapCount, int standbyCount,
      int leaderCount, int inactiveCount, int offlineCount) {
    List<Pair<Integer, ReplicaState>> list = new ArrayList<>();
    list.add(new Pair<>(bootstrapCount, ReplicaState.BOOTSTRAP));
    list.add(new Pair<>(standbyCount, ReplicaState.STANDBY));
    list.add(new Pair<>(leaderCount, ReplicaState.LEADER));
    list.add(new Pair<>(inactiveCount, ReplicaState.INACTIVE));
    list.add(new Pair<>(offlineCount, ReplicaState.OFFLINE));
    return list;
  }

  /**
   * Test {@link OperationTracker} fail logic for the specified {@link RouterOperation}.
   * @param routerOperation {@link RouterOperation} to test.
   */
  public void failedDueToOfflineReplicaTest(RouterOperation routerOperation) {
    assumeTrue(operationTrackerType.equals(SIMPLE_OP_TRACKER));
    assumeTrue(replicasStateEnabled);
    int replicaCount = 9;
    int successTarget = 2;
    int requestParallelism = 2;
    List<Boolean> includeDownReplicaValues = Arrays.asList(true, false);
    for (boolean includeDownReplicas : includeDownReplicaValues) {
      for (int notFoundCount = 9; notFoundCount >= 0; notFoundCount--) {
        int remainingReplicas = replicaCount - notFoundCount;
        for (int foundCount = remainingReplicas; foundCount >= 0; foundCount--) {
          int offlineCount = remainingReplicas - foundCount;
          if (replicaCount - offlineCount < successTarget) {
            // if there are not enough online replicas, then creation operation tracker will fail.
            continue;
          }
          List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
          List<String> mountPaths = Collections.singletonList("mockMountPath");
          datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
              new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
              new MockDataNodeId(portList, mountPaths, "dc-3")));
          mockPartition = new MockPartitionId();
          List<Pair<Integer, ReplicaState>> replicaStateCountPairList = new ArrayList<>();
          replicaStateCountPairList.add(new Pair<Integer, ReplicaState>(offlineCount, ReplicaState.OFFLINE));
          replicaStateCountPairList.add(
              new Pair<Integer, ReplicaState>(replicaCount - offlineCount, ReplicaState.STANDBY));
          populateReplicaList(replicaStateCountPairList);
          localDcName = datanodes.get(0).getDatacenterName();
          mockClusterMap =
              new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
          OperationTracker ot;
          switch (routerOperation) {
            case DeleteOperation:
              ot =
                  getOperationTrackerForDelete(true, successTarget, requestParallelism, RouterOperation.DeleteOperation,
                      true);
              break;
            case TtlUpdateOperation:
              ot =
                  getOperationTrackerForTtl(true, successTarget, requestParallelism, RouterOperation.TtlUpdateOperation,
                      true);
              break;
            default:
              fail(String.format("Operation %s not supported", routerOperation.name()));
              return;
          }
          int sentRequestCount = 0;
          int notFoundCounter = 0;
          int foundCounter = 0;
          while (sentRequestCount < replicaCount - offlineCount) {
            int requestsToSend = Math.min((replicaCount - offlineCount - sentRequestCount), requestParallelism);
            sentRequestCount += requestsToSend;
            sendRequests(ot, requestsToSend, false);
            while (requestsToSend > 0) {
              if (notFoundCounter < notFoundCount) {
                ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.NOT_FOUND);
                notFoundCounter++;
              } else if (foundCounter < foundCount) {
                ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
                foundCounter++;
              }
              requestsToSend--;
            }
          }
          assertTrue(String.format("Operation should be done for notfoundcount: %d, offlinecount: %d, foundcount: %s",
              notFoundCount, offlineCount, foundCount), ot.isDone());
          if (foundCount >= successTarget) {
            assertTrue(
                String.format("Operation should be successful for notfoundcount: %d, offlinecount: %d, foundcount: %s",
                    notFoundCount, offlineCount, foundCount), ot.hasSucceeded());
            assertFalse(String.format(
                "Operation should not have failed on not found for notfoundcount: %d, offlinecount: %d, foundcount: %s",
                notFoundCount, offlineCount, foundCount), ot.hasFailedOnNotFound());
          } else {
            assertFalse(
                String.format("Operation should have failed for notfoundcount: %d, offlinecount: %d, foundcount: %s",
                    notFoundCount, offlineCount, foundCount), ot.hasSucceeded());
            if (routerUnavailableDueToOfflineReplicas) {
              if (replicaCount - notFoundCount < successTarget) {
                assertTrue(String.format(
                    "Operation should have failed on not found for notfoundcount: %d, offlinecount: %d, foundcount: %s",
                    notFoundCount, offlineCount, foundCount), ot.hasFailedOnNotFound());
              }
            } else {
              assertTrue(String.format(
                  "Operation should have failed on not found for notfoundcount: %d, offlinecount: %d, foundcount: %s",
                  notFoundCount, offlineCount, foundCount), ot.hasFailedOnNotFound());
            }
          }
        }
      }
    }
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
   * @param replicaStateCountPairList {@link List} of {@link Pair} of count of replicas of specified {@link ReplicaState}.
   */
  private void populateReplicaList(List<Pair<Integer, ReplicaState>> replicaStateCountPairList) {
    for (Pair<Integer, ReplicaState> replicaStateCountPair : replicaStateCountPairList) {
      populateReplicaList(replicaStateCountPair.getFirst(), replicaStateCountPair.getSecond(), datanodes);
    }
  }

  /**
   * Populate replicas for a partition.
   * @param replicaCount The number of replicas to populate.
   * @param replicaState The {@link ReplicaState} associated with these replicas.
   * @param datanodes the datanodes to populate with replicas
   */
  private void populateReplicaList(int replicaCount, ReplicaState replicaState, List<MockDataNodeId> datanodes) {
    for (int i = 0; i < replicaCount; i++) {
      ReplicaId replicaId =
          new MockReplicaId(PORT, mockPartition, datanodes.get(i % datanodes.size()), 0, replicaState);
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
   * @param props {@link Properties} for {@link RouterConfig}.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param routerOperation the {@link RouterOperation} associate with this request.
   * @param includeDownReplicas whether to include down replicas in operation tracker.
   * @param terminateOnNotFoundEnabled whether to terminate the operation if we received not found responses.
   * @return the right {@link OperationTracker} based on {@link #operationTrackerType}.
   */
  private OperationTracker getOperationTracker(Properties props, boolean crossColoEnabled, int parallelism,
      RouterOperation routerOperation, boolean includeDownReplicas, boolean terminateOnNotFoundEnabled) {
    props.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    props.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, localDcName);
    props.setProperty(RouterConfig.ROUTER_GET_CROSS_DC_ENABLED, Boolean.toString(crossColoEnabled));
    props.setProperty(RouterConfig.ROUTER_PUT_REQUEST_PARALLELISM, Integer.toString(parallelism));
    props.setProperty(RouterConfig.ROUTER_LATENCY_TOLERANCE_QUANTILE, Double.toString(QUANTILE));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_MAX_INFLIGHT_REQUESTS, Integer.toString(parallelism));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED,
        Boolean.toString(terminateOnNotFoundEnabled));
    props.setProperty(RouterConfig.ROUTER_GET_ELIGIBLE_REPLICAS_BY_STATE_ENABLED,
        Boolean.toString(replicasStateEnabled));
    props.setProperty(RouterConfig.ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET, Boolean.toString(useDynamicTargetForPut));
    props.setProperty(RouterConfig.ROUTER_CROSS_COLO_REQUEST_TO_DC_WITH_MOST_REPLICAS,
        Boolean.toString(chooseDcWithMostReplicas));
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_INCLUDE_DOWN_REPLICAS,
        Boolean.toString(includeDownReplicas));
    props.setProperty(RouterConfig.ROUTER_UNAVAILABLE_DUE_TO_OFFLINE_REPLICAS,
        Boolean.toString(routerUnavailableDueToOfflineReplicas));
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    OperationTracker tracker;
    switch (operationTrackerType) {
      case SIMPLE_OP_TRACKER:
        tracker = new SimpleOperationTracker(routerConfig, routerOperation, mockPartition, originatingDcName, true,
            routerMetrics);
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
   * Returns the right {@link OperationTracker} for GET requests.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTargetForGet the number of successful responses required for GET operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param routerOperation the {@link RouterOperation} associate with this request.
   * @param includeDownReplicas whether to include down replicas in operation tracker.
   * @return the right {@link OperationTracker} based on {@link #operationTrackerType}.
   */
  private OperationTracker getOperationTrackerForGetOrPut(boolean crossColoEnabled, int successTargetForGet,
      int parallelism, RouterOperation routerOperation, boolean includeDownReplicas) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_GET_SUCCESS_TARGET, Integer.toString(successTargetForGet));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(parallelism));
    return getOperationTracker(props, crossColoEnabled, parallelism, routerOperation, includeDownReplicas, true);
  }

  /**
   * Returns the right {@link OperationTracker} for Ttl requests.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTargetForTtlUpdate the number of successful responses required for Ttl operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param routerOperation the {@link RouterOperation} associate with this request.
   * @param includeDownReplicas whether to include down replicas in operation tracker.
   * @return the right {@link OperationTracker} based on {@link #operationTrackerType}.
   */
  private OperationTracker getOperationTrackerForTtl(boolean crossColoEnabled, int successTargetForTtlUpdate,
      int parallelism, RouterOperation routerOperation, boolean includeDownReplicas) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_TTL_UPDATE_SUCCESS_TARGET, Integer.toString(successTargetForTtlUpdate));
    props.setProperty(RouterConfig.ROUTER_TTL_UPDATE_REQUEST_PARALLELISM, Integer.toString(parallelism));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(parallelism));
    return getOperationTracker(props, crossColoEnabled, parallelism, routerOperation, includeDownReplicas, true);
  }

  /**
   * Returns the right {@link OperationTracker} for Delete requests.
   * @param crossColoEnabled {@code true} if cross colo needs to be enabled. {@code false} otherwise.
   * @param successTargetForDelete the number of successful responses required for Ttl operation to succeed.
   * @param parallelism the number of parallel requests that can be in flight.
   * @param routerOperation the {@link RouterOperation} associate with this request.
   * @param includeDownReplicas whether to include down replicas in operation tracker.
   * @return the right {@link OperationTracker} based on {@link #operationTrackerType}.
   */
  private OperationTracker getOperationTrackerForDelete(boolean crossColoEnabled, int successTargetForDelete,
      int parallelism, RouterOperation routerOperation, boolean includeDownReplicas) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_DELETE_SUCCESS_TARGET, Integer.toString(successTargetForDelete));
    props.setProperty(RouterConfig.ROUTER_DELETE_REQUEST_PARALLELISM, Integer.toString(parallelism));
    props.setProperty(RouterConfig.ROUTER_GET_REQUEST_PARALLELISM, Integer.toString(parallelism));
    return getOperationTracker(props, crossColoEnabled, parallelism, routerOperation, includeDownReplicas, true);
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
    OperationTracker ot = getOperationTrackerForGetOrPut(true, 2, 3, RouterOperation.GetBlobOperation, true);
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

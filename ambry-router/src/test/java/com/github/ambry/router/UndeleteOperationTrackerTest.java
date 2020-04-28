/**
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
package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.junit.Assume.*;


/**
 * Unit test for the {@link UndeleteOperationTracker}.
 */
@RunWith(Parameterized.class)
public class UndeleteOperationTrackerTest {
  private static final int PORT = 6666;
  private final boolean replicasStateEnabled;

  private List<MockDataNodeId> datanodes;
  private MockPartitionId mockPartition;
  private String localDcName;
  private String originatingDcName;
  private MockClusterMap mockClusterMap;
  private final LinkedList<ReplicaId> inflightReplicas = new LinkedList<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public UndeleteOperationTrackerTest(boolean replicasStateEnabled) {
    this.replicasStateEnabled = replicasStateEnabled;
  }

  /**
   * Tests for the functions that determine if global quorum or local quorum is reached.
   */
  @Test
  public void quorumFunctionTest() {
    Map<String, Integer> totalNums = new HashMap<>();
    totalNums.put("DC1", 1);
    totalNums.put("DC2", 2);
    totalNums.put("DC3", 3);
    totalNums.put("DC4", 4);
    totalNums.put("DC5", 5);
    Map<String, Integer> currentNums = new HashMap<>();

    assertFalse(UndeleteOperationTracker.hasReachedGlobalQuorum(totalNums, currentNums));

    int[] quorums = new int[]{1, 2, 2, 3, 3};
    for (int i = 0; i < quorums.length; i++) {
      currentNums.put("DC" + (i + 1), quorums[i]);
    }

    for (int dc = 1; dc <= quorums.length; dc++) {
      int currentQuorum = quorums[dc - 1];
      for (int i = 0; i < currentQuorum; i++) {
        currentNums.put("DC" + dc, i);
        assertFalse("Expect global quorum not reached when DC" + dc + " only has " + i,
            UndeleteOperationTracker.hasReachedGlobalQuorum(totalNums, currentNums));
      }
      int currentTotal = totalNums.get("DC" + dc);
      for (int i = currentQuorum; i <= currentTotal; i++) {
        currentNums.put("DC" + dc, i);
        assertTrue("Expect global quorum reached when DC" + dc + " has " + i,
            UndeleteOperationTracker.hasReachedGlobalQuorum(totalNums, currentNums));
      }
      currentNums.put("DC" + dc, currentQuorum);
    }

    for (int i = 0; i < quorums.length; i++) {
      currentNums.put("DC" + (i + 1), 0);
    }

    for (int dc = 1; dc <= quorums.length; dc++) {
      int currentQuorum = quorums[dc - 1];
      for (int i = 0; i < currentQuorum; i++) {
        currentNums.put("DC" + dc, i);
        assertFalse("Expect any local quorum not reached when DC" + dc + " only has " + i,
            UndeleteOperationTracker.hasReachedAnyLocalQuorum(totalNums, currentNums));
      }
      int currentTotal = totalNums.get("DC" + dc);
      for (int i = currentQuorum; i <= currentTotal; i++) {
        currentNums.put("DC" + dc, i);
        assertTrue("Expect any loocal quorum reached when DC" + dc + " has " + i,
            UndeleteOperationTracker.hasReachedAnyLocalQuorum(totalNums, currentNums));
      }
      currentNums.put("DC" + dc, 0);
    }
  }

  /**
   * Test when all the responses are success.
   */
  @Test
  public void successTest() {
    initialize();
    UndeleteOperationTracker tracker = getOperationTracker(3);
    assertFalse("Operation should not have been done", tracker.isDone());
    int counter = 0;
    while (!tracker.hasSucceeded()) {
      if (inflightReplicas.isEmpty()) {
        sendRequests(tracker, 3);
      }
      tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      counter++;
    }
    assertTrue("Operation should have be done", tracker.isDone());
    assertFalse("Operation should not failed", tracker.hasFailed());
    assertTrue("Operation should at least send out 8 requests", counter >= 8);
  }

  /**
   * Test when two responses from different datacenter are failure, but the rest are success.
   */
  @Test
  public void successWithSomeRequestFailureTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    UndeleteOperationTracker tracker = getOperationTracker(3);
    assertFalse("Operation should not have been done", tracker.isDone());

    // First 3 requests should target at local datacenter, fail one of them
    sendRequests(tracker, 3);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertFalse("Operation should not have been done", tracker.isDone());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());

    // Following 3 requests should target at originating datacenter, fail one of them
    sendRequests(tracker, 3);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertFalse("Operation should not have been done", tracker.isDone());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());

    for (int counter = 0; counter < 2; counter++) {
      sendRequests(tracker, 3);
      for (int i = 0; i < 3; i++) {
        tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have be done", tracker.isDone());
    assertFalse("Operation should not failed", tracker.hasFailed());
    assertTrue("Operation should have succeeded", tracker.hasSucceeded());
  }

  /**
   * Tests when some hosts are down.
   */
  @Test
  public void successWithSufficientHostsNotDownTest() {
    initialize();
    // The first 4 replicas belong to four different datacenter, mark them as down. Since each datacenter has 3 replica,
    // then the first 8 hosts should reach the global quorum.
    for (int i = 0; i < 4; i++) {
      ((MockReplicaId) mockPartition.getReplicaIds().get(i)).markReplicaDownStatus(true);
    }
    UndeleteOperationTracker tracker = getOperationTracker(2);
    // Now we need to set all the response to be success
    for (int i = 0; i < 4; i++) {
      sendRequests(tracker, 2);
      assertFalse("Operation should not have failed", tracker.hasFailed());
      assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
      assertFalse("Operation should not be done", tracker.isDone());
      for (int j = 0; j < 2; j++) {
        tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertFalse("Operation should not have failed", tracker.hasFailed());
    assertTrue("Operation should have succeeded", tracker.hasSucceeded());
    assertTrue("Operation should be done", tracker.isDone());
  }

  /**
   * Tests when not all hosts are eligible not enough hosts are.
   */
  @Test
  public void successWithSufficientEligibleHostsTest() {
    assumeTrue(replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
        new MockDataNodeId(portList, mountPaths, "dc-3")));
    mockPartition = new MockPartitionId();
    populateReplicaList(8, ReplicaState.STANDBY);
    populateReplicaList(4, ReplicaState.INACTIVE);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);

    UndeleteOperationTracker tracker = getOperationTracker(2);
    // Now we need to set all the response to be success
    for (int i = 0; i < 4; i++) {
      sendRequests(tracker, 2);
      assertFalse("Operation should not have failed", tracker.hasFailed());
      assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
      assertFalse("Operation should not be done", tracker.isDone());
      for (int j = 0; j < 2; j++) {
        tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertFalse("Operation should not have failed", tracker.hasFailed());
    assertTrue("Operation should have succeeded", tracker.hasSucceeded());
    assertTrue("Operation should be done", tracker.isDone());
  }

  /**
   * Tests when there are two failures in one datacenter.
   */
  @Test
  public void failureTest() {
    initialize();
    UndeleteOperationTracker tracker = getOperationTracker(3);
    sendRequests(tracker, 3);
    // First 3 requests should target at local datacenter, fail two of them should fail the operation
    for (int i = 0; i < 2; i++) {
      ReplicaId replicaId = inflightReplicas.poll();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), localDcName);
      tracker.onResponse(replicaId, TrackedRequestFinalState.FAILURE);
    }
    assertTrue("Operation should have failed", tracker.hasFailed());
    assertTrue("Operation should be done", tracker.isDone());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
  }

  /**
   * Tests when there are two failures in originating datacenter.
   */
  @Test
  public void failureWithOriginatingDcTest() {
    initialize();
    originatingDcName = datanodes.get(datanodes.size() - 1).getDatacenterName();
    UndeleteOperationTracker tracker = getOperationTracker(3);
    sendRequests(tracker, 3);
    // First three requests should target at local datacenter, and request 4 to 6 should target at originating datacenter.
    for (int i = 0; i < 3; i++) {
      ReplicaId replicaId = inflightReplicas.poll();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), localDcName);
      tracker.onResponse(replicaId, TrackedRequestFinalState.SUCCESS);
    }
    assertFalse("Operation should not be done", tracker.isDone());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());

    sendRequests(tracker, 3);
    for (int i = 0; i < 2; i++) {
      ReplicaId replicaId = inflightReplicas.poll();
      assertEquals(replicaId.getDataNodeId().getDatacenterName(), originatingDcName);
      tracker.onResponse(replicaId, TrackedRequestFinalState.FAILURE);
    }

    assertTrue("Operation should have failed", tracker.hasFailed());
    assertTrue("Operation should be done", tracker.isDone());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
  }

  /**
   * Tests when there are ineligible hosts.
   */
  @Test
  public void failureWithIneligibleNodesTest() {
    assumeTrue(replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
        new MockDataNodeId(portList, mountPaths, "dc-3")));
    mockPartition = new MockPartitionId();
    populateReplicaList(8, ReplicaState.STANDBY);
    populateReplicaList(4, ReplicaState.INACTIVE);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);
    // Now any of the failure would fail the operation

    UndeleteOperationTracker tracker = getOperationTracker(2);

    sendRequests(tracker, 2);
    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
    assertFalse("Operation should not have failed", tracker.hasFailed());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
    assertFalse("Operation should not be done", tracker.isDone());

    tracker.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.FAILURE);
    assertTrue("Operation should have failed", tracker.hasFailed());
    assertFalse("Operation should not have succeeded", tracker.hasSucceeded());
    assertTrue("Operation should be done", tracker.isDone());
  }

  /**
   * Tests when there are not sufficient eligible hosts.
   */
  @Test
  public void failureForInitialization() {
    assumeTrue(replicasStateEnabled);
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    datanodes = new ArrayList<>(Arrays.asList(new MockDataNodeId(portList, mountPaths, "dc-0"),
        new MockDataNodeId(portList, mountPaths, "dc-1"), new MockDataNodeId(portList, mountPaths, "dc-2"),
        new MockDataNodeId(portList, mountPaths, "dc-3")));
    mockPartition = new MockPartitionId();
    populateReplicaList(4, ReplicaState.STANDBY);
    populateReplicaList(4, ReplicaState.INACTIVE);
    populateReplicaList(4, ReplicaState.DROPPED);
    localDcName = datanodes.get(0).getDatacenterName();
    mockClusterMap = new MockClusterMap(false, datanodes, 1, Collections.singletonList(mockPartition), localDcName);

    try {
      getOperationTracker(3);
      fail("Should fail to create undelete operation tracker because of insufficient eligible hosts");
    } catch (IllegalArgumentException e) {
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
   * Returns {@link UndeleteOperationTracker}.
   * @param parallelism the number of parallel requests that can be in flight.
   * @return {@link UndeleteOperationTracker}.
   */
  private UndeleteOperationTracker getOperationTracker(int parallelism) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    props.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, localDcName);
    props.setProperty(RouterConfig.ROUTER_OPERATION_TRACKER_TERMINATE_ON_NOT_FOUND_ENABLED, "true");
    props.setProperty(RouterConfig.ROUTER_GET_ELIGIBLE_REPLICAS_BY_STATE_ENABLED,
        Boolean.toString(replicasStateEnabled));
    props.setProperty(RouterConfig.ROUTER_UNDELETE_REQUEST_PARALLELISM, Integer.toString(parallelism));
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    return new UndeleteOperationTracker(routerConfig, mockPartition, originatingDcName);
  }

  /**
   * Send requests to all replicas provided by the {@link UndeleteOperationTracker#getReplicaIterator()}
   * @param tracker the {@link UndeleteOperationTracker} that provides replicas.
   * @param numRequestsExpected the number of requests expected to be sent out.
   */
  private void sendRequests(UndeleteOperationTracker tracker, int numRequestsExpected) {
    int sent = 0;
    Iterator<ReplicaId> replicaIdIterator = tracker.getReplicaIterator();
    while (replicaIdIterator.hasNext()) {
      ReplicaId nextReplica = replicaIdIterator.next();
      assertNotNull("There should be a replica to send a request to", nextReplica);
      assertFalse("Replica that was used for a request returned by iterator again: " + nextReplica,
          repetitionTracker.contains(nextReplica));
      inflightReplicas.offer(nextReplica);
      replicaIdIterator.remove();
      repetitionTracker.add(nextReplica);
      sent++;
    }
    assertEquals("Did not send expected number of requests: " + inflightReplicas, numRequestsExpected, sent);
  }

  /**
   * Populate replicas for a partition.
   * @param replicaCount The number of replicas to populate.
   * @param replicaState The {@link ReplicaState} associated with these replicas.
   */
  private void populateReplicaList(int replicaCount, ReplicaState replicaState) {
    for (int i = 0; i < replicaCount; i++) {
      ReplicaId replicaId = new MockReplicaId(PORT, mockPartition, datanodes.get(i % datanodes.size()), 0);
      mockPartition.replicaIds.add(replicaId);
      mockPartition.replicaAndState.put(replicaId, replicaState);
    }
  }
}

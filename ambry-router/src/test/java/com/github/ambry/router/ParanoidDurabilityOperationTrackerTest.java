/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ParanoidDurabilityOperationTracker}.
 *
 * {@link ParanoidDurabilityOperationTracker} needs to control writes to both local and remote replicas, and ensure that
 * the operation is successful only if the required number of replicas in both local and remote data centers are successful.
 */
//@RunWith(Parameterized.class)
public class ParanoidDurabilityOperationTrackerTest {
  private static final int PORT = 7777;
  private MockPartitionId mockPartition;
  private String localDcName;
  private MockClusterMap mockClusterMap;
  private final LinkedList<ReplicaId> inflightReplicas = new LinkedList<>();
  private final Set<ReplicaId> repetitionTracker = new HashSet<>();
  private List<ReplicaId> responseReplicas = new ArrayList<>();


  public ParanoidDurabilityOperationTrackerTest() { }

  /**
   * A vanilla test with good replicas everywhere and a successful outcome.
   */
  @Test
  public void basicSuccessTest() {
    int localReplicaSuccessTarget = 2;
    int remoteReplicaSuccessTarget = 1;
    int remoteAttemptLimit = 2;
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths) }));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    responseReplicas.clear();
    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, localReplicaSuccessTarget, remoteReplicaSuccessTarget, remoteAttemptLimit, true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3);
      for (int i = 0; i < 3; i++) {
        ReplicaId currentReplica = inflightReplicas.poll();
        responseReplicas.add(currentReplica);
        ot.onResponse(currentReplica, TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have failed", ot.hasFailed());
    assertTrue(localReplicaSuccessTarget + " of the successful replicas should be local and " + remoteReplicaSuccessTarget +" should be remote",
        verifySuccessfulReplicas(responseReplicas, localReplicaSuccessTarget, remoteReplicaSuccessTarget));
  }

  /**
   * A vanilla test with too few healthy remote replicas, and therefore a failure.
   */
  @Test
  public void tooFewRemoteReplicasTest() {
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.BOOTSTRAP, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.BOOTSTRAP, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.OFFLINE, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.ERROR, portList, mountPaths)}));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    try {
      ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, 2, true);
      Assert.fail("Too few remote replicas should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) { }
  }

  /**
   * A test with not enough local replicas, resulting in failure.
   */
  @Test
  public void tooFewLocalReplicasTest() {
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.DROPPED, portList, mountPaths),        // Not eligible for writes
        new ReplicaSpec(localDcName, ReplicaState.BOOTSTRAP, portList, mountPaths),      // Not eligible for writes
        new ReplicaSpec("remote-1", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths) }));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    try {
      ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, 2, true);
      Assert.fail("Too few remote replicas should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) { }
  }

  /**
   * A test with just enough local replicas, and a successful outcome.
   */
  @Test
  public void justEnoughLocalReplicasTest() {
    int localReplicaSuccessTarget = 2;
    int remoteReplicaSuccessTarget = 1;
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-1", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths) }));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    responseReplicas.clear();
    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, 2, true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3);
      for (int i = 0; i < 3; i++) {
        ReplicaId currentReplica = inflightReplicas.poll();
        responseReplicas.add(currentReplica);
        ot.onResponse(currentReplica, TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have failed", ot.hasFailed());
    assertTrue(localReplicaSuccessTarget + " of the successful replicas should be local and " + remoteReplicaSuccessTarget +" should be remote",
        verifySuccessfulReplicas(responseReplicas, localReplicaSuccessTarget, remoteReplicaSuccessTarget));
  }

  /**
   * A test with just enough remote replicas, and a successful outcome.
   */
  @Test
  public void justEnoughRemoteReplicasTest() {
    int localReplicaSuccessTarget = 2;
    int remoteReplicaSuccessTarget = 1;
    int remoteAttemptLimit = 2;
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec("remote-2", ReplicaState.STANDBY, portList, mountPaths) }));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    responseReplicas.clear();
    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, localReplicaSuccessTarget, remoteReplicaSuccessTarget, remoteAttemptLimit,  true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3);
      for (int i = 0; i < 3; i++) {
        ReplicaId currentReplica = inflightReplicas.poll();
        responseReplicas.add(currentReplica);
        ot.onResponse(currentReplica, TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have failed", ot.hasFailed());
    assertTrue(localReplicaSuccessTarget + " of the successful replicas should be local and " + (remoteReplicaSuccessTarget+1) +" should be remote",
        verifySuccessfulReplicas(responseReplicas, localReplicaSuccessTarget, remoteReplicaSuccessTarget));
  }

  /**
   * A test with 2 remote replicas, both of which time out. This should result in a successful outcome, triggering the
   * remote attempt limit exceeded mechanism.
   */
  @Test
  public void timedOutRemoteReplicasTest() {
    int localReplicaSuccessTarget = 2;
    int remoteReplicaSuccessTarget = 2;
    int remoteAttemptLimit = 2;
    localDcName = new String("dc-0");
    String remoteDcName1 = new String("remote-1");
    String remoteDcName2 = new String("remote-2");

    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(localDcName, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(remoteDcName1, ReplicaState.STANDBY, portList, mountPaths),
        new ReplicaSpec(remoteDcName2, ReplicaState.STANDBY, portList, mountPaths)}));

    mockPartition = new MockPartitionId();
    mockClusterMap = new MockClusterMap(false, buildDataNodeList(testReplicas), 1, Collections.singletonList(mockPartition), localDcName);
    populateReplicaList(testReplicas);

    responseReplicas.clear();
    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 2, localReplicaSuccessTarget, remoteReplicaSuccessTarget, remoteAttemptLimit,  true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 4);
      for (int i = 0; i < 4; i++) {
        ReplicaId currentReplica = inflightReplicas.poll();
        responseReplicas.add(currentReplica);
        if (currentReplica.getDataNodeId().getDatacenterName().equals(remoteDcName1) ||
            currentReplica.getDataNodeId().getDatacenterName().equals(remoteDcName2) ) {
          ot.onResponse(currentReplica, TrackedRequestFinalState.TIMED_OUT);
        } else {
          ot.onResponse(currentReplica, TrackedRequestFinalState.SUCCESS);
        }
        //ot.onResponse(currentReplica, TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
    assertFalse("Operation should not have failed", ot.hasFailed());
    assertTrue(localReplicaSuccessTarget + " of the successful replicas should be local and " + (remoteReplicaSuccessTarget+1) +" should be remote",
        verifySuccessfulReplicas(responseReplicas, localReplicaSuccessTarget, remoteReplicaSuccessTarget));
  }



  /**
   * Generates a list of {@link MockDataNodeId} based on the given {@link ReplicaSpec}.
   * @param replicaSpecs
   * @return a list of {@link MockDataNodeId}.
   */
  private List<MockDataNodeId> buildDataNodeList(List<ReplicaSpec> replicaSpecs) {
    List<MockDataNodeId> dataNodes = new ArrayList<MockDataNodeId>();
    for(ReplicaSpec replicaSpec : replicaSpecs) {
      dataNodes.add(replicaSpec.getDataNode());
    }
    return dataNodes;
  }

  /**
   * Configures a mock {@link ParanoidDurabilityOperationTracker}.
   * @return {@link ParanoidDurabilityOperationTracker}.
   */
  private ParanoidDurabilityOperationTracker getParanoidDurabilityOperationTracker(int localParallelism, int remoteParallelism,
      int localSuccessTarget, int remoteSuccessTarget, int remoteAttemptLimit, boolean useDynamicSuccessTarget) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    props.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, localDcName);
    props.setProperty(RouterConfig.ROUTER_PUT_REQUEST_PARALLELISM, Integer.toString(localParallelism));
    props.setProperty(RouterConfig.ROUTER_PUT_REMOTE_REQUEST_PARALLELISM, Integer.toString(remoteParallelism));
    props.setProperty(RouterConfig.ROUTER_PUT_SUCCESS_TARGET, Integer.toString(localSuccessTarget));
    props.setProperty(RouterConfig.ROUTER_PUT_REMOTE_SUCCESS_TARGET, Integer.toString(remoteSuccessTarget));
    props.setProperty(RouterConfig.ROUTER_PUT_REMOTE_ATTEMPT_LIMIT, Integer.toString(remoteAttemptLimit));
    props.setProperty(RouterConfig.ROUTER_PUT_USE_DYNAMIC_SUCCESS_TARGET, Boolean.toString(useDynamicSuccessTarget));

    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(props));
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    return new ParanoidDurabilityOperationTracker(routerConfig, mockPartition, localDcName, routerMetrics);
  }

  /**
   * Send requests to all replicas provided by the {@link ParanoidDurabilityOperationTracker#getReplicaIterator()}
   * @param tracker the {@link ParanoidDurabilityOperationTracker} that provides replicas.
   * @param numRequestsExpected the number of requests expected to be sent out.
   */
  private void sendRequests(ParanoidDurabilityOperationTracker tracker, int numRequestsExpected) {
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
   * @param specs The list of {@link ReplicaSpec} that contains the information of the replicas.
   */
  private void populateReplicaList(List<ReplicaSpec> specs) {
    for (ReplicaSpec spec : specs) {
      ReplicaId replicaId = new MockReplicaId(PORT, mockPartition, spec.getDataNode(), 0, spec.getState());
      mockPartition.replicaIds.add(replicaId);
      mockPartition.replicaAndState.put(replicaId, spec.getState());
    }
  }

  /**
   * Verify that the expected number of local and remote replicas successfully processed the request.
   * @param replicas The list of replicas to verify.
   * @param expectedLocalSuccessCount The expected number of local replicas.
   * @param expectedRemoteSuccessCount The expected number of remote replicas.
   * @return {@code true} if the number of local and remote replicas match the expected values, {@code false} otherwise.
   */
  private boolean verifySuccessfulReplicas(List<ReplicaId> replicas, int expectedLocalSuccessCount, int expectedRemoteSuccessCount) {
    int actualLocalSuccessCount = 0;
    int actualRemoteSuccessCount = 0;
    for (ReplicaId replica: replicas) {
      if (replica.getDataNodeId().getDatacenterName().equals(localDcName)) {
        actualLocalSuccessCount++;
      } else {
        actualRemoteSuccessCount++;
      }
    }
    return actualLocalSuccessCount == expectedLocalSuccessCount && actualRemoteSuccessCount == expectedRemoteSuccessCount;
  }

  /**
   * A class that represents the specification of a replica, for easy mocking with state and data center information.
   */
  private class ReplicaSpec {
    private final MockDataNodeId dataNode;
    private final ReplicaState helixState;

    public ReplicaSpec(String dcName, ReplicaState state, List<Port> ports, List<String> mounts) {
      helixState = state;
      dataNode = new MockDataNodeId(ports, mounts, dcName);
    }

    public ReplicaState getState() { return helixState; }

    public MockDataNodeId getDataNode() {
      return dataNode;
    }
  }
}

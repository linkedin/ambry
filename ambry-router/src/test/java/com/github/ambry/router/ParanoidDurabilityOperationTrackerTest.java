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


  public ParanoidDurabilityOperationTrackerTest() { }

  /**
   * A vanilla test with good replicas everywhere and a successful outcome.
   */
  @Test
  public void basicSuccessTest() {
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

    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3);
      for (int i = 0; i < 3; i++) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
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
      ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, true);
      Assert.fail("Too few remote replicas should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) { }
  }

  /**
   * A test with just enough local replicas, and a successful outcome.
   */
  @Test
  public void tooFewLocalReplicasTest() {
    localDcName = new String("dc-0");
    List<Port> portList = Collections.singletonList(new Port(PORT, PortType.PLAINTEXT));
    List<String> mountPaths = Collections.singletonList("mockMountPath");
    List<ReplicaSpec> testReplicas = new ArrayList<ReplicaSpec>(Arrays.asList(new ReplicaSpec[] {
        new ReplicaSpec(localDcName, ReplicaState.LEADER, portList, mountPaths),
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
      ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, true);
      Assert.fail("Too few remote replicas should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) { }
  }

  /**
   * A test with just enough local replicas, and a successful outcome.
   */
  @Test
  public void justEnoughLocalReplicasTest() {
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

    ParanoidDurabilityOperationTracker ot = getParanoidDurabilityOperationTracker(2, 1, 2, 1, true);
    while (!ot.hasSucceeded()) {
      sendRequests(ot, 3);
      for (int i = 0; i < 3; i++) {
        ot.onResponse(inflightReplicas.poll(), TrackedRequestFinalState.SUCCESS);
      }
    }
    assertTrue("Operation should have succeeded", ot.hasSucceeded());
    assertTrue("Operation should be done", ot.isDone());
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
      int localSuccessTarget, int remoteSuccessTarget, boolean useDynamicSuccessTarget) {
    Properties props = new Properties();
    props.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    props.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, localDcName);
    props.setProperty(RouterConfig.ROUTER_PUT_REQUEST_PARALLELISM, Integer.toString(localParallelism));
    props.setProperty(RouterConfig.ROUTER_PUT_REMOTE_REQUEST_PARALLELISM, Integer.toString(remoteParallelism));
    props.setProperty(RouterConfig.ROUTER_PUT_SUCCESS_TARGET, Integer.toString(localSuccessTarget));
    props.setProperty(RouterConfig.ROUTER_PUT_REMOTE_SUCCESS_TARGET, Integer.toString(remoteSuccessTarget));
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
      ReplicaId replicaId = new MockReplicaId(PORT, mockPartition, spec.getDataNode(), 0);
      mockPartition.replicaIds.add(replicaId);
      mockPartition.replicaAndState.put(replicaId, spec.getState());
    }
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

    public ReplicaState getState() {
      return helixState;
    }

    public MockDataNodeId getDataNode() {
      return dataNode;
    }
  }
}

/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for the dynamic cluster manager components {@link AmbryDataNode}, {@link AmbryDisk}, {@link AmbryPartition}
 * and {@link AmbryReplica}.
 */
public class DynamicClusterManagerComponentsTest {
  private static final int PORT_NUM1 = 2000;
  private static final int PORT_NUM2 = 2001;
  private static final long RACK_ID = 1;
  private static final int SSL_PORT_NUM = 3000;
  private static final String HOST_NAME = TestUtils.getLocalHost();
  private final ClusterMapConfig clusterMapConfig1;
  private final ClusterMapConfig clusterMapConfig2;

  /**
   * Instantiate and initialize clustermap configs.
   */
  public DynamicClusterManagerComponentsTest() {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", HOST_NAME);
    props.setProperty("clustermap.cluster.name", "clusterName");
    props.setProperty("clustermap.datacenter.name", "DC0");
    props.setProperty("clustermap.ssl.enabled.datacenters", "DC1");
    clusterMapConfig1 = new ClusterMapConfig(new VerifiableProperties(props));
    props.setProperty("clustermap.datacenter.name", "DC1");
    clusterMapConfig2 = new ClusterMapConfig(new VerifiableProperties(props));
  }

  /**
   * Test {@link AmbryDataNode}, {@link AmbryDisk}, {@link AmbryPartition} and {@link AmbryReplica}.
   * @throws Exception
   */
  @Test
  public void helixClusterManagerComponentsTest() throws Exception {
    // AmbryDataNode test
    try {
      new AmbryDataNode("DC1", clusterMapConfig2, HOST_NAME, PORT_NUM1, RACK_ID, null);
      fail("Datanode construction should have failed when SSL is enabled and SSL port is null");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      new AmbryDataNode("DC1", clusterMapConfig1, HOST_NAME, MAX_PORT + 1, RACK_ID, null);
      fail("Datanode construction should have failed when port num is outside the valid range");
    } catch (IllegalArgumentException e) {
      // OK
    }
    AmbryDataNode datanode1 = new AmbryDataNode("DC0", clusterMapConfig1, HOST_NAME, PORT_NUM1, RACK_ID, SSL_PORT_NUM);
    AmbryDataNode datanode2 = new AmbryDataNode("DC1", clusterMapConfig2, HOST_NAME, PORT_NUM2, RACK_ID, SSL_PORT_NUM);
    assertEquals(datanode1.getDatacenterName(), "DC0");
    assertEquals(datanode1.getHostname(), HOST_NAME);
    assertEquals(datanode1.getPort(), PORT_NUM1);
    assertEquals(datanode1.getSSLPort(), SSL_PORT_NUM);
    assertEquals(datanode1.getRackId(), RACK_ID);
    assertTrue(datanode1.hasSSLPort());
    assertEquals(PortType.PLAINTEXT, datanode1.getPortToConnectTo().getPortType());
    assertTrue(datanode2.hasSSLPort());
    assertEquals(PortType.SSL, datanode2.getPortToConnectTo().getPortType());
    assertEquals(HardwareState.AVAILABLE, datanode1.getState());
    datanode1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, datanode1.getState());
    datanode1.setState(HardwareState.AVAILABLE);
    assertEquals(HardwareState.AVAILABLE, datanode1.getState());
    assertTrue(datanode1.compareTo(datanode1) == 0);
    assertTrue(datanode1.compareTo(datanode2) != 0);

    // AmbryDisk tests
    String mountPath1 = "/mnt/1";
    String mountPath2 = "/mnt/2";
    try {
      new AmbryDisk(clusterMapConfig1, null, mountPath1, HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, null, HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, "", HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, "0", HardwareState.UNAVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }
    try {
      new AmbryDisk(clusterMapConfig1, datanode1, mountPath1, HardwareState.UNAVAILABLE,
          MAX_DISK_CAPACITY_IN_BYTES + 1);
      fail("disk initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    AmbryDisk disk1 =
        new AmbryDisk(clusterMapConfig1, datanode1, mountPath1, HardwareState.AVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
    AmbryDisk disk2 =
        new AmbryDisk(clusterMapConfig2, datanode2, mountPath2, HardwareState.AVAILABLE, MAX_DISK_CAPACITY_IN_BYTES);
    assertEquals(mountPath1, disk1.getMountPath());
    assertEquals(MAX_DISK_CAPACITY_IN_BYTES, disk1.getRawCapacityInBytes());
    assertEquals(HardwareState.AVAILABLE, disk1.getState());
    disk1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, disk1.getState());
    disk1.setState(HardwareState.AVAILABLE);
    assertEquals(HardwareState.AVAILABLE, disk1.getState());
    assertTrue(disk1.getDataNode().equals(datanode1));
    datanode1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, disk1.getState());

    // AmbryPartition tests
    // All partitions are READ_WRITE initially.
    MockClusterManagerCallback mockClusterManagerCallback = new MockClusterManagerCallback();
    AmbryPartition partition1 = new AmbryPartition(1, mockClusterManagerCallback);
    AmbryPartition partition2 = new AmbryPartition(2, mockClusterManagerCallback);
    assertTrue(partition1.isEqual(partition1.toPathString()));
    assertTrue(partition1.compareTo(partition1) == 0);
    assertFalse(partition1.isEqual(partition2.toPathString()));
    assertTrue(partition1.compareTo(partition2) != 0);
    partition1.setState(PartitionState.READ_ONLY);
    assertEquals(partition1.getPartitionState(), PartitionState.READ_ONLY);
    assertEquals(partition2.getPartitionState(), PartitionState.READ_WRITE);

    // AmbryReplica tests
    try {
      new AmbryReplica(null, disk1, MAX_REPLICA_CAPACITY_IN_BYTES);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    try {
      new AmbryReplica(partition1, null, MAX_REPLICA_CAPACITY_IN_BYTES);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    try {
      new AmbryReplica(partition1, disk1, MAX_REPLICA_CAPACITY_IN_BYTES + 1);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    // Create a few replicas and make the mockClusterManagerCallback aware of the association.
    AmbryReplica replica1 = new AmbryReplica(partition1, disk1, MAX_REPLICA_CAPACITY_IN_BYTES);
    mockClusterManagerCallback.addReplicaToPartition(partition1, replica1);
    AmbryReplica replica2 = new AmbryReplica(partition2, disk2, MIN_REPLICA_CAPACITY_IN_BYTES);
    mockClusterManagerCallback.addReplicaToPartition(partition2, replica2);
    AmbryReplica replica3 = new AmbryReplica(partition1, disk2, MIN_REPLICA_CAPACITY_IN_BYTES);
    mockClusterManagerCallback.addReplicaToPartition(partition1, replica3);

    assertEquals(replica1.getDiskId().getMountPath(), replica1.getMountPath());
    List<AmbryReplica> peerReplicas = replica1.getPeerReplicaIds();
    assertEquals(1, peerReplicas.size());
    assertEquals(replica1.getPartitionId(), peerReplicas.get(0).getPartitionId());
    assertEquals(replica3, peerReplicas.get(0));

    List<AmbryReplica> replicaList1 = partition1.getReplicaIds();
    List<AmbryReplica> replicaList2 = partition2.getReplicaIds();
    assertEquals("Found: " + replicaList1.toString(), 2, replicaList1.size());
    assertTrue(replicaList1.contains(replica1));
    assertTrue(replicaList1.contains(replica3));
    assertEquals(1, replicaList2.size());
    assertTrue(replicaList2.contains(replica2));
  }

  /**
   * A helper class that mocks the {@link ClusterManagerCallback} and stores partition to replicas mapping internally
   * as told.
   */
  private class MockClusterManagerCallback implements ClusterManagerCallback {
    Map<AmbryPartition, List<AmbryReplica>> partitionToReplicas = new HashMap<>();

    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      return new ArrayList<AmbryReplica>(partitionToReplicas.get(partition));
    }

    /**
     * Associate the replica with the given partition.
     * @param partition the {@link AmbryPartition}.
     * @param replica the {@link AmbryReplica}.
     */
    void addReplicaToPartition(AmbryPartition partition, AmbryReplica replica) {
      if (!partitionToReplicas.containsKey(partition)) {
        partitionToReplicas.put(partition, new ArrayList<AmbryReplica>());
      }
      partitionToReplicas.get(partition).add(replica);
    }
  }
}

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
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for the dynamic cluster manager components {@link AmbryDataNode}, {@link AmbryDisk}, {@link AmbryPartition}
 * and {@link AmbryReplica}.
 */
public class DynamicClusterManagerComponentsTest {
  private static final int PORT_NUM1 = 2000;
  private static final int PORT_NUM2 = 2001;
  private static final String RACK_ID = "1";
  private static final long XID = 64;
  private static final int SSL_PORT_NUM = 3000;
  private static final String HOST_NAME = TestUtils.getLocalHost();
  private final ClusterMapConfig clusterMapConfig1;
  private final ClusterMapConfig clusterMapConfig2;
  private AtomicLong sealedStateChangeCounter;

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
    MockClusterManagerCallback mockClusterManagerCallback = new MockClusterManagerCallback();
    // AmbryDataNode test
    try {
      new AmbryServerDataNode("DC1", clusterMapConfig2, HOST_NAME, PORT_NUM1, RACK_ID, null, null, XID,
          mockClusterManagerCallback);
      fail("Datanode construction should have failed when SSL is enabled and SSL port is null");
    } catch (IllegalArgumentException e) {
      // OK
    }
    try {
      new AmbryServerDataNode("DC1", clusterMapConfig1, HOST_NAME, MAX_PORT + 1, RACK_ID, null, null, XID,
          mockClusterManagerCallback);
      fail("Datanode construction should have failed when port num is outside the valid range");
    } catch (IllegalArgumentException e) {
      // OK
    }
    AmbryDataNode datanode1 =
        new AmbryServerDataNode("DC0", clusterMapConfig1, HOST_NAME, PORT_NUM1, RACK_ID, SSL_PORT_NUM, null, XID,
            mockClusterManagerCallback);
    AmbryDataNode datanode2 =
        new AmbryServerDataNode("DC1", clusterMapConfig2, HOST_NAME, PORT_NUM2, RACK_ID, SSL_PORT_NUM, null, XID,
            mockClusterManagerCallback);
    assertEquals(datanode1.getDatacenterName(), "DC0");
    assertEquals(datanode1.getHostname(), HOST_NAME);
    assertEquals(datanode1.getPort(), PORT_NUM1);
    assertEquals(datanode1.getSSLPort(), SSL_PORT_NUM);
    assertEquals(datanode1.getRackId(), RACK_ID);
    assertEquals(datanode1.getXid(), XID);
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
    // the snapshot is for debug info, but just check that it works without throwing exceptions.
    assertNotNull(datanode1.getSnapshot());
    assertNotNull(datanode1.toString());

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
    assertEquals(disk1.getDataNode(), datanode1);
    datanode1.setState(HardwareState.UNAVAILABLE);
    assertEquals(HardwareState.UNAVAILABLE, disk1.getState());

    // AmbryPartition tests
    // All partitions are READ_WRITE initially.
    sealedStateChangeCounter = new AtomicLong(0);
    String partition1Class = getRandomString(10);
    String partition2Class = getRandomString(10);
    AmbryPartition partition1 = new AmbryPartition(1, partition1Class, mockClusterManagerCallback);
    AmbryPartition partition2 = new AmbryPartition(2, partition2Class, mockClusterManagerCallback);
    assertTrue(partition1.isEqual(partition1.toPathString()));
    assertTrue(partition1.compareTo(partition1) == 0);
    assertFalse(partition1.isEqual(partition2.toPathString()));
    assertTrue(partition1.compareTo(partition2) != 0);
    assertEquals("Partition class not as expected", partition1Class, partition1.getPartitionClass());
    assertEquals("Partition class not as expected", partition2Class, partition2.getPartitionClass());

    // AmbryReplica tests
    try {
      new AmbryServerReplica(clusterMapConfig1, null, disk1, false, MAX_REPLICA_CAPACITY_IN_BYTES, false);
      fail("Replica initialization should fail with invalid arguments");
    } catch (NullPointerException e) {
      // OK
    }

    try {
      new AmbryServerReplica(clusterMapConfig1, partition1, null, false, MAX_REPLICA_CAPACITY_IN_BYTES, false);
      fail("Replica initialization should fail with invalid arguments");
    } catch (NullPointerException e) {
      // OK
    }

    try {
      new AmbryServerReplica(clusterMapConfig1, partition1, disk1, false, MAX_REPLICA_CAPACITY_IN_BYTES + 1, false);
      fail("Replica initialization should fail with invalid arguments");
    } catch (IllegalStateException e) {
      // OK
    }

    // Create a few replicas and make the mockClusterManagerCallback aware of the association.
    AmbryReplica replica1 =
        new AmbryServerReplica(clusterMapConfig1, partition1, disk1, false, MAX_REPLICA_CAPACITY_IN_BYTES, false);
    mockClusterManagerCallback.addReplicaToPartition(partition1, replica1);
    AmbryReplica replica2 =
        new AmbryServerReplica(clusterMapConfig1, partition2, disk1, false, MIN_REPLICA_CAPACITY_IN_BYTES, false);
    mockClusterManagerCallback.addReplicaToPartition(partition2, replica2);
    AmbryReplica replica3 =
        new AmbryServerReplica(clusterMapConfig2, partition1, disk2, false, MIN_REPLICA_CAPACITY_IN_BYTES, false);
    mockClusterManagerCallback.addReplicaToPartition(partition1, replica3);
    AmbryReplica replica4 =
        new AmbryServerReplica(clusterMapConfig2, partition2, disk2, false, MIN_REPLICA_CAPACITY_IN_BYTES, true);
    mockClusterManagerCallback.addReplicaToPartition(partition2, replica4);
    AmbryReplica replica5 =
        new AmbryServerReplica(clusterMapConfig1, partition1, disk1, true, MIN_REPLICA_CAPACITY_IN_BYTES, false);
    mockClusterManagerCallback.addReplicaToPartition(partition1, replica5);

    sealedStateChangeCounter.incrementAndGet();

    assertEquals(replica1.getDiskId().getMountPath(), replica1.getMountPath());
    List<AmbryReplica> peerReplicas = replica1.getPeerReplicaIds();
    assertEquals(2, peerReplicas.size());
    assertEquals(replica1.getPartitionId(), peerReplicas.get(0).getPartitionId());
    assertEquals(replica3, peerReplicas.get(0));
    assertEquals(replica5, peerReplicas.get(1));
    assertTrue("Replica should be in stopped state", peerReplicas.get(1).isDown());

    List<AmbryReplica> replicaList1 = partition1.getReplicaIds();
    List<AmbryReplica> replicaList2 = partition2.getReplicaIds();
    assertEquals("Mismatch in number of replicas. Found: " + replicaList1.toString(), 3, replicaList1.size());
    assertTrue(replicaList1.contains(replica1));
    assertTrue(replicaList1.contains(replica3));
    assertTrue(replicaList1.contains(replica5));
    assertEquals(2, replicaList2.size());
    assertTrue(replicaList2.contains(replica2));
    assertTrue(replicaList2.contains(replica4));

    assertEquals(partition1.getPartitionState(), PartitionState.READ_WRITE);
    assertEquals(partition2.getPartitionState(), PartitionState.READ_ONLY);
    replica1.setSealedState(true);
    sealedStateChangeCounter.incrementAndGet();
    assertEquals(partition1.getPartitionState(), PartitionState.READ_ONLY);
    replica3.setSealedState(true);
    sealedStateChangeCounter.incrementAndGet();
    assertEquals(partition1.getPartitionState(), PartitionState.READ_ONLY);
    replica1.setSealedState(false);
    sealedStateChangeCounter.incrementAndGet();
    assertEquals(partition1.getPartitionState(), PartitionState.READ_ONLY);
    replica3.setSealedState(false);
    sealedStateChangeCounter.incrementAndGet();
    assertEquals(partition1.getPartitionState(), PartitionState.READ_WRITE);
    replica4.setSealedState(false);
    sealedStateChangeCounter.incrementAndGet();
    assertEquals(partition2.getPartitionState(), PartitionState.READ_WRITE);

    // the snapshot is for debug info, but just check that it works without throwing exceptions.
    assertNotNull(replica1.getSnapshot());
  }

  @Test
  public void cloudServiceClusterManagerComponentsTest() throws Exception {
    MockClusterManagerCallback mockClusterManagerCallback = new MockClusterManagerCallback();
    AmbryPartition partition = new AmbryPartition(1, "abc", mockClusterManagerCallback);

    String dcName = "cloud-dc";
    CloudServiceDataNode node = new CloudServiceDataNode(dcName, clusterMapConfig1);
    assertEquals(dcName, node.getHostname());
    assertEquals(dcName, node.getDatacenterName());
    assertEquals(DataNodeId.UNKNOWN_PORT, node.getPort());
    assertEquals(new Port(DataNodeId.UNKNOWN_PORT, PortType.PLAINTEXT), node.getPortToConnectTo());
    assertNull(node.getRackId());
    assertEquals(DEFAULT_XID, node.getXid());
    // the snapshot is for debug info, but just check that it works without throwing exceptions.
    assertNotNull(node.getSnapshot());

    CloudServiceReplica replica =
        new CloudServiceReplica(clusterMapConfig1, node, partition, MAX_REPLICA_CAPACITY_IN_BYTES);

    assertException(UnsupportedOperationException.class, replica::getDiskId, null);
    assertEquals(node, replica.getDataNodeId());
    assertException(UnsupportedOperationException.class, replica::getMountPath, null);
    assertException(UnsupportedOperationException.class, replica::getReplicaPath, null);
    assertEquals(ReplicaType.CLOUD_BACKED, replica.getReplicaType());
    assertNotNull(replica.getSnapshot());
    assertNotNull(replica.toString());
    assertException(UnsupportedOperationException.class, replica::markDiskDown, null);
    assertException(UnsupportedOperationException.class, replica::markDiskUp, null);
  }

  /**
   * A helper class that mocks the {@link ClusterManagerCallback} and stores partition to replicas mapping internally
   * as told.
   */
  private class MockClusterManagerCallback
      implements ClusterManagerCallback<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> {
    Map<AmbryPartition, List<AmbryReplica>> partitionToReplicas = new HashMap<>();
    Map<AmbryDataNode, Set<AmbryDisk>> dataNodeToDisks = new HashMap<>();

    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      return new ArrayList<>(partitionToReplicas.get(partition));
    }

    @Override
    public List<AmbryReplica> getReplicaIdsByState(AmbryPartition partition, ReplicaState state, String dcName) {
      throw new UnsupportedOperationException("Temporarily unsupported");
    }

    @Override
    public Collection<AmbryDisk> getDisks(AmbryDataNode dataNode) {
      if (dataNode != null) {
        return dataNodeToDisks.getOrDefault(dataNode, Collections.emptySet());
      }
      List<AmbryDisk> disksToReturn = new ArrayList<>();
      for (Set<AmbryDisk> disks : dataNodeToDisks.values()) {
        disksToReturn.addAll(disks);
      }
      return disksToReturn;
    }

    @Override
    public long getSealedStateChangeCounter() {
      return sealedStateChangeCounter.get();
    }

    @Override
    public Collection<AmbryPartition> getPartitions() {
      return partitionToReplicas.keySet();
    }

    /**
     * Associate the replica with the given partition.
     * @param partition the {@link AmbryPartition}.
     * @param replica the {@link AmbryReplica}.
     */
    void addReplicaToPartition(AmbryPartition partition, AmbryReplica replica) {
      if (!partitionToReplicas.containsKey(partition)) {
        partitionToReplicas.put(partition, new ArrayList<>());
      }
      partitionToReplicas.get(partition).add(replica);
    }
  }
}

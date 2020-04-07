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
package com.github.ambry.replication;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockClusterSpectator;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;

import static com.github.ambry.clustermap.CloudReplica.*;
import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link CloudToStoreReplicationManager} when adding/removing cloud replica. The tests also cover the case
 * where new replica is added due to "move replica".
 */
public class CloudToStoreReplicationManagerTest {
  private static final String NEW_PARTITION_NAME = "12";
  private static final String CLOUD_DC_NAME = "CloudDc";
  private static final String VCR_MOUNT_PATH = CLOUD_REPLICA_MOUNT + "/1";
  private static final String VCR_REPLICA_THREAD_PREFIX = "VcrReplicaThread-";
  private final VerifiableProperties verifiableProperties;
  private final ScheduledExecutorService mockScheduler;
  private final StoreKeyFactory storeKeyFactory;
  private final ClusterMapConfig clusterMapConfig;
  private final ReplicationConfig replicationConfig;
  private final ServerConfig serverConfig;
  private final StoreConfig storeConfig;
  private final MockStoreKeyConverterFactory storeKeyConverterFactory;
  private final MockDataNodeId vcrNode;
  private final DataNodeId currentNode;
  private final MockHelixParticipant mockHelixParticipant;
  private final MockClusterSpectator mockClusterSpectator;
  private final MockClusterMap clusterMap;

  public CloudToStoreReplicationManagerTest() throws Exception {
    List<TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new TestUtils.ZkInfo(null, "DC1", (byte) 0, 2299, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    storeKeyConverterFactory = new MockStoreKeyConverterFactory(null, null);
    storeKeyConverterFactory.setConversionMap(new HashMap<>());
    mockScheduler = Mockito.mock(ScheduledExecutorService.class);
    storeKeyFactory = new StoreKeyFactory() {
      @Override
      public StoreKey getStoreKey(DataInputStream stream) throws IOException {
        return null;
      }

      @Override
      public StoreKey getStoreKey(String input) throws IOException {
        return null;
      }
    };
    // create vcr node
    List<Port> vcrPortList = Arrays.asList(new Port(12310, PortType.PLAINTEXT), new Port(12410, PortType.SSL));
    vcrNode = new MockDataNodeId("localhost", vcrPortList, Collections.singletonList(VCR_MOUNT_PATH), CLOUD_DC_NAME);
    clusterMap = new MockClusterMap();
    currentNode = clusterMap.getDataNodeIds().get(0);
    mockClusterSpectator = new MockClusterSpectator(Collections.singletonList(vcrNode));
    long replicaCapacity = clusterMap.getAllPartitionIds(null).get(0).getReplicaIds().get(0).getCapacityInBytes();
    Properties properties = new Properties();
    properties.setProperty("store.segment.size.in.bytes", Long.toString(replicaCapacity / 2L));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "DC1");
    properties.setProperty("clustermap.vcr.datacenter.name", "DC1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    properties.setProperty("replication.cloud.token.factory", "com.github.ambry.replication.MockFindTokenFactory");
    properties.setProperty("disk.manager.enable.segment.pooling", "true");
    verifiableProperties = new VerifiableProperties(properties);
    storeConfig = new StoreConfig(verifiableProperties);
    clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    replicationConfig = new ReplicationConfig(verifiableProperties);
    serverConfig = new ServerConfig(verifiableProperties);
    MockHelixParticipant.metricRegistry = new MetricRegistry();
    mockHelixParticipant = new MockHelixParticipant(clusterMapConfig);
  }

  /**
   * Test both success and failure cases when adding cloud replica
   * @throws Exception
   */
  @Test
  public void cloudReplicaAdditionTest() throws Exception {
    StorageManager storageManager =
        new StorageManager(storeConfig, new DiskManagerConfig(verifiableProperties), Utils.newScheduler(1, true),
            clusterMap.getMetricRegistry(), null, clusterMap, currentNode, null, mockHelixParticipant, new MockTime(),
            null);
    CloudToStoreReplicationManager cloudToStoreReplicationManager =
        new CloudToStoreReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager,
            storeKeyFactory, clusterMap, mockScheduler, currentNode, null, clusterMap.getMetricRegistry(), null,
            storeKeyConverterFactory, serverConfig.serverMessageTransformer, mockClusterSpectator,
            mockHelixParticipant);
    storageManager.start();
    cloudToStoreReplicationManager.start();
    mockClusterSpectator.spectate();
    // 1. test adding cloud replica that is not present locally
    mockHelixParticipant.onPartitionBecomeLeaderFromStandby(NEW_PARTITION_NAME);
    assertNull("Cloud replica thread should not be created", TestUtils.getThreadByThisName(VCR_REPLICA_THREAD_PREFIX));
    // create a new partition and add corresponding store in storage manager
    PartitionId newPartition =
        new MockPartitionId(Long.parseLong(NEW_PARTITION_NAME), MockClusterMap.DEFAULT_PARTITION_CLASS,
            clusterMap.getDataNodes(), 0);
    ReplicaId replicaToAdd = newPartition.getReplicaIds().get(0);
    assertTrue("Adding new store should succeed", storageManager.addBlobStore(replicaToAdd));
    // 2. we deliberately shut down the store to induce failure when adding cloud replica
    storageManager.shutdownBlobStore(newPartition);
    mockHelixParticipant.onPartitionBecomeLeaderFromStandby(NEW_PARTITION_NAME);
    assertNull("Cloud replica thread should not be created", TestUtils.getThreadByThisName(VCR_REPLICA_THREAD_PREFIX));
    storageManager.startBlobStore(newPartition);
    // 3. mock success case
    mockHelixParticipant.onPartitionBecomeLeaderFromStandby(NEW_PARTITION_NAME);
    assertNotNull("Cloud replica thread should be created for DC1",
        TestUtils.getThreadByThisName(VCR_REPLICA_THREAD_PREFIX));
    cloudToStoreReplicationManager.shutdown();
    storageManager.shutdown();
  }

  /**
   * Test both success and failure cases when removing cloud replica.
   * @throws Exception
   */
  @Test
  public void cloudReplicaRemovalTest() throws Exception {
    StorageManager storageManager =
        new StorageManager(storeConfig, new DiskManagerConfig(verifiableProperties), Utils.newScheduler(1, true),
            clusterMap.getMetricRegistry(), null, clusterMap, currentNode, null, mockHelixParticipant, new MockTime(),
            null);
    CloudToStoreReplicationManager cloudToStoreReplicationManager =
        new CloudToStoreReplicationManager(replicationConfig, clusterMapConfig, storeConfig, storageManager,
            storeKeyFactory, clusterMap, mockScheduler, currentNode, null, clusterMap.getMetricRegistry(), null,
            storeKeyConverterFactory, serverConfig.serverMessageTransformer, mockClusterSpectator,
            mockHelixParticipant);
    storageManager.start();
    cloudToStoreReplicationManager.start();
    mockClusterSpectator.spectate();
    PartitionId localPartition = storageManager.getLocalPartitions().iterator().next();
    // 1. add cloud replica first for subsequent removal test
    mockHelixParticipant.onPartitionBecomeLeaderFromStandby(localPartition.toPathString());
    String replicaPath = Cloud_Replica_Keyword + File.separator + localPartition.toPathString() + File.separator
        + localPartition.toPathString();
    RemoteReplicaInfo remoteReplicaInfo =
        cloudToStoreReplicationManager.getRemoteReplicaInfo(localPartition, vcrNode.getHostname(), replicaPath);
    assertNotNull("Remote replica info should not be null", remoteReplicaInfo);
    assertEquals("There should be only one cloud replica thread created", 1,
        TestUtils.getAllThreadsByThisName(VCR_REPLICA_THREAD_PREFIX).size());

    // 2. before removing cloud replica of local partition let's remove a non-existent partition first
    mockHelixParticipant.onPartitionBecomeStandbyFromLeader(NEW_PARTITION_NAME);
    // ensure there is no change in replica thread
    assertEquals("There should be only one cloud replica thread created", 1,
        TestUtils.getAllThreadsByThisName(VCR_REPLICA_THREAD_PREFIX).size());

    // 3. remove the cloud replica by calling Leader-To-Standby transition on local partition
    mockHelixParticipant.onPartitionBecomeStandbyFromLeader(localPartition.toPathString());
    // ensure that the remote replica info has been successfully removed from replica thread
    assertNull("Cloud replica should be removed and no thread is assigned to it", remoteReplicaInfo.getReplicaThread());
    cloudToStoreReplicationManager.shutdown();
    storageManager.shutdown();
  }
}

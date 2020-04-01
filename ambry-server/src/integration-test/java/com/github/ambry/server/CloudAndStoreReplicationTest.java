/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.cloud.VcrServer;
import com.github.ambry.cloud.VcrTestUtil;
import com.github.ambry.clustermap.ClusterMapSnapshotConstants;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterAgentsFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Test for replication from cloud to store and store to store.
 */
@RunWith(Parameterized.class)
public class CloudAndStoreReplicationTest {
  private Properties recoveryProperties;
  private MockCluster recoveryCluster;
  private MockDataNodeId partitionLeaderRecoveryNode;
  private List<MockDataNodeId> allRecoveryNodes;
  private VcrServer vcrServer;
  private HelixControllerManager helixControllerManager;
  private TestUtils.ZkInfo zkInfo;
  private PartitionId partitionId;
  private LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination;
  private List<BlobId> cloudBlobIds;
  private List<BlobId> serverBlobIds;
  private final short accountId = Utils.getRandomShort(TestUtils.RANDOM);
  private final short containerId = Utils.getRandomShort(TestUtils.RANDOM);
  private final static int FOUR_MB_SZ = 4194304;
  private final String vcrRecoveryPartitionConfig;

  /**
   * Parameterized constructor.
   * @param vcrRecoveryPartitionConfig vcrRecoveryPartitionConfig value.
   */
  public CloudAndStoreReplicationTest(String vcrRecoveryPartitionConfig) {
    this.vcrRecoveryPartitionConfig = vcrRecoveryPartitionConfig;
  }

  /**
   * static method to generate parameters.
   * @return {@link Collection} of parameters.
   */
  @Parameterized.Parameters
  public static List<Object[]> input() {
    return Arrays.asList(new Object[][]{{"0"}, {""}});
  }

  /**
   * Create a cluster with one vcr node and two ambry server data nodes.
   * @throws Exception on {@link Exception}
   */
  @Before
  public void setup() throws Exception {
    String cloudDc = "CloudDc";
    String vcrMountPath = ClusterMapSnapshotConstants.CLOUD_REPLICA_MOUNT + "/1";
    recoveryProperties = new Properties();
    recoveryProperties.setProperty("replication.metadata.request.version", "2");
    recoveryProperties.setProperty("replication.enabled.with.vcr.cluster", "true");
    recoveryProperties.setProperty("clustermap.vcr.datacenter.name", cloudDc);
    if (!vcrRecoveryPartitionConfig.isEmpty()) {
      recoveryProperties.setProperty("vcr.recovery.partitions", vcrRecoveryPartitionConfig);
    }
    TestSSLUtils.addHttp2Properties(recoveryProperties, SSLFactory.Mode.SERVER, true);

    // create vcr node
    List<Port> vcrPortList = Arrays.asList(new Port(12310, PortType.PLAINTEXT), new Port(12410, PortType.SSL));

    MockDataNodeId vcrNode =
        new MockDataNodeId("localhost", vcrPortList, Collections.singletonList(vcrMountPath), cloudDc);

    // create ambry server recovery cluster
    MockClusterMap serverClusterMap = new MockClusterMap(false, 2, 1, 1, true, false);
    recoveryCluster = new MockCluster(serverClusterMap, Collections.singletonList(vcrNode), recoveryProperties);
    partitionId = recoveryCluster.getClusterMap().getWritablePartitionIds(null).get(0);
    allRecoveryNodes = serverClusterMap.getDataNodes();

    // record ambry server node which will get partition leadership notification.
    partitionLeaderRecoveryNode = allRecoveryNodes.get(0);
    MockClusterAgentsFactory leaderMockClusterAgentsFactory = new MockClusterAgentsFactory(serverClusterMap,
        serverClusterMap.getAllPartitionIds(null).stream().map(PartitionId::toPathString).collect(Collectors.toList()));

    // Start Helix Controller and ZK Server.
    int zkPort = 31999;
    String zkConnectString = "localhost:" + zkPort;
    String vcrClusterName = "vcrTestCluster";
    zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), cloudDc, (byte) 1, zkPort, true);
    helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, recoveryCluster.getClusterMap());

    Properties vcrProperties =
        VcrTestUtil.createVcrProperties(vcrNode.getDatacenterName(), "vcrTestCluster", zkConnectString, 12310, 12410,
            null);
    vcrProperties.putAll(recoveryProperties);
    MockNotificationSystem notificationSystem = new MockNotificationSystem(recoveryCluster.getClusterMap());

    // Create blobs and data for upload to vcr.
    int blobCount = 10;
    cloudBlobIds =
        ServerTestUtil.createBlobIds(blobCount, recoveryCluster.getClusterMap(), accountId, containerId, partitionId);
    serverBlobIds =
        ServerTestUtil.createBlobIds(blobCount, recoveryCluster.getClusterMap(), accountId, containerId, partitionId);

    // Create cloud destination and start vcr server.
    latchBasedInMemoryCloudDestination = new LatchBasedInMemoryCloudDestination(cloudBlobIds);
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(vcrProperties), recoveryCluster.getClusterAgentsFactory(),
            notificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    // initialize and start ambry servers
    for (MockDataNodeId serverNode : allRecoveryNodes) {
      recoveryCluster.initializeServer(serverNode, recoveryProperties, false, true, notificationSystem,
          SystemTime.getInstance(),
          serverNode.equals(partitionLeaderRecoveryNode) ? leaderMockClusterAgentsFactory : null);
    }

    recoveryCluster.startServers();
  }

  /**
   * Shutdown the cluster.
   * @throws IOException on {@link IOException}
   */
  @After
  public void cleanUp() throws IOException {
    recoveryCluster.stopServers();
    vcrServer.shutdown();
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  /**
   * Test replication from vcr to server nodes, and from server to server nodes. Creates one vcr node and two server nodes.
   * Uploads data to vcr node and verifies that they have been replicated.
   * Uploads data to one of the server nodes and verifies that they have been replicated.
   * @throws Exception If an exception happens.
   */
  @Test
  public void basicCloudRecoveryTest() throws Exception {
    // Create blobs and upload to cloud destination.
    Map<BlobId, Integer> blobIdToSizeMap = new HashMap<>();
    for (BlobId blobId : cloudBlobIds) {
      int blobSize = Utils.getRandomShort(TestUtils.RANDOM);
      PutMessageFormatInputStream putMessageFormatInputStream =
          ServerTestUtil.getPutMessageInputStreamForBlob(blobId, blobSize, blobIdToSizeMap, accountId, containerId);
      long time = System.currentTimeMillis();
      CloudBlobMetadata cloudBlobMetadata =
          new CloudBlobMetadata(blobId, time, Utils.Infinite_Time, putMessageFormatInputStream.getSize(),
              CloudBlobMetadata.EncryptionOrigin.NONE);
      latchBasedInMemoryCloudDestination.uploadBlob(blobId, putMessageFormatInputStream.getSize(), cloudBlobMetadata,
          putMessageFormatInputStream);
    }

    // Create blobs and upload to one of the server nodes.
    sendBlobToDataNode(partitionLeaderRecoveryNode, Utils.getRandomShort(TestUtils.RANDOM), blobIdToSizeMap);

    // Waiting for download attempt
    assertTrue("Did not recover all blobs in 1 minute",
        latchBasedInMemoryCloudDestination.awaitDownload(1, TimeUnit.MINUTES));

    // Waiting for replication to complete
    Thread.sleep(10000);

    // Test cloud to store and store to store replication by sending get request to server nodes.
    testGetOnServerNodes(blobIdToSizeMap);
  }

  /**
   * Test replication from vcr to server nodes, and from server to server nodes for large blobs.
   * Creates one vcr node and two server nodes.
   * Uploads data to vcr node and verifies that they have been replicated.
   * Uploads data to one of the server nodes and verifies that they have been replicated.
   * @throws Exception If an exception happens.
   */
  @Test
  public void cloudRecoveryTestForLargeBlob() throws Exception {
    // Create blobs and upload to cloud destination.
    Map<BlobId, Integer> blobIdToSizeMap = new HashMap<>();
    int blobSize = FOUR_MB_SZ; // Currently ambry supports max size of 4MB for blobs.
    for (BlobId blobId : cloudBlobIds) {
      PutMessageFormatInputStream putMessageFormatInputStream =
          ServerTestUtil.getPutMessageInputStreamForBlob(blobId, blobSize, blobIdToSizeMap, accountId, containerId);
      long time = System.currentTimeMillis();
      CloudBlobMetadata cloudBlobMetadata =
          new CloudBlobMetadata(blobId, time, Utils.Infinite_Time, putMessageFormatInputStream.getSize(),
              CloudBlobMetadata.EncryptionOrigin.NONE);
      latchBasedInMemoryCloudDestination.uploadBlob(blobId, putMessageFormatInputStream.getSize(), cloudBlobMetadata,
          putMessageFormatInputStream);
    }

    // Create blobs and upload to one of the server nodes.
    sendBlobToDataNode(partitionLeaderRecoveryNode, blobSize, blobIdToSizeMap);

    // Waiting for download attempt
    assertTrue("Did not recover all blobs in 1 minute",
        latchBasedInMemoryCloudDestination.awaitDownload(1, TimeUnit.MINUTES));

    // Waiting for replication to complete
    Thread.sleep(10000);

    // Test cloud to store and store to store replication by sending get request to server nodes.
    testGetOnServerNodes(blobIdToSizeMap);
  }

  /**
   * Do a get on recovery server node to test that all the blobids that were uploaded to vcr node have been recovered on
   * recovery node.
   * @param blobIdToSizeMap {@link Map} of blobid to size uploaded to vcr node.
   * @param node recovery server node
   * @throws IOException on {@link IOException}
   */
  private void testGetOnServerNode(Map<BlobId, Integer> blobIdToSizeMap, DataNodeId node) throws IOException {
    ConnectedChannel channel =
        ServerTestUtil.getBlockingChannelBasedOnPortType(node.getPortToConnectTo(), node.getHostname(), null, null);
    channel.connect();

    AtomicInteger correlationIdGenerator = new AtomicInteger(0);
    List<BlobId> allBlobIds = Stream.concat(cloudBlobIds.stream(), serverBlobIds.stream()).collect(Collectors.toList());
    List<PartitionRequestInfo> partitionRequestInfoList =
        Collections.singletonList(new PartitionRequestInfo(partitionId, allBlobIds));
    GetRequest getRequest = new GetRequest(correlationIdGenerator.incrementAndGet(),
        GetRequest.Replication_Client_Id_Prefix + node.getHostname(), MessageFormatFlags.All, partitionRequestInfoList,
        new ReplicationConfig(new VerifiableProperties(recoveryProperties)).replicationIncludeAll
            ? GetOption.Include_All : GetOption.None);

    channel.send(getRequest);
    GetResponse getResponse =
        GetResponse.readFrom(new DataInputStream(channel.receive().getInputStream()), recoveryCluster.getClusterMap());

    for (PartitionResponseInfo partitionResponseInfo : getResponse.getPartitionResponseInfoList()) {
      assertEquals("Error in getting the recovered blobs", ServerErrorCode.No_Error,
          partitionResponseInfo.getErrorCode());
      for (MessageInfo messageInfo : partitionResponseInfo.getMessageInfoList()) {
        assertEquals(blobIdToSizeMap.get(messageInfo.getStoreKey()) + 272, messageInfo.getSize());
      }
    }
  }

  /**
   * Do a get on all server nodes to test that all the blobids that were uploaded to vcr node have been recovered
   * on all server nodes.
   * @param blobIdToSizeMap {@link Map} of blobid to size uploaded to vcr node.
   * @throws IOException on {@link IOException}
   */
  private void testGetOnServerNodes(Map<BlobId, Integer> blobIdToSizeMap) throws IOException {
    for (DataNodeId dataNodeId : allRecoveryNodes) {
      testGetOnServerNode(blobIdToSizeMap, dataNodeId);
    }
  }

  /**
   * Send blobs to given server dataNode and update {@code blobIdToSizeMap}.
   * @param dataNode the target node.
   * @param blobSize size of blobs to send.
   * @param blobIdToSizeMap {@link Map} of {@link BlobId} to size of blob uploaded.
   */
  private void sendBlobToDataNode(DataNodeId dataNode, int blobSize, Map<BlobId, Integer> blobIdToSizeMap)
      throws Exception {
    int userMetaDataSize = 100;
    // Send blobs to DataNode
    byte[] userMetadata = new byte[userMetaDataSize];
    byte[] data = new byte[blobSize];
    BlobProperties properties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, -1, accountId, containerId, false, null);
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);

    Port port = new Port(dataNode.getPort(), PortType.PLAINTEXT);
    ConnectedChannel channel =
        ServerTestUtil.getBlockingChannelBasedOnPortType(port, dataNode.getHostname(), null, null);
    channel.connect();
    CountDownLatch latch = new CountDownLatch(1);
    DirectSender runnable = new DirectSender(channel, serverBlobIds, data, userMetadata, properties, null, latch);
    Thread threadToRun = new Thread(runnable);
    threadToRun.start();
    assertTrue("Did not put all blobs in 2 minutes", latch.await(2, TimeUnit.MINUTES));
    List<BlobId> blobIds = runnable.getBlobIds();
    for (BlobId blobId : blobIds) {
      blobIdToSizeMap.put(blobId, blobSize);
    }
  }
}

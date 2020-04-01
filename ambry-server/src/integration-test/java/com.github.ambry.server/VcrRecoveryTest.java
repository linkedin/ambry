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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for recovery from cloud node to disk based data node.
 */
public class VcrRecoveryTest {
  private Properties recoveryProperties;
  private MockCluster recoveryCluster;
  private Port recoveryNodePort;
  private MockDataNodeId recoveryNode;
  private VcrServer vcrServer;
  private HelixControllerManager helixControllerManager;
  private TestUtils.ZkInfo zkInfo;
  private PartitionId partitionId;
  private LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination;
  private List<BlobId> blobIds;
  final private short accountId = Utils.getRandomShort(TestUtils.RANDOM);
  final private short containerId = Utils.getRandomShort(TestUtils.RANDOM);
  final private static int FOUR_MB_SZ = 4194304;

  /**
   * Create a cluster with a vcr node and a recovery (ambry data) node.
   * @throws Exception on {@link Exception}
   */
  @Before
  public void setup() throws Exception {
    String dcName = "DC1";
    String vcrMountPath = ClusterMapSnapshotConstants.CLOUD_REPLICA_MOUNT + "/1";
    recoveryProperties = new Properties();
    recoveryProperties.setProperty("replication.metadata.request.version", "2");

    // create vcr node
    List<Port> vcrPortList = new ArrayList<>(2);
    Port vcrClusterMapPort = new Port(12310, PortType.PLAINTEXT);
    Port vcrSslPort = new Port(12410, PortType.SSL);
    vcrPortList.add(vcrClusterMapPort);
    vcrPortList.add(vcrSslPort);

    MockDataNodeId vcrNode =
        new MockDataNodeId("localhost", vcrPortList, Collections.singletonList(vcrMountPath), dcName);

    // create recovery node
    recoveryNodePort = new Port(12311, PortType.PLAINTEXT);
    ArrayList<Port> recoveryPortList = new ArrayList<>(2);
    recoveryPortList.add(recoveryNodePort);
    recoveryNode = MockClusterMap.createDataNode(recoveryPortList, dcName, 1);

    // create cluster for recovery
    recoveryCluster = MockCluster.createOneNodeRecoveryCluster(vcrNode, recoveryNode, dcName);
    partitionId = recoveryCluster.getClusterMap().getWritablePartitionIds(null).get(0);

    // Start Helix Controller and ZK Server.
    int zkPort = 31999;
    String zkConnectString = "localhost:" + zkPort;
    String vcrClusterName = "vcrTestCluster";
    zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), dcName, (byte) 1, zkPort, true);
    helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, recoveryCluster.getClusterMap());

    Properties vcrProperties =
        VcrTestUtil.createVcrProperties(vcrNode.getDatacenterName(), "vcrTestCluster", zkConnectString, 12310, 12410,
            null);
    vcrProperties.putAll(recoveryProperties);
    NotificationSystem notificationSystem = new MockNotificationSystem(recoveryCluster.getClusterMap());

    // Create blobs and data for upload to vcr.
    int blobCount = 10;
    blobIds =
        ServerTestUtil.createBlobIds(blobCount, recoveryCluster.getClusterMap(), accountId, containerId, partitionId);

    // Create cloud destination and start vcr server.
    latchBasedInMemoryCloudDestination = new LatchBasedInMemoryCloudDestination(blobIds);
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(vcrProperties), recoveryCluster.getClusterAgentsFactory(),
            notificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    // start ambry server with data node
    recoveryCluster.initializeServers(notificationSystem, vcrNode, recoveryProperties);
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
   * Do a get on recovery node to test that all the blobids that were uploaded to vcr node have been recovered on recovery node.
   * @param blobIdToSizeMap {@link Map} of blobid to size uploaded to vcr node.
   * @throws IOException on {@link IOException}
   */
  private void testGetOnRecoveryNode(Map<BlobId, Integer> blobIdToSizeMap) throws IOException {
    ConnectedChannel channel =
        ServerTestUtil.getBlockingChannelBasedOnPortType(recoveryNodePort, "localhost", null, null);
    channel.connect();

    AtomicInteger correlationIdGenerator = new AtomicInteger(0);
    List<PartitionRequestInfo> partitionRequestInfoList =
        Collections.singletonList(new PartitionRequestInfo(partitionId, blobIds));
    GetRequest getRequest = new GetRequest(correlationIdGenerator.incrementAndGet(),
        GetRequest.Replication_Client_Id_Prefix + recoveryNode.getHostname(), MessageFormatFlags.All,
        partitionRequestInfoList,
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
   * Test recovery from one vcr node to one disk based data node.
   * Creates a vcr node and a disk based data node. Uploads data to vcr node and verifies that they have been downloaded.
   * @throws Exception If an exception happens.
   */
  @Test
  public void basicCloudRecoveryTest() throws Exception {
    // Create blobs and upload to cloud destination.
    int userMetaDataSize = 100;
    byte[] userMetadata = new byte[userMetaDataSize];
    TestUtils.RANDOM.nextBytes(userMetadata);
    Map<BlobId, Integer> blobIdToSizeMap = new HashMap<>();
    for (BlobId blobId : blobIds) {
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

    // Waiting for download attempt
    assertTrue("Did not recover all blobs in 1 minute",
        latchBasedInMemoryCloudDestination.awaitDownload(1, TimeUnit.MINUTES));

    // Waiting for replication to complete
    Thread.sleep(10000);

    // Test recovery by sending get request to recovery node
    testGetOnRecoveryNode(blobIdToSizeMap);
  }

  /**
   * Test recovery from one vcr node to one disk based data node for large blobs.
   * Creates a vcr node and a disk based data node. Uploads data to vcr node and verifies that they have been downloaded.
   * @throws Exception If an exception happens.
   */
  @Test
  public void cloudRecoveryTestForLargeBlob() throws Exception {
    // Create blobs and upload to cloud destination.
    int userMetaDataSize = 100;
    byte[] userMetadata = new byte[userMetaDataSize];
    TestUtils.RANDOM.nextBytes(userMetadata);
    Map<BlobId, Integer> blobIdToSizeMap = new HashMap<>();
    int blobSize = FOUR_MB_SZ; // Currently ambry supports max size of 4MB for blobs.
    for (BlobId blobId : blobIds) {
      PutMessageFormatInputStream putMessageFormatInputStream =
          ServerTestUtil.getPutMessageInputStreamForBlob(blobId, blobSize, blobIdToSizeMap, accountId, containerId);
      long time = System.currentTimeMillis();
      CloudBlobMetadata cloudBlobMetadata =
          new CloudBlobMetadata(blobId, time, Utils.Infinite_Time, putMessageFormatInputStream.getSize(),
              CloudBlobMetadata.EncryptionOrigin.NONE);
      latchBasedInMemoryCloudDestination.uploadBlob(blobId, putMessageFormatInputStream.getSize(), cloudBlobMetadata,
          putMessageFormatInputStream);
    }

    // Waiting for download attempt
    assertTrue("Did not recover all blobs in 1 minute",
        latchBasedInMemoryCloudDestination.awaitDownload(1, TimeUnit.MINUTES));

    // Waiting for replication to complete
    Thread.sleep(10000);

    // Test recovery by sending get request to recovery node
    testGetOnRecoveryNode(blobIdToSizeMap);
  }
}

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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for recovery from cloud node to disk based data node.
 */
public class VcrRecoveryTest {

  /**
   * Test recovery from one vcr node to one disk based data node.
   * Creates a vcr node and a disk based data node. Uploads data to vcr node and verifies that they have been downloaded.
   * @throws Exception If an exception happens.
   */
  @Test
  public void cloudRecoveryTest() throws Exception {
    String dcName = "DC1";
    Properties recoveryProperties = new Properties();
    recoveryProperties.setProperty("replication.metadatarequest.version", "2");

    // create vcr node
    Port vcrClusterMapPort = new Port(12310, PortType.PLAINTEXT);
    Port vcrSslPort = new Port(12410, PortType.SSL);
    List<Port> vcrPortList = new ArrayList<>(2);
    vcrPortList.add(vcrClusterMapPort);
    vcrPortList.add(vcrSslPort);
    String vcrMountPath = "/vcr/1";
    MockDataNodeId vcrNode =
        new MockDataNodeId("localhost", vcrPortList, Collections.singletonList(vcrMountPath), dcName, true);

    // create data node
    Port recoveryClusterMapPort = new Port(12311, PortType.PLAINTEXT);
    ArrayList<Port> recoveryPortList = new ArrayList<>(2);
    recoveryPortList.add(recoveryClusterMapPort);
    MockDataNodeId recoveryNode = MockClusterMap.createDataNode(recoveryPortList, dcName, 1);

    //create cluster for recovery
    MockCluster recoveryCluster = MockCluster.createOneNodeRecoveryCluster(vcrNode, recoveryNode, dcName);

    // Start Helix Controller and ZK Server.
    int zkPort = 31999;
    String zkConnectString = "localhost:" + zkPort;
    String vcrClusterName = "vcrTestCluster";
    TestUtils.ZkInfo zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), dcName, (byte) 1, zkPort, true);
    HelixControllerManager helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, recoveryCluster.getClusterMap());

    Properties props =
        VcrTestUtil.createVcrProperties(vcrNode.getDatacenterName(), "vcrTestCluster", zkConnectString, 12310, 12410,
            null);
    props.putAll(recoveryProperties);

    // Create blobs and data for upload to vcr.
    int blobCount = 10;
    int blobSize = 100;
    int userMetaDataSize = 100;
    byte[] userMetadata = new byte[userMetaDataSize];
    byte[] data = new byte[blobSize];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, Utils.Infinite_Time, accountId, containerId,
            false, null);
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);

    List<BlobId> blobIds = new ArrayList<>(blobCount);
    PartitionId partitionId = recoveryCluster.getClusterMap().getWritablePartitionIds(null).get(0);

    for (int i = 0; i < blobCount; i++) {
      BlobId blobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
          recoveryCluster.getClusterMap().getLocalDatacenterId(), blobProperties.getAccountId(),
          blobProperties.getContainerId(), partitionId, false, BlobId.BlobDataType.DATACHUNK);
      blobIds.add(blobId);
    }

    // Create cloud destination and start vcr server.
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(blobIds);
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);

    NotificationSystem notificationSystem = new MockNotificationSystem(recoveryCluster.getClusterMap());
    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), recoveryCluster.getClusterAgentsFactory(),
            notificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    // Upload created blobs to cloud destination.
    for (BlobId blobId : blobIds) {
      long time = System.currentTimeMillis();
      CloudBlobMetadata cloudBlobMetadata =
          new CloudBlobMetadata(blobId, time, Utils.Infinite_Time, blobSize, null, null, null);
      latchBasedInMemoryCloudDestination.uploadBlob(blobId, blobSize, cloudBlobMetadata,
          new ByteArrayInputStream(data));
    }

    // start ambry server with data node
    recoveryCluster.initializeServers(notificationSystem, vcrNode, recoveryProperties);
    recoveryCluster.startServers();

    //Waiting for recovery
    assertTrue("Did not recover all blobs in 2 minutes",
        latchBasedInMemoryCloudDestination.awaitDownload(2, TimeUnit.MINUTES));

    recoveryCluster.stopServers();
    vcrServer.shutdown();
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }
}

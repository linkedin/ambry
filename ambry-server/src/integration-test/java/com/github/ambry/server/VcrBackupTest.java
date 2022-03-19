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
package com.github.ambry.server;

import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.cloud.LeaderStandbyHelixVcrStateModelFactory;
import com.github.ambry.cloud.OnlineOfflineHelixVcrStateModelFactory;
import com.github.ambry.cloud.VcrServer;
import com.github.ambry.cloud.VcrTestUtil;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.utils.HelixControllerManager;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.tools.ClusterVerifiers.StrictMatchExternalViewVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.server.ServerTestUtil.*;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class VcrBackupTest {
  private static final Logger logger = LoggerFactory.getLogger(VcrBackupTest.class);
  private static TestUtils.ZkInfo zkInfo;
  private static int zkPort = 31996;
  private static String zkConnectString = "localhost:" + zkPort;
  private static String vcrClusterName = "vcrBackupTestCluster";
  private static int clusterMapPort = 12310;
  private final String vcrHelixStateModelFactoryClass;
  private final String vcrStateModelName;
  private MockNotificationSystem notificationSystem;
  private MockCluster mockCluster;
  private HelixControllerManager helixControllerManager;
  private DataNodeId dataNode;
  private int numOfPartitions = 20;
  private int blobSize = 10;
  private File trustStoreFile;
  private Properties serverSSLProps;
  private Properties clientSSLProps;

  /**
   * Constructor for {@link VcrBackupTest}.
   * @param vcrHelixStateModelFactoryClass state model factory class.
   */
  public VcrBackupTest(String vcrHelixStateModelFactoryClass, String vcrStateModelName) {
    this.vcrHelixStateModelFactoryClass = vcrHelixStateModelFactoryClass;
    this.vcrStateModelName = vcrStateModelName;
    clusterMapPort += 1000;
  }

  /**
   * Run for both onlineoffline and leaderstandby state models.
   * @return an array with factory class and state model names for both the state models.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{OnlineOfflineHelixVcrStateModelFactory.class.getName(), OnlineOfflineSMD.name},
        {LeaderStandbyHelixVcrStateModelFactory.class.getName(), LeaderStandbySMD.name}});
  }

  @Before
  public void setup() throws Exception {
    Properties props = new Properties();
    TestSSLUtils.addHttp2Properties(props, SSLFactory.Mode.SERVER, true);
    // VCR use this mockCluster to initiate cluster map.
    props.setProperty("clustermap.enable.http2.replication", "true");
    mockCluster = new MockCluster(props, false, SystemTime.getInstance(), 1, 1, numOfPartitions);
    notificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    mockCluster.initializeServers(notificationSystem);
    mockCluster.startServers();
    dataNode = mockCluster.getClusterMap().getDataNodeIds().get(0);

    // Start ZK Server and Helix Controller
    if (!zkInfo.isZkServerStarted()) {
      zkInfo.startZkServer();
    }
    helixControllerManager =
        VcrTestUtil.populateZkInfoAndStartController(zkConnectString, vcrClusterName, mockCluster.getClusterMap(),
            vcrStateModelName);

    trustStoreFile = File.createTempFile("truststore", ".jks");
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, true);

    clientSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(clientSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile,
        "http2-blocking-channel-client");
    TestSSLUtils.addHttp2Properties(clientSSLProps, SSLFactory.Mode.CLIENT, true);
  }

  @After
  public void cleanup() throws IOException {
    logger.info("Start to clean up.");
    mockCluster.cleanup();
    helixControllerManager.syncStop();
    zkInfo.shutdown();
  }

  /**
   * Basic test to make sure VCR can backup with HelixVcrCluster.
   */
  @Test
  public void basicTest() throws Exception {
    List<BlobId> blobIds = sendBlobToDataNode(dataNode, 10);
    // Start the VCR and CloudBackupManager
    Properties props =
        VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString, clusterMapPort,
            12410, 12510, serverSSLProps, vcrHelixStateModelFactoryClass, true);
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(blobIds, mockCluster.getClusterMap());
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
            notificationSystem, cloudDestinationFactory);
    vcrServer.startup();
    // Waiting for backup done
    assertTrue("Did not backup all blobs in 2 minutes",
        latchBasedInMemoryCloudDestination.awaitUpload(2, TimeUnit.MINUTES));

    // Verify a blob by making a http2 request.
    MockClusterMap clusterMap = mockCluster.getClusterMap();
    SSLConfig clientSSLConfig = new SSLConfig(new VerifiableProperties(clientSSLProps));
    ConnectedChannel channel = ServerTestUtil.getBlockingChannelBasedOnPortType(
        new Port(clusterMap.getDataNodes().get(0).getHttp2Port(), PortType.HTTP2), "localhost", null, clientSSLConfig);
    BlobId blobToVerify = blobIds.get(0);
    ArrayList<BlobId> idList = new ArrayList<>(Arrays.asList(blobToVerify));
    ArrayList<PartitionRequestInfo> partitionRequestInfoList = new ArrayList<PartitionRequestInfo>();
    PartitionRequestInfo partitionRequestInfo = new PartitionRequestInfo(blobToVerify.getPartition(), idList);
    partitionRequestInfoList.add(partitionRequestInfo);
    GetRequest getRequest1 =
        new GetRequest(1, "clientid1", MessageFormatFlags.BlobProperties, partitionRequestInfoList, GetOption.None);
    DataInputStream stream = channel.sendAndReceive(getRequest1).getInputStream();
    GetResponse resp1 = GetResponse.readFrom(stream, clusterMap);
    try {
      BlobProperties propertyOutput = MessageFormatRecord.deserializeBlobProperties(resp1.getInputStream());
      // Do a simple check
      assertEquals(blobSize, propertyOutput.getBlobSize());
      releaseNettyBufUnderneathStream(stream);
    } catch (MessageFormatException e) {
      fail();
    }

    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
  }

  /**
   * Test single VCR up and down without persisted token.
   */
  @Test
  public void singleNodeUpDownTestWithoutPersist() throws Exception {
    StrictMatchExternalViewVerifier helixBalanceVerifier =
        new StrictMatchExternalViewVerifier(zkConnectString, vcrClusterName,
            Collections.singleton(VcrTestUtil.helixResource), null);
    int numberOfBlobs = 20;
    sendBlobToDataNode(dataNode, numberOfBlobs);
    // Create in memory cloud destination.
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(new ArrayList<>(), mockCluster.getClusterMap());
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    // Start the VCR with token persistor off.
    Properties props =
        VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString, clusterMapPort,
            12410, 12510, null, vcrHelixStateModelFactoryClass, true);
    props.setProperty("replication.persist.token.on.shutdown.or.replica.remove", "false");
    MockNotificationSystem vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
            vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();
    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    final MockNotificationSystem vcrNotificationSystemCopy = vcrNotificationSystem;
    assertTrue("Blob count is not correct.",
        TestUtils.checkAndSleep(numberOfBlobs, () -> vcrNotificationSystemCopy.getBlobIds().size(), 400));
    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());
    assertEquals("No token is expected.", 0, latchBasedInMemoryCloudDestination.getTokenMap().size());

    // Start VCR again with same cloud destination
    vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    vcrServer = VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
        vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    // Because same cloud destination is used, getMissingKey() will filter out all keys.
    assertEquals("Number of blobs doesn't match", 0, vcrNotificationSystem.getBlobIds().size());
    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());

    // Start VCR again with different cloud destination
    latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(new ArrayList<>(), mockCluster.getClusterMap());
    cloudDestinationFactory = new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);

    vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    vcrServer = VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
        vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    // Because same cloud destination is not used and no token persisted, everything will be backed again.
    final MockNotificationSystem vcrNotificationSystemCopy2 = vcrNotificationSystem;
    assertTrue("Blob count is not correct.",
        TestUtils.checkAndSleep(numberOfBlobs, () -> vcrNotificationSystemCopy2.getBlobIds().size(), 200));
    vcrServer.shutdown();
    assertTrue("VCR shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());
  }

  /**
   * Test single VCR up and down with persisted token.
   */
  @Test
  public void singleNodeUpDownTestWithPersist() throws Exception {
    StrictMatchExternalViewVerifier helixBalanceVerifier =
        new StrictMatchExternalViewVerifier(zkConnectString, vcrClusterName,
            Collections.singleton(VcrTestUtil.helixResource), null);
    int numberOfBlobs = 20;
    sendBlobToDataNode(dataNode, numberOfBlobs);
    // Create in memory cloud destination.
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(new ArrayList<>(), mockCluster.getClusterMap());
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    // Start the VCR with token persistor on.
    Properties props =
        VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString, clusterMapPort,
            12410, 12510, null, vcrHelixStateModelFactoryClass, true);
    props.setProperty("replication.persist.token.on.shutdown.or.replica.remove", "true");
    MockNotificationSystem vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
            vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();
    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    final MockNotificationSystem vcrNotificationSystemCopy = vcrNotificationSystem;
    assertTrue("Blob count is not correct.",
        TestUtils.checkAndSleep(numberOfBlobs, () -> vcrNotificationSystemCopy.getBlobIds().size(), 400));
    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());
    assertTrue("Token is expected.", latchBasedInMemoryCloudDestination.getTokenMap().size() > 0);

    // Start VCR again with same cloud destination
    vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    vcrServer = VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
        vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    // Because token is reloaded, back up number is 0.
    assertEquals("Number of blobs doesn't match", 0, vcrNotificationSystem.getBlobIds().size());
    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());

    // Start VCR again with token.
    LatchBasedInMemoryCloudDestination newLatchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(new ArrayList<>(), mockCluster.getClusterMap());
    for (Map.Entry<String, byte[]> entry : latchBasedInMemoryCloudDestination.getTokenMap().entrySet()) {
      newLatchBasedInMemoryCloudDestination.getTokenMap().put(entry.getKey(), entry.getValue());
    }
    cloudDestinationFactory = new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);

    vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    vcrServer = VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
        vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();

    makeSureHelixBalance(vcrServer, helixBalanceVerifier);
    final MockNotificationSystem vcrNotificationSystemCopy2 = vcrNotificationSystem;
    assertTrue("Blob count is not correct.",
        TestUtils.checkAndSleep(0, () -> vcrNotificationSystemCopy2.getBlobIds().size(), 400));
    vcrServer.shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServer.awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0,
        vcrServer.getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());
  }

  /**
   * A multiple VCR test to test helix assignment and backup.
   */
  @Test
  public void multipleVcrTest() throws Exception {
    StrictMatchExternalViewVerifier helixBalanceVerifier =
        new StrictMatchExternalViewVerifier(zkConnectString, vcrClusterName,
            Collections.singleton(VcrTestUtil.helixResource), null);
    int initialNumOfVcrs = 5;
    // create a shared in memory destination.
    LatchBasedInMemoryCloudDestination latchBasedInMemoryCloudDestination =
        new LatchBasedInMemoryCloudDestination(new ArrayList<>(), mockCluster.getClusterMap());
    CloudDestinationFactory cloudDestinationFactory =
        new LatchBasedInMemoryCloudDestinationFactory(latchBasedInMemoryCloudDestination);
    // 1st phase: Start VCRs to do backup.
    List<VcrServer> vcrServers = new ArrayList<>();
    List<MockNotificationSystem> vcrNotificationSystems = new ArrayList<>();
    for (int port = 12310; port < 12310 + initialNumOfVcrs; port++) {
      Properties props =
          VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString, port,
              port + 100, port + 200, null, vcrHelixStateModelFactoryClass, true);
      MockNotificationSystem vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
      VcrServer vcrServer =
          VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
              vcrNotificationSystem, cloudDestinationFactory);
      vcrServer.startup();
      vcrServers.add(vcrServer);
      vcrNotificationSystems.add(vcrNotificationSystem);
    }
    makeSureHelixBalance(vcrServers.get(vcrServers.size() - 1), helixBalanceVerifier);
    int numOfBlobs = 100;
    sendBlobToDataNode(dataNode, numOfBlobs);
    // Make sure blobs are backed up.
    TestUtils.checkAndSleep(numOfBlobs,
        () -> vcrNotificationSystems.stream().mapToInt(i -> i.getBlobIds().size()).sum(), 5000);

    // verify each VCR is only replicating partitions assigned to it.
    for (int i = 0; i < initialNumOfVcrs; i++) {
      Set<PartitionId> partitionIdSet = vcrNotificationSystems.get(i).getBlobIds().stream().map(blobIdStr -> {
        try {
          return new BlobId(blobIdStr, mockCluster.getClusterMap()).getPartition();
        } catch (IOException e) {
          e.printStackTrace();
          return null;
        }
      }).collect(Collectors.toSet());
      assertTrue("Each VCR should have some assignment.",
          vcrServers.get(i).getVcrClusterParticipant().getAssignedPartitionIds().size() > 0);
      assertTrue("Each VCR should only backup its assigned partitions.",
          new HashSet<>(vcrServers.get(i).getVcrClusterParticipant().getAssignedPartitionIds()).containsAll(
              partitionIdSet));
    }
    logger.info("Phase 1 done.");

    // 2nd phase: Add a new VCR to cluster.
    Properties props = VcrTestUtil.createVcrProperties(dataNode.getDatacenterName(), vcrClusterName, zkConnectString,
        12310 + initialNumOfVcrs, 12310 + initialNumOfVcrs + 100, 12310 + initialNumOfVcrs + 200, null,
        vcrHelixStateModelFactoryClass, true);
    MockNotificationSystem vcrNotificationSystem = new MockNotificationSystem(mockCluster.getClusterMap());
    VcrServer vcrServer =
        VcrTestUtil.createVcrServer(new VerifiableProperties(props), mockCluster.getClusterAgentsFactory(),
            vcrNotificationSystem, cloudDestinationFactory);
    vcrServer.startup();
    vcrServers.add(vcrServer);
    vcrNotificationSystems.add(vcrNotificationSystem);

    makeSureHelixBalance(vcrServers.get(vcrServers.size() - 1), helixBalanceVerifier);
    int secondNumOfBlobs = 100;
    sendBlobToDataNode(dataNode, secondNumOfBlobs);
    Assert.assertTrue("All blobs should be back up.", TestUtils.checkAndSleep(numOfBlobs + secondNumOfBlobs,
        () -> vcrNotificationSystems.stream().mapToInt(i -> i.getBlobIds().size()).sum(), 5000));
    logger.info("Phase 2 done.");

    // 3rd phase: Remove last VCR from cluster.
    vcrServers.get(vcrServers.size() - 1).shutdown();
    assertTrue("VCR server shutdown timeout.", vcrServers.get(vcrServers.size() - 1).awaitShutdown(5000));
    // Error metrics should be zero.
    Assert.assertEquals("Error count should be zero", 0, vcrServers.get(vcrServers.size() - 1)
        .getVcrReplicationManager()
        .getVcrMetrics().addPartitionErrorCount.getCount());
    Assert.assertEquals("Error count should be zero", 0, vcrServers.get(vcrServers.size() - 1)
        .getVcrReplicationManager()
        .getVcrMetrics().removePartitionErrorCount.getCount());
    int temp = vcrNotificationSystems.get(vcrNotificationSystems.size() - 1).getBlobIds().size();

    assertTrue("Helix balance timeout.", helixBalanceVerifier.verify(5000));
    int thirdNumOfBlobs = 100;
    sendBlobToDataNode(dataNode, thirdNumOfBlobs);
    Assert.assertTrue("All blobs should be back up.",
        TestUtils.checkAndSleep(numOfBlobs + secondNumOfBlobs + thirdNumOfBlobs,
            () -> vcrNotificationSystems.stream().mapToInt(i -> i.getBlobIds().size()).sum(), 5000));
    Assert.assertEquals("The removed vcr shouldn't have any change", temp,
        vcrNotificationSystems.get(vcrNotificationSystems.size() - 1).getBlobIds().size());
    logger.info("Phase 3 done.");

    // Shutdown all others.
    for (int i = 0; i < initialNumOfVcrs; i++) {
      // Error metrics should be zero.
      Assert.assertEquals("Error count should be zero", 0,
          vcrServers.get(i).getVcrReplicationManager().getVcrMetrics().addPartitionErrorCount.getCount());
      Assert.assertEquals("Error count should be zero", 0,
          vcrServers.get(i).getVcrReplicationManager().getVcrMetrics().removePartitionErrorCount.getCount());
      vcrServers.get(i).shutdown();
      assertTrue("VCR server shutdown timeout.", vcrServers.get(i).awaitShutdown(5000));
    }

    logger.info("Test done.");
  }

  /**
   * Helper function to make sure helix rebalanced.
   * @param vcrServer a sample server to detect ideal state change.
   * @param helixBalanceVerifier helix balance verifier.
   */
  static void makeSureHelixBalance(VcrServer vcrServer, StrictMatchExternalViewVerifier helixBalanceVerifier) {
    Assert.assertTrue("Helix topology change timeout.",
        TestUtils.checkAndSleep(true, () -> vcrServer.getVcrClusterParticipant().getAssignedPartitionIds().size() > 0,
            15000));
    assertTrue("Helix balance timeout.", helixBalanceVerifier.verify(15000));
  }

  /**
   * Send blobs to given dataNode.
   * @param dataNode the target node.
   * @param blobCount number of blobs to send.
   * @return list of blobs successfully sent.
   */
  private List<BlobId> sendBlobToDataNode(DataNodeId dataNode, int blobCount) throws Exception {
    int userMetaDataSize = 10;
    // Send blobs to DataNode
    byte[] userMetadata = new byte[userMetaDataSize];
    byte[] data = new byte[blobSize];
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobProperties properties =
        new BlobProperties(blobSize, "serviceid1", null, null, false, -1, accountId, containerId, false, null, null,
            null);
    TestUtils.RANDOM.nextBytes(userMetadata);
    TestUtils.RANDOM.nextBytes(data);

    Port port = new Port(dataNode.getPort(), PortType.PLAINTEXT);
    ConnectedChannel channel = ServerTestUtil.getBlockingChannelBasedOnPortType(port, "localhost", null, null);
    channel.connect();
    CountDownLatch latch = new CountDownLatch(1);
    DirectSender runnable =
        new DirectSender(mockCluster, channel, blobCount, data, userMetadata, properties, null, latch);
    Thread threadToRun = new Thread(runnable);
    threadToRun.start();
    assertTrue("Did not put all blobs in 2 minutes", latch.await(2, TimeUnit.MINUTES));
    List<BlobId> blobIds = runnable.getBlobIds();
    for (BlobId blobId : blobIds) {
      notificationSystem.awaitBlobCreations(blobId.getID());
    }
    return blobIds;
  }

  static {
    try {
      zkInfo = new TestUtils.ZkInfo(TestUtils.getTempDir("helixVcr"), "DC1", (byte) 1, zkPort, true);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}

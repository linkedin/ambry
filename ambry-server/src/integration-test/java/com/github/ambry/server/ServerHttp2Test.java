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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.http2.Http2BlockingChannel;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.*;


@RunWith(Parameterized.class)
public class ServerHttp2Test {
  private Properties routerProps;
  private MockNotificationSystem notificationSystem;
  private MockCluster http2Cluster;
  private final boolean testEncryption;
  private SSLConfig clientSSLConfig1;
  private SSLConfig clientSSLConfig2;
  private SSLConfig clientSSLConfig3;
  private File trustStoreFile;
  // Per-test-instance tracker for Http2BlockingChannels we own. Scoped to this class so
  // closing it in @After only affects channels we created — channels created by other
  // test classes (e.g., RouterServerSSLTest) are not registered here.
  private final List<Http2BlockingChannel> trackedHttp2Channels = new ArrayList<>();
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  // Per-test MockCluster lifecycle (matches ServerPlaintextTest/ServerSSLTest/etc.).
  // Sharing http2Cluster across tests via @BeforeClass let blob-store, replication-token,
  // and replica/disk state leak between tests, which surfaced as flaky failures in
  // replicateBlobV2MultipleCases (e.g. expected:<BlobNotFound> but was:<NoError>,
  // expected:<NoError> but was:<ReplicaUnavailable>).
  @Before
  public void before() throws Exception {
    nettyByteBufLeakHelper.beforeTest();
    // Opt this test class into Http2BlockingChannel auto-tracking. Other test classes
    // that don't call enable... aren't affected.
    ServerTestUtil.enableHttp2ChannelTracking(trackedHttp2Channels);

    trustStoreFile = File.createTempFile("truststore", ".jks");

    Properties clientSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(clientSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile,
        "http2-blocking-channel-client");
    TestSSLUtils.addHttp2Properties(clientSSLProps, SSLFactory.Mode.CLIENT, false);
    clientSSLConfig1 = new SSLConfig(new VerifiableProperties(clientSSLProps));
    clientSSLConfig2 = new SSLConfig(new VerifiableProperties(clientSSLProps));
    clientSSLConfig3 = new SSLConfig(new VerifiableProperties(clientSSLProps));

    // Router
    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    routerProps.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    routerProps.setProperty(RouterConfig.ROUTER_ENABLE_HTTP2_NETWORK_CLIENT, "true");
    TestSSLUtils.addHttp2Properties(routerProps, SSLFactory.Mode.CLIENT, false);
    TestSSLUtils.addSSLProperties(routerProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "router-client");

    // Server
    Properties serverSSLProps;
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, false);
    serverSSLProps.setProperty("clustermap.enable.http2.replication", "true");
    http2Cluster =
        new MockCluster(serverSSLProps, false, new MockTime(SystemTime.getInstance().milliseconds()), 9, 3, 3);
    notificationSystem = new MockNotificationSystem(http2Cluster.getClusterMap());
    http2Cluster.initializeServers(notificationSystem);
    http2Cluster.startServers();

    // Warm replication: PUT a sentinel blob and wait for it to fully replicate. After this
    // returns, every replica has confirmed at least one successful poll+fetch cycle, so
    // subsequent test PUTs replicate within the (small) configured throttle window rather
    // than waiting for cold-start peer discovery. Converts wall-clock-racey awaits into
    // semantically deterministic ones.
    warmUpReplication();
  }

  /**
   * PUT a small probe blob to one server and wait for it to fully replicate to all replicas.
   * Returns once replication is observably alive on every replica of the sentinel partition.
   */
  private void warmUpReplication() throws Exception {
    DataNodeId dataNode = http2Cluster.getGeneralDataNode();
    Port http2Port = new Port(dataNode.getHttp2Port(), PortType.HTTP2);
    PartitionId partition =
        http2Cluster.getClusterMap().getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);

    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    byte[] data = new byte[100];
    byte[] usermetadata = new byte[100];
    TestUtils.RANDOM.nextBytes(data);
    TestUtils.RANDOM.nextBytes(usermetadata);

    BlobProperties props =
        new BlobProperties(100, "warmup", null, null, false, TestUtils.TTL_SECS, http2Cluster.time.milliseconds(),
            accountId, containerId, false, null, null, null, null);
    BlobId blobId =
        new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, ClusterMap.UNKNOWN_DATACENTER_ID, accountId,
            containerId, partition, false, BlobId.BlobDataType.DATACHUNK);

    ConnectedChannel channel =
        ServerTestUtil.getBlockingChannelBasedOnPortType(http2Port, "localhost", null, clientSSLConfig1);
    channel.connect();
    try {
      PutRequest putRequest = new PutRequest(1, "warmup-client", blobId, props, ByteBuffer.wrap(usermetadata),
          Unpooled.wrappedBuffer(data), props.getBlobSize(), BlobType.DataBlob, null);
      DataInputStream stream = channel.sendAndReceive(putRequest).getInputStream();
      PutResponse putResponse = PutResponse.readFrom(stream);
      ServerTestUtil.releaseNettyBufUnderneathStream(stream);
      if (putResponse.getError() == ServerErrorCode.NoError) {
        notificationSystem.awaitBlobCreations(blobId.getID());
      }
    } finally {
      channel.disconnect();
    }
  }

  @After
  public void after() throws IOException {
    // Each cleanup wrapped so a single failure doesn't skip the remaining ones.
    try {
      if (http2Cluster != null) {
        http2Cluster.cleanup();
      }
    } finally {
      try {
        // Close only Http2BlockingChannel instances allocated during this test (tracked
        // via the per-class tracker). Channels created by other test classes are not in
        // this list, so this @After can't accidentally close them.
        for (Http2BlockingChannel channel : trackedHttp2Channels) {
          try {
            channel.close();
          } catch (Exception e) {
            // Best-effort; one failed close shouldn't stop the others.
          }
        }
        trackedHttp2Channels.clear();
        ServerTestUtil.disableHttp2ChannelTracking();
      } finally {
        try {
          if (trustStoreFile != null && trustStoreFile.exists()) {
            trustStoreFile.delete();
          }
        } finally {
          // Run leak check AFTER cluster teardown so any ByteBufs released during cleanup
          // are reflected before NettyByteBufLeakHelper measures pending allocations.
          nettyByteBufLeakHelper.afterTest();
        }
      }
    }
  }

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{true}, {false}});
  }

  public ServerHttp2Test(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

  @Test
  public void endToEndTest() throws Exception {
    DataNodeId dataNodeId = http2Cluster.getGeneralDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), "DC1", http2Cluster,
        clientSSLConfig1, null, routerProps, testEncryption);
  }

  @Test
  public void replicatePutBlobV1Format() {
    // ReplicateBlob has two modes: write-repair-mode and non-write-repair mode.
    // Refer to handleReplicateBlobRequest.localStoreHasTheKey
    boolean writeRepair = false;
    ServerTestUtil.replicateBlobCaseTest(http2Cluster, clientSSLConfig1, testEncryption, notificationSystem,
        writeRepair);
  }

  @Test
  public void replicateTombstoneV1Format() {
    // test ReplicateBlob delete tombstone record
    ServerTestUtil.replicateDeleteTomeStoneTest(http2Cluster, clientSSLConfig1, testEncryption, notificationSystem);
  }

  @Test
  public void replicateBlobV2MultipleCases() {
    ServerTestUtil.replicateBlobV2CaseTest(http2Cluster, clientSSLConfig1, testEncryption, notificationSystem);
  }

  @Test
  public void repairRequestTest() throws Exception {
    // test not encrypted case is enough. repairRequest is running at high level.
    assumeTrue(!testEncryption);
    ServerTestUtil.repairRequestTest(http2Cluster, clientSSLConfig1, testEncryption, notificationSystem);
  }

  @Test
  public void endToEndHttp2ReplicationWithMultiNodeMultiPartition() throws Exception {
    DataNodeId dataNode = http2Cluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = new ArrayList<>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = http2Cluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionTest(dataNode.getPort(),
        new Port(dataNodes.get(0).getHttp2Port(), PortType.HTTP2),
        new Port(dataNodes.get(1).getHttp2Port(), PortType.HTTP2),
        new Port(dataNodes.get(2).getHttp2Port(), PortType.HTTP2), http2Cluster, clientSSLConfig1, clientSSLConfig2,
        clientSSLConfig3, null, null, null, notificationSystem, testEncryption);
  }
}

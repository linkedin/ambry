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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ServerHttp2Test {
  private static Properties routerProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster http2Cluster;
  private final boolean testEncryption;
  private static SSLConfig clientSSLConfig1;
  private static SSLConfig clientSSLConfig2;
  private static SSLConfig clientSSLConfig3;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  @BeforeClass
  public static void initializeTests() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");

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

  @AfterClass
  public static void cleanup() throws IOException {
    if (http2Cluster != null) {
      http2Cluster.cleanup();
    }
  }

  @Test
  public void endToEndTest() throws Exception {
    DataNodeId dataNodeId = http2Cluster.getGeneralDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getHttp2Port(), PortType.HTTP2), "DC1", http2Cluster,
        clientSSLConfig1, null, routerProps, testEncryption);
  }

  @Test
  public void replicateBlobCaseTest() {
    // ReplicateBlob has two modes: write-repair-mode and non-write-repair mode.
    // Refer to handleReplicateBlobRequest.localStoreHasTheKey
    boolean writeRepair = false;
    ServerTestUtil.replicateBlobCaseTest(http2Cluster, clientSSLConfig1, routerProps, testEncryption,
        notificationSystem, writeRepair);
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

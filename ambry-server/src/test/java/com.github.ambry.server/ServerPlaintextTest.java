/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ServerPlaintextTest {
  private static Properties coordinatorProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster plaintextCluster;

  @BeforeClass
  public static void initializeTests()
      throws Exception {
    coordinatorProps = new Properties();
    notificationSystem = new MockNotificationSystem(9);
    plaintextCluster =
        new MockCluster(notificationSystem, SystemTime.getInstance());
    plaintextCluster.startServers();
  }

  public ServerPlaintextTest()
      throws Exception {
  }

  @AfterClass
  public static void cleanup() {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void startStopTest()
      throws IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
  }

  @Test
  public void endToEndTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNodeId = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ServerTestUtil
        .endToEndTest(new Port(dataNodeId.getPort(), PortType.PLAINTEXT), "DC1", "", plaintextCluster, null, null, coordinatorProps);
  }

  @Test
  public void endToEndReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNodeId = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = Utils.splitString("DC1,DC2,DC3", ",");
    List<DataNodeId> dataNodes = plaintextCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeSinglePartitionTest("DC1", "", dataNodeId.getPort(),
        new Port(dataNodes.get(0).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(1).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(2).getPort(), PortType.PLAINTEXT), plaintextCluster, null,
        null, notificationSystem, coordinatorProps);
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNode = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = Utils.splitString("DC1,DC2,DC3", ",");
    List<DataNodeId> dataNodes = plaintextCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionTest(dataNode.getPort(),
        new Port(dataNodes.get(0).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(1).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(2).getPort(), PortType.PLAINTEXT), plaintextCluster, null, null,
        null, null, null, null, notificationSystem);
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest()
      throws Exception {
    ServerTestUtil
        .endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", "", PortType.PLAINTEXT, plaintextCluster,
            notificationSystem, coordinatorProps);
  }
}
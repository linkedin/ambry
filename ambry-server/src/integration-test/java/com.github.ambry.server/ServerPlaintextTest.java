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
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ServerPlaintextTest {
  private static Properties routerProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster plaintextCluster;
  private final boolean testEncryption;

  @BeforeClass
  public static void initializeTests() throws Exception {
    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    routerProps.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    plaintextCluster = new MockCluster(false, SystemTime.getInstance());
    notificationSystem = new MockNotificationSystem(plaintextCluster.getClusterMap());
    plaintextCluster.initializeServers(notificationSystem);
    plaintextCluster.startServers();
  }

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public ServerPlaintextTest(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

  @AfterClass
  public static void cleanup() throws IOException {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("ServerPlaintextTest::About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println("ServerPlaintextTest::cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void endToEndTest() {
    DataNodeId dataNodeId = plaintextCluster.getGeneralDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getPort(), PortType.PLAINTEXT), "DC1", plaintextCluster, null, null,
        routerProps, testEncryption);
  }

  /**
   * Do endToEndTest with the last dataNode whose storeEnablePrefetch is true.
   */
  @Test
  public void endToEndTestWithPrefetch() {
    DataNodeId dataNodeId = plaintextCluster.getPrefetchDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getPort(), PortType.PLAINTEXT), "DC1", plaintextCluster, null, null,
        routerProps, testEncryption);
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionTest() throws Exception {
    DataNodeId dataNode = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = Utils.splitString("DC1,DC2,DC3", ",");
    List<DataNodeId> dataNodes = plaintextCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionTest(dataNode.getPort(),
        new Port(dataNodes.get(0).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(1).getPort(), PortType.PLAINTEXT),
        new Port(dataNodes.get(2).getPort(), PortType.PLAINTEXT), plaintextCluster, null, null, null, null, null, null,
        notificationSystem, testEncryption);
  }

  @Test
  public void endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest() throws Exception {
    // this test uses router to Put and direct GetRequest to verify Gets. So, no way to get access to encryptionKey against
    // which to compare the GetResponse. Hence skipping encryption flow for this test
    if (!testEncryption) {
      ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", "", PortType.PLAINTEXT,
          plaintextCluster, notificationSystem, routerProps);
    }
  }
}

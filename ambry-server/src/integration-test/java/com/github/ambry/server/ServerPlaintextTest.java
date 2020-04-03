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
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assume.*;


@RunWith(Parameterized.class)
public class ServerPlaintextTest {
  private Properties routerProps;
  private MockNotificationSystem notificationSystem;
  private MockCluster plaintextCluster;
  private final boolean testEncryption;

  @Before
  public void initializeTests() throws Exception {
    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    routerProps.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);

    Properties serverProperties = new Properties();
    TestSSLUtils.addHttp2Properties(serverProperties, SSLFactory.Mode.SERVER, true);
    plaintextCluster = new MockCluster(serverProperties, false, SystemTime.getInstance());
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

  @After
  public void cleanup() throws IOException {
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

  /**
   * Do end to end cloud backup test.
   */
  @Test
  public void endToEndCloudBackupTest() throws Exception {
    assumeTrue(testEncryption);
    DataNodeId dataNode = plaintextCluster.getClusterMap().getDataNodeIds().get(0);
    ServerTestUtil.endToEndCloudBackupTest(plaintextCluster, dataNode, null, null, notificationSystem, null,
        Utils.Infinite_Time, false);
    ServerTestUtil.endToEndCloudBackupTest(plaintextCluster, dataNode, null, null, notificationSystem, null,
        System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1), true);
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

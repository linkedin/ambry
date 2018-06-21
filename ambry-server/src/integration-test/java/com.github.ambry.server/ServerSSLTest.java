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
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ServerSSLTest {
  private static SSLFactory sslFactory;
  private static SSLConfig clientSSLConfig1;
  private static SSLConfig clientSSLConfig2;
  private static SSLConfig clientSSLConfig3;
  private static SSLSocketFactory clientSSLSocketFactory1;
  private static SSLSocketFactory clientSSLSocketFactory2;
  private static SSLSocketFactory clientSSLSocketFactory3;
  private static File trustStoreFile;
  private static Properties serverSSLProps;
  private static Properties routerProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster sslCluster;
  private final boolean testEncryption;

  @BeforeClass
  public static void initializeTests() throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    clientSSLConfig1 =
        new SSLConfig(TestSSLUtils.createSslProps("DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client1"));
    clientSSLConfig2 =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client2"));
    clientSSLConfig3 =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2", SSLFactory.Mode.CLIENT, trustStoreFile, "client3"));
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    routerProps.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    TestSSLUtils.addSSLProperties(routerProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "router-client");
    sslCluster = new MockCluster(serverSSLProps, false, SystemTime.getInstance());
    notificationSystem = new MockNotificationSystem(sslCluster.getClusterMap());
    sslCluster.initializeServers(notificationSystem);
    sslCluster.startServers();
    //client
    sslFactory = SSLFactory.getNewInstance(clientSSLConfig1);
    SSLContext sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory1 = sslContext.getSocketFactory();
    sslFactory = SSLFactory.getNewInstance(clientSSLConfig2);
    sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory2 = sslContext.getSocketFactory();
    sslFactory = SSLFactory.getNewInstance(clientSSLConfig3);
    sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory3 = sslContext.getSocketFactory();
  }

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public ServerSSLTest(boolean testEncryption) throws Exception {
    this.testEncryption = testEncryption;
  }

  @AfterClass
  public static void cleanup() throws IOException {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("ServerSSLTest::About to invoke cluster.cleanup()");
    if (sslCluster != null) {
      sslCluster.cleanup();
    }
    System.out.println("ServerSSLTest::cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void endToEndSSLTest() {
    DataNodeId dataNodeId = sslCluster.getGeneralDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getSSLPort(), PortType.SSL), "DC1", sslCluster, clientSSLConfig1,
        clientSSLSocketFactory1, routerProps, testEncryption);
  }

  /**
   * Do endToEndTest with the last dataNode whose storeEnablePrefetch is true.
   */
  @Test
  public void endToEndSSLTestWithPrefetch() {
    DataNodeId dataNodeId = sslCluster.getPrefetchDataNode();
    ServerTestUtil.endToEndTest(new Port(dataNodeId.getSSLPort(), PortType.SSL), "DC1", sslCluster, clientSSLConfig1,
        clientSSLSocketFactory1, routerProps, testEncryption);
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionTest() throws Exception {
    DataNodeId dataNode = sslCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = new ArrayList<String>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = sslCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionTest(dataNode.getPort(),
        new Port(dataNodes.get(0).getSSLPort(), PortType.SSL), new Port(dataNodes.get(1).getSSLPort(), PortType.SSL),
        new Port(dataNodes.get(2).getSSLPort(), PortType.SSL), sslCluster, clientSSLConfig1, clientSSLConfig2,
        clientSSLConfig3, clientSSLSocketFactory1, clientSSLSocketFactory2, clientSSLSocketFactory3, notificationSystem,
        testEncryption);
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionMultiDCTest() throws Exception {
    // this test uses router to Put and direct GetRequest to verify Gets. So, no way to get access to encryptionKey against
    // which to compare the GetResponse. Hence skipping encryption flow for this test
    if (!testEncryption) {
      ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", "DC1,DC2,DC3", PortType.SSL,
          sslCluster, notificationSystem, routerProps);
    }
  }
}

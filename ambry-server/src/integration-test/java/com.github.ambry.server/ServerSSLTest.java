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
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.network.TestSSLUtils;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


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

  @BeforeClass
  public static void initializeTests()
      throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    clientSSLConfig1 = TestSSLUtils.createSSLConfig("DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client1");
    clientSSLConfig2 = TestSSLUtils.createSSLConfig("DC1,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client2");
    clientSSLConfig3 = TestSSLUtils.createSSLConfig("DC1,DC2", SSLFactory.Mode.CLIENT, trustStoreFile, "client3");
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    routerProps = new Properties();
    TestSSLUtils.addSSLProperties(routerProps, "", SSLFactory.Mode.CLIENT, trustStoreFile, "router-client");
    notificationSystem = new MockNotificationSystem(9);
    sslCluster =
        new MockCluster(notificationSystem, true, "DC1,DC2,DC3", serverSSLProps, false, SystemTime.getInstance());
    sslCluster.startServers();
    //client
    sslFactory = new SSLFactory(clientSSLConfig1);
    SSLContext sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory1 = sslContext.getSocketFactory();
    sslFactory = new SSLFactory(clientSSLConfig2);
    sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory2 = sslContext.getSocketFactory();
    sslFactory = new SSLFactory(clientSSLConfig3);
    sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory3 = sslContext.getSocketFactory();
  }

  public ServerSSLTest()
      throws Exception {
  }

  @AfterClass
  public static void cleanup()
      throws IOException {
    long start = System.currentTimeMillis();
    // cleanup appears to hang sometimes. And, it sometimes takes a long time. Printing some info until cleanup is fast
    // and reliable.
    System.out.println("About to invoke cluster.cleanup()");
    if (sslCluster != null) {
      sslCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Test
  public void startStopTest()
      throws IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
  }

  @Test
  public void endToEndSSLTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNodeId = sslCluster.getClusterMap().getDataNodeIds().get(3);
    ServerTestUtil
        .endToEndTest(new Port(dataNodeId.getSSLPort(), PortType.SSL), "DC1", "DC2,DC3", sslCluster, clientSSLConfig1,
            clientSSLSocketFactory1, routerProps);
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNode = sslCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = new ArrayList<String>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = sslCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeMultiPartitionTest(dataNode.getPort(),
        new Port(dataNodes.get(0).getSSLPort(), PortType.SSL), new Port(dataNodes.get(1).getSSLPort(), PortType.SSL),
        new Port(dataNodes.get(2).getSSLPort(), PortType.SSL), sslCluster, clientSSLConfig1, clientSSLConfig2,
        clientSSLConfig3, clientSSLSocketFactory1, clientSSLSocketFactory2, clientSSLSocketFactory3,
        notificationSystem);
  }

  @Test
  public void endToEndSSLReplicationWithMultiNodeMultiPartitionMultiDCTest()
      throws Exception {
    ServerTestUtil
        .endToEndReplicationWithMultiNodeMultiPartitionMultiDCTest("DC1", "DC1,DC2,DC3", PortType.SSL, sslCluster,
            notificationSystem, routerProps);
  }
}

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
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class ServerSSLTokenTest {
  private static SSLFactory sslFactory;
  private static SSLConfig clientSSLConfig;
  private static SSLSocketFactory clientSSLSocketFactory;
  private static File trustStoreFile;
  private static Properties serverSSLProps;
  private static Properties routerProps;
  private static MockNotificationSystem notificationSystem;
  private static MockCluster sslCluster;
  private final boolean testEncryption;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Instantiates {@link ServerSSLTokenTest}
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public ServerSSLTokenTest(boolean testEncryption) {
    this.testEncryption = testEncryption;
  }

  @Before
  public void initializeTests() throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client1"));
    serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server");
    routerProps = new Properties();
    routerProps.setProperty("kms.default.container.key", TestUtils.getRandomKey(32));
    TestSSLUtils.addSSLProperties(routerProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "router-client");
    notificationSystem = new MockNotificationSystem(9);
    sslCluster = new MockCluster(notificationSystem, serverSSLProps, false, SystemTime.getInstance());
    sslCluster.startServers();
    //client
    sslFactory = new SSLFactory(clientSSLConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    clientSSLSocketFactory = sslContext.getSocketFactory();
  }

  @After
  public void cleanup() throws IOException {
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
  public void endToEndSSLReplicationWithMultiNodeSinglePartitionTest()
      throws InterruptedException, IOException, InstantiationException, URISyntaxException, GeneralSecurityException {
    DataNodeId dataNodeId = sslCluster.getClusterMap().getDataNodeIds().get(0);
    ArrayList<String> dataCenterList = new ArrayList<String>(Arrays.asList("DC1", "DC2", "DC3"));
    List<DataNodeId> dataNodes = sslCluster.getOneDataNodeFromEachDatacenter(dataCenterList);
    ServerTestUtil.endToEndReplicationWithMultiNodeSinglePartitionTest("DC1", "DC2,DC3", dataNodeId.getPort(),
        new Port(dataNodes.get(0).getSSLPort(), PortType.SSL), new Port(dataNodes.get(1).getSSLPort(), PortType.SSL),
        new Port(dataNodes.get(2).getSSLPort(), PortType.SSL), sslCluster, clientSSLConfig, clientSSLSocketFactory,
        notificationSystem, routerProps, testEncryption);
  }
}

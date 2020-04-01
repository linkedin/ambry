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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.server.RouterServerTestFramework.*;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.server.RouterServerTestFramework.*;


@RunWith(Parameterized.class)
public class RouterServerHttp2Test {
  private static MockCluster http2Cluster;
  private static RouterServerTestFramework testFramework;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Instantiates {@link RouterServerHttp2Test}
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public RouterServerHttp2Test(boolean testEncryption) {
    testFramework.setTestEncryption(testEncryption);
  }

  @BeforeClass
  public static void initializeTests() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");

    // Set up router properties
    Properties routerProps = getRouterProperties("DC1");
    TestSSLUtils.addSSLProperties(routerProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "http2-router");
    TestSSLUtils.addHttp2Properties(routerProps, SSLFactory.Mode.CLIENT, false);
    routerProps.setProperty(RouterConfig.ROUTER_ENABLE_HTTP2_NETWORK_CLIENT, "true");

    // Set up server properties
    Properties serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile,
        "http2-server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, false);
    serverSSLProps.setProperty("server.enable.store.data.prefetch", "true");

    http2Cluster = new MockCluster(serverSSLProps, false, SystemTime.getInstance());
    MockNotificationSystem notificationSystem = new MockNotificationSystem(http2Cluster.getClusterMap());
    http2Cluster.initializeServers(notificationSystem);
    http2Cluster.startServers();
    MockClusterMap routerClusterMap = http2Cluster.getClusterMap();
    // MockClusterMap returns a new registry by default. This is to ensure that each node (server, router and so on,
    // get a different registry. But at this point all server nodes have been initialized, and we want the router and
    // its components, which are going to be created, to use the same registry.
    routerClusterMap.createAndSetPermanentMetricRegistry();

    testFramework = new RouterServerTestFramework(routerProps, routerClusterMap, notificationSystem);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("RouterServerSSLTest::About to invoke cluster.cleanup()");
    if (http2Cluster != null) {
      http2Cluster.cleanup();
    }
    System.out.println("RouterServerSSLTest::cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Before
  public void before() {
  }

  @After
  public void after() {
  }

  /**
   * Test that the non blocking router can handle a large number of concurrent (small blob) operations without errors.
   * This test creates chains of operations without waiting for previous operations to finish.
   * @throws Exception
   */
  @Test
  public void interleavedOperationsTest() throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      switch (i % 3) {
        case 0:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET_AUTHORIZATION_FAILURE);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          operations.add(OperationType.TTL_UPDATE);
          operations.add(OperationType.AWAIT_TTL_UPDATE);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED_SUCCESS);
          operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
          break;
        case 1:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.DELETE_AUTHORIZATION_FAILURE);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED_SUCCESS);
          operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
          break;
        case 2:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_AUTHORIZATION_FAILURE);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          operations.add(OperationType.TTL_UPDATE);
          operations.add(OperationType.AWAIT_TTL_UPDATE);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          break;
      }
      int blobSize = random.nextInt(1024);
      opChains.add(testFramework.startOperationChain(blobSize, null, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  /**
   * Test that the http2 network based router can handle multi-chunk blobs.
   * @throws Exception
   */
  @Test
  public void largeBlobTest() throws Exception {
    final int blobSize = 2 * RouterServerTestFramework.CHUNK_SIZE + 1;
    List<OperationChain> opChains = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT);
      operations.add(OperationType.AWAIT_CREATION);
      opChains.add(testFramework.startOperationChain(blobSize, null, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

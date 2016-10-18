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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.network.Selector;
import com.github.ambry.network.TestSSLUtils;
import com.github.ambry.server.RouterServerTestFramework.OperationChain;
import com.github.ambry.server.RouterServerTestFramework.OperationType;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.server.RouterServerTestFramework.getRouterProperties;
import static com.github.ambry.server.RouterServerTestFramework.plaintextReceiveBytesMetricName;
import static com.github.ambry.server.RouterServerTestFramework.plaintextSendBytesMetricName;
import static com.github.ambry.server.RouterServerTestFramework.sslReceiveBytesMetricName;
import static com.github.ambry.server.RouterServerTestFramework.sslSendBytesMetricName;


public class RouterServerSSLTest {
  private static MockCluster sslCluster;
  private static RouterServerTestFramework testFramework;
  private static MetricRegistry routerMetricRegistry;
  private static long sslSendBytesCountBeforeTest;
  private static long sslReceiveBytesCountBeforeTest;

  @BeforeClass
  public static void initializeTests()
      throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    String sslEnabledDataCentersStr = "DC1,DC2,DC3";
    Properties serverSSLProps = new Properties();
    TestSSLUtils
        .addSSLProperties(serverSSLProps, sslEnabledDataCentersStr, SSLFactory.Mode.SERVER, trustStoreFile, "server");
    Properties routerProps = getRouterProperties("DC1");
    TestSSLUtils.addSSLProperties(routerProps, sslEnabledDataCentersStr, SSLFactory.Mode.CLIENT, trustStoreFile,
        "router-client");
    MockNotificationSystem notificationSystem = new MockNotificationSystem(9);
    sslCluster = new MockCluster(notificationSystem, serverSSLProps, false, SystemTime.getInstance());
    sslCluster.startServers();
    MockClusterMap routerClusterMap = sslCluster.getClusterMap();
    // MockClusterMap returns a new registry by default. This is to ensure that each node (server, router and so on,
    // get a different registry. But at this point all server nodes have been initialized, and we want the router and
    // its components, which are going to be created, to use the same registry.
    routerClusterMap.createAndSetPermanentMetricRegistry();
    testFramework = new RouterServerTestFramework(routerProps, routerClusterMap, notificationSystem);
    routerMetricRegistry = routerClusterMap.getMetricRegistry();
  }

  @AfterClass
  public static void cleanup()
      throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("About to invoke cluster.cleanup()");
    if (sslCluster != null) {
      sslCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Before
  public void before() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    sslSendBytesCountBeforeTest = meters.get(sslSendBytesMetricName).getCount();
    sslReceiveBytesCountBeforeTest = meters.get(sslReceiveBytesMetricName).getCount();
  }

  @After
  public void after() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    Assert.assertTrue("Router should have sent over SSL",
        meters.get(sslSendBytesMetricName).getCount() != sslSendBytesCountBeforeTest);
    Assert.assertTrue("Router should have received over SSL",
        meters.get(sslReceiveBytesMetricName).getCount() != sslReceiveBytesCountBeforeTest);
    Assert.assertTrue("Router should not have sent over Plain Text",
        meters.get(plaintextSendBytesMetricName).getCount() == 0);
    Assert.assertTrue("Router should not have received over Plain Text",
        meters.get(plaintextReceiveBytesMetricName).getCount() == 0);
  }

  /**
   * Test that the non blocking router can handle a large number of concurrent (small blob) operations without errors.
   * This test creates chains of operations without waiting for previous operations to finish.
   * @throws Exception
   */
  @Test
  public void interleavedOperationsTest()
      throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      switch (i % 3) {
        case 0:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          break;
        case 1:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.DELETE);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          operations.add(OperationType.GET_DELETED);
          operations.add(OperationType.GET_INFO_DELETED);
          break;
        case 2:
          operations.add(OperationType.PUT);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET);
          operations.add(OperationType.GET_INFO);
          break;
      }
      int blobSize = random.nextInt(100 * 1024);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

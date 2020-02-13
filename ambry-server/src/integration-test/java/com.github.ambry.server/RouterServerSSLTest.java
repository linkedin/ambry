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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.server.RouterServerTestFramework.*;


@RunWith(Parameterized.class)
public class RouterServerSSLTest {
  private static MockCluster sslCluster;
  private static RouterServerTestFramework testFramework;
  private static MetricRegistry routerMetricRegistry;
  private static long transmissionSendBytesCountBeforeTest;
  private static long transmissionReceiveBytesCountBeforeTest;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Instantiates {@link RouterServerSSLTest}
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public RouterServerSSLTest(boolean testEncryption) {
    testFramework.setTestEncryption(testEncryption);
  }

  @BeforeClass
  public static void initializeTests() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    String sslEnabledDataCentersStr = "DC1,DC2,DC3";
    Properties serverSSLProps = new Properties();
    TestSSLUtils.addSSLProperties(serverSSLProps, sslEnabledDataCentersStr, SSLFactory.Mode.SERVER, trustStoreFile,
        "server");
    TestSSLUtils.addHttp2Properties(serverSSLProps, SSLFactory.Mode.SERVER, true);
    Properties routerProps = getRouterProperties("DC1");
    TestSSLUtils.addSSLProperties(routerProps, sslEnabledDataCentersStr, SSLFactory.Mode.CLIENT, trustStoreFile,
        "router-client");
    sslCluster = new MockCluster(serverSSLProps, false, SystemTime.getInstance());
    MockNotificationSystem notificationSystem = new MockNotificationSystem(sslCluster.getClusterMap());
    sslCluster.initializeServers(notificationSystem);
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
  public static void cleanup() throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("RouterServerSSLTest::About to invoke cluster.cleanup()");
    if (sslCluster != null) {
      sslCluster.cleanup();
    }
    System.out.println("RouterServerSSLTest::cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
  }

  @Before
  public void before() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    transmissionSendBytesCountBeforeTest = meters.get(transmissionSendBytesMetricName).getCount();
    transmissionReceiveBytesCountBeforeTest = meters.get(transmissionReceiveBytesMetricName).getCount();
  }

  @After
  public void after() {
    Map<String, Meter> meters = routerMetricRegistry.getMeters();
    Assert.assertTrue("Router should have been sent",
        meters.get(transmissionSendBytesMetricName).getCount() != transmissionSendBytesCountBeforeTest);
    Assert.assertTrue("Router should have been sent",
        meters.get(transmissionReceiveBytesMetricName).getCount() != transmissionReceiveBytesCountBeforeTest);
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
      int blobSize = random.nextInt(100 * 1024);
      opChains.add(testFramework.startOperationChain(blobSize, null, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

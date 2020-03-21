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
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.server.RouterServerTestFramework.*;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
public class RouterServerPlaintextTest {
  private static MockCluster plaintextCluster;
  private static RouterServerTestFramework testFramework;
  private static MetricRegistry routerMetricRegistry;
  private static long transmissionSendBytesCountBeforeTest;
  private static long transmissionReceiveBytesCountBeforeTest;

  private static Account refAccount;
  private static List<Container> refContainers = new ArrayList<>();

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Instantiates {@link RouterServerPlaintextTest}
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public RouterServerPlaintextTest(boolean testEncryption) {
    testFramework.setTestEncryption(testEncryption);
  }

  @BeforeClass
  public static void initializeTests() throws Exception {
    Properties properties = getRouterProperties("DC1");

    Properties serverProperties = new Properties();
    TestSSLUtils.addHttp2Properties(serverProperties, SSLFactory.Mode.SERVER, true);
    plaintextCluster = new MockCluster(serverProperties, false, SystemTime.getInstance());
    MockNotificationSystem notificationSystem = new MockNotificationSystem(plaintextCluster.getClusterMap());
    plaintextCluster.initializeServers(notificationSystem);
    plaintextCluster.startServers();
    MockClusterMap routerClusterMap = plaintextCluster.getClusterMap();
    // MockClusterMap returns a new registry by default. This is to ensure that each node (server, router and so on,
    // get a different registry. But at this point all server nodes have been initialized, and we want the router and
    // its components, which are going to be created, to use the same registry.
    routerClusterMap.createAndSetPermanentMetricRegistry();
    testFramework = new RouterServerTestFramework(properties, routerClusterMap, notificationSystem);
    routerMetricRegistry = routerClusterMap.getMetricRegistry();

    refAccount = testFramework.accountService.createAndAddRandomAccount();
    Iterator<Container> allContainers = refAccount.getAllContainers().iterator();
    // container with null replication policy
    Container container = allContainers.next();
    container = testFramework.accountService.addReplicationPolicyToContainer(container, null);
    refContainers.add(container);
    // container with configured default replication policy
    container = allContainers.next();
    container =
        testFramework.accountService.addReplicationPolicyToContainer(container, MockClusterMap.DEFAULT_PARTITION_CLASS);
    refContainers.add(container);
    // container with a special replication policy
    container = allContainers.next();
    container =
        testFramework.accountService.addReplicationPolicyToContainer(container, MockClusterMap.SPECIAL_PARTITION_CLASS);
    refContainers.add(container);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("RouterServerPlaintextTest::About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println(
        "RouterServerPlaintextTest::cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
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
    for (int i = 0; i < 20; i++) {
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
      int contIdx = i % refContainers.size();
      opChains.add(testFramework.startOperationChain(blobSize, refContainers.get(contIdx), i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  /**
   * Test that the non-blocking router can handle simple operation chains where each chain is completed before
   * the next one runs. This means that operations on only one blob are being dealt with at a time.
   * @throws Exception
   */
  @Test
  public void nonInterleavedOperationsTest() throws Exception {
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.GET_AUTHORIZATION_FAILURE);
      operations.add(OperationType.GET);
      operations.add(OperationType.TTL_UPDATE);
      operations.add(OperationType.AWAIT_TTL_UPDATE);
      operations.add(OperationType.GET);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.DELETE_AUTHORIZATION_FAILURE);
      operations.add(OperationType.DELETE);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED);
      operations.add(OperationType.GET_DELETED);
      operations.add(OperationType.GET_DELETED_SUCCESS);
      operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
      int blobSize = random.nextInt(100 * 1024);
      int contIdx = i % refContainers.size();
      testFramework.checkOperationChains(Collections.singletonList(
          testFramework.startOperationChain(blobSize, refContainers.get(contIdx), i, operations)));
    }
  }

  /**
   * Test that the non-blocking router can handle multi-chunk blobs.
   * @throws Exception
   */
  @Test
  public void largeBlobTest() throws Exception {
    final int blobSize = RouterServerTestFramework.CHUNK_SIZE * 2 + 1;
    List<OperationChain> opChains = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.GET_AUTHORIZATION_FAILURE);
      operations.add(OperationType.GET);
      operations.add(OperationType.TTL_UPDATE);
      operations.add(OperationType.AWAIT_TTL_UPDATE);
      operations.add(OperationType.GET);
      operations.add(OperationType.GET_INFO);
      operations.add(OperationType.DELETE_AUTHORIZATION_FAILURE);
      operations.add(OperationType.DELETE);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED);
      operations.add(OperationType.GET_DELETED);
      operations.add(OperationType.GET_DELETED_SUCCESS);
      operations.add(OperationType.GET_INFO_DELETED_SUCCESS);
      opChains.add(testFramework.startOperationChain(blobSize, null, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

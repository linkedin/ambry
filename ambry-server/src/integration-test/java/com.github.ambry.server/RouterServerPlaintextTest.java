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

import com.github.ambry.server.RouterServerTestFramework.OperationChain;
import com.github.ambry.server.RouterServerTestFramework.OperationType;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.github.ambry.server.RouterServerTestFramework.getRouterProperties;


public class RouterServerPlaintextTest {
  private static MockCluster plaintextCluster;
  private static RouterServerTestFramework testFramework;

  @BeforeClass
  public static void initializeTests()
      throws Exception {
    MockNotificationSystem notificationSystem = new MockNotificationSystem(9);
    plaintextCluster = new MockCluster(notificationSystem, false, SystemTime.getInstance());
    plaintextCluster.startServers();
    testFramework = new RouterServerTestFramework(getRouterProperties("DC1"), plaintextCluster, notificationSystem);
  }

  @AfterClass
  public static void cleanup()
      throws IOException {
    testFramework.cleanup();
    long start = System.currentTimeMillis();
    System.out.println("About to invoke cluster.cleanup()");
    if (plaintextCluster != null) {
      plaintextCluster.cleanup();
    }
    System.out.println("cluster.cleanup() took " + (System.currentTimeMillis() - start) + " ms.");
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
    for (int i = 0; i < 20; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      switch (i % 3) {
        case 0:
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_INFO_NB);
          operations.add(OperationType.DELETE_NB);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED_NB);
          operations.add(OperationType.GET_INFO_DELETED_NB);
          break;
        case 1:
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.DELETE_NB);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED_NB);
          operations.add(OperationType.GET_INFO_DELETED_NB);
          operations.add(OperationType.GET_DELETED_NB);
          operations.add(OperationType.GET_INFO_DELETED_NB);
          break;
        case 2:
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_INFO_NB);
          break;
      }
      int blobSize = random.nextInt(100 * 1024);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  /**
   * Test that the non-blocking router can handle simple operation chains where each chain is completed before
   * the next one runs. This means that operations on only one blob are being dealt with at a time.
   * @throws Exception
   */
  @Test
  public void nonInterleavedOperationsTest()
      throws Exception {
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT_NB);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO_NB);
      operations.add(OperationType.GET_NB);
      operations.add(OperationType.DELETE_NB);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED_NB);
      operations.add(OperationType.GET_DELETED_NB);
      int blobSize = random.nextInt(100 * 1024);
      testFramework.checkOperationChains(
          Collections.singletonList(testFramework.startOperationChain(blobSize, i, operations)));
    }
  }

  /**
   * Test that the non-blocking router can handle multi-chunk blobs.
   * @throws Exception
   */
  @Test
  public void largeBlobTest()
      throws Exception {
    final int blobSize = RouterServerTestFramework.CHUNK_SIZE * 2 + 1;
    List<OperationChain> opChains = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(OperationType.PUT_NB);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO_NB);
      operations.add(OperationType.GET_NB);
      operations.add(OperationType.DELETE_NB);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED_NB);
      operations.add(OperationType.GET_DELETED_NB);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  /**
   * Test that the coordinator-backed and non-blocking router are compatible with each other for operations on
   * single-chunk blobs.  This performs puts and deletes with either the non-blocking or coordinator backed
   * router and tests get operations with both types of routers.
   * @throws Exception
   */
  @Test
  public void coordinatorNonBlockingCompatibilityTest()
      throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      operations.add(i % 2 == 0 ? OperationType.PUT_NB : OperationType.PUT_COORD);
      operations.add(OperationType.AWAIT_CREATION);
      operations.add(OperationType.GET_INFO_COORD);
      operations.add(OperationType.GET_INFO_NB);
      operations.add(OperationType.GET_COORD);
      operations.add(OperationType.GET_NB);
      operations.add(i % 2 == 0 ? OperationType.DELETE_COORD : OperationType.DELETE_NB);
      operations.add(OperationType.AWAIT_DELETION);
      operations.add(OperationType.GET_INFO_DELETED_NB);
      operations.add(OperationType.GET_INFO_DELETED_COORD);
      operations.add(OperationType.GET_DELETED_NB);
      operations.add(OperationType.GET_DELETED_COORD);
      int blobSize = random.nextInt(100 * 1024);
      opChains.add(testFramework.startOperationChain(blobSize, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

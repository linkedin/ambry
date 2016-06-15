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

  @Test
  public void interleavedOperationsTest()
      throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Queue<OperationType> operations = new LinkedList<>();
      switch (i % 4) {
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
          operations.add(OperationType.GET_INFO_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET_INFO_NB);
          operations.add(OperationType.GET_NB);
          break;
        case 2:
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.DELETE_NB);
          operations.add(OperationType.AWAIT_DELETION);
          operations.add(OperationType.GET_DELETED_NB);
          operations.add(OperationType.GET_INFO_DELETED_NB);
          operations.add(OperationType.GET_DELETED_NB);
          operations.add(OperationType.GET_INFO_DELETED_NB);
          break;
        case 3:
          operations.add(OperationType.PUT_NB);
          operations.add(OperationType.AWAIT_CREATION);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_NB);
          operations.add(OperationType.GET_INFO_NB);
          break;
      }
      opChains.add(testFramework.startOperationChain(32 * 1024, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  @Test
  public void nonInterleavedOperationsTest()
      throws Exception {
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
      testFramework.checkOperationChains(
          Collections.singletonList(testFramework.startOperationChain(32 * 1024, i, operations)));
    }
  }

  @Test
  public void largeBlobTest()
      throws Exception {
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
      opChains.add(testFramework.startOperationChain(RouterServerTestFramework.CHUNK_SIZE * 4 + 1, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }

  @Test
  public void coordinatorNonBlockingCompatibilityTest()
      throws Exception {
    List<OperationChain> opChains = new ArrayList<>();
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
      opChains.add(testFramework.startOperationChain(32 * 1024, i, operations));
    }
    testFramework.checkOperationChains(opChains);
  }
}

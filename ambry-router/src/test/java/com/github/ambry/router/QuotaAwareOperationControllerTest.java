/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.SendWithCorrelationId;
import com.github.ambry.quota.Chargeable;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link QuotaAwareOperationController}.
 */
@RunWith(Parameterized.class)
public class QuotaAwareOperationControllerTest {
  private static final String HOST = "HOST";
  private static final Port PORT = new Port(80, PortType.PLAINTEXT);
  private static final SendWithCorrelationId SEND = Mockito.mock(SendWithCorrelationId.class);
  private static final ReplicaId REPLICA_ID = Mockito.mock(ReplicaId.class);
  private static final QuotaResource TEST_QUOTA_RESOURCE = new QuotaResource("test", QuotaResourceType.ACCOUNT);

  private final PutManager putManager = Mockito.mock(PutManager.class);
  private final GetManager getManager = Mockito.mock(GetManager.class);
  private final NonBlockingRouter nonBlockingRouter = Mockito.mock(NonBlockingRouter.class);
  private final DeleteManager deleteManager = Mockito.mock(DeleteManager.class);
  private final TtlUpdateManager ttlUpdateManager = Mockito.mock(TtlUpdateManager.class);
  private final UndeleteManager undeleteManager = Mockito.mock(UndeleteManager.class);
  private final List<RequestInfo> requestsToSend = new LinkedList<>();
  private final Set<Integer> requestsToDrop = new HashSet<>();
  private final QuotaMethod quotaMethod;
  private QuotaAwareOperationController quotaAwareOperationController;

  /**
   * Constructor for {@link QuotaAwareOperationControllerTest}.
   * @param quotaMethod {@link QuotaMethod} to use for the test run.
   */
  public QuotaAwareOperationControllerTest(QuotaMethod quotaMethod) {
    this.quotaMethod = quotaMethod;
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{QuotaMethod.READ}, {QuotaMethod.WRITE}});
  }

  @Before
  public void setupMocks() throws Exception {
    NetworkClientFactory networkClientFactory = mock(NetworkClientFactory.class);
    NetworkClient networkClient = mock(NetworkClient.class);
    when(networkClientFactory.getNetworkClient()).thenReturn(networkClient);
    ClusterMap clusterMap = mock(ClusterMap.class);
    when(clusterMap.getLocalDatacenterId()).thenReturn((byte) 1);
    when(clusterMap.getDatacenterName((byte) 1)).thenReturn("test");
    when(clusterMap.getMetricRegistry()).thenReturn(new MetricRegistry());
    MockDataNodeId mockDataNodeId = new MockDataNodeId(Collections.singletonList(new Port(80, PortType.PLAINTEXT)),
        Collections.singletonList("/a/b"), "test");
    List<MockDataNodeId> dataNodeIds = new ArrayList<>();
    dataNodeIds.add(mockDataNodeId);
    doReturn(dataNodeIds).when(clusterMap).getDataNodeIds();
    when(networkClient.warmUpConnections(anyList(), anyByte(), anyLong(), anyList())).thenReturn(1);
    Properties properties = new Properties();
    properties.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, "test");
    properties.setProperty(RouterConfig.ROUTER_HOSTNAME, "test");
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(properties));
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    quotaAwareOperationController =
        new QuotaAwareOperationController(null, null, null, networkClientFactory, clusterMap, routerConfig, null, null,
            routerMetrics, null, null, null, null, nonBlockingRouter);
    quotaAwareOperationController.putManager.close(); // closing existing put manager before setting mock to clean up the threads.
    FieldSetter.setField(quotaAwareOperationController,
        quotaAwareOperationController.getClass().getSuperclass().getDeclaredField("putManager"), putManager);
    FieldSetter.setField(quotaAwareOperationController,
        quotaAwareOperationController.getClass().getSuperclass().getDeclaredField("getManager"), getManager);
    FieldSetter.setField(quotaAwareOperationController,
        quotaAwareOperationController.getClass().getSuperclass().getDeclaredField("deleteManager"), deleteManager);
    FieldSetter.setField(quotaAwareOperationController,
        quotaAwareOperationController.getClass().getSuperclass().getDeclaredField("undeleteManager"), undeleteManager);
    FieldSetter.setField(quotaAwareOperationController,
        quotaAwareOperationController.getClass().getSuperclass().getDeclaredField("ttlUpdateManager"),
        ttlUpdateManager);
    doNothing().when(getManager).poll(requestsToSend, requestsToDrop);
    doNothing().when(deleteManager).poll(requestsToSend, requestsToDrop);
    doNothing().when(ttlUpdateManager).poll(requestsToSend, requestsToDrop);
    doNothing().when(nonBlockingRouter).initiateBackgroundDeletes(anyList());
  }

  @Test
  public void testSimpleDrainageEmpty() {
    TestChargeable chargeable = new TestChargeable(true, true, false, TEST_QUOTA_RESOURCE, quotaMethod);
    doAnswer((Answer<Void>) invocation -> {
      List<RequestInfo> requestsToSend = (List<RequestInfo>) invocation.getArguments()[0];
      requestsToSend.add(new RequestInfo(HOST, PORT, SEND, REPLICA_ID, chargeable));
      return null;
    }).when(putManager).poll(requestsToSend, requestsToDrop);
    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(0, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
    chargeable.verifyCalls(1, 1, 0, 1);
  }

  @Test
  public void testSimpleDrainageOutOfQuota() {
    TestChargeable testChargeable =
        new TestChargeable(Arrays.asList(false, true, false, true, true), true, false, TEST_QUOTA_RESOURCE,
            Collections.singletonList(quotaMethod));
    doAnswer((Answer<Void>) invocation -> {
      List<RequestInfo> requestsToSend = (List<RequestInfo>) invocation.getArguments()[0];
      requestsToSend.add(new RequestInfo(HOST, PORT, SEND, REPLICA_ID, testChargeable));
      return null;
    }).when(putManager).poll(requestsToSend, requestsToDrop);
    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(1, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
    assertEquals(1, quotaAwareOperationController.getRequestQueue(quotaMethod).get(TEST_QUOTA_RESOURCE).size());
    testChargeable.verifyCalls(1, 0, 1, 1);

    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(1, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
    assertEquals(1, quotaAwareOperationController.getRequestQueue(quotaMethod).get(TEST_QUOTA_RESOURCE).size());
    testChargeable.verifyCalls(3, 1, 2, 2);

    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(0, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
    testChargeable.verifyCalls(4, 2, 2, 2);
  }

  @Test
  public void testSimpleDrainageOutOfQuotaWithExceedAllowed() {
    doAnswer((Answer<Void>) invocation -> {
      List<RequestInfo> requestsToSend = (List<RequestInfo>) invocation.getArguments()[0];
      requestsToSend.add(new RequestInfo(HOST, PORT, SEND, REPLICA_ID,
          new TestChargeable(false, true, true, TEST_QUOTA_RESOURCE, quotaMethod)));
      return null;
    }).when(putManager).poll(requestsToSend, requestsToDrop);
    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(0, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
  }

  @Test
  public void testDrainageForNullResourceIdOnly() {
    doAnswer((Answer<Void>) invocation -> {
      List<RequestInfo> requestsToSend = (List<RequestInfo>) invocation.getArguments()[0];
      requestsToSend.add(new RequestInfo(HOST, PORT, SEND, REPLICA_ID,
          new TestChargeable(false, true, false, null, quotaMethod)));
      return null;
    }).when(putManager).poll(requestsToSend, requestsToDrop);
    quotaAwareOperationController.pollForRequests(requestsToSend, requestsToDrop);
    assertEquals(0, quotaAwareOperationController.getRequestQueue(quotaMethod).size());
  }

  /**
   * {@link Chargeable} implementation for test.
   */
  static class TestChargeable implements Chargeable {
    private final boolean chargeOutput;
    private final boolean quotaExceedAllowed;
    private final QuotaResource quotaResource;
    private final List<Boolean> checkOutputs;
    private final List<QuotaMethod> quotaMethods;
    private int numCheckCalls;
    private int numChargeCalls;
    private int numQuotaExceedCalls;
    private int numGetQuotaResourceCalls;
    private int numGetQuotaMethodCalls;

    /**
     * Constructor for {@link TestChargeable}.
     * @param checkOutput output of check method.
     * @param chargeOutput output of charge method.
     * @param quotaExceedAllowed output of quotaExceedAllowed method.
     * @param quotaResource output of getQuotaResource method.
     * @param quotaMethod output of the getQuotaMethod method.
     */
    public TestChargeable(boolean checkOutput, boolean chargeOutput, boolean quotaExceedAllowed,
        QuotaResource quotaResource, QuotaMethod quotaMethod) {
      this.checkOutputs = Collections.singletonList(checkOutput);
      this.chargeOutput = chargeOutput;
      this.quotaExceedAllowed = quotaExceedAllowed;
      this.quotaResource = quotaResource;
      this.quotaMethods = Collections.singletonList(quotaMethod);
    }

    /**
     * Constructor for {@link TestChargeable}.
     * @param checkOutputs {@link List} representing the sequence of outputs of check method.
     * @param chargeOutput output of charge method.
     * @param quotaExceedAllowed output of quotaExceedAllowed method.
     * @param quotaResource output of getQuotaResource method.
     * @param quotaMethods {@link List} of {@link QuotaMethod}s representing the sequence of output of get quota method calls.
     */
    public TestChargeable(List<Boolean> checkOutputs, boolean chargeOutput, boolean quotaExceedAllowed,
        QuotaResource quotaResource, List<QuotaMethod> quotaMethods) {
      this.checkOutputs = checkOutputs;
      this.chargeOutput = chargeOutput;
      this.quotaExceedAllowed = quotaExceedAllowed;
      this.quotaResource = quotaResource;
      this.quotaMethods = quotaMethods;
    }

    @Override
    public boolean check() {
      boolean out = checkOutputs.get(numCheckCalls % checkOutputs.size());
      numCheckCalls++;
      return out;
    }

    @Override
    public boolean charge() {
      numChargeCalls++;
      return chargeOutput;
    }

    @Override
    public boolean quotaExceedAllowed() {
      numQuotaExceedCalls++;
      return quotaExceedAllowed;
    }

    @Override
    public QuotaResource getQuotaResource() {
      numGetQuotaResourceCalls++;
      return quotaResource;
    }

    @Override
    public QuotaMethod getQuotaMethod() {
      QuotaMethod out = quotaMethods.get(numGetQuotaMethodCalls % quotaMethods.size());
      numGetQuotaMethodCalls++;
      return out;
    }

    /**
     * Verify that the interface methods have been called expected number of times.
     * @param numCheckCalls expected number of check calls.
     * @param numChargeCalls expected number of charge calls.
     * @param numQuotaExceedCalls expected number of quotaExceedAllowed calls.
     * @param numGetQuotaResourceCalls expected number of getQuotaResource calls.
     */
    public void verifyCalls(int numCheckCalls, int numChargeCalls, int numQuotaExceedCalls,
        int numGetQuotaResourceCalls) {
      assertEquals("Invalid charge calls", numChargeCalls, this.numChargeCalls);
      assertEquals("Invalid check calls", numCheckCalls, this.numCheckCalls);
      assertEquals("Invalid quotaExceeded calls", numQuotaExceedCalls, this.numQuotaExceedCalls);
      assertEquals("Invalid getQuotaResource calls", numGetQuotaResourceCalls, this.numGetQuotaResourceCalls);
    }
  }
}

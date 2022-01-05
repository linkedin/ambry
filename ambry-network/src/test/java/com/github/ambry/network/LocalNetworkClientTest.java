/*
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
 *
 */
package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test {@link LocalNetworkClient} and {@link LocalNetworkClientFactory}.
 */
public class LocalNetworkClientTest {

  public static final int POLL_TIMEOUT_MS = 100;
  private int nextCorrelationId = 0;
  private final LocalRequestResponseChannel mockLocalRequestResponseChannel;
  private final LocalNetworkClientFactory factory;
  private final NetworkMetrics networkMetrics;

  public LocalNetworkClientTest() {
    mockLocalRequestResponseChannel = mock(LocalRequestResponseChannel.class);
    networkMetrics = new NetworkMetrics(new MetricRegistry());
    factory = new LocalNetworkClientFactory(mockLocalRequestResponseChannel,
        new NetworkConfig(new VerifiableProperties(new Properties())), networkMetrics, new MockTime());
  }

  /**
   * Test {@link LocalNetworkClientFactory}
   */
  @Test
  public void testFactory() {
    int processorId = 0;
    LocalNetworkClient localNetworkClient1 = factory.getNetworkClient();
    LocalNetworkClient localNetworkClient2 = factory.getNetworkClient();
    assertNotNull("Network client is null", localNetworkClient1);
    assertNotNull("Network client is null", localNetworkClient2);
    assertEquals("Processor id of network client is wrong", processorId++, localNetworkClient1.getProcessorId());
    assertEquals("Processor id of network client is wrong", processorId, localNetworkClient2.getProcessorId());
  }

  /**
   * Test {@link LocalNetworkClient#sendAndPoll}
   */
  @Test
  public void testSendAndPoll() throws Exception {

    NetworkClient localNetworkClient = factory.getNetworkClient();

    // 1. response is received for sent request
    List<RequestInfo> requestInfoList = Collections.singletonList(getRequestInfo());
    List<ResponseInfo> expectedResponseInfoList = Collections.singletonList(
        new ResponseInfo(requestInfoList.get(0), null, requestInfoList.get(0).getReplicaId().getDataNodeId(),
            new SocketRequestResponseChannelTest.MockSend()));
    when(mockLocalRequestResponseChannel.receiveResponses(anyInt(), anyInt())).thenReturn(expectedResponseInfoList);
    List<ResponseInfo> responseInfoList =
        localNetworkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    assertEquals("Unexpected response list", expectedResponseInfoList, responseInfoList);
    verify(mockLocalRequestResponseChannel).sendRequest(argThat(argument -> {
      RequestInfo requestInfo = ((LocalRequestResponseChannel.LocalChannelRequest) argument).getRequestInfo();
      return requestInfo.equals(requestInfoList.get(0));
    }));

    // 2. verify empty response is handled
    resetMocks();
    when(mockLocalRequestResponseChannel.receiveResponses(anyInt(), anyInt())).thenReturn(Collections.emptyList());
    responseInfoList = localNetworkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());

    // 3. When LocalRequestResponseChannel throws exception, metrics should be updated
    resetMocks();
    doThrow(new RuntimeException()).when(mockLocalRequestResponseChannel).sendRequest(any());
    localNetworkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    assertEquals("Expected count of network client exceptions to be 1", 1,
        networkMetrics.networkClientException.getCount());

    // 4. calling sendAndPoll after the network client is closed should throw exception
    resetMocks();
    localNetworkClient.close();
    TestUtils.assertException(IllegalStateException.class,
        () -> localNetworkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS), null);
  }

  /**
   * Test {@link LocalNetworkClient#wakeup}
   */
  @Test
  public void testWakeup() {
    LocalNetworkClient localNetworkClient = factory.getNetworkClient();
    localNetworkClient.wakeup();
    verify(mockLocalRequestResponseChannel).wakeup(eq(localNetworkClient.getProcessorId()));
  }

  /**
   * Test {@link LocalNetworkClient#close}
   */
  @Test
  public void testClose() {
    LocalNetworkClient localNetworkClient = factory.getNetworkClient();
    localNetworkClient.close();
    assertTrue("Network client should be closed", localNetworkClient.isClosed());
  }

  /**
   * @return a new {@link RequestInfo} with a new correlation ID.
   */
  private RequestInfo getRequestInfo() {
    return new RequestInfo("a", new Port(1, PortType.SSL), new MockSend(nextCorrelationId++),
        new MockReplicaId(ReplicaType.CLOUD_BACKED), null);
  }

  private void resetMocks() {
    reset(mockLocalRequestResponseChannel);
  }
}

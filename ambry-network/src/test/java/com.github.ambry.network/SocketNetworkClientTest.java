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
package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Test the {@link SocketNetworkClient}
 */
public class SocketNetworkClientTest {
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 4;
  public static final int POLL_TIMEOUT_MS = 100;
  public static final int TIME_FOR_WARM_UP_MS = 2000;

  private final MockTime time;

  private MockSelector selector;
  private SocketNetworkClient networkClient;
  private NetworkMetrics networkMetrics;
  private String sslHost;
  private Port sslPort;
  private ReplicaId replicaOnSslNode;
  private List<DataNodeId> localPlainTextDataNodes;
  private List<DataNodeId> localSslDataNodes;
  private MockClusterMap sslEnabledClusterMap;
  private MockClusterMap sslDisabledClusterMap;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Test the {@link NetworkClientFactory}
   */
  @Test
  public void testNetworkClientFactory() throws IOException {
    Properties props = new Properties();
    props.setProperty("router.connection.checkout.timeout.ms", "1000");
    VerifiableProperties vprops = new VerifiableProperties(props);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    NetworkMetrics networkMetrics = new NetworkMetrics(new MetricRegistry());
    NetworkClientFactory networkClientFactory =
        new SocketNetworkClientFactory(networkMetrics, networkConfig, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockTime());
    Assert.assertNotNull("SocketNetworkClient returned should be non-null", networkClientFactory.getNetworkClient());
  }

  public SocketNetworkClientTest() throws IOException {
    Properties props = new Properties();
    props.setProperty(NetworkConfig.NETWORK_CLIENT_ENABLE_CONNECTION_REPLENISHMENT, "true");
    VerifiableProperties vprops = new VerifiableProperties(props);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    selector = new MockSelector(networkConfig);
    time = new MockTime();
    networkMetrics = new NetworkMetrics(new MetricRegistry());
    networkClient =
        new SocketNetworkClient(selector, networkConfig, networkMetrics, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, time);
    sslEnabledClusterMap = new MockClusterMap(true, 9, 3, 3, false, false);
    localSslDataNodes = sslEnabledClusterMap.getDataNodeIds()
        .stream()
        .filter(dataNodeId -> sslEnabledClusterMap.getDatacenterName(sslEnabledClusterMap.getLocalDatacenterId())
            .equals(dataNodeId.getDatacenterName()))
        .collect(Collectors.toList());
    sslDisabledClusterMap = new MockClusterMap();
    localPlainTextDataNodes = sslDisabledClusterMap.getDataNodeIds()
        .stream()
        .filter(dataNodeId -> sslDisabledClusterMap.getDatacenterName(sslDisabledClusterMap.getLocalDatacenterId())
            .equals(dataNodeId.getDatacenterName()))
        .collect(Collectors.toList());
    DataNodeId dataNodeId = localSslDataNodes.get(0);
    sslHost = dataNodeId.getHostname();
    sslPort = dataNodeId.getPortToConnectTo();
    replicaOnSslNode = sslEnabledClusterMap.getReplicaIds(dataNodeId).get(0);
  }

  /**
   * Test {@link SocketNetworkClient#warmUpConnections(List, int, long, List)}
   */
  @Test
  public void testWarmUpConnectionsSslAndPlainText() {
    // warm up plain-text connections with SSL enabled nodes.
    doTestWarmUpConnections(localSslDataNodes, MAX_PORTS_PLAIN_TEXT, PortType.PLAINTEXT);
    // enable SSL to local DC.
    for (DataNodeId dataNodeId : localSslDataNodes) {
      ((MockDataNodeId) dataNodeId).setSslEnabledDataCenters(Collections.singletonList(
          sslEnabledClusterMap.getDatacenterName(sslEnabledClusterMap.getLocalDatacenterId())));
    }
    // warm up SSL connections.`
    doTestWarmUpConnections(localSslDataNodes, MAX_PORTS_SSL, PortType.SSL);
  }

  /**
   * Test connection warm up failed case.
   */
  @Test
  public void testWarmUpConnectionsFailedAll() {
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    List<ResponseInfo> responseInfos = new ArrayList<>();
    Assert.assertEquals("Connection count is not expected", 0,
        networkClient.warmUpConnections(localPlainTextDataNodes, 100, TIME_FOR_WARM_UP_MS, responseInfos));
    // verify that the connections to all local nodes get disconnected
    Assert.assertEquals("Mismatch in timeout responses", localPlainTextDataNodes.size() * MAX_PORTS_PLAIN_TEXT,
        responseInfos.size());
    selector.setState(MockSelectorState.Good);
  }

  /**
   * tests basic request sending, polling and receiving responses correctly associated with the requests.
   */
  @Test
  public void testBasicSendAndPoll() {
    DataNodeId dataNodeId = localPlainTextDataNodes.get(0);
    ReplicaId replicaId = sslDisabledClusterMap.getReplicaIds(dataNodeId).get(0);
    List<RequestInfo> requestInfoList = new ArrayList<>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(
        new RequestInfo(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(), new MockSend(1), replicaId));
    requestInfoList.add(
        new RequestInfo(dataNodeId.getHostname(), dataNodeId.getPortToConnectTo(), new MockSend(2), replicaId));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        MockSend send = (MockSend) responseInfo.getRequestInfo().getRequest();
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuf response = responseInfo.content();
        Assert.assertNull("Should not have encountered an error", error);
        Assert.assertNotNull("Should receive a valid response", response);
        int correlationIdInRequest = send.getCorrelationId();
        int correlationIdInResponse = response.readInt();
        Assert.assertEquals("Received response for the wrong request", correlationIdInRequest, correlationIdInResponse);
        responseCount++;
        responseInfo.release();
      }
    } while (requestCount > responseCount);
    Assert.assertEquals("Should receive only as many responses as there were requests", requestCount, responseCount);

    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where requests remain too long in the {@link SocketNetworkClient}'s pending requests queue.
   */
  @Test
  public void testConnectionUnavailable() {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(3), replicaOnSslNode));
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(4), replicaOnSslNode));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    requestInfoList.clear();
    // The first sendAndPoll() initiates the connections. So, after the selector poll, new connections
    // would have been established, but no new responses or disconnects, so the SocketNetworkClient should not have been
    // able to create any ResponseInfos.
    Assert.assertEquals("There are no responses expected", 0, responseInfoList.size());
    // the requests were queued. Now increment the time so that they get timed out in the next sendAndPoll.
    time.sleep(CHECKOUT_TIMEOUT_MS + 1);

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuf response = responseInfo.content();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error",
            NetworkClientErrorCode.ConnectionUnavailable, error);
        Assert.assertNull("Should not have received a valid response", response);
        responseCount++;
        responseInfo.release();
      }
    } while (requestCount > responseCount);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where connections get disconnected after requests are sent out.
   */
  @Test
  public void testNetworkError() {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(5), replicaOnSslNode));
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(6), replicaOnSslNode));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    // set beBad so that requests end up failing due to "network error".
    selector.setState(MockSelectorState.DisconnectOnSend);
    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuf response = responseInfo.content();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error", NetworkClientErrorCode.NetworkError,
            error);
        Assert.assertNull("Should not have received a valid response", response);
        responseCount++;
        responseInfo.release();
      }
    } while (requestCount > responseCount);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test exceptions thrown in sendAndPoll().
   */
  @Test
  public void testExceptionOnConnect() {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    // test that IllegalStateException would be thrown if replica is not specified in RequestInfo
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(-1), null));
    networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals("NetworkClientException should increase because replica is null in request", 1,
        networkMetrics.networkClientException.getCount());
    requestInfoList.clear();
    // test that IOException occurs when selector invokes connect()
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(3), replicaOnSslNode));
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(4), replicaOnSslNode));
    selector.setState(MockSelectorState.ThrowExceptionOnConnect);
    try {
      List<ResponseInfo> responseInfos =
          networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      responseInfos.forEach(ResponseInfo::release);
    } catch (Exception e) {
      Assert.fail("If selector throws on connect, sendAndPoll() should not throw");
    }
  }

  /**
   * Test to ensure two things:
   * 1. If a request comes in and there are no available connections to the destination,
   * only one connection is initiated, even if that connection is found to be pending when the
   * same request is looked at during a subsequent sendAndPoll.
   * 2. For the above situation, if the subsequent pending connection fails, then the request is
   * immediately failed.
   */
  @Test
  public void testConnectionInitializationFailures() {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(0), replicaOnSslNode));
    selector.setState(MockSelectorState.IdlePoll);
    Assert.assertEquals(0, selector.connectCallCount());
    // this sendAndPoll() should initiate a connect().
    List<ResponseInfo> responseInfoList =
        networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    // At this time a single connection would have been initiated for the above request.
    Assert.assertEquals(1, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();

    // Subsequent calls to sendAndPoll() should not initiate any connections.
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(1, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // Another connection should get initialized if a new request comes in for the same destination.
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(1), replicaOnSslNode));
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();

    // Subsequent calls to sendAndPoll() should not initiate any more connections.
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // Once connect failure kicks in, the pending requests should be failed immediately.
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(2, responseInfoList.size());
    Assert.assertEquals(NetworkClientErrorCode.NetworkError, responseInfoList.get(0).getError());
    Assert.assertEquals(NetworkClientErrorCode.NetworkError, responseInfoList.get(1).getError());
    responseInfoList.forEach(ResponseInfo::release);
    responseInfoList.clear();
  }

  /**
   * Test that connections get replenished in {@link SocketNetworkClient#sendAndPoll(List, Set, int)} to maintain the minimum
   * number of active connections.
   */
  @Test
  public void testConnectionReplenishment() {
    AtomicInteger nextCorrelationId = new AtomicInteger(1);
    Function<Integer, List<RequestInfo>> requestGen = numRequests -> IntStream.range(0, numRequests)
        .mapToObj(
            i -> new RequestInfo(sslHost, sslPort, new MockSend(nextCorrelationId.getAndIncrement()), replicaOnSslNode))
        .collect(Collectors.toList());
    // 1 host x 1 port x 3 connections x 100%
    int warmUpPercentage = 100;
    AtomicInteger expectedConnectCalls = new AtomicInteger(warmUpPercentage * 3 / 100);
    Runnable checkConnectCalls = () -> Assert.assertEquals(expectedConnectCalls.get(), selector.connectCallCount());

    networkClient.warmUpConnections(Collections.singletonList(replicaOnSslNode.getDataNodeId()), warmUpPercentage,
        TIME_FOR_WARM_UP_MS, new ArrayList<>());
    checkConnectCalls.run();

    selector.setState(MockSelectorState.Good);
    // 1. this sendAndPoll() should use one of the pre-warmed connections
    List<ResponseInfo> responseInfoList =
        networkClient.sendAndPoll(requestGen.apply(3), Collections.emptySet(), POLL_TIMEOUT_MS);
    checkConnectCalls.run();
    Assert.assertEquals(3, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // 2. this sendAndPoll() should disconnect two of the pre-warmed connections
    selector.setState(MockSelectorState.DisconnectOnSend);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(2), Collections.emptySet(), POLL_TIMEOUT_MS);
    checkConnectCalls.run();
    Assert.assertEquals(2, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // 3. the two connections lost in the previous sendAndPoll should not be replenished yet since a second has not yet
    // passed since startup
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(1), Collections.emptySet(), POLL_TIMEOUT_MS);
    checkConnectCalls.run();
    Assert.assertEquals(1, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // 4. one of the connection lost in sendAndPoll 3 should be replenished
    time.setCurrentMilliseconds(time.milliseconds() + Time.MsPerSec);
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(0), Collections.emptySet(), POLL_TIMEOUT_MS);
    expectedConnectCalls.addAndGet(1);
    checkConnectCalls.run();
    Assert.assertEquals(0, responseInfoList.size());

    // 5. no connections replenished this time since only half a second passed.
    time.setCurrentMilliseconds(time.milliseconds() + 500);
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(0), Collections.emptySet(), POLL_TIMEOUT_MS);
    checkConnectCalls.run();
    Assert.assertEquals(0, responseInfoList.size());

    // 6. the second connection lost in sendAndPoll 3 should be replenished
    time.setCurrentMilliseconds(time.milliseconds() + 500);
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(0), Collections.emptySet(), POLL_TIMEOUT_MS);
    expectedConnectCalls.addAndGet(1);
    checkConnectCalls.run();
    Assert.assertEquals(0, responseInfoList.size());

    // 7. this call should use the existing connections in the pool
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(3), Collections.emptySet(), POLL_TIMEOUT_MS);
    checkConnectCalls.run();
    Assert.assertEquals(3, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);
  }

  /**
   * Test dropping of requests by closing
   */
  @Test
  public void testRequestDropping() {
    AtomicInteger nextCorrelationId = new AtomicInteger(1);
    Function<Integer, List<RequestInfo>> requestGen = numRequests -> IntStream.range(0, numRequests)
        .mapToObj(
            i -> new RequestInfo(sslHost, sslPort, new MockSend(nextCorrelationId.getAndIncrement()), replicaOnSslNode))
        .collect(Collectors.toList());
    List<ResponseInfo> responseInfoList;

    // Drop requests while the requests are waiting for a connection.
    // First poll will require connections to be created, so no responses will be returned.
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(3), Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals("No responses expected in first poll.", 0, responseInfoList.size());
    // Drop requests on the second poll. The requests should be removed from the pending request list as a result.
    responseInfoList =
        networkClient.sendAndPoll(Collections.emptyList(), new HashSet<>(Arrays.asList(2, 3)), POLL_TIMEOUT_MS);
    Assert.assertEquals("Should receive only as many responses as there were requests", 3, responseInfoList.size());
    for (ResponseInfo responseInfo : responseInfoList) {
      MockSend send = (MockSend) responseInfo.getRequestInfo().getRequest();
      if (send.getCorrelationId() == 1) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuf response = responseInfo.content();
        Assert.assertNull("Should not have encountered an error", error);
        Assert.assertNotNull("Should receive a valid response", response);
        int correlationIdInRequest = send.getCorrelationId();
        int correlationIdInResponse = response.readInt();
        Assert.assertEquals("Received response for the wrong request", correlationIdInRequest, correlationIdInResponse);
      } else {
        Assert.assertEquals("Expected connection unavailable on dropped request",
            NetworkClientErrorCode.ConnectionUnavailable, responseInfo.getError());
        Assert.assertNull("Should not receive a response", responseInfo.content());
      }
    }
    responseInfoList.forEach(ResponseInfo::release);

    // Test dropping of requests while the requests are in flight.
    // Set the selector to idle mode to prevent responses from coming back (even though connections are available at
    // this moment in time).
    selector.setState(MockSelectorState.IdlePoll);
    responseInfoList = networkClient.sendAndPoll(requestGen.apply(3), Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals("No responses expected in idle poll.", 0, responseInfoList.size());
    // Set the selector back to normal mode and drop a request. It should be dropped by closing the connection.
    selector.setState(MockSelectorState.Good);
    responseInfoList = networkClient.sendAndPoll(Collections.emptyList(), Collections.singleton(4), POLL_TIMEOUT_MS);
    Assert.assertEquals("Should receive only as many responses as there were requests", 3, responseInfoList.size());
    for (ResponseInfo responseInfo : responseInfoList) {
      MockSend send = (MockSend) responseInfo.getRequestInfo().getRequest();
      if (send.getCorrelationId() != 4) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuf response = responseInfo.content();
        Assert.assertNull("Should not have encountered an error", error);
        Assert.assertNotNull("Should receive a valid response", response);
        int correlationIdInRequest = send.getCorrelationId();
        int correlationIdInResponse = response.readInt();
        Assert.assertEquals("Received response for the wrong request", correlationIdInRequest, correlationIdInResponse);
        responseInfo.release();
      } else {
        Assert.assertEquals("Expected network error (from closed connection for dropped request)",
            NetworkClientErrorCode.NetworkError, responseInfo.getError());
        Assert.assertNull("Should not receive a response", responseInfo.content());
        responseInfo.release();
      }
    }

    // Dropping a request that is not currently pending or in flight should be a no-op.
    responseInfoList = networkClient.sendAndPoll(Collections.emptyList(), Collections.singleton(1), POLL_TIMEOUT_MS);
    Assert.assertEquals("No more responses expected.", 0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);
  }

  /**
   * Test the following case:
   * Connection C1 gets initiated in the context of Request R1
   * Connection C2 gets initiated in the context of Request R2
   * Connection C2 gets established first.
   * Request R1 checks out connection C2 because it is earlier in the queue
   * (although C2 was initiated on behalf of R2)
   * Request R1 gets sent on C2
   * Connection C1 gets disconnected, which was initiated in the context of Request R1
   * Request R1 is completed.
   * Request R2 reuses C1 and gets completed.
   */
  @Test
  public void testOutOfOrderConnectionEstablishment() {
    selector.setState(MockSelectorState.DelayFailAlternateConnect);
    List<RequestInfo> requestInfoList = new ArrayList<>();
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(2), replicaOnSslNode));
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(3), replicaOnSslNode));
    List<ResponseInfo> responseInfoList =
        networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    requestInfoList.clear();
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);

    // Invoke sendAndPoll() again, the Connection C1 will get disconnected
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    // Verify that no more connection is initiated in network client
    Assert.assertEquals(2, selector.connectCallCount());
    // There should be 2 responses, one is success from Request R1, another is from Connection C1 timeout.
    Assert.assertEquals(2, responseInfoList.size());
    Assert.assertNull(responseInfoList.get(1).getError());
    // Verify that Request R1 get sent via Connection C2. Note that 2 is correlation Id of R1
    Assert.assertEquals(2, ((MockSend) responseInfoList.get(1).getRequestInfo().getRequest()).getCorrelationId());
    // Verify the timeout connection event is added into response list, which will be handled by ResponseHandler directly.
    Assert.assertEquals("Mismatch in error code", NetworkClientErrorCode.NetworkError,
        responseInfoList.get(0).getError());
    Assert.assertNull(responseInfoList.get(0).getRequestInfo());
    responseInfoList.forEach(ResponseInfo::release);
    responseInfoList.clear();

    // Invoke sendAndPoll() again, Request R2 will get sent via Connection C2
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(1, responseInfoList.size());
    Assert.assertNull(responseInfoList.get(0).getError());
    // Verify the correlation Id of Request R2
    Assert.assertEquals(3, responseInfoList.get(0).getRequestInfo().getRequest().getCorrelationId());
    responseInfoList.forEach(ResponseInfo::release);
    responseInfoList.clear();
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Tests the case where a pending request for which a connection was initiated times out in the same
   * sendAndPoll cycle in which the connection disconnection is received.
   * @throws Exception
   */
  @Test
  public void testPendingRequestTimeOutWithDisconnection() throws Exception {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    selector.setState(MockSelectorState.IdlePoll);
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(4), replicaOnSslNode));
    List<ResponseInfo> responseInfoList =
        networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList.forEach(ResponseInfo::release);
    requestInfoList.clear();
    // now make the selector return any attempted connections as disconnections.
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    // increment the time so that the request times out in the next cycle.
    time.sleep(2000);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
    // the size of responseInfoList should be 2 because first response comes from dropping request in the queue that
    // waits too long. (This response would be handled by corresponding manager, i.e PutManager, GetManager, etc); second
    // response comes from underlying connection timeout in nioSelector (usually due to remote node is down). This response
    // will be handled by ResponseHandler to mark remote node down immediately.
    Assert.assertEquals("Mismatch in number of responses", 2, responseInfoList.size());
    Assert.assertEquals("Error received in first response should be ConnectionUnavailable",
        NetworkClientErrorCode.ConnectionUnavailable, responseInfoList.get(0).getError());
    Assert.assertEquals("Error received in second response should be NetworkError", NetworkClientErrorCode.NetworkError,
        responseInfoList.get(1).getError());
    responseInfoList.forEach(ResponseInfo::release);
    responseInfoList.clear();
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test exception on poll
   */
  @Test
  public void testExceptionOnPoll() {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(3), replicaOnSslNode));
    requestInfoList.add(new RequestInfo(sslHost, sslPort, new MockSend(4), replicaOnSslNode));
    selector.setState(MockSelectorState.ThrowExceptionOnPoll);
    try {
      List<ResponseInfo> responseInfoList =
          networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      responseInfoList.forEach(ResponseInfo::release);
    } catch (Exception e) {
      Assert.fail("If selector throws on poll, sendAndPoll() should not throw.");
    }
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test that the SocketNetworkClient wakeup wakes up the associated Selector.
   */
  @Test
  public void testWakeup() {
    Assert.assertFalse("Selector should not have been woken up at this point", selector.getAndClearWokenUpStatus());
    networkClient.wakeup();
    Assert.assertTrue("Selector should have been woken up at this point", selector.getAndClearWokenUpStatus());
  }

  /**
   * Test to ensure subsequent operations after a close throw an {@link IllegalStateException}.
   */
  @Test
  public void testClose() {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    networkClient.close();
    try {
      networkClient.sendAndPoll(requestInfoList, Collections.emptySet(), POLL_TIMEOUT_MS);
      Assert.fail("Polling after close should throw");
    } catch (IllegalStateException e) {
    }
  }

  /**
   * Helper function to test {@link SocketNetworkClient#warmUpConnections(List, int, long, List)}. This will build up to 100%
   * pre-warmed connections.
   */
  private void doTestWarmUpConnections(List<DataNodeId> localDataNodeIds, int maxPort, PortType expectedPortType) {
    Assert.assertEquals("Port type is not expected.", expectedPortType,
        localDataNodeIds.get(0).getPortToConnectTo().getPortType());
    Assert.assertEquals("Connection count is not expected", 0,
        networkClient.warmUpConnections(localDataNodeIds, 0, TIME_FOR_WARM_UP_MS, new ArrayList<>()));
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    Assert.assertEquals("Connection count is not expected", 0,
        networkClient.warmUpConnections(localDataNodeIds, 100, TIME_FOR_WARM_UP_MS, new ArrayList<>()));
    selector.setState(MockSelectorState.Good);
    int halfConnections = 50 * maxPort / 100 * localDataNodeIds.size();
    int allConnections = maxPort * localDataNodeIds.size();
    Assert.assertEquals("Connection count is not expected", halfConnections,
        networkClient.warmUpConnections(localDataNodeIds, 50, TIME_FOR_WARM_UP_MS, new ArrayList<>()));
    Assert.assertEquals("Connection count is not expected", allConnections - halfConnections,
        networkClient.warmUpConnections(localDataNodeIds, 100, TIME_FOR_WARM_UP_MS, new ArrayList<>()));
  }
}

/**
 * A mock implementation of the {@link Send} interface that simply stores a correlation id that can be used to
 * identify this request.
 */
class MockSend implements SendWithCorrelationId {
  private final ByteBuffer buf;
  private final int correlationId;
  private final int size;

  /**
   * Construct a MockSend
   * @param correlationId the id associated with this MockSend.
   */
  MockSend(int correlationId) {
    this.correlationId = correlationId;
    buf = ByteBuffer.allocate(16);
    size = 16;
  }

  /**
   * @return the correlation id of this MockSend.
   */
  @Override
  public int getCorrelationId() {
    return correlationId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = channel.write(buf);
    return written;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSendComplete() {
    return buf.remaining() == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long sizeInBytes() {
    return size;
  }

  @Override
  public String toString() {
    return "MockSend{" + "buf=" + buf + ", correlationId=" + correlationId + ", size=" + size + '}';
  }
}

/**
 * A mock implementation of {@link BoundedNettyByteBufReceive} that constructs a buffer with the passed in correlation
 * id.
 */
class MockBoundedNettyByteBufReceive extends BoundedNettyByteBufReceive {

  /**
   * Construct a MockBoundedByteBufferReceive with the given correlation id.
   * @param correlationId the correlation id associated with this object.
   */
  public MockBoundedNettyByteBufReceive(int correlationId) {
    super(ByteBufAllocator.DEFAULT.heapBuffer(16).writeInt(correlationId), (long) 16);
  }
}

/**
 * An enum that reflects the state of the MockSelector.
 */
enum MockSelectorState {
  /**
   * The Good state.
   */
  Good,
  /**
   * A state that causes all connect calls to throw an IOException.
   */
  ThrowExceptionOnConnect,
  /**
   * A state that causes disconnections of connections on which a send is attempted.
   */
  DisconnectOnSend,
  /**
   * A state that causes all poll calls to throw an IOException.
   */
  ThrowExceptionOnPoll,
  /**
   * A state that causes all connections initiated to fail during poll.
   */
  FailConnectionInitiationOnPoll,
  /**
   * A state that simulates inactivity during a poll. The poll itself may do work,
   * but as long as this state is set, calls to connected(), disconnected(), completedReceives() etc.
   * will return empty lists.
   */
  IdlePoll,
  /**
   * Fail every other connect.
   */
  DelayFailAlternateConnect;
}

/**
 * A class that mocks the {@link Selector} and simply queues connection requests and send requests within itself and
 * returns them in the next calls to {@link #connected()} and {@link #completedSends()} calls.
 */
class MockSelector extends Selector {
  private int index;
  private Set<String> connectionIds = new HashSet<>();
  private List<String> connected = new ArrayList<>();
  private List<String> nextConnected = new ArrayList<>();
  private List<String> disconnected = new ArrayList<>();
  private final List<String> closedConnections = new ArrayList<>();
  private final List<String> delayedFailFreshList = new ArrayList<>();
  private final List<String> delayedFailPassedList = new ArrayList<>();
  private List<NetworkSend> sends = new ArrayList<>();
  private List<NetworkReceive> receives = new ArrayList<>();
  private MockSelectorState state = MockSelectorState.Good;
  private boolean wakeUpCalled = false;
  private int connectCallCount = 0;
  private boolean isOpen = true;

  /**
   * Create a MockSelector
   * @throws IOException if {@link Selector} throws.
   */
  MockSelector(NetworkConfig networkConfig) throws IOException {
    super(new NetworkMetrics(new MetricRegistry()), new MockTime(), null, networkConfig);
    super.close();
  }

  /**
   * Set the state of this selector. Based on the state, connect or poll may throw, or all sends will result in
   * disconnections in the poll.
   * @param state the MockSelectorState to set this MockSelector to.
   */
  void setState(MockSelectorState state) {
    this.state = state;
  }

  /**
   * Mocks the connect by simply keeping track of the connection requests to a (host, port)
   * @param address The address to connect to
   * @param sendBufferSize not used.
   * @param receiveBufferSize not used.
   * @param portType {@PortType} which represents the type of connection to establish
   * @return the connection id for the connection.
   */
  @Override
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType)
      throws IOException {
    connectCallCount++;
    if (state == MockSelectorState.ThrowExceptionOnConnect) {
      throw new IOException("Mock connect exception");
    }
    String hostPortString = address.getHostString() + address.getPort() + index++;
    if (state == MockSelectorState.DelayFailAlternateConnect && connectCallCount % 2 != 0) {
      // add this connection to the delayed fail fresh list. These will not be returned as failed in the very
      // next poll (when it is fresh), but the subsequent poll.
      delayedFailFreshList.add(hostPortString);
    } else {
      nextConnected.add(hostPortString);
      connectionIds.add(hostPortString);
    }
    return hostPortString;
  }

  /**
   * Return the number of times connect was called.
   */
  int connectCallCount() {
    return connectCallCount;
  }

  /**
   * Mocks sending and polling. Creates a response for every send to be returned after the next poll,
   * with the correlation id in the Send, unless beBad state is on. If beBad is on,
   * all sends will result in disconnections.
   * @param timeoutMs Ignored.
   * @param sends The list of new sends.
   *
   */
  @Override
  public void poll(long timeoutMs, List<NetworkSend> sends) throws IOException {
    if (state == MockSelectorState.ThrowExceptionOnPoll) {
      throw new IOException("Mock exception on poll");
    }
    disconnected = new ArrayList<>();
    if (state == MockSelectorState.FailConnectionInitiationOnPoll) {
      disconnected.addAll(nextConnected);
      connected = new ArrayList<>();
      nextConnected = new ArrayList<>();
    } else if (state != MockSelectorState.IdlePoll) {
      connected = nextConnected;
      nextConnected = new ArrayList<>();
    }
    disconnected.addAll(delayedFailPassedList);
    delayedFailPassedList.clear();
    delayedFailPassedList.addAll(delayedFailFreshList);
    delayedFailFreshList.clear();
    disconnected.addAll(closedConnections);
    this.sends = sends;
    if (sends != null) {
      for (NetworkSend send : sends) {
        MockSend mockSend = (MockSend) send.getPayload();
        if (state == MockSelectorState.DisconnectOnSend) {
          disconnected.add(send.getConnectionId());
        } else if (!closedConnections.contains(send.getConnectionId())) {
          receives.add(new NetworkReceive(send.getConnectionId(),
              new MockBoundedNettyByteBufReceive(mockSend.getCorrelationId()), new MockTime()));
        }
      }
    }
    closedConnections.clear();
  }

  /**
   * Returns a list of connection ids created between the last two poll() calls (or since instantiation if only one
   * {@link #poll(long, List)} was done).
   * @return a list of connection ids.
   */
  @Override
  public List<String> connected() {
    if (state == MockSelectorState.IdlePoll) {
      return new ArrayList<>();
    }
    return connected;
  }

  /**
   * Returns a list of connection ids destroyed between the last two poll() calls.
   * @return a list of connection ids.
   */
  @Override
  public List<String> disconnected() {
    if (state == MockSelectorState.IdlePoll) {
      return new ArrayList<>();
    }
    return disconnected;
  }

  /**
   * Returns a list of {@link NetworkSend} sent as part of the last poll.
   * @return a lit of {@link NetworkSend} initiated previously.
   */
  @Override
  public List<NetworkSend> completedSends() {
    if (state == MockSelectorState.IdlePoll) {
      return new ArrayList<>();
    }
    List<NetworkSend> toReturn = sends;
    sends = new ArrayList<>();
    return toReturn;
  }

  /**
   * Returns a list of {@link NetworkReceive} constructed in the last poll to simulate a response for every send.
   * @return a list of {@line NetworkReceive} for every initiated send.
   */
  @Override
  public List<NetworkReceive> completedReceives() {
    if (state == MockSelectorState.IdlePoll) {
      return new ArrayList<>();
    }
    List<NetworkReceive> toReturn = receives;
    receives = new ArrayList<>();
    return toReturn;
  }

  /**
   * Return whether wakeup() was called and clear the woken up status before returning.
   * @return true if wakeup() was called previously.
   */
  boolean getAndClearWokenUpStatus() {
    boolean ret = wakeUpCalled;
    wakeUpCalled = false;
    return ret;
  }

  /**
   * wakes up the MockSelector if sleeping.
   */
  @Override
  public void wakeup() {
    wakeUpCalled = true;
  }

  /**
   * Close the given connection.
   * @param conn connection id to close.
   */
  @Override
  public void close(String conn) {
    if (connectionIds.contains(conn)) {
      closedConnections.add(conn);
      receives.removeIf((receive) -> {
        boolean r = conn.equals(receive.getConnectionId());
        if (r) {
          receive.getReceivedBytes().release();
        }
        return r;
      });
    }
  }

  /**
   * Close the MockSelector.
   */
  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Check whether the MockSelector is open.
   * @return true, if the MockSelector is open.
   */
  @Override
  public boolean isOpen() {
    return isOpen;
  }
}

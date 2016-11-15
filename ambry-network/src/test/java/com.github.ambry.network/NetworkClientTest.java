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
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test the {@link NetworkClient}
 */
public class NetworkClientTest {
  private final int CHECKOUT_TIMEOUT_MS = 1000;
  private final int MAX_PORTS_PLAIN_TEXT = 3;
  private final int MAX_PORTS_SSL = 3;
  private final Time time;

  MockSelector selector;
  NetworkClient networkClient;
  String host1 = "host1";
  Port port1 = new Port(2222, PortType.PLAINTEXT);
  String host2 = "host2";
  Port port2 = new Port(3333, PortType.SSL);

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
        new NetworkClientFactory(networkMetrics, networkConfig, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockTime());
    Assert.assertNotNull("NetworkClient returned should be non-null", networkClientFactory.getNetworkClient());
  }

  public NetworkClientTest() throws IOException {
    Properties props = new Properties();
    VerifiableProperties vprops = new VerifiableProperties(props);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    selector = new MockSelector();
    time = new MockTime();
    networkClient =
        new NetworkClient(selector, networkConfig, new NetworkMetrics(new MetricRegistry()), MAX_PORTS_PLAIN_TEXT,
            MAX_PORTS_SSL, CHECKOUT_TIMEOUT_MS, time);
  }

  /**
   * tests basic request sending, polling and receiving responses correctly associated with the requests.
   */
  @Test
  public void testBasicSendAndPoll() throws IOException {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(new RequestInfo(host1, port1, new MockSend(1)));
    requestInfoList.add(new RequestInfo(host1, port1, new MockSend(2)));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        MockSend send = (MockSend) responseInfo.getRequestInfo().getRequest();
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNull("Should not have encountered an error", error);
        Assert.assertNotNull("Should receive a valid response", response);
        int correlationIdInRequest = send.getCorrelationId();
        int correlationIdInResponse = response.getInt();
        Assert.assertEquals("Received response for the wrong request", correlationIdInRequest, correlationIdInResponse);
        responseCount++;
      }
    } while (requestCount > responseCount);
    Assert.assertEquals("Should receive only as many responses as there were requests", requestCount, responseCount);

    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where requests remain too long in the {@link NetworkClient}'s pending requests queue.
   */
  @Test
  public void testConnectionUnavailable() throws IOException, InterruptedException {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(3)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(4)));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    requestInfoList.clear();
    // The first sendAndPoll() initiates the connections. So, after the selector poll, new connections
    // would have been established, but no new responses or disconnects, so the NetworkClient should not have been
    // able to create any ResponseInfos.
    Assert.assertEquals("There are no responses expected", 0, responseInfoList.size());
    // the requests were queued. Now increment the time so that they get timed out in the next sendAndPoll.
    time.sleep(CHECKOUT_TIMEOUT_MS + 1);

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error",
            NetworkClientErrorCode.ConnectionUnavailable, error);
        Assert.assertNull("Should not have received a valid response", response);
        responseCount++;
      }
    } while (requestCount > responseCount);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where connections get disconnected after requests are sent out.
   */
  @Test
  public void testNetworkError() throws IOException, InterruptedException {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(5)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(6)));
    int requestCount = requestInfoList.size();
    int responseCount = 0;

    // set beBad so that requests end up failing due to "network error".
    selector.setState(MockSelectorState.DisconnectOnSend);
    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error", NetworkClientErrorCode.NetworkError,
            error);
        Assert.assertNull("Should not have received a valid response", response);
        responseCount++;
      }
    } while (requestCount > responseCount);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test exception on connect
   */
  @Test
  public void testExceptionOnConnect() {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(3)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(4)));
    selector.setState(MockSelectorState.ThrowExceptionOnConnect);
    try {
      networkClient.sendAndPoll(requestInfoList, 100);
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
  public void testConnectionInitializationFailures() throws Exception {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(0)));
    selector.setState(MockSelectorState.IdlePoll);
    Assert.assertEquals(0, selector.connectCallCount());
    // this sendAndPoll() should initiate a connect().
    List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    // At this time a single connection would have been initiated for the above request.
    Assert.assertEquals(1, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    requestInfoList.clear();

    // Subsequent calls to sendAndPoll() should not initiate any connections.
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(1, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());

    // Another connection should get initialized if a new request comes in for the same destination.
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(1)));
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    requestInfoList.clear();

    // Subsequent calls to sendAndPoll() should not initiate any more connections.
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());

    // Once connect failure kicks in, the pending requests should be failed immediately.
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(2, responseInfoList.size());
    Assert.assertEquals(NetworkClientErrorCode.NetworkError, responseInfoList.get(0).getError());
    Assert.assertEquals(NetworkClientErrorCode.NetworkError, responseInfoList.get(1).getError());
    responseInfoList.clear();
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
   *
   * @throws Exception
   */
  @Test
  public void testOutOfOrderConnectionEstablishment() throws Exception {
    selector.setState(MockSelectorState.DelayFailAlternateConnect);
    List<RequestInfo> requestInfoList = new ArrayList<>();
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(2)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(3)));
    List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    requestInfoList.clear();
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(0, responseInfoList.size());
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(1, responseInfoList.size());
    Assert.assertEquals(null, responseInfoList.get(0).getError());
    Assert.assertEquals(2, ((MockSend) responseInfoList.get(0).getRequestInfo().getRequest()).getCorrelationId());
    responseInfoList.clear();
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(2, selector.connectCallCount());
    Assert.assertEquals(1, responseInfoList.size());
    Assert.assertEquals(null, responseInfoList.get(0).getError());
    Assert.assertEquals(3, ((MockSend) responseInfoList.get(0).getRequestInfo().getRequest()).getCorrelationId());
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
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(4)));
    List<ResponseInfo> responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(0, responseInfoList.size());
    requestInfoList.clear();
    // now make the selector return any attempted connections as disconnections.
    selector.setState(MockSelectorState.FailConnectionInitiationOnPoll);
    // increment the time so that the request times out in the next cycle.
    time.sleep(2000);
    responseInfoList = networkClient.sendAndPoll(requestInfoList, 100);
    Assert.assertEquals(1, responseInfoList.size());
    Assert.assertEquals("Error received should be ConnectionUnavailable", NetworkClientErrorCode.ConnectionUnavailable,
        responseInfoList.get(0).getError());
    responseInfoList.clear();
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test exception on poll
   */
  @Test
  public void testExceptionOnPoll() {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(3)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(4)));
    selector.setState(MockSelectorState.ThrowExceptionOnPoll);
    try {
      networkClient.sendAndPoll(requestInfoList, 100);
    } catch (Exception e) {
      Assert.fail("If selector throws on poll, sendAndPoll() should not throw.");
    }
    selector.setState(MockSelectorState.Good);
  }

  /**
   * Test that the NetworkClient wakeup wakes up the associated Selector.
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
  public void testClose() throws IOException {
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    networkClient.close();
    try {
      networkClient.sendAndPoll(requestInfoList, 100);
      Assert.fail("Polling after close should throw");
    } catch (IllegalStateException e) {
    }
  }
}

/**
 * A mock implementation of the {@link Send} interface that simply stores a correlation id that can be used to
 * identify this request.
 */
class MockSend implements Send {
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
  int getCorrelationId() {
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
}

/**
 * A mock implementation of {@link BoundedByteBufferReceive} that constructs a buffer with the passed in correlation
 * id and returns that buffer as part of {@link #getPayload()}.
 */
class MockBoundedByteBufferReceive extends BoundedByteBufferReceive {
  private final ByteBuffer buf;

  /**
   * Construct a MockBoundedByteBufferReceive with the given correlation id.
   * @param correlationId the correlation id associated with this object.
   */
  public MockBoundedByteBufferReceive(int correlationId) {
    buf = ByteBuffer.allocate(16);
    buf.putInt(0, correlationId);
    buf.rewind();
  }

  /**
   * Return the buffer associated with this object.
   * @return the buffer associated with this object.
   */
  @Override
  public ByteBuffer getPayload() {
    return buf;
  }
}

/**
 * An enum that reflects the state of the MockSelector.
 */
enum MockSelectorState {
  /**
   * The Good state.
   */
  Good, /**
   * A state that causes all connect calls to throw an IOException.
   */
  ThrowExceptionOnConnect, /**
   * A state that causes disconnections of connections on which a send is attempted.
   */
  DisconnectOnSend, /**
   * A state that causes all poll calls to throw an IOException.
   */
  ThrowExceptionOnPoll, /**
   * A state that causes all connections initiated to fail during poll.
   */
  FailConnectionInitiationOnPoll, /**
   * A state that simulates inactivity during a poll. The poll itself may do work,
   * but as long as this state is set, calls to connected(), disconnected(), completedReceives() etc.
   * will return empty lists.
   */
  IdlePoll, /**
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
  private Set<String> connectionIds = new HashSet<String>();
  private List<String> connected = new ArrayList<String>();
  private List<String> disconnected = new ArrayList<String>();
  private final List<String> delayedFailFreshList = new ArrayList<>();
  private final List<String> delayedFailPassedList = new ArrayList<>();
  private List<NetworkSend> sends = new ArrayList<NetworkSend>();
  private List<NetworkReceive> receives = new ArrayList<NetworkReceive>();
  private MockSelectorState state = MockSelectorState.Good;
  private boolean wakeUpCalled = false;
  private int connectCallCount = 0;
  private boolean isOpen = true;

  /**
   * Create a MockSelector
   * @throws IOException if {@link Selector} throws.
   */
  MockSelector() throws IOException {
    super(new NetworkMetrics(new MetricRegistry()), new MockTime(), null);
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
      connected.add(hostPortString);
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
    if (state == MockSelectorState.FailConnectionInitiationOnPoll) {
      for (String connId : connected) {
        disconnected.add(connId);
      }
      connected.clear();
    }
    for (String connId : delayedFailPassedList) {
      disconnected.add(connId);
    }
    delayedFailPassedList.clear();
    for (String connId : delayedFailFreshList) {
      delayedFailPassedList.add(connId);
    }
    delayedFailFreshList.clear();
    this.sends = sends;
    if (sends != null) {
      for (NetworkSend send : sends) {
        MockSend mockSend = (MockSend) send.getPayload();
        if (state == MockSelectorState.DisconnectOnSend) {
          disconnected.add(send.getConnectionId());
        } else {
          receives.add(
              new NetworkReceive(send.getConnectionId(), new MockBoundedByteBufferReceive(mockSend.getCorrelationId()),
                  new MockTime()));
        }
      }
    }
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
    List<String> toReturn = connected;
    connected = new ArrayList<String>();
    return toReturn;
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
    List<String> toReturn = disconnected;
    disconnected = new ArrayList<String>();
    return toReturn;
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
    sends = new ArrayList<NetworkSend>();
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
    receives = new ArrayList<NetworkReceive>();
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
      disconnected.add(conn);
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

/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link NettyServerRequestResponseChannel}.
 */
public class NettyServerRequestResponseChannelTest {
  private final static int TIMEOUT = 500;
  private final static int CAPACITY = 2;

  /**
   * Test send and receive requests
   * @throws Exception
   */
  @Test
  public void testSendAndReceiveRequest() throws Exception {
    Properties properties = new Properties();
    RequestResponseChannel channel =
        new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            null);

    channel.sendRequest(createNettyServerRequest(13));
    NetworkRequest request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    assertEquals(13, ((NettyServerRequest) request).content().readableBytes());

    channel.sendRequest(createEmptyNettyServerRequest());
    channel.sendRequest(createNettyServerRequest(19));
    channel.sendRequest(createNettyServerRequest(23));
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    assertEquals(0, ((NettyServerRequest) request).content().readableBytes());
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    assertEquals(19, ((NettyServerRequest) request).content().readableBytes());
    request = channel.receiveRequest();
    Assert.assertTrue(request instanceof NettyServerRequest);
    assertEquals(23, ((NettyServerRequest) request).content().readableBytes());
  }

  @Test
  public void testNoRejectRequests() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            null));
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    // 1. Verify rejectRequest() is not invoked
    verify(channel, never()).rejectRequest(any(), anyBoolean());
  }

  @Test
  public void testRejectRequestsOnOverFlow() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            requestResponseHelper));
    doNothing().when(channel).rejectRequest(any(), anyBoolean());
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    // 1. Verify rejectRequest() is invoked for last request
    NettyServerRequest overFlowRequest = createNettyServerRequest(1);
    channel.sendRequest(overFlowRequest);
    verify(channel, times(1)).rejectRequest(eq(overFlowRequest), eq(false));
  }

  @Test
  public void testRejectRequestsOnExpiry() throws InterruptedException {
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(TIMEOUT, mockTime, CAPACITY);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new Http2ServerMetrics(new MetricRegistry()),
            new ServerMetrics(new MetricRegistry(), this.getClass()), requestResponseHelper, requestQueue));
    doNothing().when(channel).rejectRequest(any(), anyBoolean());
    // Queue 2 requests with a sleep between them so that 1st request expires
    NettyServerRequest expiredRequest = createNettyServerRequest(mockTime.milliseconds());
    channel.sendRequest(expiredRequest);
    mockTime.sleep(TIMEOUT + 1);
    NettyServerRequest validRequest = createNettyServerRequest(mockTime.milliseconds());
    channel.sendRequest(validRequest);
    // 1. Verify received request is 2nd request
    NetworkRequest receiveRequest = channel.receiveRequest();
    assertEquals("Mismatch in request dequeued", validRequest, receiveRequest);
    // 2. Verify rejectRequest() is invoked with 1st request
    verify(channel, times(1)).rejectRequest(eq(expiredRequest), eq(true));
  }

  @Test
  public void testRejectRequestsOnOverFlowWithException() throws IOException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            requestResponseHelper));
    doNothing().when(channel).sendResponse(any(), any(), any());
    when(requestResponseHelper.getDecodedRequest(any())).thenThrow(
        new UnsupportedOperationException("Request type not supported"));
    try {
      channel.sendRequest(createNettyServerRequest(1));
      channel.sendRequest(createNettyServerRequest(1));
      // 1. Verify rejectRequest() is invoked for last request
      NettyServerRequest overFlowRequest = createNettyServerRequest(1);
      channel.sendRequest(overFlowRequest);
      verify(channel, times(1)).rejectRequest(eq(overFlowRequest), eq(false));
      // 2. Verify close connection is invoked due to exception thrown and caught inside rejectRequest()
      verify(channel, times(1)).closeConnection(any());
    } catch (Exception e) {
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testRejectRequestsOnExpiryWithException() throws IOException, InterruptedException {
    ServerRequestResponseHelper requestResponseHelper = mock(ServerRequestResponseHelper.class);
    MockTime mockTime = new MockTime();
    FifoNetworkRequestQueue requestQueue = new FifoNetworkRequestQueue(TIMEOUT, mockTime, CAPACITY);
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new Http2ServerMetrics(new MetricRegistry()),
            new ServerMetrics(new MetricRegistry(), this.getClass()), requestResponseHelper, requestQueue));
    doNothing().when(channel).sendResponse(any(), any(), any());
    when(requestResponseHelper.getDecodedRequest(any())).thenThrow(
        new UnsupportedOperationException("Request type not supported"));
    try {
      // Queue 2 requests with a sleep between them so that 1st request expires
      NettyServerRequest expiredRequest = createNettyServerRequest(mockTime.milliseconds());
      channel.sendRequest(expiredRequest);
      mockTime.sleep(TIMEOUT + 1);
      NettyServerRequest validRequest = createNettyServerRequest(mockTime.milliseconds());
      channel.sendRequest(validRequest);
      // 1. Verify received request is 2nd request
      NetworkRequest receiveRequest = channel.receiveRequest();
      assertEquals("Mismatch in request dequeued", validRequest, receiveRequest);
      // 2. Verify rejectRequest() is invoked with 1st request
      verify(channel, times(1)).rejectRequest(eq(expiredRequest), eq(true));
      // Verify close connection is invoked due to exception thrown and caught in rejectRequests()
      verify(channel, times(1)).closeConnection(any());
    } catch (Exception e) {
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testRejectRequestErrorCode() throws IOException, InterruptedException {
    // Create a delete network request
    MockClusterMap clusterMap = new MockClusterMap();
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    BlobId id1 = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMap.UNKNOWN_DATACENTER_ID, accountId, containerId,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    long deletionTimeMs = Utils.getRandomLong(TestUtils.RANDOM, Long.MAX_VALUE);
    int correlationId = TestUtils.RANDOM.nextInt();
    String clientId = "clientId";
    DeleteRequest deleteRequest;
    deleteRequest = new DeleteRequest(correlationId, clientId, id1, deletionTimeMs);
    DataInputStream requestStream = serAndPrepForRead(deleteRequest);
    MockRequest mockRequest = new MockRequest(requestStream);

    // Create NettyServerRequestResponseChannel
    ServerRequestResponseHelper requestResponseHelper = new ServerRequestResponseHelper(clusterMap, null);
    Properties properties = new Properties();
    properties.setProperty(NetworkConfig.REQUEST_QUEUE_CAPACITY, "2");
    NettyServerRequestResponseChannel channel =
        spy(new NettyServerRequestResponseChannel(new NetworkConfig(new VerifiableProperties(properties)),
            new Http2ServerMetrics(new MetricRegistry()), new ServerMetrics(new MetricRegistry(), this.getClass()),
            requestResponseHelper));
    doNothing().when(channel).sendResponse(any(), any(), any());

    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(createNettyServerRequest(1));
    channel.sendRequest(mockRequest);
    // 1.Verify rejectRequest() is invoked
    verify(channel, times(1)).rejectRequest(eq(mockRequest), eq(false));
    // 2. Verify send response is called
    ArgumentCaptor<DeleteResponse> argument = ArgumentCaptor.forClass(DeleteResponse.class);
    verify(channel, atLeastOnce()).sendResponse(argument.capture(), any(), any());
    // 3. Verify error code is correct
    assertEquals("Mismatch in response type", argument.getValue().getRequestType(),
        RequestOrResponseType.DeleteResponse);
    assertEquals("Mismatch in response correlation id", argument.getValue().getCorrelationId(), correlationId);
    assertEquals("Mismatch in response client id", argument.getValue().getClientId(), clientId);
    assertEquals("Mismatch in error code", argument.getValue().getError(), ServerErrorCode.RetryAfterBackoff);
  }

  private NettyServerRequest createNettyServerRequest(int len) {
    byte[] array = new byte[len];
    TestUtils.RANDOM.nextBytes(array);
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }

  private NettyServerRequest createNettyServerRequest(long creationTime) {
    byte[] array = new byte[1];
    TestUtils.RANDOM.nextBytes(array);
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content, creationTime);
  }

  private NettyServerRequest createEmptyNettyServerRequest() {
    byte[] array = new byte[0];
    ByteBuf content = Unpooled.wrappedBuffer(array);
    return new NettyServerRequest(null, content);
  }

  private DataInputStream serAndPrepForRead(RequestOrResponse requestOrResponse) throws IOException {
    DataInputStream stream;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    do {
      ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) requestOrResponse.sizeInBytes()));
      requestOrResponse.writeTo(channel);
      ByteBuffer underlyingBuf = channel.getBuffer();
      underlyingBuf.flip();
      outputStream.write(underlyingBuf.array(), underlyingBuf.arrayOffset(), underlyingBuf.remaining());
    } while (!requestOrResponse.isSendComplete());
    stream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    // read length
    stream.readLong();
    return stream;
  }

  /**
   * Implementation of {@link NetworkRequest} to help with tests.
   */
  private static class MockRequest implements NetworkRequest {

    private final InputStream stream;

    public MockRequest(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream getInputStream() {
      return stream;
    }

    @Override
    public long getStartTimeInMs() {
      return 0;
    }
  }
}

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
package com.github.ambry.router;

import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.compression.Compression;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.LongStream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class PutOperationTest {
  private final RouterConfig routerConfig;
  private final MockClusterMap mockClusterMap = new MockClusterMap();
  private final NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, null);
  private final Time time;
  private final Map<Integer, PutOperation> correlationIdToPutOperation = new TreeMap<>();
  private final MockServer mockServer = new MockServer(mockClusterMap, "");
  private final RequestRegistrationCallback<PutOperation> requestRegistrationCallback =
      new RequestRegistrationCallback<>(correlationIdToPutOperation);
  private final QuotaChargeCallback quotaChargeCallback = QuotaTestUtils.createTestQuotaChargeCallback();
  private final int chunkSize = 10;
  private final int requestParallelism = 3;
  private final int successTarget = 1;
  private final Random random = new Random();
  private final CompressionService compressionService;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  public PutOperationTest() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    properties.setProperty("router.compression.enabled", "true");
    properties.setProperty("router.compression.minimal.ratio", "1.0");
    properties.setProperty("router.compression.minimal.content.size", "1");
    VerifiableProperties vProps = new VerifiableProperties(properties);
    routerConfig = new RouterConfig(vProps);
    time = new MockTime();
    compressionService = new CompressionService(routerConfig.getCompressionConfig(), routerMetrics.compressionMetrics);
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Ensure that if any of the requests associated with the buffer of a PutChunk is not completely read out even
   * after the associated chunk is complete, the buffer is not reused even though the PutChunk is reused.
   */
  @Test
  public void testSendIncomplete() throws Exception {
    int numChunks = routerConfig.routerMaxInMemPutChunks + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, null, time, blobProperties,
            MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    op.startOperation();
    List<RequestInfo> requestInfos = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestInfos);
    // Since this channel is in memory, one call to fill chunks would end up filling the maximum number of PutChunks.
    op.fillChunks();
    Assert.assertTrue("ReadyForPollCallback should have been invoked as chunks were fully filled",
        mockNetworkClient.getAndClearWokenUpStatus());
    // A poll should therefore return requestParallelism number of requests from each chunk
    op.poll(requestRegistrationCallback);
    Assert.assertEquals(routerConfig.routerMaxInMemPutChunks * requestParallelism, requestInfos.size());

    // There are routerMaxInMemPutChunks + 1 data chunks for this blob (and a metadata chunk).
    // Once the first chunk is completely sent out, the first PutChunk will be reused. What the test verifies is that
    // the buffer of the first PutChunk does not get reused. It does this as follows:
    // For the first chunk,
    // 1. use first request to succeed the chunk (the successTarget is set to 1).
    // 2. read and store from the second for comparing later.
    // 3. read from the third after the first PutChunk gets reused and ensure that the data from the third is the
    //    same as from what was saved off from the second. This means that the buffer was not reused by the first
    //    PutChunk.

    // 1.
    ResponseInfo responseInfo = getResponseInfo(requestInfos.get(0));
    PutResponse putResponse =
        responseInfo.getError() == null ? PutResponse.readFrom(new NettyByteBufDataInputStream(responseInfo.content()))
            : null;
    op.handleResponse(responseInfo, putResponse);
    requestInfos.get(0).getRequest().release();
    responseInfo.release();
    // 2.
    PutRequest putRequest = (PutRequest) requestInfos.get(1).getRequest();
    ByteBuffer buf = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(buf);
    // read it out (which also marks this request as complete).
    putRequest.writeTo(bufChannel);
    putRequest.release();
    byte[] expectedRequestContent = buf.array();

    //3.
    // first save the third request
    PutRequest savedRequest = (PutRequest) requestInfos.get(2).getRequest();

    // succeed all the other requests.
    for (int i = 3; i < requestInfos.size(); i++) {
      responseInfo = getResponseInfo(requestInfos.get(i));
      putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfo.content())) : null;
      op.handleResponse(responseInfo, putResponse);
      requestInfos.get(i).getRequest().release();
      responseInfo.release();
    }
    // fill the first PutChunk with the last chunk.
    op.fillChunks();
    // Verify that the last chunk was filled.
    requestInfos.clear();
    op.poll(requestRegistrationCallback);
    Assert.assertEquals(1 * requestParallelism, requestInfos.size());

    // Verify that the buffer of the third request is not affected.
    buf = ByteBuffer.allocate((int) savedRequest.sizeInBytes());
    bufChannel = new ByteBufferChannel(buf);
    savedRequest.writeTo(bufChannel);
    savedRequest.release();
    byte[] savedRequestContent = buf.array();

    // reset the correlation id as they will be different between the two requests.
    resetCorrelationId(expectedRequestContent);
    resetCorrelationId(savedRequestContent);
    Assert.assertArrayEquals("Underlying buffer should not have be reused", expectedRequestContent,
        savedRequestContent);

    // now that all the requests associated with the original buffer have been read,
    // the next poll will free this buffer. We cannot actually verify it via the tests directly, as this is very
    // internal to the chunk (though this can be verified via coverage).
    for (int i = 0; i < requestInfos.size(); i++) {
      responseInfo = getResponseInfo(requestInfos.get(i));
      putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfo.content())) : null;
      op.handleResponse(responseInfo, putResponse);
      requestInfos.get(i).getRequest().release();
      responseInfo.release();
    }
    requestInfos.clear();
    // this should return requests for the metadata chunk
    op.poll(requestRegistrationCallback);
    Assert.assertEquals(1 * requestParallelism, requestInfos.size());
    Assert.assertFalse("Operation should not be complete yet", op.isOperationComplete());
    // once the metadata request succeeds, it should complete the operation.
    responseInfo = getResponseInfo(requestInfos.get(0));
    putResponse =
        responseInfo.getError() == null ? PutResponse.readFrom(new NettyByteBufDataInputStream(responseInfo.content()))
            : null;
    op.handleResponse(responseInfo, putResponse);
    responseInfo.release();
    requestInfos.forEach(info -> info.getRequest().release());
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    Assert.assertEquals("Metrics should show creation of metadata chunk", 1,
        routerMetrics.metadataChunkCreationCount.getCount());
  }

  /**
   * Test the case when there is an empty bytebuf in the end of the {@link ReadableByteChannel}, make sure that
   * setting operation completed to true wouldn't end up with NPE.
   * @throws Exception
   */
  @Test
  public void testEmptyByteBufInReadableStreamChannel() throws Exception {
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content), true);
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, null, time, blobProperties,
            MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    op.startOperation();
    // Calling fillChunks would fetch the buffer chunk and the empty chunk from the channel
    op.fillChunks();
    // Now fail the operation
    op.setOperationCompleted();
    // Calling fillChunks again, this would force fillChunk to release the empty chunk
    op.fillChunks();
    // Now call fillChunk again
    op.fillChunks();
    Assert.assertNull(op.getOperationException());
  }

  /**
   * Test PUT operation that handles ServerErrorCode = Temporarily_Disabled and Replica_Unavailable
   * @throws Exception
   */
  @Test
  public void testHandleResponseWithServerErrors() throws Exception {
    int numChunks = routerConfig.routerMaxInMemPutChunks + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, new FutureResult<>(),
            null, new RouterCallback(new MockNetworkClient(), new ArrayList<>()), null, null, null, null, time,
            blobProperties, MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    op.startOperation();
    List<RequestInfo> requestInfos = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestInfos);
    // fill chunks would end up filling the maximum number of PutChunks.
    op.fillChunks();
    // poll to populate request
    op.poll(requestRegistrationCallback);
    // make 1st request of first chunk encounter Temporarily_Disabled
    mockServer.setServerErrorForAllRequests(ServerErrorCode.Temporarily_Disabled);
    ResponseInfo responseInfo = getResponseInfo(requestInfos.get(0));
    PutResponse putResponse =
        responseInfo.getError() == null ? PutResponse.readFrom(new NettyByteBufDataInputStream(responseInfo.content()))
            : null;
    op.handleResponse(responseInfo, putResponse);
    responseInfo.release();
    PutOperation.PutChunk putChunk = op.getPutChunks().get(0);
    SimpleOperationTracker operationTracker = (SimpleOperationTracker) putChunk.getOperationTrackerInUse();
    Assert.assertEquals("Disabled count should be 1", 1, operationTracker.getDisabledCount());
    Assert.assertEquals("Disabled count should be 0", 0, operationTracker.getFailedCount());
    // make 2nd request of first chunk encounter Replica_Unavailable
    mockServer.setServerErrorForAllRequests(ServerErrorCode.Replica_Unavailable);
    responseInfo = getResponseInfo(requestInfos.get(1));
    putResponse =
        responseInfo.getError() == null ? PutResponse.readFrom(new NettyByteBufDataInputStream(responseInfo.content()))
            : null;
    op.handleResponse(responseInfo, putResponse);
    responseInfo.release();
    putChunk = op.getPutChunks().get(0);
    Assert.assertEquals("Failure count should be 1", 1,
        ((SimpleOperationTracker) putChunk.getOperationTrackerInUse()).getFailedCount());
    mockServer.resetServerErrors();
    Assert.assertEquals("Metrics should show no metadata chunk was created", 0,
        routerMetrics.metadataChunkCreationCount.getCount());

    // Release all the other requests
    requestInfos.forEach(info -> info.getRequest().release());
  }

  /**
   * Test PUT operation with quota rejected errors.
   * @throws Exception
   */
  @Test
  public void testHandleQuotaRejectedErrors() throws Exception {
    int numChunks = routerConfig.routerMaxInMemPutChunks + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, new FutureResult<>(),
            null, new RouterCallback(new MockNetworkClient(), new ArrayList<>()), null, null, null, null, time,
            blobProperties, MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    op.startOperation();
    List<RequestInfo> requestInfos = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestInfos);
    // fill chunks would end up filling the maximum number of PutChunks.
    op.fillChunks();
    // poll to populate request
    op.poll(requestRegistrationCallback);
    // make 1st request of all chunks encounter quota rejection.
    for (RequestInfo requestInfo : requestInfos) {
      ResponseInfo responseInfo = new ResponseInfo(requestInfo, true);
      op.handleResponse(responseInfo, null);
      responseInfo.release();
      if (op.isOperationComplete()) {
        break;
      }
    }

    requestInfos = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestInfos);
    // poll to populate request
    op.poll(requestRegistrationCallback);
    // If any slipped put requests also encounters quota rejection, then the operation is terminated.
    ResponseInfo responseInfo = new ResponseInfo(requestInfos.get(0), true);
    op.handleResponse(responseInfo, null);
    responseInfo.release();
    Assert.assertTrue(op.isOperationComplete());
    Assert.assertEquals(RouterErrorCode.TooManyRequests, ((RouterException)op.getOperationException()).getErrorCode());
    Assert.assertEquals("Metrics should show no metadata chunk was created", 0,
        routerMetrics.metadataChunkCreationCount.getCount());
  }

  /**
   * Test PUT operation that handles ServerErrorCode = Temporarily_Disabled and Replica_Unavailable
   * @throws Exception
   */
  @Test
  public void testSlippedPutsWithServerErrors() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    // Expect at least two successes so that you can create slipped puts.
    properties.setProperty("router.put.success.target", Integer.toString(2));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);

    int numChunks = 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, new FutureResult<>(),
            null, new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, null, time,
            blobProperties, MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    op.startOperation();
    List<RequestInfo> requestInfos = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestInfos);
    // fill chunks would end up filling the maximum number of PutChunks.
    op.fillChunks();
    Assert.assertTrue("ReadyForPollCallback should have been invoked as chunks were fully filled",
        mockNetworkClient.getAndClearWokenUpStatus());

    // poll to populate request
    op.poll(requestRegistrationCallback);

    // Set up server errors such that put fails on 2 out 3 nodes, hence creating a slipped put on the succeeding node.
    // Second attempts on all node succeed.
    List<ServerErrorCode> serverErrorList = new ArrayList<>();
    // Success on the first host, slipped put
    serverErrorList.add(ServerErrorCode.No_Error);
    // Fail on the second host
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    // Fail on the third host
    serverErrorList.add(ServerErrorCode.Unknown_Error);

    // Success on the second attempts on all hosts
    serverErrorList.add(ServerErrorCode.No_Error);
    serverErrorList.add(ServerErrorCode.No_Error);
    serverErrorList.add(ServerErrorCode.No_Error);

    mockServer.setServerErrors(serverErrorList);

    // Send all requests.
    for (int i = 0; i < requestInfos.size(); i++) {
      ResponseInfo responseInfo = getResponseInfo(requestInfos.get(i));
      PutResponse putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfo.content())) : null;
      op.handleResponse(responseInfo, putResponse);
      requestInfos.get(i).getRequest().release();
      responseInfo.release();
    }
    Assert.assertEquals("Number of slipped puts should be 1", 1, op.getSlippedPutBlobIds().size());

    // fill chunks again.
    op.fillChunks();

    requestInfos.clear();

    // poll to populate request
    op.poll(requestRegistrationCallback);

    // Send all requests again.
    for (int i = 0; i < requestInfos.size(); i++) {
      ResponseInfo responseInfo = getResponseInfo(requestInfos.get(i));
      PutResponse putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfo.content())) : null;
      op.handleResponse(responseInfo, putResponse);
      requestInfos.get(i).getRequest().release();
      responseInfo.release();
    }
    Assert.assertEquals("Number of slipped puts should be 1", 1, op.getSlippedPutBlobIds().size());
    PutOperation.PutChunk putChunk = op.getPutChunks().get(0);

    // Make sure the chunk blob id which has been put successfully is not part of the slipped puts.
    Assert.assertFalse(op.getSlippedPutBlobIds().contains(putChunk.chunkBlobId));
  }


  /**
   * Test the Errors {@link RouterErrorCode} received by Put Operation. The operation exception is set
   * based on the priority of these errors.
   * @throws Exception
   */
  @Test
  public void testSetOperationExceptionAndComplete() throws Exception {
    int numChunks = routerConfig.routerMaxInMemPutChunks + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, null, time, blobProperties,
            MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    RouterErrorCode[] routerErrorCodes = new RouterErrorCode[5];
    routerErrorCodes[0] = RouterErrorCode.OperationTimedOut;
    routerErrorCodes[1] = RouterErrorCode.UnexpectedInternalError;
    routerErrorCodes[2] = RouterErrorCode.AmbryUnavailable;
    routerErrorCodes[3] = RouterErrorCode.InsufficientCapacity;
    routerErrorCodes[4] = RouterErrorCode.InvalidBlobId;

    for (int i = 0; i < routerErrorCodes.length; ++i) {
      op.setOperationExceptionAndComplete(new RouterException("RouterError", routerErrorCodes[i]));
      Assert.assertEquals(((RouterException) op.getOperationException()).getErrorCode(), routerErrorCodes[i]);
    }
    for (int i = routerErrorCodes.length - 1; i >= 0; --i) {
      op.setOperationExceptionAndComplete(new RouterException("RouterError", routerErrorCodes[i]));
      Assert.assertEquals(((RouterException) op.getOperationException()).getErrorCode(),
          routerErrorCodes[routerErrorCodes.length - 1]);
    }

    Exception nonRouterException = new Exception();
    op.setOperationExceptionAndComplete(nonRouterException);
    Assert.assertEquals(nonRouterException, op.getOperationException());

    // test edge case where current operationException is non RouterException
    op.setOperationExceptionAndComplete(new RouterException("RouterError", RouterErrorCode.InsufficientCapacity));
    Assert.assertEquals(((RouterException) op.getOperationException()).getErrorCode(),
        RouterErrorCode.InsufficientCapacity);
    Assert.assertEquals("Metrics should show no metadata chunk was created", 0,
        routerMetrics.metadataChunkCreationCount.getCount());
  }

  /**
   * Ensure that errors while stitching blobs do not result in data chunk deletions.
   * @throws Exception
   */
  @Test
  public void testStitchErrorDataChunkHandling() throws Exception {
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
    byte[] userMetadata = new byte[10];
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    List<ChunkInfo> chunksToStitch =
        RouterTestHelpers.buildChunkList(mockClusterMap, BlobId.BlobDataType.DATACHUNK, Utils.Infinite_Time,
            LongStream.of(10, 10, 11));
    PutOperation op =
        PutOperation.forStitching(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
            new InMemAccountService(true, false), userMetadata, chunksToStitch, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, time, blobProperties,
            MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);
    // Trigger an exception by making the last chunk size too large.
    op.startOperation();
    Assert.assertTrue("Operation should be completed", op.isOperationComplete());
    Assert.assertEquals("Wrong RouterException error code", RouterErrorCode.InvalidPutArgument,
        ((RouterException) op.getOperationException()).getErrorCode());
    // Ensure that the operation does not provide the background deleter with any data chunks to delete.
    Assert.assertEquals("List of chunks to delete should be empty", 0,
        op.getSuccessfullyPutChunkIdsIfCompositeDirectUpload().size());
  }

  /**
   *  Reset the correlation id field of a {@link PutRequest} to 0.
   */
  private void resetCorrelationId(byte[] request) {
    // correlation id is an int that comes after size (long), type (short) and version (short).
    int offsetOfCorrelationId = 8 + 2 + 2;
    ByteBuffer wrapped = ByteBuffer.wrap(request);
    wrapped.putInt(offsetOfCorrelationId, 0);
  }

  /**
   * Get the {@link ResponseInfo} for the given {@link RequestInfo} using the {@link MockServer}
   * @param requestInfo the {@link RequestInfo} for which the response is to be returned.
   * @return the {@link ResponseInfo} the response for the request.
   * @throws IOException if there is an error sending the request.
   */
  private ResponseInfo getResponseInfo(RequestInfo requestInfo) throws IOException {
    NetworkReceive networkReceive = new NetworkReceive(null, mockServer.send(requestInfo.getRequest()), time);
    return new ResponseInfo(requestInfo, null, networkReceive.getReceivedBytes().content());
  }

  @Test
  public void compressChunk()
      throws IOException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    // Setup router config.
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    RouterConfig routerConfig = new RouterConfig(new VerifiableProperties(properties));

    // Create the blob properties for testing.
    BlobProperties blobProperties = new BlobProperties(-1, "serviceId", "memberId",
        "text/javascript", false, Utils.Infinite_Time, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);

    // Create an instance of PutOperation.
    int numChunks = routerConfig.routerMaxInMemPutChunks + 1;
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op = PutOperation.forUpload(routerConfig, routerMetrics, mockClusterMap, new LoggingNotificationSystem(),
        new InMemAccountService(true, false), userMetadata, channel, PutBlobOptions.DEFAULT, future, null,
        new RouterCallback(mockNetworkClient, new ArrayList<>()), null, null, null, null, time, blobProperties,
        MockClusterMap.DEFAULT_PARTITION_CLASS, quotaChargeCallback, compressionService);

    // PutOperation constructor creates an instance of PutChunk.  Set the PutChunk's source buffer.
    byte[] sourceBuffer = ("Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message."
        + "Test Message for testing purpose.  The Message is part of the testing message.").getBytes();
    ByteBuf sourceByteBuf = Unpooled.wrappedBuffer(sourceBuffer);
    PutOperation.PutChunk putChunk = op.new PutChunk();
    putChunk.buf = sourceByteBuf;

    // Invoke the PutChunk.compressChunk() method.  The new buffer is stored in "buf" field.
    MethodUtils.invokeMethod(putChunk, true, "compressChunk", false);

    // Verify the chunk is compressed.
    Assert.assertTrue((boolean) FieldUtils.readField(putChunk, "isChunkCompressed", true));

    // Release the buf field.
    ByteBuf buf = (ByteBuf) FieldUtils.readField(putChunk, "buf", true);
    buf.release();
  }
}

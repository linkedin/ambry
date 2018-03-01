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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Test;


public class PutOperationTest {
  private final RouterConfig routerConfig;
  private final MockClusterMap mockClusterMap = new MockClusterMap();
  private final NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(mockClusterMap);
  private final ResponseHandler responseHandler;
  private final Time time;
  private final Map<Integer, PutOperation> correlationIdToPutOperation = new TreeMap<>();
  private final MockServer mockServer = new MockServer(mockClusterMap, "");

  private class PutTestRequestRegistrationCallbackImpl implements RequestRegistrationCallback<PutOperation> {
    private List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(PutOperation putOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToPutOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), putOperation);
    }
  }

  private final PutTestRequestRegistrationCallbackImpl requestRegistrationCallback =
      new PutTestRequestRegistrationCallbackImpl();

  private final int chunkSize = 10;
  private final int requestParallelism = 3;
  private final int successTarget = 1;
  private final Random random = new Random();

  public PutOperationTest() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    routerConfig = new RouterConfig(vProps);
    responseHandler = new ResponseHandler(mockClusterMap);
    time = new MockTime();
  }

  /**
   * Ensure that if any of the requests associated with the buffer of a PutChunk is not completely read out even
   * after the associated chunk is complete, the buffer is not reused even though the PutChunk is reused.
   */
  @Test
  public void testSendIncomplete() throws Exception {
    int numChunks = NonBlockingRouter.MAX_IN_MEM_CHUNKS + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        new PutOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, new LoggingNotificationSystem(),
            userMetadata, channel, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<BackgroundDeleteRequest>()), null, null, null, null,
            time, blobProperties);
    op.startReadingFromChannel();
    List<RequestInfo> requestInfos = new ArrayList<>();
    requestRegistrationCallback.requestListToFill = requestInfos;
    // Since this channel is in memory, one call to fill chunks would end up filling the maximum number of PutChunks.
    op.fillChunks();
    Assert.assertTrue("ReadyForPollCallback should have been invoked as chunks were fully filled",
        mockNetworkClient.getAndClearWokenUpStatus());
    // A poll should therefore return requestParallelism number of requests from each chunk
    op.poll(requestRegistrationCallback);
    Assert.assertEquals(NonBlockingRouter.MAX_IN_MEM_CHUNKS * requestParallelism, requestInfos.size());

    // There are MAX_IN_MEM_CHUNKS + 1 data chunks for this blob (and a metadata chunk).
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
    PutResponse putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
        new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse()))) : null;
    op.handleResponse(responseInfo, putResponse);
    // 2.
    PutRequest putRequest = (PutRequest) requestInfos.get(1).getRequest();
    ByteBuffer buf = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(buf);
    // read it out (which also marks this request as complete).
    putRequest.writeTo(bufChannel);
    byte[] expectedRequestContent = buf.array();

    //3.
    // first save the third request
    PutRequest savedRequest = (PutRequest) requestInfos.get(2).getRequest();

    // succeed all the other requests.
    for (int i = 3; i < requestInfos.size(); i++) {
      responseInfo = getResponseInfo(requestInfos.get(i));
      putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
          new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse()))) : null;
      op.handleResponse(responseInfo, putResponse);
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
          new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse()))) : null;
      op.handleResponse(responseInfo, putResponse);
    }
    requestInfos.clear();
    // this should return requests for the metadata chunk
    op.poll(requestRegistrationCallback);
    Assert.assertEquals(1 * requestParallelism, requestInfos.size());
    Assert.assertFalse("Operation should not be complete yet", op.isOperationComplete());
    // once the metadata request succeeds, it should complete the operation.
    responseInfo = getResponseInfo(requestInfos.get(0));
    putResponse = responseInfo.getError() == null ? PutResponse.readFrom(
        new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse()))) : null;
    op.handleResponse(responseInfo, putResponse);
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
  }

  /**
   * Test the Errors {@link RouterErrorCode} received by Put Operation. The operation exception is set
   * based on the priority of these errors.
   * @throws Exception
   */
  @Test
  public void testSetOperationExceptionAndComplete() throws Exception {
    int numChunks = NonBlockingRouter.MAX_IN_MEM_CHUNKS + 1;
    BlobProperties blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false);
    byte[] userMetadata = new byte[10];
    byte[] content = new byte[chunkSize * numChunks];
    random.nextBytes(content);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(content));
    FutureResult<String> future = new FutureResult<>();
    MockNetworkClient mockNetworkClient = new MockNetworkClient();
    PutOperation op =
        new PutOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, new LoggingNotificationSystem(),
            userMetadata, channel, future, null,
            new RouterCallback(mockNetworkClient, new ArrayList<BackgroundDeleteRequest>()), null, null, null, null,
            time, blobProperties);
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
    return new ResponseInfo(requestInfo, null, networkReceive.getReceivedBytes().getPayload());
  }
}


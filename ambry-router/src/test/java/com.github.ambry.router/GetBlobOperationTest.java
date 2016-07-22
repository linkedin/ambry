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
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.router.RouterTestHelpers.ErrorCodeChecker;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.router.RouterTestHelpers.testWithErrorCodes;


/**
 * Tests for {@link GetBlobOperation}
 * This class creates a {@link NonBlockingRouter} with a {@link MockServer} and does puts through it. The gets,
 * however are done directly by the tests - that is, the tests create {@link GetBlobOperation} and get requests from
 * it and then use a {@link NetworkClient} directly to send requests to and get responses from the {@link MockServer}.
 * Since the {@link NetworkClient} used by the router and the test are different, and since the
 * {@link GetBlobOperation} created by the tests are never known by the router, there are no conflicts with the
 * RequestResponseHandler of the router.
 * Many of the variables are made member variables, so that they can be shared between the router and the
 * {@link GetBlobOperation}s.
 */
public class GetBlobOperationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private final int replicasCount;
  private final int maxChunkSize;
  private final MockTime time = new MockTime();
  private final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<>();
  private final Random random = new Random();
  private final FutureResult<ReadableStreamChannel> operationFuture = new FutureResult<>();
  private final MockClusterMap mockClusterMap;
  private final BlobIdFactory blobIdFactory;
  private final NonBlockingRouterMetrics routerMetrics;
  private final MockServerLayout mockServerLayout;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
  private final ResponseHandler responseHandler;
  private final NonBlockingRouter router;
  private final MockNetworkClient mockNetworkClient;
  private final NetworkClient networkClient;
  private final ReadyForPollCallback readyForPollCallback;

  // Certain tests recreate the routerConfig with different properties.
  private RouterConfig routerConfig;
  private int blobSize;

  // Parameters for puts which are also used to verify the gets.
  private String blobIdStr;
  private byte[] putContent;

  private final GetTestRequestRegistrationCallbackImpl requestRegistrationCallback =
      new GetTestRequestRegistrationCallbackImpl();

  private class GetTestRequestRegistrationCallbackImpl implements RequestRegistrationCallback<GetOperation> {
    List<RequestInfo> requestListToFill;

    @Override
    public void registerRequestToSend(GetOperation getOperation, RequestInfo requestInfo) {
      requestListToFill.add(requestInfo);
      correlationIdToGetOperation.put(((RequestOrResponse) requestInfo.getRequest()).getCorrelationId(), getOperation);
    }
  }

  private final AtomicInteger operationsCount = new AtomicInteger(0);
  private final OperationCompleteCallback operationCompleteCallback = new OperationCompleteCallback(operationsCount);

  /**
   * A checker that either asserts that a get operation succeeds or returns the specified error code.
   */
  private final ErrorCodeChecker getErrorCodeChecker = new ErrorCodeChecker() {
    @Override
    public void testAndAssert(RouterErrorCode expectedError)
        throws Exception {
      if (expectedError == null) {
        getAndAssertSuccess();
      } else {
        GetBlobOperation op = createOperationAndComplete(null);
        checkErrorCode(op, expectedError);
      }
    }
  };

  @After
  public void after() {
    router.close();
    Assert.assertEquals("All operations should have completed", 0, operationsCount.get());
  }

  /**
   * Instantiate a router, perform a put, close the router. The blob that was put will be saved in the MockServer,
   * and can be queried by the getBlob operations in the test.
   */
  public GetBlobOperationTest()
      throws Exception {
    // Defaults. Tests may override these and do new puts as appropriate.
    maxChunkSize = random.nextInt(1024 * 1024) + 1;
    // a blob size that is greater than the maxChunkSize and is not a multiple of it. Will result in a composite blob.
    blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    mockSelectorState.set(MockSelectorState.Good);
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties());
    routerConfig = new RouterConfig(vprops);
    mockClusterMap = new MockClusterMap();
    blobIdFactory = new BlobIdFactory(mockClusterMap);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    replicasCount = mockClusterMap.getWritablePartitionIds().get(0).getReplicaIds().size();
    responseHandler = new ResponseHandler(mockClusterMap);
    MockNetworkClientFactory networkClientFactory =
        new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap), networkClientFactory,
        new LoggingNotificationSystem(), mockClusterMap, time);
    networkClient = networkClientFactory.getNetworkClient();
    mockNetworkClient = new MockNetworkClient();
    readyForPollCallback = new ReadyForPollCallback(mockNetworkClient);
  }

  /**
   * Generates random content, and does a single put of the content, and saves the blob id string returned. The tests
   * use this blob id string to perform the gets. Tests asserting success compare the contents of the returned blob
   * with the content that is generated within this method.
   * @throws Exception
   */
  private void doPut()
      throws Exception {
    BlobProperties blobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    blobIdStr = router.putBlob(blobProperties, userMetadata, putChannel).get();
  }

  /**
   * Test {@link GetBlobOperation} instantiation and validate the get methods.
   * @throws Exception
   */
  @Test
  public void testInstantiation()
      throws Exception {
    Callback<ReadableStreamChannel> operationCallback = new Callback<ReadableStreamChannel>() {
      @Override
      public void onCompletion(ReadableStreamChannel result, Exception exception) {
        // no op.
      }
    };

    // test a bad case
    try {
      new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, "invalid_id", operationFuture,
          operationCallback, operationCompleteCallback, readyForPollCallback, blobIdFactory, time);
      Assert.fail("Instantiation of GetBlobOperation with an invalid blob id must fail");
    } catch (RouterException e) {
      Assert.assertEquals("Unexpected exception received on creating GetBlobOperation", RouterErrorCode.InvalidBlobId,
          e.getErrorCode());
    }

    blobIdStr = new BlobId(mockClusterMap.getWritablePartitionIds().get(0)).getID();
    // test a good case
    // operationCount is not incremented here as this operation is not taken to completion.
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobIdStr, operationFuture,
            operationCallback, operationCompleteCallback, readyForPollCallback, blobIdFactory, time);

    Assert.assertEquals("Callbacks must match", operationCallback, op.getCallback());
    Assert.assertEquals("Futures must match", operationFuture, op.getFuture());
    Assert.assertEquals("Blob ids must match", blobIdStr, op.getBlobIdStr());
  }

  /**
   * Put blobs that result in a single chunk; perform gets of the blob and ensure success.
   */
  @Test
  public void testSimpleBlobGetSuccess()
      throws Exception {
    for (int i = 0; i < 10; i++) {
      // blobSize in the range [1, maxChunkSize]
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      getAndAssertSuccess();
    }
  }

  /**
   * Put a blob with no data, perform get and ensure success.
   */
  @Test
  public void testZeroSizedBlobGetSuccess()
      throws Exception {
    blobSize = 0;
    doPut();
    getAndAssertSuccess();
  }

  /**
   * Put blobs that result in multiple chunks and at chunk boundaries; perform gets and ensure success.
   */
  @Test
  public void testCompositeBlobChunkSizeMultipleGetSuccess()
      throws Exception {
    for (int i = 2; i < 10; i++) {
      blobSize = maxChunkSize * i;
      doPut();
      getAndAssertSuccess();
    }
  }

  /**
   * Put blobs that result in multiple chunks with the last chunk less than max chunk size; perform gets and ensure
   * success.
   */
  @Test
  public void testCompositeBlobNotChunkSizeMultipleGetSuccess()
      throws Exception {
    for (int i = 0; i < 10; i++) {
      blobSize = maxChunkSize * i + random.nextInt(maxChunkSize - 1) + 1;
      doPut();
      getAndAssertSuccess();
    }
  }

  /**
   * Test the case where all requests time out within the GetOperation.
   * @throws Exception
   */
  @Test
  public void testRouterRequestTimeoutAllFailure()
      throws Exception {
    doPut();
    GetBlobOperation op = createOperation(null);
    op.poll(requestRegistrationCallback);
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
    }
    // At this time requests would have been created for all replicas, as none of them were delivered,
    // and cross-colo proxying is enabled by default.
    Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
        correlationIdToGetOperation.size());
    checkErrorCode(op, RouterErrorCode.OperationTimedOut);
  }

  /**
   * Test the case where all requests time out within the NetworkClient.
   * @throws Exception
   */
  @Test
  public void testNetworkClientTimeoutAllFailure()
      throws Exception {
    doPut();
    GetBlobOperation op = createOperation(null);
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      for (RequestInfo requestInfo : requestRegistrationCallback.requestListToFill) {
        ResponseInfo fakeResponse = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
        op.handleResponse(fakeResponse, null);
        if (op.isOperationComplete()) {
          break;
        }
      }
      requestRegistrationCallback.requestListToFill.clear();
    }

    // At this time requests would have been created for all replicas, as none of them were delivered,
    // and cross-colo proxying is enabled by default.
    Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
        correlationIdToGetOperation.size());
    checkErrorCode(op, RouterErrorCode.OperationTimedOut);
  }

  /**
   * Test the case where every server returns Blob_Not_Found. All servers must have been contacted,
   * due to cross-colo proxying.
   * @throws Exception
   */
  @Test
  public void testBlobNotFoundCase()
      throws Exception {
    doPut();
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.Blob_Not_Found, replicasCount), mockServerLayout,
        RouterErrorCode.BlobDoesNotExist, new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError)
              throws Exception {
            GetBlobOperation op = createOperationAndComplete(null);
            Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
                correlationIdToGetOperation.size());
            checkErrorCode(op, expectedError);
          }
        });
  }

  /**
   * Test the case with Blob_Not_Found errors from most servers, and Blob_Deleted at just one server. The latter
   * should be the exception received for the operation.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithBlobDeletedAndExpiredCase()
      throws Exception {
    doPut();
    Map<ServerErrorCode, RouterErrorCode> serverErrorToRouterError = new HashMap<>();
    serverErrorToRouterError.put(ServerErrorCode.Blob_Deleted, RouterErrorCode.BlobDeleted);
    serverErrorToRouterError.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    for (Map.Entry<ServerErrorCode, RouterErrorCode> entry : serverErrorToRouterError.entrySet()) {
      Map<ServerErrorCode, Integer> errorCounts = new HashMap<>();
      errorCounts.put(ServerErrorCode.Blob_Not_Found, replicasCount - 1);
      errorCounts.put(entry.getKey(), 1);
      testWithErrorCodes(errorCounts, mockServerLayout, entry.getValue(), getErrorCodeChecker);
    }
  }

  /**
   * Test the case with multiple errors (server level and partition level) from multiple servers,
   * with just one server returning a successful response. The operation should succeed.
   * @throws Exception
   */
  @Test
  public void testSuccessInThePresenceOfVariousErrors()
      throws Exception {
    doPut();
    // The put for the blob being requested happened.
    String dcWherePutHappened = routerConfig.routerDatacenterName;

    // test requests coming in from local dc as well as cross-colo.
    Properties props = getDefaultNonBlockingRouterProperties();
    props.setProperty("router.datacenter.name", "DC1");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);

    props = getDefaultNonBlockingRouterProperties();
    props.setProperty("router.datacenter.name", "DC2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);

    props = getDefaultNonBlockingRouterProperties();
    props.setProperty("router.datacenter.name", "DC3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);
  }

  /**
   * Helper method to simulate errors from the servers. Only one node in the datacenter where the put happened will
   * return success. No matter what order the servers are contacted, as long as one of them returns success, the whole
   * operation should succeed.
   * @param dcWherePutHappened the datacenter where the put happened.
   */
  private void testVariousErrors(String dcWherePutHappened)
      throws Exception {
    ArrayList<MockServer> mockServers = new ArrayList<>(mockServerLayout.getMockServers());
    ArrayList<ServerErrorCode> serverErrors = new ArrayList<>(Arrays.asList(ServerErrorCode.values()));
    // set the status to various server level or partition level errors (not Blob_Deleted or Blob_Expired - as they
    // are final), except for one of the servers in the datacenter where the put happened (we do this as puts only go
    // to the local dc, whereas gets go cross colo).
    serverErrors.remove(ServerErrorCode.Blob_Deleted);
    serverErrors.remove(ServerErrorCode.Blob_Expired);
    serverErrors.remove(ServerErrorCode.No_Error);
    boolean goodServerMarked = false;
    for (MockServer mockServer : mockServers) {
      if (!goodServerMarked && mockServer.getDataCenter().equals(dcWherePutHappened)) {
        mockServer.setServerErrorForAllRequests(ServerErrorCode.No_Error);
        goodServerMarked = true;
      } else {
        mockServer.setServerErrorForAllRequests(serverErrors.get(random.nextInt(serverErrors.size())));
      }
    }
    getAndAssertSuccess();
  }

  /**
   * Test that read succeeds when all chunks are received before read is called.
   * @throws Exception
   */
  @Test
  public void testReadNotCalledBeforeChunkArrival()
      throws Exception {
    // 3 chunks so blob can be cached completely before reading
    blobSize = maxChunkSize * 2 + 1;
    doPut();
    getAndAssertSuccess(true, null);
  }

  /**
   * Test that read succeeds when read is called immediately after callback, and chunks come in delayed.
   * @throws Exception
   */
  @Test
  public void testDelayedChunks()
      throws Exception {
    doPut();
    final CountDownLatch readIntoLatch = new CountDownLatch(1);
    RouterTestHelpers.setDataBlobGetLatchForAllServers(readIntoLatch, mockServerLayout);
    getAndAssertSuccess(false, readIntoLatch);
    RouterTestHelpers.setDataBlobGetLatchForAllServers(null, mockServerLayout);
  }

  /**
   * Test that data chunk errors notify the reader callback and set the error code correctly.
   * @throws Exception
   */
  @Test
  public void testDataChunkFailure()
      throws Exception {
    for (ServerErrorCode serverErrorCode : ServerErrorCode.values()) {
      if (serverErrorCode != ServerErrorCode.No_Error) {
        testDataChunkError(serverErrorCode, RouterErrorCode.UnexpectedInternalError);
      }
    }
  }

  /**
   * Test that gets work for blobs with the old blob format (V1).
   * @throws Exception
   */
  @Test
  public void testLegacyBlobGetSuccess()
      throws Exception {
    RouterTestHelpers.setBlobFormatForAllServers(false, mockServerLayout);
    for (int i = 0; i < 10; i++) {
      // blobSize in the range [1, maxChunkSize]
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      getAndAssertSuccess();
    }
    RouterTestHelpers.setBlobFormatForAllServers(true, mockServerLayout);
  }

  // @todo: test operation completion behavior when stream is not read completely. This will require addin

  private void testDataChunkError(ServerErrorCode serverErrorCode, final RouterErrorCode expectedErrorCode)
      throws Exception {
    blobSize = maxChunkSize * 2 + 1;
    doPut();
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    final AtomicReference<Exception> readCompleteException = new AtomicReference<>(null);
    final ByteBufferAsyncWritableChannel asyncWritableChannel = new ByteBufferAsyncWritableChannel();
    RouterTestHelpers.setGetErrorOnDataBlobOnlyForAllServers(true, mockServerLayout);
    RouterTestHelpers.testWithErrorCodes(Collections.singletonMap(serverErrorCode, 9), mockServerLayout,
        expectedErrorCode, new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError)
              throws Exception {
            Callback<ReadableStreamChannel> callback = new Callback<ReadableStreamChannel>() {
              @Override
              public void onCompletion(final ReadableStreamChannel result, final Exception exception) {
                if (exception != null) {
                  asyncWritableChannel.close();
                  readCompleteLatch.countDown();
                } else {
                  Utils.newThread(new Runnable() {
                    @Override
                    public void run() {
                      try {
                        result.readInto(asyncWritableChannel, new Callback<Long>() {
                          @Override
                          public void onCompletion(Long result, Exception exception) {
                            asyncWritableChannel.close();
                          }
                        });
                        asyncWritableChannel.getNextChunk();
                      } catch (Exception e) {
                        readCompleteException.set(e);
                      } finally {
                        readCompleteLatch.countDown();
                      }
                    }
                  }, false).start();
                }
              }
            };
            GetBlobOperation op = createOperationAndComplete(callback);
            Assert.assertTrue(readCompleteLatch.await(2, TimeUnit.SECONDS));
            Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
            if (readCompleteException.get() != null) {
              throw readCompleteException.get();
            }
            Assert.assertFalse("AsyncWriteableChannel should have been closed.", asyncWritableChannel.isOpen());
            checkErrorCode(op, expectedError);
          }
        });
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   */
  private void getAndAssertSuccess()
      throws Exception {
    getAndAssertSuccess(false, null);
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   * @param getChunksBeforeRead {@code true} if all chunks should be cached by the router before reading from the
   *                            stream.
   * @param readIntoLatch if non-null, this latch will be flipped right after
   *                      {@link ReadableStreamChannel#readInto(AsyncWritableChannel, Callback)} is called
   */
  private void getAndAssertSuccess(final boolean getChunksBeforeRead, final CountDownLatch readIntoLatch)
      throws Exception {
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    final AtomicReference<Exception> readCompleteException = new AtomicReference<>(null);
    final AtomicLong readCompleteResult = new AtomicLong(0);
    final AtomicReference<Exception> operationException = new AtomicReference<>(null);
    final AtomicReference<GetBlobOperation> opRef = new AtomicReference<>(null);

    Callback<ReadableStreamChannel> callback = new Callback<ReadableStreamChannel>() {
      @Override
      public void onCompletion(final ReadableStreamChannel result, final Exception exception) {
        if (exception != null) {
          operationException.set(exception);
          readCompleteLatch.countDown();
        } else {
          Utils.newThread(new Runnable() {
            @Override
            public void run() {
              if (getChunksBeforeRead) {
                try {
                  // wait for all 3 chunks to be received
                  while (opRef.get() == null || opRef.get().getNumChunksRetrieved() < opRef.get().getNumChunksTotal()) {
                    Thread.sleep(10);
                  }
                } catch (InterruptedException ignored) {
                }
              }
              assertSuccess(result, readCompleteLatch, readCompleteResult, readCompleteException, readIntoLatch);
            }
          }, false).start();
        }
      }
    };

    GetBlobOperation op = createOperationAndComplete(callback, opRef);

    readCompleteLatch.await();
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    if (operationException.get() != null) {
      throw operationException.get();
    }
    if (readCompleteException.get() != null) {
      throw readCompleteException.get();
    }
    Assert.assertEquals("Size read must equal size written", blobSize, readCompleteResult.get());
  }

  /**
   * Create a getBlob operation with the specified callback and poll until completion.
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createOperationAndComplete(Callback<ReadableStreamChannel> callback)
      throws Exception {
    return createOperationAndComplete(callback, null);
  }

  /**
   * Create a getBlob operation with the specified callback and poll until completion.
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @param opReference if non-null, this will point to the {@link GetBlobOperation} right after creation
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createOperationAndComplete(Callback<ReadableStreamChannel> callback,
      AtomicReference<GetBlobOperation> opReference)
      throws Exception {
    GetBlobOperation op = createOperation(callback, opReference);
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.requestListToFill);
      for (ResponseInfo response : responses) {
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse
            .readFrom(new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())), mockClusterMap)
            : null;
        op.handleResponse(response, getResponse);
      }
    }
    return op;
  }

  /**
   * Create a getBlob operation with the specified callback
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createOperation(Callback<ReadableStreamChannel> callback)
      throws Exception {
    return createOperation(callback, null);
  }

  /**
   * Create a getBlob operation with the specified callback
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @param opReference if non-null, this will point to the {@link GetBlobOperation} right after creation
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createOperation(Callback<ReadableStreamChannel> callback,
      AtomicReference<GetBlobOperation> opReference)
      throws Exception {
    operationsCount.incrementAndGet();
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobIdStr, operationFuture,
            callback, operationCompleteCallback, readyForPollCallback, blobIdFactory, time);
    requestRegistrationCallback.requestListToFill = new ArrayList<>();
    if (opReference != null) {
      opReference.set(op);
    }
    return op;
  }

  /**
   * Check that an operation is complete and has the specified {@link RouterErrorCode} set.
   * @param op The operation to check.
   * @param expectedError The error code expected.
   */
  private void checkErrorCode(GetBlobOperation op, RouterErrorCode expectedError) {
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    RouterException routerException = (RouterException) op.getOperationException();
    if (routerException == null) {
      Assert.fail("Expected getBlobOperation to fail");
    }
    Assert.assertEquals(expectedError, routerException.getErrorCode());
  }

  /**
   * Assert that the operation is complete and successful. Note that the future completion and callback invocation
   * happens outside of the GetOperation, so those are not checked here. But at this point, the operation result should
   * be ready.
   */
  private void assertSuccess(ReadableStreamChannel readableStreamChannel, CountDownLatch readCompleteLatch,
      AtomicLong readCompleteResult, AtomicReference<Exception> readCompleteException, CountDownLatch readIntoLatch) {
    try {
      ByteBufferAsyncWritableChannel asyncWritableChannel = new ByteBufferAsyncWritableChannel();
      long written;
      Future<Long> readIntoFuture = readableStreamChannel.readInto(asyncWritableChannel, null);
      if (readIntoLatch != null) {
        readIntoLatch.countDown();
      }
      Assert.assertTrue("ReadyForPollCallback should have been invoked as readInto() was called",
          mockNetworkClient.getAndClearWokenUpStatus());
      // Compare byte by byte.
      int readBytes = 0;
      do {
        ByteBuffer buf = asyncWritableChannel.getNextChunk();
        int bufLength = buf.remaining();
        Assert.assertTrue("total content read should not be greater than length of put content",
            readBytes + bufLength <= putContent.length);
        while (buf.hasRemaining()) {
          Assert.assertEquals("Get and Put blob content should match", putContent[readBytes++], buf.get());
        }
        asyncWritableChannel.resolveOldestChunk(null);
        Assert.assertTrue("ReadyForPollCallback should have been invoked as writable channel callback was called",
            mockNetworkClient.getAndClearWokenUpStatus());
      } while (readBytes < putContent.length);
      written = readIntoFuture.get();
      Assert.assertEquals("the returned length in the future should be the length of data written", (long) readBytes,
          written);
      Assert.assertNull("There should be no more data in the channel", asyncWritableChannel.getNextChunk(0));
      readCompleteResult.set(written);
    } catch (Exception e) {
      readCompleteException.set(e);
    } finally {
      readCompleteLatch.countDown();
    }
  }

  /**
   * Submit all the requests that were handed over by the operation and wait until a response is received for every
   * one of them.
   * @param requestList the list containing the requests handed over by the operation.
   * @return the list of responses from the network client.
   * @throws IOException
   */
  private List<ResponseInfo> sendAndWaitForResponses(List<RequestInfo> requestList)
      throws IOException {
    int sendCount = requestList.size();
    // Shuffle the replicas to introduce randomness in the order in which responses arrive.
    Collections.shuffle(requestList);
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(networkClient.sendAndPoll(requestList, 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(networkClient.sendAndPoll(requestList, 100));
    }
    return responseList;
  }

  /**
   * Get the default {@link Properties} for the {@link NonBlockingRouter}.
   * @return the constructed {@link Properties}
   */
  private Properties getDefaultNonBlockingRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
    properties.setProperty("router.put.success.target", Integer.toString(2));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
    properties.setProperty("router.get.success.target", Integer.toString(1));
    return properties;
  }
}

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
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.router.RouterTestHelpers.*;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.PutManagerTest.*;
import static com.github.ambry.router.RouterTestHelpers.*;


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
@RunWith(Parameterized.class)
public class GetBlobOperationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private final int replicasCount;
  private final int maxChunkSize;
  private final MockTime time = new MockTime();
  private final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<>();
  private final Random random = new Random();
  private final MockClusterMap mockClusterMap;
  private final BlobIdFactory blobIdFactory;
  private final NonBlockingRouterMetrics routerMetrics;
  private final MockServerLayout mockServerLayout;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
  private final ResponseHandler responseHandler;
  private final NonBlockingRouter router;
  private final MockNetworkClient mockNetworkClient;
  private final RouterCallback routerCallback;
  private final String operationTrackerType;
  private final boolean testEncryption;
  private MockKeyManagementService kms = null;
  private MockCryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;

  // Certain tests recreate the routerConfig with different properties.
  private RouterConfig routerConfig;
  private int blobSize;

  // Parameters for puts which are also used to verify the gets.
  private String blobIdStr;
  private BlobId blobId;
  private BlobProperties blobProperties;
  private byte[] userMetadata;
  private byte[] putContent;

  // Options which are passed into GetBlobOperations
  private GetBlobOptionsInternal options;

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

  /**
   * A checker that either asserts that a get operation succeeds or returns the specified error code.
   */
  private final ErrorCodeChecker getErrorCodeChecker = new ErrorCodeChecker() {
    @Override
    public void testAndAssert(RouterErrorCode expectedError) throws Exception {
      if (expectedError == null) {
        getAndAssertSuccess();
      } else {
        GetBlobOperation op = createOperationAndComplete(null);
        assertFailureAndCheckErrorCode(op, expectedError);
      }
    }
  };

  @After
  public void after() {
    router.close();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    if (cryptoJobHandler != null) {
      cryptoJobHandler.close();
    }
  }

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker} with and without encryption
   * @return an array of Pairs of {{@link SimpleOperationTracker}, Non-Encrypted}, {{@link SimpleOperationTracker}, Encrypted}
   * and {{@link AdaptiveOperationTracker}, Non-Encrypted}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{SimpleOperationTracker.class.getSimpleName(), false}, {SimpleOperationTracker.class.getSimpleName(), true}, {AdaptiveOperationTracker.class.getSimpleName(), false}});
  }

  /**
   * Instantiate a router, perform a put, close the router. The blob that was put will be saved in the MockServer,
   * and can be queried by the getBlob operations in the test.
   * @param operationTrackerType the type of {@link OperationTracker} to use.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public GetBlobOperationTest(String operationTrackerType, boolean testEncryption) throws Exception {
    this.operationTrackerType = operationTrackerType;
    this.testEncryption = testEncryption;
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
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    replicasCount = mockClusterMap.getWritablePartitionIds().get(0).getReplicaIds().size();
    responseHandler = new ResponseHandler(mockClusterMap);
    MockNetworkClientFactory networkClientFactory =
        new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    if (testEncryption) {
      kms = new MockKeyManagementService(new KMSConfig(vprops),
          TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
      cryptoService = new MockCryptoService(new CryptoServiceConfig(vprops));
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    }
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap), networkClientFactory,
        new LoggingNotificationSystem(), mockClusterMap, kms, cryptoService, cryptoJobHandler, time);
    mockNetworkClient = networkClientFactory.getMockNetworkClient();
    routerCallback = new RouterCallback(mockNetworkClient, new ArrayList<BackgroundDeleteRequest>());
  }

  /**
   * Generates random content, and does a single put of the content, and saves the blob id string returned. The tests
   * use this blob id string to perform the gets. Tests asserting success compare the contents of the returned blob
   * with the content that is generated within this method.
   * @throws Exception
   */
  private void doPut() throws Exception {
    blobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption);
    userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    blobIdStr = router.putBlob(blobProperties, userMetadata, putChannel).get();
    blobId = RouterUtils.getBlobIdFromString(blobIdStr, mockClusterMap);
  }

  /**
   * Test {@link GetBlobOperation} instantiation and validate the get methods.
   * @throws Exception
   */
  @Test
  public void testInstantiation() throws Exception {
    Callback<GetBlobResultInternal> getRouterCallback = new Callback<GetBlobResultInternal>() {
      @Override
      public void onCompletion(GetBlobResultInternal result, Exception exception) {
        // no op.
      }
    };

    blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), mockClusterMap.getWritablePartitionIds().get(0), false);
    blobIdStr = blobId.getID();
    // test a good case
    // operationCount is not incremented here as this operation is not taken to completion.
    GetBlobOperation op = new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId,
        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet),
        getRouterCallback, routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time);
    Assert.assertEquals("Callbacks must match", getRouterCallback, op.getCallback());
    Assert.assertEquals("Blob ids must match", blobIdStr, op.getBlobIdStr());

    // test the case where the tracker type is bad
    Properties properties = getDefaultNonBlockingRouterProperties();
    properties.setProperty("router.get.operation.tracker.type", "NonExistentTracker");
    RouterConfig badConfig = new RouterConfig(new VerifiableProperties(properties));
    try {
      new GetBlobOperation(badConfig, routerMetrics, mockClusterMap, responseHandler, blobId,
          new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet),
          getRouterCallback, routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time);
      Assert.fail("Instantiation of GetBlobOperation with an invalid tracker type must fail");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Put blobs that result in a single chunk; perform gets of the blob and ensure success.
   */
  @Test
  public void testSimpleBlobGetSuccess() throws Exception {
    for (int i = 0; i < 10; i++) {
      // blobSize in the range [1, maxChunkSize]
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      switch (i % 2) {
        case 0:
          options = new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).build(), false,
              routerMetrics.ageAtGet);
          break;
        case 1:
          options = new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data).build(), false,
              routerMetrics.ageAtGet);
          break;
      }
      getAndAssertSuccess();
    }
  }

  /**
   * Put a blob with no data, perform get and ensure success.
   */
  @Test
  public void testZeroSizedBlobGetSuccess() throws Exception {
    blobSize = 0;
    doPut();
    getAndAssertSuccess();
  }

  /**
   * Put blobs that result in multiple chunks and at chunk boundaries; perform gets and ensure success.
   */
  @Test
  public void testCompositeBlobChunkSizeMultipleGetSuccess() throws Exception {
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
  public void testCompositeBlobNotChunkSizeMultipleGetSuccess() throws Exception {
    for (int i = 0; i < 10; i++) {
      blobSize = maxChunkSize * i + random.nextInt(maxChunkSize - 1) + 1;
      doPut();
      switch (i % 2) {
        case 0:
          options = new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).build(), false,
              routerMetrics.ageAtGet);
          break;
        case 1:
          options = new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data).build(), false,
              routerMetrics.ageAtGet);
          break;
      }
      getAndAssertSuccess();
    }
  }

  /**
   * Test the case where all requests time out within the GetOperation.
   * @throws Exception
   */
  @Test
  public void testRouterRequestTimeoutAllFailure() throws Exception {
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
    assertFailureAndCheckErrorCode(op, RouterErrorCode.OperationTimedOut);
  }

  /**
   * Test the case where all requests time out within the NetworkClient.
   * @throws Exception
   */
  @Test
  public void testNetworkClientTimeoutAllFailure() throws Exception {
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
    assertFailureAndCheckErrorCode(op, RouterErrorCode.OperationTimedOut);
  }

  /**
   * Test the case where every server returns Blob_Not_Found. All servers must have been contacted,
   * due to cross-colo proxying.
   * @throws Exception
   */
  @Test
  public void testBlobNotFoundCase() throws Exception {
    doPut();
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.Blob_Not_Found, replicasCount), mockServerLayout,
        RouterErrorCode.BlobDoesNotExist, new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError) throws Exception {
            GetBlobOperation op = createOperationAndComplete(null);
            Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
                correlationIdToGetOperation.size());
            assertFailureAndCheckErrorCode(op, expectedError);
          }
        });
  }

  /**
   * Test the case with Blob_Not_Found errors from most servers, and Blob_Deleted at just one server. The latter
   * should be the exception received for the operation.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithBlobDeletedAndExpiredCase() throws Exception {
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
  public void testSuccessInThePresenceOfVariousErrors() throws Exception {
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
   * Test failure with KMS
   * @throws Exception
   */
  @Test
  public void testKMSFailure() throws Exception {
    if (testEncryption) {
      // simple Blob
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      kms.exceptionToThrow.set(GSE);
      GetBlobOperation op = createOperationAndComplete(null);
      assertFailureAndCheckErrorCode(op, RouterErrorCode.UnexpectedInternalError);

      // composite blob
      kms.exceptionToThrow.set(null);
      blobSize = maxChunkSize * random.nextInt(10);
      doPut();
      kms.exceptionToThrow.set(GSE);
      op = createOperationAndComplete(null);
      assertFailureAndCheckErrorCode(op, RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Test failure with CryptoService
   * @throws Exception
   */
  @Test
  public void testCryptoServiceFailure() throws Exception {
    if (testEncryption) {
      // simple Blob
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      cryptoService.exceptionOnDecryption.set(GSE);
      GetBlobOperation op = createOperationAndComplete(null);
      assertFailureAndCheckErrorCode(op, RouterErrorCode.UnexpectedInternalError);

      // composite blob
      cryptoService.exceptionOnDecryption.set(null);
      blobSize = maxChunkSize * random.nextInt(10);
      doPut();
      cryptoService.exceptionOnDecryption.set(GSE);
      op = createOperationAndComplete(null);
      assertFailureAndCheckErrorCode(op, RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Helper method to simulate errors from the servers. Only one node in the datacenter where the put happened will
   * return success. No matter what order the servers are contacted, as long as one of them returns success, the whole
   * operation should succeed.
   * @param dcWherePutHappened the datacenter where the put happened.
   */
  private void testVariousErrors(String dcWherePutHappened) throws Exception {
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
  public void testReadNotCalledBeforeChunkArrival() throws Exception {
    // 3 chunks so blob can be cached completely before reading
    blobSize = maxChunkSize * 2 + 1;
    doPut();
    getAndAssertSuccess(true, false);
  }

  /**
   * Test that read succeeds when read is called immediately after callback, and chunks come in delayed.
   * @throws Exception
   */
  @Test
  public void testDelayedChunks() throws Exception {
    doPut();
    getAndAssertSuccess(false, true);
  }

  /**
   * Test that data chunk errors notify the reader callback and set the error code correctly.
   * @throws Exception
   */
  @Test
  public void testDataChunkFailure() throws Exception {
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
  public void testLegacyBlobGetSuccess() throws Exception {
    RouterTestHelpers.setBlobFormatVersionForAllServers(MessageFormatRecord.Blob_Version_V1, mockServerLayout);
    for (int i = 0; i < 10; i++) {
      // blobSize in the range [1, maxChunkSize]
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      getAndAssertSuccess();
    }
    RouterTestHelpers.setBlobFormatVersionForAllServers(MessageFormatRecord.Blob_Version_V2, mockServerLayout);
  }

  /**
   * Test range requests on a single chunk blob.
   * @throws Exception
   */
  @Test
  public void testRangeRequestSimpleBlob() throws Exception {
    // Random valid ranges
    for (int i = 0; i < 5; i++) {
      blobSize = random.nextInt(maxChunkSize) + 1;
      int randomOne = random.nextInt(blobSize);
      int randomTwo = random.nextInt(blobSize);
      testRangeRequestOffsetRange(Math.min(randomOne, randomTwo), Math.max(randomOne, randomTwo), true);
    }
    blobSize = random.nextInt(maxChunkSize) + 1;
    // Entire blob
    testRangeRequestOffsetRange(0, blobSize - 1, true);
    // Range that extends to end of blob
    testRangeRequestFromStartOffset(random.nextInt(blobSize), true);
    // Last n bytes of the blob
    testRangeRequestLastNBytes(random.nextInt(blobSize) + 1, true);
    // Last blobSize + 1 bytes (should not succeed)
    testRangeRequestLastNBytes(blobSize + 1, false);
    // Range over the end of the blob (should not succeed)
    testRangeRequestOffsetRange(random.nextInt(blobSize), blobSize + 5, false);
    // Ranges that start past the end of the blob (should not succeed)
    testRangeRequestFromStartOffset(blobSize, false);
    testRangeRequestOffsetRange(blobSize, blobSize + 20, false);
    // 0 byte range
    testRangeRequestLastNBytes(0, true);
    // 1 byte ranges
    testRangeRequestOffsetRange(0, 0, true);
    testRangeRequestOffsetRange(blobSize - 1, blobSize - 1, true);
    testRangeRequestFromStartOffset(blobSize - 1, true);
    testRangeRequestLastNBytes(1, true);
  }

  /**
   * Test range requests on a composite blob.
   * @throws Exception
   */
  @Test
  public void testRangeRequestCompositeBlob() throws Exception {
    // Random valid ranges
    for (int i = 0; i < 5; i++) {
      blobSize = random.nextInt(maxChunkSize) + maxChunkSize * random.nextInt(10);
      int randomOne = random.nextInt(blobSize);
      int randomTwo = random.nextInt(blobSize);
      testRangeRequestOffsetRange(Math.min(randomOne, randomTwo), Math.max(randomOne, randomTwo), true);
    }

    blobSize = random.nextInt(maxChunkSize) + maxChunkSize * random.nextInt(10);
    // Entire blob
    testRangeRequestOffsetRange(0, blobSize - 1, true);
    // Range that extends to end of blob
    testRangeRequestFromStartOffset(random.nextInt(blobSize), true);
    // Last n bytes of the blob
    testRangeRequestLastNBytes(random.nextInt(blobSize) + 1, true);
    // Last blobSize + 1 bytes (should not succeed)
    testRangeRequestLastNBytes(blobSize + 1, false);
    // Range over the end of the blob (should not succeed)
    testRangeRequestOffsetRange(random.nextInt(blobSize), blobSize + 5, false);
    // Ranges that start past the end of the blob (should not succeed)
    testRangeRequestFromStartOffset(blobSize, false);
    testRangeRequestOffsetRange(blobSize, blobSize + 20, false);
    // 0 byte range
    testRangeRequestLastNBytes(0, true);
    // 1 byte ranges
    testRangeRequestOffsetRange(0, 0, true);
    testRangeRequestOffsetRange(blobSize - 1, blobSize - 1, true);
    testRangeRequestFromStartOffset(blobSize - 1, true);
    testRangeRequestLastNBytes(1, true);

    blobSize = maxChunkSize * 2 + random.nextInt(maxChunkSize);
    // Single start chunk
    testRangeRequestOffsetRange(0, maxChunkSize - 1, true);
    // Single intermediate chunk
    testRangeRequestOffsetRange(maxChunkSize, maxChunkSize * 2 - 1, true);
    // Single end chunk
    testRangeRequestOffsetRange(maxChunkSize * 2, blobSize - 1, true);
    // Over chunk boundaries
    testRangeRequestOffsetRange(maxChunkSize / 2, maxChunkSize + maxChunkSize / 2, true);
    testRangeRequestFromStartOffset(maxChunkSize + maxChunkSize / 2, true);
  }

  /**
   * Test that the operation is completed and an exception with the error code {@link RouterErrorCode#ChannelClosed} is
   * set when the {@link ReadableStreamChannel} is closed before all chunks are read.
   * @throws Exception
   */
  @Test
  public void testEarlyReadableStreamChannelClose() throws Exception {
    for (int numChunksInBlob = 0; numChunksInBlob <= 4; numChunksInBlob++) {
      for (int numChunksToRead = 0; numChunksToRead < numChunksInBlob; numChunksToRead++) {
        testEarlyReadableStreamChannelClose(numChunksInBlob, numChunksToRead);
      }
    }
  }

  /**
   * Test the Errors {@link RouterErrorCode} received by Get Operation. The operation exception is set
   * based on the priority of these errors.
   * @throws Exception
   */
  @Test
  public void testSetOperationException() throws Exception {
    doPut();
    GetBlobOperation op = createOperation(null);
    RouterErrorCode[] routerErrorCodes = new RouterErrorCode[8];
    routerErrorCodes[0] = RouterErrorCode.BlobDoesNotExist;
    routerErrorCodes[1] = RouterErrorCode.OperationTimedOut;
    routerErrorCodes[2] = RouterErrorCode.UnexpectedInternalError;
    routerErrorCodes[3] = RouterErrorCode.AmbryUnavailable;
    routerErrorCodes[4] = RouterErrorCode.RangeNotSatisfiable;
    routerErrorCodes[5] = RouterErrorCode.BlobExpired;
    routerErrorCodes[6] = RouterErrorCode.BlobDeleted;
    routerErrorCodes[7] = RouterErrorCode.InvalidBlobId;

    for (int i = 0; i < routerErrorCodes.length; ++i) {
      op.setOperationException(new RouterException("RouterError", routerErrorCodes[i]));
      op.poll(requestRegistrationCallback);
      while (!op.isOperationComplete()) {
        time.sleep(routerConfig.routerRequestTimeoutMs + 1);
        op.poll(requestRegistrationCallback);
      }
      Assert.assertEquals(((RouterException) op.operationException.get()).getErrorCode(), routerErrorCodes[i]);
    }
    for (int i = routerErrorCodes.length - 1; i >= 0; --i) {
      op.setOperationException(new RouterException("RouterError", routerErrorCodes[i]));
      op.poll(requestRegistrationCallback);
      while (!op.isOperationComplete()) {
        time.sleep(routerConfig.routerRequestTimeoutMs + 1);
        op.poll(requestRegistrationCallback);
      }
      Assert.assertEquals(((RouterException) op.operationException.get()).getErrorCode(),
          routerErrorCodes[routerErrorCodes.length - 1]);
    }

    // set null to test non RouterException
    op.operationException.set(null);
    Exception nonRouterException = new Exception();
    op.setOperationException(nonRouterException);
    op.poll(requestRegistrationCallback);
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
    }
    Assert.assertEquals(nonRouterException, op.operationException.get());

    // test the edge case where current operationException is non RouterException
    op.setOperationException(new RouterException("RouterError", RouterErrorCode.BlobDeleted));
    op.poll(requestRegistrationCallback);
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
    }
    Assert.assertEquals(((RouterException) op.operationException.get()).getErrorCode(), RouterErrorCode.BlobDeleted);
  }

  /**
   * Test that the operation is completed and an exception with the error code {@link RouterErrorCode#ChannelClosed} is
   * set when the {@link ReadableStreamChannel} is closed before all chunks are read for a specific blob size and
   * number of chunks to read.
   * @param numChunksInBlob the number of chunks in the blob to put/get.
   * @param numChunksToRead the number of chunks to read from the {@link AsyncWritableChannel} before closing the
   *                        {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private void testEarlyReadableStreamChannelClose(int numChunksInBlob, final int numChunksToRead) throws Exception {
    final AtomicReference<Exception> callbackException = new AtomicReference<>();
    final AtomicReference<Future<Long>> readIntoFuture = new AtomicReference<>();
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    Callback<GetBlobResultInternal> callback = new Callback<GetBlobResultInternal>() {
      @Override
      public void onCompletion(final GetBlobResultInternal result, Exception exception) {
        if (exception != null) {
          callbackException.set(exception);
          readCompleteLatch.countDown();
        } else {
          final ByteBufferAsyncWritableChannel writableChannel = new ByteBufferAsyncWritableChannel();
          readIntoFuture.set(result.getBlobResult.getBlobDataChannel().readInto(writableChannel, null));
          Utils.newThread(new Runnable() {
            @Override
            public void run() {
              try {
                int chunksLeftToRead = numChunksToRead;
                while (chunksLeftToRead > 0) {
                  writableChannel.getNextChunk();
                  writableChannel.resolveOldestChunk(null);
                  chunksLeftToRead--;
                }
                result.getBlobResult.getBlobDataChannel().close();
              } catch (Exception e) {
                callbackException.set(e);
              } finally {
                readCompleteLatch.countDown();
              }
            }
          }, false).start();
        }
      }
    };

    blobSize = numChunksInBlob * maxChunkSize;
    doPut();
    GetBlobOperation op = createOperationAndComplete(callback);

    Assert.assertTrue("Timeout waiting for read to complete", readCompleteLatch.await(2, TimeUnit.SECONDS));
    if (callbackException.get() != null) {
      throw callbackException.get();
    }
    try {
      readIntoFuture.get().get();
      Assert.fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      Assert.assertTrue("Unexpected type for exception: " + e.getCause(), e.getCause() instanceof RouterException);
      Assert.assertEquals("Unexpected RouterErrorCode", RouterErrorCode.ChannelClosed,
          ((RouterException) e.getCause()).getErrorCode());
    }
    Exception operationException = op.getOperationException();
    Assert.assertTrue("Unexpected type for exception: " + operationException,
        operationException instanceof RouterException);
    Assert.assertEquals("Unexpected RouterErrorCode", RouterErrorCode.ChannelClosed,
        ((RouterException) operationException).getErrorCode());
  }

  /**
   * Send a range request and test that it either completes successfully or fails with a
   * {@link RouterErrorCode#RangeNotSatisfiable} error.
   * @param startOffset The start byte offset for the range request.
   * @param endOffset The end byte offset for the range request
   * @param rangeSatisfiable {@code true} if the range request should succeed.
   * @throws Exception
   */
  private void testRangeRequestOffsetRange(long startOffset, long endOffset, boolean rangeSatisfiable)
      throws Exception {
    doPut();
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
        .range(ByteRange.fromOffsetRange(startOffset, endOffset))
        .build(), false, routerMetrics.ageAtGet);
    getErrorCodeChecker.testAndAssert(rangeSatisfiable ? null : RouterErrorCode.RangeNotSatisfiable);
  }

  /**
   * Send a range request from a {@code startOffset} on and test that it either completes successfully or fails with a
   * {@link RouterErrorCode#RangeNotSatisfiable} error.
   * @param startOffset The start byte offset for the range request.
   * @param rangeSatisfiable {@code true} if the range request should succeed.
   * @throws Exception
   */
  private void testRangeRequestFromStartOffset(long startOffset, boolean rangeSatisfiable) throws Exception {
    doPut();
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
        .range(ByteRange.fromStartOffset(startOffset))
        .build(), false, routerMetrics.ageAtGet);
    getErrorCodeChecker.testAndAssert(rangeSatisfiable ? null : RouterErrorCode.RangeNotSatisfiable);
  }

  /**
   * Send a range request for the {@code lastNBytes} of an object and test that it either completes successfully or
   * fails with a {@link RouterErrorCode#RangeNotSatisfiable} error.
   * @param lastNBytes The start byte offset for the range request.
   * @param rangeSatisfiable {@code true} if the range request should succeed.
   * @throws Exception
   */
  private void testRangeRequestLastNBytes(long lastNBytes, boolean rangeSatisfiable) throws Exception {
    doPut();
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All)
        .range(ByteRange.fromLastNBytes(lastNBytes))
        .build(), false, routerMetrics.ageAtGet);
    getErrorCodeChecker.testAndAssert(rangeSatisfiable ? null : RouterErrorCode.RangeNotSatisfiable);
  }

  /**
   * Test that an operation is completed with a specified {@link RouterErrorCode} when all gets on data chunks
   * in a multi-part blob return a specified {@link ServerErrorCode}
   * @param serverErrorCode The error code to be returned when fetching data chunks.
   * @param expectedErrorCode The operation's expected error code.
   * @throws Exception
   */
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
          public void testAndAssert(RouterErrorCode expectedError) throws Exception {
            Callback<GetBlobResultInternal> callback = new Callback<GetBlobResultInternal>() {
              @Override
              public void onCompletion(final GetBlobResultInternal result, final Exception exception) {
                if (exception != null) {
                  asyncWritableChannel.close();
                  readCompleteLatch.countDown();
                } else {
                  Utils.newThread(new Runnable() {
                    @Override
                    public void run() {
                      try {
                        result.getBlobResult.getBlobDataChannel().readInto(asyncWritableChannel, new Callback<Long>() {
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
            assertFailureAndCheckErrorCode(op, expectedError);
          }
        });
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   */
  private void getAndAssertSuccess() throws Exception {
    getAndAssertSuccess(false, false);
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   * @param getChunksBeforeRead {@code true} if all chunks should be cached by the router before reading from the
   *                            stream.
   * @param initiateReadBeforeChunkGet Whether readInto() should be initiated immediately before data chunks are
   *                                   fetched by the router to simulate chunk arrival delay.
   */
  private void getAndAssertSuccess(final boolean getChunksBeforeRead, final boolean initiateReadBeforeChunkGet)
      throws Exception {
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    final AtomicReference<Exception> readCompleteException = new AtomicReference<>(null);
    final AtomicLong readCompleteResult = new AtomicLong(0);
    final AtomicReference<Exception> operationException = new AtomicReference<>(null);
    final int numChunks = ((blobSize + maxChunkSize - 1) / maxChunkSize) + (blobSize > maxChunkSize ? 1 : 0);
    mockNetworkClient.resetProcessedResponseCount();
    Callback<GetBlobResultInternal> callback = new Callback<GetBlobResultInternal>() {
      @Override
      public void onCompletion(final GetBlobResultInternal result, final Exception exception) {
        if (exception != null) {
          operationException.set(exception);
          readCompleteLatch.countDown();
        } else {
          try {
            switch (options.getBlobOptions.getOperationType()) {
              case All:
                BlobInfo blobInfo = result.getBlobResult.getBlobInfo();
                Assert.assertTrue("Blob properties must be the same",
                    RouterTestHelpers.haveEquivalentFields(blobProperties, blobInfo.getBlobProperties()));
                Assert.assertEquals("Blob size should in received blobProperties should be the same as actual",
                    blobSize, blobInfo.getBlobProperties().getBlobSize());
                Assert.assertArrayEquals("User metadata must be the same", userMetadata, blobInfo.getUserMetadata());
                break;
              case Data:
                Assert.assertNull("Unexpected blob info in operation result", result.getBlobResult.getBlobInfo());
                break;
            }
          } catch (Exception e) {
            readCompleteException.set(e);
          }

          final ByteBufferAsyncWritableChannel asyncWritableChannel = new ByteBufferAsyncWritableChannel();
          final Future<Long> preSetReadIntoFuture =
              initiateReadBeforeChunkGet ? result.getBlobResult.getBlobDataChannel()
                  .readInto(asyncWritableChannel, null) : null;
          Utils.newThread(new Runnable() {
            @Override
            public void run() {
              if (getChunksBeforeRead) {
                // wait for all chunks (data + metadata) to be received
                while (mockNetworkClient.getProcessedResponseCount()
                    < numChunks * routerConfig.routerGetRequestParallelism) {
                  Thread.yield();
                }
              }
              Future<Long> readIntoFuture = initiateReadBeforeChunkGet ? preSetReadIntoFuture
                  : result.getBlobResult.getBlobDataChannel().readInto(asyncWritableChannel, null);
              assertBlobReadSuccess(options.getBlobOptions, readIntoFuture, asyncWritableChannel,
                  result.getBlobResult.getBlobDataChannel(), readCompleteLatch, readCompleteResult,
                  readCompleteException);
            }
          }, false).start();
        }
      }
    };

    GetBlobOperation op = createOperationAndComplete(callback);

    readCompleteLatch.await();
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    if (operationException.get() != null) {
      throw operationException.get();
    }
    if (readCompleteException.get() != null) {
      throw readCompleteException.get();
    }
    // Ensure that a ChannelClosed exception is not set when the ReadableStreamChannel is closed correctly.
    Assert.assertNull("Callback operation exception should be null", op.getOperationException());
    if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo) {
      int sizeWritten = blobSize;
      if (options.getBlobOptions.getRange() != null) {
        ByteRange range = options.getBlobOptions.getRange().toResolvedByteRange(blobSize);
        sizeWritten = (int) (range.getEndOffset() - range.getStartOffset() + 1);
      }
      Assert.assertEquals("Size read must equal size written", sizeWritten, readCompleteResult.get());
    }
  }

  /**
   * Create a getBlob operation with the specified callback and poll until completion.
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createOperationAndComplete(Callback<GetBlobResultInternal> callback) throws Exception {
    GetBlobOperation op = createOperation(callback);
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.requestListToFill);
      for (ResponseInfo responseInfo : responses) {
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(
            new DataInputStream(new ByteBufferInputStream(responseInfo.getResponse())), mockClusterMap) : null;
        op.handleResponse(responseInfo, getResponse);
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
  private GetBlobOperation createOperation(Callback<GetBlobResultInternal> callback) throws Exception {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, callback,
            routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time);
    requestRegistrationCallback.requestListToFill = new ArrayList<>();
    return op;
  }

  /**
   * Check that an operation is complete and assert that it has failed with the specified {@link RouterErrorCode} set.
   * @param op The operation to check.
   * @param expectedError The error code expected.
   */
  private void assertFailureAndCheckErrorCode(GetBlobOperation op, RouterErrorCode expectedError) {
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
   * @param options The {@link GetBlobOptions} for the operation to check.
   * @param readIntoFuture The future associated with the read on the {@link ReadableStreamChannel} result of the
   *                       operation.
   * @param asyncWritableChannel The {@link ByteBufferAsyncWritableChannel} to which bytes will be written by the
   *                             operation.
   * @param readableStreamChannel The {@link ReadableStreamChannel} that bytes are read from in the operation.
   * @param readCompleteLatch The latch to count down once the read is completed.
   * @param readCompleteResult This will contain the bytes written on return.
   * @param readCompleteException This will contain any exceptions encountered during the read.
   */
  private void assertBlobReadSuccess(GetBlobOptions options, Future<Long> readIntoFuture,
      ByteBufferAsyncWritableChannel asyncWritableChannel, ReadableStreamChannel readableStreamChannel,
      CountDownLatch readCompleteLatch, AtomicLong readCompleteResult,
      AtomicReference<Exception> readCompleteException) {
    try {
      ByteBuffer putContentBuf = ByteBuffer.wrap(putContent);
      // If a range is set, compare the result against the specified byte range.
      if (options != null && options.getRange() != null) {
        ByteRange range = options.getRange().toResolvedByteRange(blobSize);
        putContentBuf = ByteBuffer.wrap(putContent, (int) range.getStartOffset(), (int) range.getRangeSize());
      }
      long written;
      Assert.assertTrue("ReadyForPollCallback should have been invoked as readInto() was called",
          mockNetworkClient.getAndClearWokenUpStatus());
      // Compare byte by byte.
      final int bytesToRead = putContentBuf.remaining();
      int readBytes = 0;
      do {
        ByteBuffer buf = asyncWritableChannel.getNextChunk();
        int bufLength = buf.remaining();
        Assert.assertTrue("total content read should not be greater than length of put content",
            readBytes + bufLength <= bytesToRead);
        while (buf.hasRemaining()) {
          Assert.assertEquals("Get and Put blob content should match", putContentBuf.get(), buf.get());
          readBytes++;
        }
        asyncWritableChannel.resolveOldestChunk(null);
        Assert.assertTrue("ReadyForPollCallback should have been invoked as writable channel callback was called",
            mockNetworkClient.getAndClearWokenUpStatus());
      } while (readBytes < bytesToRead);
      written = readIntoFuture.get();
      Assert.assertEquals("the returned length in the future should be the length of data written", (long) readBytes,
          written);
      Assert.assertNull("There should be no more data in the channel", asyncWritableChannel.getNextChunk(0));
      readableStreamChannel.close();
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
  private List<ResponseInfo> sendAndWaitForResponses(List<RequestInfo> requestList) throws IOException {
    int sendCount = requestList.size();
    // Shuffle the replicas to introduce randomness in the order in which responses arrive.
    Collections.shuffle(requestList);
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(mockNetworkClient.sendAndPoll(requestList, 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(mockNetworkClient.sendAndPoll(requestList, 100));
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
    properties.setProperty("router.get.operation.tracker.type", operationTrackerType);
    return properties;
  }
}

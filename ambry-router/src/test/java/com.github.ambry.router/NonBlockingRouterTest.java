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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;


/**
 * Class to test the {@link NonBlockingRouter}
 */
public class NonBlockingRouterTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int REQUEST_TIMEOUT_MS = 1000;
  private static final int PUT_REQUEST_PARALLELISM = 3;
  private static final int PUT_SUCCESS_TARGET = 2;
  private static final int GET_REQUEST_PARALLELISM = 2;
  private static final int GET_SUCCESS_TARGET = 1;
  private static final int DELETE_REQUEST_PARALLELISM = 3;
  private static final int DELETE_SUCCESS_TARGET = 2;
  private static final int AWAIT_TIMEOUT_MS = 2000;
  private static final int PUT_CONTENT_SIZE = 1000;
  private int maxPutChunkSize = PUT_CONTENT_SIZE;
  private final Random random = new Random();
  private NonBlockingRouter router;
  private PutManager putManager;
  private GetManager getManager;
  private DeleteManager deleteManager;
  private AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<MockSelectorState>();
  private final MockTime mockTime;
  private final MockClusterMap mockClusterMap;

  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;

  /**
   * Initialize parameters common to all tests.
   * @throws Exception
   */
  public NonBlockingRouterTest() throws Exception {
    mockTime = new MockTime();
    mockClusterMap = new MockClusterMap();
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @return the created VerifiableProperties instance.
   */
  private Properties getNonBlockingRouterProperties(String routerDataCenter) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(PUT_REQUEST_PARALLELISM));
    properties.setProperty("router.put.success.target", Integer.toString(PUT_SUCCESS_TARGET));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxPutChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(GET_REQUEST_PARALLELISM));
    properties.setProperty("router.get.success.target", Integer.toString(GET_SUCCESS_TARGET));
    properties.setProperty("router.delete.request.parallelism", Integer.toString(DELETE_REQUEST_PARALLELISM));
    properties.setProperty("router.delete.success.target", Integer.toString(DELETE_SUCCESS_TARGET));
    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.request.timeout.ms", Integer.toString(REQUEST_TIMEOUT_MS));
    return properties;
  }

  /**
   * Construct {@link Properties} and {@link MockServerLayout} and initialize and set the
   * router with them.
   */
  private void setRouter() throws IOException {
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap));
  }

  /**
   * Initialize and set the router with the given {@link Properties} and {@link MockServerLayout}
   * @param props the {@link Properties}
   * @param mockServerLayout the {@link MockServerLayout}
   */
  private void setRouter(Properties props, MockServerLayout mockServerLayout) throws IOException {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap,
        mockTime);
  }

  private void setOperationParams() {
    putBlobProperties =
        new BlobProperties(PUT_CONTENT_SIZE, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[PUT_CONTENT_SIZE];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Test the {@link NonBlockingRouterFactory}
   */
  @Test
  public void testNonBlockingRouterFactory() throws Exception {
    Properties props = getNonBlockingRouterProperties("NotInClusterMap");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    try {
      router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
          new LoggingNotificationSystem()).getRouter();
      Assert.fail("NonBlockingRouterFactory instantiation should have failed because the router datacenter is not in "
          + "the cluster map");
    } catch (IllegalStateException e) {
    }
    props = getNonBlockingRouterProperties("DC1");
    verifiableProperties = new VerifiableProperties((props));
    router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem()).getRouter();
    assertExpectedThreadCounts(1);
    router.close();
    assertExpectedThreadCounts(0);
  }

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic() throws Exception {
    setRouter();
    assertExpectedThreadCounts(1);
    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    router.getBlob(blobId, new GetBlobOptions()).get();
    router.getBlob(blobId, new GetBlobOptions(GetBlobOptions.OperationType.BlobInfo, GetOption.None, null)).get();
    router.deleteBlob(blobId).get();
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test behavior with various null inputs to router methods.
   * @throws Exception
   */
  @Test
  public void testNullArguments() throws Exception {
    setRouter();
    assertExpectedThreadCounts(1);
    setOperationParams();

    try {
      router.getBlob(null, new GetBlobOptions());
      Assert.fail("null blobId should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.getBlob("", null);
      Assert.fail("null options should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.putBlob(putBlobProperties, putUserMetadata, null);
      Assert.fail("null channel should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.putBlob(null, putUserMetadata, putChannel);
      Assert.fail("null blobProperties should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      router.deleteBlob(null);
      Assert.fail("null blobId should have resulted in IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    // null user metadata should work.
    router.putBlob(putBlobProperties, null, putChannel).get();

    router.close();
    assertExpectedThreadCounts(0);
    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test router put operation in a scenario where there are no partitions available.
   */
  @Test
  public void testRouterPartitionsUnavailable() throws Exception {
    setRouter();
    setOperationParams();
    mockClusterMap.markAllPartitionsUnavailable();
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
      Assert.fail("Put should have failed if there are no partitions");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals("Should have received AmbryUnavailable error", RouterErrorCode.AmbryUnavailable,
          r.getErrorCode());
    }
    router.close();
    assertExpectedThreadCounts(0);
    assertClosed();
  }

  /**
   * Test router put operation in a scenario where there are partitions, but none in the local DC.
   * This should not ideally happen unless there is a bad config, but the router should be resilient and
   * just error out these operations.
   */
  @Test
  public void testRouterNoPartitionInLocalDC() throws Exception {
    // set the local DC to invalid, so that for puts, no partitions are available locally.
    Properties props = getNonBlockingRouterProperties("invalidDC");
    setRouter(props, new MockServerLayout(mockClusterMap));
    setOperationParams();
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
      Assert.fail("Put should have failed if there are no partitions");
    } catch (Exception e) {
      RouterException r = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.UnexpectedInternalError, r.getErrorCode());
    }
    router.close();
    assertExpectedThreadCounts(0);
    assertClosed();
  }

  /**
   * Test RequestResponseHandler thread exit flow. If the RequestResponseHandlerThread exits on its own (due to a
   * Throwable), then the router gets closed immediately along with the completion of all the operations.
   */
  @Test
  public void testRequestResponseHandlerThreadExitFlow() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, mockTime);

    assertExpectedThreadCounts(1);

    setOperationParams();
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnAllPoll);
    Future future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    try {
      while (!future.isDone()) {
        mockTime.sleep(1000);
        Thread.yield();
      }
      future.get();
      Assert.fail("The operation should have failed");
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.OperationTimedOut, ((RouterException) e.getCause()).getErrorCode());
    }

    setOperationParams();
    mockSelectorState.set(MockSelectorState.ThrowThrowableOnSend);
    future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);

    // Now wait till the thread dies
    TestUtils.getThreadByThisName("RequestResponseHandlerThread").join();

    try {
      future.get();
      Assert.fail("The operation should have failed");
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.RouterClosed, ((RouterException) e.getCause()).getErrorCode());
    }

    assertClosed();

    // Ensure that both operations failed and with the right exceptions.
    Assert.assertEquals("No ChunkFiller Thread should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("No RequestResponseHandler should be running after the router is closed", 0,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Test that if a composite blob put fails, the successfully put data chunks are deleted.
   */
  @Test
  public void testUnsuccessfulPutDataChunkDelete() throws Exception {
    // Ensure there are 4 chunks.
    maxPutChunkSize = PUT_CONTENT_SIZE / 4;
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    // Since this test wants to ensure that successfully put data chunks are deleted when the overall put operation
    // fails, it uses a notification system to track the deletions.
    final CountDownLatch deletesDoneLatch = new CountDownLatch(2);
    LoggingNotificationSystem deleteTrackingNotificationSystem = new LoggingNotificationSystem() {
      @Override
      public void onBlobDeleted(String blobId) {
        deletesDoneLatch.countDown();
      }
    };
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), deleteTrackingNotificationSystem, mockClusterMap,
        mockTime);

    setOperationParams();

    List<DataNodeId> dataNodeIds = mockClusterMap.getDataNodeIds();
    List<ServerErrorCode> serverErrorList = new ArrayList<>();
    // There are 4 chunks for this blob.
    // All put operations make one request to each local server as there are 3 servers overall in the local DC.
    // Set the state of the mock servers so that they return success for the first 2 requests in order to succeed
    // the first two chunks.
    serverErrorList.add(ServerErrorCode.No_Error);
    serverErrorList.add(ServerErrorCode.No_Error);
    // fail requests for third and fourth data chunks including the slipped put attempts:
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    serverErrorList.add(ServerErrorCode.Unknown_Error);
    // all subsequent requests (no more puts, but there will be deletes) will succeed.
    for (DataNodeId dataNodeId : dataNodeIds) {
      MockServer server = mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setServerErrors(serverErrorList);
    }

    // Submit the put operation and wait for it to fail.
    try {
      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    } catch (ExecutionException e) {
      Assert.assertEquals(RouterErrorCode.AmbryUnavailable, ((RouterException) e.getCause()).getErrorCode());
    }

    // Now, wait until the deletes of the successfully put blobs are complete.
    Assert.assertTrue("Deletes should not take longer than " + AWAIT_TIMEOUT_MS,
        deletesDoneLatch.await(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    router.close();
    assertClosed();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit() throws Exception {
    final int SCALING_UNITS = 3;
    Properties props = getNonBlockingRouterProperties("DC1");
    props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
    setRouter(props, new MockServerLayout(mockClusterMap));
    assertExpectedThreadCounts(SCALING_UNITS);

    // Submit a few jobs so that all the scaling units get exercised.
    for (int i = 0; i < SCALING_UNITS * 10; i++) {
      setOperationParams();
      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    }
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    setOperationParams();
    assertClosed();
  }

  /**
   * Response handling related tests for all operation managers.
   */
  @Test
  public void testResponseHandling() throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    setOperationParams();
    final List<ReplicaId> failedReplicaIds = new ArrayList<>();
    final AtomicInteger successfulResponseCount = new AtomicInteger(0);
    final AtomicBoolean invalidResponse = new AtomicBoolean(false);
    ResponseHandler mockResponseHandler = new ResponseHandler(mockClusterMap) {
      @Override
      public void onEvent(ReplicaId replicaId, Object e) {
        if (e instanceof ServerErrorCode) {
          if (e == ServerErrorCode.No_Error) {
            successfulResponseCount.incrementAndGet();
          } else {
            invalidResponse.set(true);
          }
        } else {
          failedReplicaIds.add(replicaId);
        }
      }
    };

    // Instantiate a router just to put a blob successfully.
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    setRouter(props, mockServerLayout);
    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    router.close();
    for (MockServer mockServer : mockServerLayout.getMockServers()) {
      mockServer.setServerErrorForAllRequests(ServerErrorCode.No_Error);
    }

    NetworkClient networkClient =
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime).getNetworkClient();

    putManager = new PutManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
        new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new OperationCompleteCallback(new AtomicInteger(0)), new ReadyForPollCallback(networkClient),
        new ArrayList<String>(), 0, mockTime);
    OperationHelper opHelper = new OperationHelper(OperationType.PUT);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, null, successfulResponseCount,
        invalidResponse, PUT_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, null, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, null);

    opHelper = new OperationHelper(OperationType.GET);
    getManager = new GetManager(mockClusterMap, mockResponseHandler, new RouterConfig(verifiableProperties),
        new NonBlockingRouterMetrics(mockClusterMap), new OperationCompleteCallback(new AtomicInteger(0)),
        new ReadyForPollCallback(networkClient), mockTime);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, GET_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, blobId, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, blobId);

    opHelper = new OperationHelper(OperationType.DELETE);
    deleteManager = new DeleteManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
        new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new OperationCompleteCallback(new AtomicInteger(0)), mockTime);
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testFailureDetectorNotification(opHelper, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, DELETE_REQUEST_PARALLELISM - 1);
    testNoResponseNoNotification(opHelper, failedReplicaIds, blobId, successfulResponseCount, invalidResponse);
    testResponseDeserializationError(opHelper, networkClient, blobId);
    putManager.close();
    getManager.close();
    deleteManager.close();
  }

  /**
   * Test that failure detector is correctly notified for all responses regardless of the order in which successful
   * and failed responses arrive.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link NetworkClient}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   * @param indexToFail if greater than 0, the index representing which response for which failure is to be simulated.
   *                    For example, if index is 0, then the first response will be failed.
   *                    If the index is -1, no responses will be failed, and successful responses will be returned to
   *                    the operation managers.
   */
  private void testFailureDetectorNotification(OperationHelper opHelper, NetworkClient networkClient,
      List<ReplicaId> failedReplicaIds, String blobId, AtomicInteger successfulResponseCount,
      AtomicBoolean invalidResponse, int indexToFail) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
    }
    ReplicaId replicaIdToFail =
        indexToFail == -1 ? null : ((RouterRequestInfo) allRequests.get(indexToFail)).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      ResponseInfo responseInfo;
      if (replicaIdToFail != null && replicaIdToFail.equals(((RouterRequestInfo) requestInfo).getReplicaId())) {
        responseInfo = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
      } else {
        List<RequestInfo> requestInfoListToSend = new ArrayList<>();
        requestInfoListToSend.add(requestInfo);
        List<ResponseInfo> responseInfoList;
        loopStartTimeMs = SystemTime.getInstance().milliseconds();
        do {
          if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
            Assert.fail("Waited too long for the response.");
          }
          responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, 10);
          requestInfoListToSend.clear();
        } while (responseInfoList.size() == 0);
        responseInfo = responseInfoList.get(0);
      }
      opHelper.handleResponse(responseInfo);
    }
    futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    if (indexToFail == -1) {
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          opHelper.requestParallelism, successfulResponseCount.get());
      Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    } else {
      Assert.assertEquals("Failure detector should have been notified", 1, failedReplicaIds.size());
      Assert.assertEquals("Failed notification should have arrived for the failed replica", replicaIdToFail,
          failedReplicaIds.get(0));
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          opHelper.requestParallelism - 1, successfulResponseCount.get());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    }
  }

  /**
   * Test that failure detector is not notified when the router times out requests.
   * @param opHelper the {@link OperationHelper}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   */
  private void testNoResponseNoNotification(OperationHelper opHelper, List<ReplicaId> failedReplicaIds, String blobId,
      AtomicInteger successfulResponseCount, AtomicBoolean invalidResponse) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (!futureResult.isDone()) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
      mockTime.sleep(REQUEST_TIMEOUT_MS + 1);
    }
    Assert.assertEquals("Successful notification should not have arrived for replicas that were up", 0,
        successfulResponseCount.get());
    Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
    Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
  }

  /**
   * Test that operations succeed even in the presence of responses that are corrupt and fail to deserialize.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link NetworkClient}
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @throws Exception
   */
  private void testResponseDeserializationError(OperationHelper opHelper, NetworkClient networkClient, String blobId)
      throws Exception {
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests);
    }
    List<ResponseInfo> responseInfoList = new ArrayList<>();
    loopStartTimeMs = SystemTime.getInstance().milliseconds();
    do {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for the response.");
      }
      responseInfoList.addAll(networkClient.sendAndPoll(allRequests, 10));
      allRequests.clear();
    } while (responseInfoList.size() < requestParallelism);
    // corrupt the first response.
    ByteBuffer response = responseInfoList.get(0).getResponse();
    byte b = response.get(response.limit() - 1);
    response.put(response.limit() - 1, (byte) ~b);
    for (ResponseInfo responseInfo : responseInfoList) {
      opHelper.handleResponse(responseInfo);
    }
    try {
      futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.fail("Operation should have succeeded with one corrupt response");
    }
  }

  /**
   * Assert that the number of ChunkFiller and RequestResponseHandler threads running are as expected.
   * @param expectedCount the expected number of ChunkFiller and RequestResponseHandler threads.
   */
  private void assertExpectedThreadCounts(int expectedCount) {
    Assert.assertEquals("Number of RequestResponseHandler threads running should be as expected", expectedCount,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("Number of chunkFiller threads running should be as expected", expectedCount,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    if (expectedCount == 0) {
      Assert.assertFalse("Router should be closed if there are no worker threads running", router.isOpen());
      Assert.assertEquals("All operations should have completed if the router is closed", 0,
          router.getOperationsCount());
    }
  }

  /**
   * Assert that submission after closing the router returns a future that is already done and an appropriate
   * exception.
   */
  private void assertClosed() {
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }

  /**
   * Enum for the three operation types.
   */
  private enum OperationType {
    PUT, GET, DELETE,
  }

  /**
   * A helper class to abstract away the details about specific operation manager.
   */
  private class OperationHelper {
    final OperationType opType;
    int requestParallelism = 0;

    /**
     * Construct an OperationHelper object with the associated type.
     * @param opType the type of operation.
     */
    OperationHelper(OperationType opType) {
      this.opType = opType;
      switch (opType) {
        case PUT:
          requestParallelism = PUT_REQUEST_PARALLELISM;
          break;
        case GET:
          requestParallelism = GET_REQUEST_PARALLELISM;
          break;
        case DELETE:
          requestParallelism = DELETE_REQUEST_PARALLELISM;
          break;
      }
    }

    /**
     * Submit a put, get or delete operation based on the associated {@link OperationType} of this object.
     * @param blobId the blobId to get or delete. For puts, this is ignored.
     * @return the {@link FutureResult} associated with the submitted operation.
     */
    FutureResult submitOperation(String blobId) {
      FutureResult futureResult = null;
      switch (opType) {
        case PUT:
          futureResult = new FutureResult<String>();
          ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
          putManager.submitPutBlobOperation(putBlobProperties, putUserMetadata, putChannel, futureResult, null);
          break;
        case GET:
          futureResult = new FutureResult<BlobInfo>();
          getManager.submitGetBlobOperation(blobId,
              new GetBlobOptions(GetBlobOptions.OperationType.BlobInfo, GetOption.None, null), futureResult, null);
          break;
        case DELETE:
          futureResult = new FutureResult<BlobInfo>();
          deleteManager.submitDeleteBlobOperation(blobId, futureResult, null);
          break;
      }
      return futureResult;
    }

    /**
     * Poll the associated operation manager.
     * @param requestInfos the list of {@link RequestInfo} to pass in the poll call.
     */
    void pollOpManager(List<RequestInfo> requestInfos) {
      switch (opType) {
        case PUT:
          putManager.poll(requestInfos);
          break;
        case GET:
          getManager.poll(requestInfos);
          break;
        case DELETE:
          deleteManager.poll(requestInfos);
          break;
      }
    }

    /**
     * Hand over a responseInfo to the operation manager.
     * @param responseInfo the {@link ResponseInfo} to hand over.
     */
    void handleResponse(ResponseInfo responseInfo) {
      switch (opType) {
        case PUT:
          putManager.handleResponse(responseInfo);
          break;
        case GET:
          getManager.handleResponse(responseInfo);
          break;
        case DELETE:
          deleteManager.handleResponse(responseInfo);
          break;
      }
    }
  }
}

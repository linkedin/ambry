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
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
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
  private static final int PUT_REQUEST_PARALLELISM = 3;
  private static final int PUT_SUCCESS_TARGET = 2;
  private static final int GET_REQUEST_PARALLELISM = 2;
  private static final int GET_SUCCESS_TARGET = 1;
  private static final int DELETE_REQUEST_PARALLELISM = 3;
  private static final int DELETE_SUCCESS_TARGET = 2;
  private final Random random = new Random();
  private NonBlockingRouter router;
  private AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<MockSelectorState>();

  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;

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
    properties.setProperty("router.get.request.parallelism", Integer.toString(GET_REQUEST_PARALLELISM));
    properties.setProperty("router.get.success.target", Integer.toString(GET_SUCCESS_TARGET));
    properties.setProperty("router.delete.request.parallelism", Integer.toString(DELETE_REQUEST_PARALLELISM));
    properties.setProperty("router.delete.success.target", Integer.toString(DELETE_SUCCESS_TARGET));
    return properties;
  }

  private void setOperationParams() {
    putBlobProperties = new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[100];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Test the {@link NonBlockingRouterFactory}
   */
  @Test
  public void testNonBlockingRouterFactory()
      throws Exception {
    MockClusterMap mockClusterMap = new MockClusterMap();
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
  public void testRouterBasic()
      throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, mockTime);

    assertExpectedThreadCounts(1);

    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    router.getBlob(blobId);
    router.getBlobInfo(blobId);
    router.deleteBlob(blobId);
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit()
      throws Exception {
    final int SCALING_UNITS = 3;
    Properties props = getNonBlockingRouterProperties("DC1");
    props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, mockTime);

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
   * Test that response handler is correctly notified for all responses regardless of the order in which successful
   * and failed responses arrive.
   */
  @Test
  public void testResponseHandlerNotification()
      throws Exception {
    Properties props = getNonBlockingRouterProperties("DC1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    setOperationParams();
    final List<ReplicaId> failedReplicaIds = new ArrayList<>();
    final AtomicInteger successfulResponseCount = new AtomicInteger(0);
    final AtomicBoolean invalidResponse = new AtomicBoolean(false);
    ResponseHandler mockResponseHandler = new ResponseHandler(mockClusterMap) {
      @Override
      public void onRequestResponseException(ReplicaId replicaId, Exception e) {
        failedReplicaIds.add(replicaId);
      }

      @Override
      public void onRequestResponseError(ReplicaId replicaId, ServerErrorCode serverErrorCode) {
        if (serverErrorCode == ServerErrorCode.No_Error) {
          successfulResponseCount.incrementAndGet();
        } else {
          invalidResponse.set(true);
        }
      }
    };

    // Instantiate a router just to put a blob successfully.
    MockServerLayout mockServerLayout = new MockServerLayout(mockClusterMap);
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap,
        mockTime);
    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    router.close();
    for (MockServer mockServer : mockServerLayout.getMockServers()) {
      mockServer.setBlobIdToServerErrorCode(blobId, ServerErrorCode.No_Error);
    }

    NetworkClient networkClient =
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime).getNetworkClient();

    PutManager putManager = new PutManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
        new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
        new OperationCompleteCallback(new AtomicInteger(0)), new ReadyForPollCallback(networkClient), 0, mockTime);
    testPutResponseNotification(putManager, networkClient, failedReplicaIds, successfulResponseCount, invalidResponse,
        -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testPutResponseNotification(putManager, networkClient, failedReplicaIds, successfulResponseCount, invalidResponse,
        0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testPutResponseNotification(putManager, networkClient, failedReplicaIds, successfulResponseCount, invalidResponse,
        PUT_REQUEST_PARALLELISM - 1);

    GetManager getManager = new GetManager(mockClusterMap, mockResponseHandler, new RouterConfig(verifiableProperties),
        new NonBlockingRouterMetrics(mockClusterMap), new OperationCompleteCallback(new AtomicInteger(0)),
        new ReadyForPollCallback(networkClient), mockTime);
    testGetResponseNotification(getManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testGetResponseNotification(getManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testGetResponseNotification(getManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, GET_REQUEST_PARALLELISM - 1);

    DeleteManager deleteManager =
        new DeleteManager(mockClusterMap, mockResponseHandler, new LoggingNotificationSystem(),
            new RouterConfig(verifiableProperties), new NonBlockingRouterMetrics(mockClusterMap),
            new OperationCompleteCallback(new AtomicInteger(0)), mockTime);
    testDeleteResponseNotification(deleteManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, -1);
    // Test that if a failed response comes before the operation is completed, failure detector is notified.
    testDeleteResponseNotification(deleteManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, 0);
    // Test that if a failed response comes after the operation is completed, failure detector is notified.
    testDeleteResponseNotification(deleteManager, networkClient, failedReplicaIds, blobId, successfulResponseCount,
        invalidResponse, DELETE_REQUEST_PARALLELISM - 1);
    putManager.close();
    getManager.close();
    deleteManager.close();
  }

  /**
   * Test that the failure handler is notified for put responses.
   * @param putManager the {@link PutManager}
   * @param networkClient the {@link NetworkClient}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   * @param indexToFail the index representing which response for which failure is to be simulated. For example,
   *                    if index is 0, then the first response will be failed. If the index is -1, no responses will be
   *                    failed.
   */
  void testPutResponseNotification(PutManager putManager, NetworkClient networkClient, List<ReplicaId> failedReplicaIds,
      AtomicInteger successfulResponseCount, AtomicBoolean invalidResponse, int indexToFail)
      throws Exception {
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    FutureResult<String> futureResult = new FutureResult<>();
    putManager.submitPutBlobOperation(putBlobProperties, putUserMetadata, putChannel, futureResult, null);
    List<RequestInfo> allRequests = new ArrayList<>();
    while (allRequests.size() < PUT_REQUEST_PARALLELISM) {
      putManager.poll(allRequests);
    }
    ReplicaId replicaIdToFail =
        indexToFail == -1 ? null : ((RouterRequestInfo) allRequests.get(indexToFail)).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      if (replicaIdToFail != null && replicaIdToFail.equals(((RouterRequestInfo) requestInfo).getReplicaId())) {
        mockSelectorState.set(MockSelectorState.DisconnectOnSend);
      } else {
        mockSelectorState.set(MockSelectorState.Good);
      }
      List<RequestInfo> requestInfoListToSend = new ArrayList<>();
      requestInfoListToSend.add(requestInfo);
      List<ResponseInfo> responseInfoList;
      do {
        responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, 10);
        requestInfoListToSend.clear();
      } while (responseInfoList.size() == 0);
      putManager.handleResponse(responseInfoList.get(0));
    }
    futureResult.await();
    if (indexToFail == -1) {
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          PUT_REQUEST_PARALLELISM, successfulResponseCount.get());
      Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    } else {
      Assert.assertEquals("Failure detector should have been notified", 1, failedReplicaIds.size());
      Assert.assertEquals("Failed notification should have arrived for the failed replica", replicaIdToFail,
          failedReplicaIds.get(0));
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          PUT_REQUEST_PARALLELISM - 1, successfulResponseCount.get());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    }
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
  }

  /**
   * Test that the failure handler is notified for get responses.
   * @param getManager the {@link GetManager}
   * @param networkClient the {@link NetworkClient}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to delete.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   * @param indexToFail the index representing which response for which failure is to be simulated. For example,
   *                    if index is 0, then the first response will be failed. If the index is -1, no responses will be
   *                    failed.
   */
  private void testGetResponseNotification(GetManager getManager, NetworkClient networkClient,
      List<ReplicaId> failedReplicaIds, String blobId, AtomicInteger successfulResponseCount,
      AtomicBoolean invalidResponse, int indexToFail)
      throws Exception {
    FutureResult<BlobInfo> futureResult = new FutureResult<>();
    getManager.submitGetBlobInfoOperation(blobId, futureResult, null);
    List<RequestInfo> allRequests = new ArrayList<>();
    while (allRequests.size() < GET_REQUEST_PARALLELISM) {
      getManager.poll(allRequests);
    }
    ReplicaId replicaIdToFail =
        indexToFail == -1 ? null : ((RouterRequestInfo) allRequests.get(indexToFail)).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      if (replicaIdToFail != null && replicaIdToFail.equals(((RouterRequestInfo) requestInfo).getReplicaId())) {
        mockSelectorState.set(MockSelectorState.DisconnectOnSend);
      } else {
        mockSelectorState.set(MockSelectorState.Good);
      }
      List<RequestInfo> requestInfoListToSend = new ArrayList<>();
      requestInfoListToSend.add(requestInfo);
      List<ResponseInfo> responseInfoList;
      do {
        responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, 10);
        requestInfoListToSend.clear();
      } while (responseInfoList.size() == 0);
      getManager.handleResponse(responseInfoList.get(0));
    }
    futureResult.get();
    if (indexToFail == -1) {
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          GET_REQUEST_PARALLELISM, successfulResponseCount.get());
      Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    } else {
      Assert.assertEquals("Failure detector should have been notified", 1, failedReplicaIds.size());
      Assert.assertEquals("Failed notification should have arrived for the failed replica", replicaIdToFail,
          failedReplicaIds.get(0));
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          GET_REQUEST_PARALLELISM - 1, successfulResponseCount.get());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    }
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
  }

  /**
   * Test that the failure handler is notified for delete responses.
   * @param deleteManager the {@link DeleteManager}
   * @param networkClient the {@link NetworkClient}
   * @param failedReplicaIds the list that will contain all the replicas for which failure was notified.
   * @param blobId the id of the blob to delete.
   * @param successfulResponseCount the AtomicInteger that will contain the count of replicas for which success was
   *                                notified.
   * @param invalidResponse the AtomicBoolean that will contain whether an unexpected failure was notified.
   * @param indexToFail the index representing which response for which failure is to be simulated. For example,
   *                    if index is 0, then the first response will be failed. If the index is -1, no responses will be
   *                    failed.
   */
  private void testDeleteResponseNotification(DeleteManager deleteManager, NetworkClient networkClient,
      List<ReplicaId> failedReplicaIds, String blobId, AtomicInteger successfulResponseCount,
      AtomicBoolean invalidResponse, int indexToFail)
      throws Exception {
    FutureResult<Void> futureResult = new FutureResult<>();
    deleteManager.submitDeleteBlobOperation(blobId, futureResult, null);
    List<RequestInfo> allRequests = new ArrayList<>();
    while (allRequests.size() < DELETE_REQUEST_PARALLELISM) {
      deleteManager.poll(allRequests);
    }
    ReplicaId replicaIdToFail =
        indexToFail == -1 ? null : ((RouterRequestInfo) allRequests.get(indexToFail)).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      if (replicaIdToFail != null && replicaIdToFail.equals(((RouterRequestInfo) requestInfo).getReplicaId())) {
        mockSelectorState.set(MockSelectorState.DisconnectOnSend);
      } else {
        mockSelectorState.set(MockSelectorState.Good);
      }
      List<RequestInfo> requestInfoListToSend = new ArrayList<>();
      requestInfoListToSend.add(requestInfo);
      List<ResponseInfo> responseInfoList;
      do {
        responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, 10);
        requestInfoListToSend.clear();
      } while (responseInfoList.size() == 0);
      deleteManager.handleResponse(responseInfoList.get(0));
    }
    futureResult.await();
    if (indexToFail == -1) {
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          DELETE_REQUEST_PARALLELISM, successfulResponseCount.get());
      Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    } else {
      Assert.assertEquals("Failure detector should have been notified", 1, failedReplicaIds.size());
      Assert.assertEquals("Failed notification should have arrived for the failed replica", replicaIdToFail,
          failedReplicaIds.get(0));
      Assert.assertEquals("Successful notification should have arrived for replicas that were up",
          DELETE_REQUEST_PARALLELISM - 1, successfulResponseCount.get());
      Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    }
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
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
      Assert
          .assertEquals("All operations should have completed if the router is closed", 0, router.getOperationsCount());
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
}

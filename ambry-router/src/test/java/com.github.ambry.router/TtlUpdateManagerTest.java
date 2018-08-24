/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link TtlUpdateManager}
 */
public class TtlUpdateManagerTest {
  private static final int DEFAULT_PARALLELISM = 3;
  // changing this may fail tests
  private static final int DEFAULT_SUCCESS_TARGET = 2;
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int ADVANCE_TIME_INCREMENT_MS = 1000;
  private static final byte[] PUT_CONTENT = new byte[1000];
  private static final int BLOBS_COUNT = 5;
  private static final String UPDATE_SERVICE_ID = "update-service-id";
  private static final String LOCAL_DC = "DC1";

  static {
    TestUtils.RANDOM.nextBytes(PUT_CONTENT);
  }

  private final NonBlockingRouter router;
  private final TtlUpdateManager ttlUpdateManager;
  private final NetworkClient networkClient;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
  private final MockClusterMap clusterMap = new MockClusterMap();
  private final MockServerLayout serverLayout = new MockServerLayout(clusterMap);
  private final MockTime time = new MockTime();
  private final List<String> blobIds = new ArrayList<>(BLOBS_COUNT);
  private final TtlUpdateNotificationSystem notificationSystem = new TtlUpdateNotificationSystem();
  private final int serverCount = serverLayout.getMockServers().size();
  private final AccountService accountService = new InMemAccountService(true, false);

  /**
   * Sets up all required components including a blob.
   * @throws IOException
   */
  public TtlUpdateManagerTest() throws Exception {
    assertTrue("Server count has to be at least 9", serverCount >= 9);
    VerifiableProperties vProps =
        new VerifiableProperties(getNonBlockingRouterProperties(DEFAULT_SUCCESS_TARGET, DEFAULT_PARALLELISM));
    RouterConfig routerConfig = new RouterConfig(vProps);
    NonBlockingRouterMetrics metrics = new NonBlockingRouterMetrics(clusterMap);
    MockNetworkClientFactory networkClientFactory =
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, time);
    router =
        new NonBlockingRouter(routerConfig, metrics, networkClientFactory, notificationSystem, clusterMap, null, null,
            null, new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);
    for (int i = 0; i < BLOBS_COUNT; i++) {
      ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(PUT_CONTENT));
      BlobProperties putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, TTL_SECS,
          Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false);
      String blobId = router.putBlob(putBlobProperties, new byte[0], putChannel, new PutBlobOptionsBuilder().build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      blobIds.add(blobId);
    }
    ttlUpdateManager =
        new TtlUpdateManager(clusterMap, new ResponseHandler(clusterMap), notificationSystem, accountService,
            routerConfig, metrics, time);
    networkClient = networkClientFactory.getNetworkClient();
  }

  /**
   * Closes the router and ttl manager and does some post verification.
   */
  @After
  public void cleanUp() {
    ttlUpdateManager.close();
    assertCloseCleanup(router);
  }

  /**
   * Basic test for a TTL update through the {@link Router} (failure cases w.r.t interaction with Router in
   * {@link NonBlockingRouterTest}.
   * @throws Exception
   */
  @Test
  public void basicThroughRouterTest() throws Exception {
    for (String blobId : blobIds) {
      assertTtl(router, Collections.singleton(blobId), TTL_SECS);
      TestCallback<Void> callback = new TestCallback<>();
      notificationSystem.reset();
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time, callback).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      notificationSystem.checkNotifications(1, null, Utils.Infinite_Time);
      assertTrue("Callback was not called", callback.getLatch().await(10, TimeUnit.MILLISECONDS));
      assertNull("There should be no exception in the callback", callback.getException());
      assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);
    }
  }

  /**
   * Test where TTL update is done for a single blob at a time.
   * @throws Exception
   */
  @Test
  public void singleBlobThroughTtlManagerTest() throws Exception {
    for (String blobId : blobIds) {
      assertTtl(router, Collections.singleton(blobId), TTL_SECS);
      executeOpAndVerify(Collections.singleton(blobId), null, false, false, false, true);
      // ok to do it again
      executeOpAndVerify(Collections.singleton(blobId), null, false, false, false, true);
    }
  }

  /**
   * Test where TTL update is done for multiple blobs at the same time
   * @throws Exception
   */
  @Test
  public void batchedThroughTtlManagerTest() throws Exception {
    assertTtl(router, blobIds, TTL_SECS);
    executeOpAndVerify(blobIds, null, false, false, false, true);
    // ok to do it again
    executeOpAndVerify(blobIds, null, false, false, false, true);
  }

  /**
   * Test to ensure that failure of a single TTL update in a batch fails the entire batch
   * @throws Exception
   */
  @Test
  public void singleFailureInBatchTtlUpdateTest() throws Exception {
    // configure failure for one of the blobs
    serverLayout.getMockServers()
        .forEach(
            mockServer -> mockServer.setErrorCodeForBlob(blobIds.get(BLOBS_COUNT / 2), ServerErrorCode.Unknown_Error));
    executeOpAndVerify(blobIds, RouterErrorCode.UnexpectedInternalError, false, false, false, false);
  }

  /**
   * Tests to make sure {@link ServerErrorCode}s map to the right {@link RouterErrorCode}.
   * @throws Exception
   */
  @Test
  public void individualErrorCodesTest() throws Exception {
    Map<ServerErrorCode, RouterErrorCode> errorCodeMap = new HashMap<>();
    errorCodeMap.put(ServerErrorCode.Blob_Deleted, RouterErrorCode.BlobDeleted);
    errorCodeMap.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    errorCodeMap.put(ServerErrorCode.Blob_Not_Found, RouterErrorCode.BlobDoesNotExist);
    errorCodeMap.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    errorCodeMap.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    errorCodeMap.put(ServerErrorCode.Blob_Update_Not_Allowed, RouterErrorCode.BlobUpdateNotAllowed);
    errorCodeMap.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    for (ServerErrorCode errorCode : ServerErrorCode.values()) {
      if (errorCode == ServerErrorCode.No_Error || errorCode == ServerErrorCode.Blob_Already_Updated) {
        continue;
      }
      ArrayList<ServerErrorCode> serverErrorCodes =
          new ArrayList<>(Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found));
      // has to be repeated because the op tracker returns failure if it sees 8/9 failures and the success target is 2
      serverErrorCodes.set(3, errorCode);
      serverErrorCodes.set(5, errorCode);
      Collections.shuffle(serverErrorCodes);
      setServerErrorCodes(serverErrorCodes, serverLayout);
      RouterErrorCode expected = errorCodeMap.getOrDefault(errorCode, RouterErrorCode.UnexpectedInternalError);
      executeOpAndVerify(blobIds, expected, false, true, true, false);
    }
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
    assertTtl(router, blobIds, TTL_SECS);
  }

  /**
   * Tests to ensure that {@link RouterErrorCode}s are properly resolved based on precedence
   * @throws Exception
   */
  @Test
  public void routerErrorCodeResolutionTest() throws Exception {
    LinkedHashMap<ServerErrorCode, RouterErrorCode> codesToSetAndTest = new LinkedHashMap<>();

    // test 4 codes
    codesToSetAndTest.put(ServerErrorCode.Blob_Deleted, RouterErrorCode.BlobDeleted);
    codesToSetAndTest.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    codesToSetAndTest.put(ServerErrorCode.Blob_Update_Not_Allowed, RouterErrorCode.BlobUpdateNotAllowed);
    codesToSetAndTest.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    doRouterErrorCodeResolutionTest(codesToSetAndTest);

    // test another 4 codes
    codesToSetAndTest.clear();
    codesToSetAndTest.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    codesToSetAndTest.put(ServerErrorCode.Blob_Update_Not_Allowed, RouterErrorCode.BlobUpdateNotAllowed);
    codesToSetAndTest.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    codesToSetAndTest.put(ServerErrorCode.IO_Error, RouterErrorCode.UnexpectedInternalError);
    doRouterErrorCodeResolutionTest(codesToSetAndTest);
  }

  /**
   * Tests to make sure that the quorum is respected
   * @throws Exception
   */
  @Test
  public void fixedCountSuccessfulResponseTest() throws Exception {
    for (int i = 0; i <= DEFAULT_SUCCESS_TARGET; i++) {
      boolean shouldSucceed = i == DEFAULT_SUCCESS_TARGET;
      doFixedCountSuccessfulResponseTest(i, shouldSucceed, ServerErrorCode.No_Error);
      doFixedCountSuccessfulResponseTest(i, shouldSucceed, ServerErrorCode.Blob_Already_Updated);
    }
  }

  /**
   * Tests for behavior on timeouts
   * @throws Exception
   */
  @Test
  public void responseTimeoutTest() throws Exception {
    // configure servers to not respond to requests
    serverLayout.getMockServers().forEach(mockServer -> mockServer.setShouldRespond(false));
    executeOpAndVerify(blobIds, RouterErrorCode.OperationTimedOut, true, true, true, false);
  }

  /**
   * Test for behavior on errors in the network client and selector
   * @throws Exception
   */
  @Test
  public void networkClientAndSelectorErrorsTest() throws Exception {
    for (MockSelectorState state : MockSelectorState.values()) {
      if (state == MockSelectorState.Good) {
        continue;
      }
      mockSelectorState.set(state);
      executeOpAndVerify(blobIds, RouterErrorCode.OperationTimedOut, true, true, true, false);
    }
  }

  /**
   * Checks that operations with duplicate blob Ids are rejected
   * @throws RouterException
   */
  @Test
  public void duplicateBlobIdsTest() throws RouterException {
    blobIds.add(blobIds.get(0));
    try {
      ttlUpdateManager.submitTtlUpdateOperation(blobIds, UPDATE_SERVICE_ID, Utils.Infinite_Time, new FutureResult<>(),
          new TestCallback<>());
      fail("Should have failed to submit operation because the provided blob id list contains duplicates");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  // helpers
  // general

  /**
   * Executes a ttl update operations and verifies results
   * @param ids the collection of ids to ttl update
   * @param expectedErrorCode the expected {@link RouterErrorCode} if failure is expected. {@code null} if expected to
   *                          succeed
   * @param advanceTime if {@code true}, advances time after each poll and handleResponse iteration
   * @param ignoreUnrecognizedRequests if {@code true}, doesn't throw an exception if a response is received for a
   *                                   request not sent in this execution of the function
   * @param verifyNoNotificationsOnFailure if {@code true}, verifies that there are no notifications on failure.
   * @param verifyTtlAfterUpdate if {@code true}, verify the TTL after the update succeeds/fails
   * @throws Exception
   */
  private void executeOpAndVerify(Collection<String> ids, RouterErrorCode expectedErrorCode, boolean advanceTime,
      boolean ignoreUnrecognizedRequests, boolean verifyNoNotificationsOnFailure, boolean verifyTtlAfterUpdate)
      throws Exception {
    FutureResult<Void> future = new FutureResult<>();
    TestCallback<Void> callback = new TestCallback<>();
    NonBlockingRouter.currentOperationsCount.addAndGet(ids.size());
    notificationSystem.reset();
    ttlUpdateManager.submitTtlUpdateOperation(ids, UPDATE_SERVICE_ID, Utils.Infinite_Time, future, callback);
    sendRequestsGetResponses(future, ttlUpdateManager, advanceTime, ignoreUnrecognizedRequests);
    long expectedTtlSecs = TTL_SECS;
    if (expectedErrorCode == null) {
      assertTrue("Future should be complete", future.isDone());
      assertEquals("Callback should be done", 0, callback.getLatch().getCount());
      if (future.error() != null) {
        throw future.error();
      }
      if (callback.getException() != null) {
        throw callback.getException();
      }
      notificationSystem.checkNotifications(ids.size(), UPDATE_SERVICE_ID, Utils.Infinite_Time);
      expectedTtlSecs = Utils.Infinite_Time;
    } else {
      assertFailureAndCheckErrorCode(future, callback, expectedErrorCode);
      if (verifyNoNotificationsOnFailure) {
        notificationSystem.checkNotifications(0, null, null);
      }
    }
    if (verifyTtlAfterUpdate) {
      assertTtl(router, ids, expectedTtlSecs);
    }
  }

  /**
   * Sends all the requests that the {@code manager} may have ready
   * @param futureResult the {@link FutureResult} that tracks the operation
   * @param manager the {@link TtlUpdateManager} to poll for requests
   * @param advanceTime if {@code true}, advances time after each poll and handleResponse iteration
   * @param ignoreUnrecognizedRequests if {@code true}, doesn't throw an exception if a response is received for a
   *                                   request not sent in this execution of the function
   */
  private void sendRequestsGetResponses(FutureResult<Void> futureResult, TtlUpdateManager manager, boolean advanceTime,
      boolean ignoreUnrecognizedRequests) {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    Set<RequestInfo> requestAcks = new HashSet<>();
    List<RequestInfo> referenceRequestInfos = new ArrayList<>();
    while (!futureResult.isDone()) {
      manager.poll(requestInfoList);
      referenceRequestInfos.addAll(requestInfoList);
      List<ResponseInfo> responseInfoList = new ArrayList<>();
      try {
        responseInfoList = networkClient.sendAndPoll(requestInfoList, AWAIT_TIMEOUT_MS);
      } catch (RuntimeException | Error e) {
        if (!advanceTime) {
          throw e;
        }
      }
      for (ResponseInfo responseInfo : responseInfoList) {
        RequestInfo requestInfo = responseInfo.getRequestInfo();
        assertNotNull("RequestInfo is null", requestInfo);
        if (!referenceRequestInfos.contains(requestInfo)) {
          if (ignoreUnrecognizedRequests) {
            continue;
          }
          throw new IllegalStateException("Received response for unrecognized request");
        } else if (requestAcks.contains(requestInfo)) {
          // received a second response for the same request
          throw new IllegalStateException("Received response more than once for a request");
        }
        requestAcks.add(requestInfo);
        RouterRequestInfo routerRequestInfo = (RouterRequestInfo) responseInfo.getRequestInfo();
        RequestOrResponseType type = ((RequestOrResponse) routerRequestInfo.getRequest()).getRequestType();
        switch (type) {
          case TtlUpdateRequest:
            manager.handleResponse(responseInfo);
            break;
          default:
            throw new IllegalStateException("Unrecognized request type: " + type);
        }
      }
      if (advanceTime) {
        time.sleep(ADVANCE_TIME_INCREMENT_MS);
      }
      requestInfoList.clear();
    }
  }

  /**
   * Generates {@link Properties} that includes initial configuration.
   *
   * @return Properties
   */
  private Properties getNonBlockingRouterProperties(int successTarget, int parallelism) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", LOCAL_DC);
    properties.setProperty("router.ttl.update.success.target", Integer.toString(successTarget));
    properties.setProperty("router.ttl.update.request.parallelism", Integer.toString(parallelism));
    return properties;
  }

  // fixedCountSuccessfulResponseTest() helpers

  /**
   * Does the fixed count successful response test by setting the appropriate number of successful responses
   * @param successfulResponsesCount the number of successful responses
   * @param shouldSucceed {@code true} if the operation must succeed
   * @param errorCodeToReturn the {@link ServerErrorCode} to configure the servers to return
   * @throws Exception
   */
  private void doFixedCountSuccessfulResponseTest(int successfulResponsesCount, boolean shouldSucceed,
      ServerErrorCode errorCodeToReturn) throws Exception {
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(LOCAL_DC)) {
        serversInLocalDc.add(mockServer);
      }
    });
    if (successfulResponsesCount > serversInLocalDc.size()) {
      throw new IllegalArgumentException(successfulResponsesCount + " > num servers: " + serverCount);
    }
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);
    for (int i = 0; i < successfulResponsesCount; i++) {
      serversInLocalDc.get(i).setServerErrorForAllRequests(errorCodeToReturn);
    }
    executeOpAndVerify(blobIds, shouldSucceed ? null : RouterErrorCode.BlobDoesNotExist, false, true, true, false);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  // routerErrorCodeResolutionTest() helpers

  /**
   * Runs the router code resolution test based on the input
   * @param codesToSetAndTest a {@link LinkedHashMap} that defines the ordering of the router error codes and also
   *                          provides the server error codes that must be set and their equivalent router error codes.
   * @throws Exception
   */
  private void doRouterErrorCodeResolutionTest(LinkedHashMap<ServerErrorCode, RouterErrorCode> codesToSetAndTest)
      throws Exception {
    if (codesToSetAndTest.size() * 2 > serverCount) {
      throw new IllegalStateException("Cannot run test because there aren't enough servers for the given codes");
    }
    List<ServerErrorCode> serverErrorCodes =
        new ArrayList<>(Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found));
    List<RouterErrorCode> expected = new ArrayList<>(codesToSetAndTest.size());
    // fill in the array with all the error codes that need resolution and knock them off one by one
    // has to be repeated because the op tracker returns failure if it sees 8/9 failures and the success target is 2
    int serverIdx = 0;
    for (Map.Entry<ServerErrorCode, RouterErrorCode> entry : codesToSetAndTest.entrySet()) {
      serverErrorCodes.set(serverIdx, entry.getKey());
      serverErrorCodes.set(serverIdx + 1, entry.getKey());
      expected.add(entry.getValue());
      serverIdx += 2;
    }
    expected.add(RouterErrorCode.BlobDoesNotExist);
    for (int i = 0; i < expected.size(); i++) {
      List<ServerErrorCode> shuffled = new ArrayList<>(serverErrorCodes);
      Collections.shuffle(shuffled);
      setServerErrorCodes(shuffled, serverLayout);
      executeOpAndVerify(blobIds, expected.get(i), false, true, true, false);
      if (i * 2 + 1 < serverErrorCodes.size()) {
        serverErrorCodes.set(i * 2, ServerErrorCode.Blob_Not_Found);
        serverErrorCodes.set(i * 2 + 1, ServerErrorCode.Blob_Not_Found);
      }
    }
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
    assertTtl(router, blobIds, TTL_SECS);
  }
}

/**
 * Derived from {@link LoggingNotificationSystem} and customized for ttl update tests
 */
class TtlUpdateNotificationSystem extends LoggingNotificationSystem {
  private final AtomicInteger updatesInitiated = new AtomicInteger();
  private final AtomicReference<String> receivedUpdateServiceId = new AtomicReference<>();
  private final AtomicReference<Long> receivedUpdateExpiresAtMs = new AtomicReference<>(null);
  private final AtomicReference<Boolean> mismatchedData = new AtomicReference<>(false);

  @Override
  public void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account,
      Container container) {
    updatesInitiated.incrementAndGet();
    if (receivedUpdateServiceId.get() == null) {
      receivedUpdateServiceId.set(serviceId);
    } else if (!receivedUpdateServiceId.get().equals(serviceId)) {
      mismatchedData.set(true);
    }
    if (receivedUpdateExpiresAtMs.get() == null) {
      receivedUpdateExpiresAtMs.set(expiresAtMs);
    } else if (receivedUpdateExpiresAtMs.get() != expiresAtMs) {
      mismatchedData.set(true);
    }
  }

  /**
   * Resets the tracking variables of the notification system
   */
  void reset() {
    updatesInitiated.set(0);
    receivedUpdateServiceId.set(null);
    receivedUpdateExpiresAtMs.set(null);
    mismatchedData.set(false);
  }

  /**
   * Checks the notification system updates
   * @param expectedNumUpdates the number of update events expected
   * @param expectedServiceId the service id expected in the update events
   * @param expectedExpiresAtMs the expiry time (ms) expected in the update events
   */
  void checkNotifications(int expectedNumUpdates, String expectedServiceId, Long expectedExpiresAtMs) {
    assertEquals("Incorrect number of updates", expectedNumUpdates, updatesInitiated.get());
    if (expectedNumUpdates > 0) {
      assertFalse("Received mismatched data in notification system update", mismatchedData.get());
      assertEquals("Unexpected value for service ID", expectedServiceId, receivedUpdateServiceId.get());
      assertEquals("Unexpected value for expiresAtMs", expectedExpiresAtMs, receivedUpdateExpiresAtMs.get());
    }
  }
}

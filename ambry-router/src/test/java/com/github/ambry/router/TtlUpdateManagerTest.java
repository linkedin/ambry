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
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.NettyByteBufLeakHelper;
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
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
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
  private String localDc;
  private final NonBlockingRouter router;
  private final TtlUpdateManager ttlUpdateManager;
  private final SocketNetworkClient networkClient;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
  private final MockClusterMap clusterMap = new MockClusterMap();
  private final MockServerLayout serverLayout = new MockServerLayout(clusterMap);
  private final MockTime time = new MockTime();
  private final List<String> blobIds = new ArrayList<>(BLOBS_COUNT);
  private final TtlUpdateNotificationSystem notificationSystem = new TtlUpdateNotificationSystem();
  private final int serverCount = serverLayout.getMockServers().size();
  private final AccountService accountService = new InMemAccountService(true, false);
  private final QuotaChargeCallback quotaChargeCallback = QuotaTestUtils.createTestQuotaChargeCallback();
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  /**
   * Sets up all required components including a blob.
   * @throws IOException
   */
  public TtlUpdateManagerTest() throws Exception {
    assertTrue("Server count has to be at least 9", serverCount >= 9);
    localDc = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    VerifiableProperties vProps =
        new VerifiableProperties(getNonBlockingRouterProperties(DEFAULT_SUCCESS_TARGET, DEFAULT_PARALLELISM));
    RouterConfig routerConfig = new RouterConfig(vProps);
    NonBlockingRouterMetrics metrics = new NonBlockingRouterMetrics(clusterMap, null);
    MockNetworkClientFactory networkClientFactory =
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, time);
    router =
        new NonBlockingRouter(routerConfig, metrics, networkClientFactory, notificationSystem, clusterMap, null, null,
            null, new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    for (int i = 0; i < BLOBS_COUNT; i++) {
      ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(PUT_CONTENT));
      BlobProperties putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, TTL_SECS,
          Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null, null, null);
      String blobId = router.putBlob(putBlobProperties, new byte[0], putChannel, new PutBlobOptionsBuilder().build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      blobIds.add(blobId);
    }
    ttlUpdateManager =
        new TtlUpdateManager(clusterMap, new ResponseHandler(clusterMap), notificationSystem, accountService,
            routerConfig, metrics, time, router);
    networkClient = networkClientFactory.getNetworkClient();
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  /**
   * Closes the router and ttl manager and does some post verification.
   */
  @After
  public void cleanUp() {
    ttlUpdateManager.close();
    assertCloseCleanup(router);

    nettyByteBufLeakHelper.afterTest();
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
      router.updateBlobTtl(blobId, null, Utils.Infinite_Time, callback, quotaChargeCallback)
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
      executeOpAndVerify(Collections.singleton(blobId), null, false, false, false, true, false);
      // ok to do it again
      executeOpAndVerify(Collections.singleton(blobId), null, false, false, false, true, false);
    }
  }

  /**
   * Test to ensure that the metadata chunk gets updated last.
   * @throws Exception
   */
  @Test
  public void metadataChunkUpdatedLastTest() throws Exception {
    // configure failure for the metadata chunk
    serverLayout.getMockServers()
        .forEach(mockServer -> mockServer.setErrorCodeForBlob(blobIds.get(0), ServerErrorCode.Unknown_Error));
    executeOpAndVerifyWithInfinity(blobIds, RouterErrorCode.UnexpectedInternalError, false, false, false,
        blobIds.subList(1, blobIds.size()));
  }

  /**
   * Test for the case when delete operation fails due to quota compliance.
   */
  @Test
  public void testQuotaRejected() throws Exception {
    executeOpAndVerify(blobIds, RouterErrorCode.TooManyRequests, false, true, true, false, true);
  }

  /**
   * Test where TTL update is done for multiple blobs at the same time
   * @throws Exception
   */
  @Test
  public void batchedThroughTtlManagerTest() throws Exception {
    assertTtl(router, blobIds, TTL_SECS);
    executeOpAndVerify(blobIds, null, false, false, false, true, false);
    // ok to do it again
    executeOpAndVerify(blobIds, null, false, false, false, true, false);
  }

  /**
   * Test to ensure that failure of a single TTL update in a batch fails the entire batch
   * @throws Exception
   */
  @Test
  public void singleFailureInBatchTtlUpdateTest() throws Exception {
    assertTtl(router, blobIds, TTL_SECS);
    // configure failure for one of the blobs
    serverLayout.getMockServers()
        .forEach(
            mockServer -> mockServer.setErrorCodeForBlob(blobIds.get(BLOBS_COUNT / 2), ServerErrorCode.Unknown_Error));
    executeOpAndVerify(blobIds, RouterErrorCode.UnexpectedInternalError, false, false, false, false, false);
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
    // In production, disk_unavailable usually means disk is bad with I/O errors. For now, the only way to fix this is
    // to replace disk and relies on replication to restore data. If all replicas return disk unavailable (should be
    // extremely rare in real world), it means blob is no long present and it's should be ok to return BlobDoesNotExist.
    // But for simplicity, we will return AmbryUnavailable. Once disks are replaced, we will begin to return
    // BlobDoesNotExist.
    errorCodeMap.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    errorCodeMap.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    errorCodeMap.put(ServerErrorCode.Blob_Update_Not_Allowed, RouterErrorCode.BlobUpdateNotAllowed);
    errorCodeMap.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    for (ServerErrorCode errorCode : ServerErrorCode.values()) {
      if (errorCode == ServerErrorCode.No_Error || errorCode == ServerErrorCode.Blob_Already_Updated) {
        continue;
      }
      ArrayList<ServerErrorCode> serverErrorCodes =
          new ArrayList<>(Collections.nCopies(serverCount, ServerErrorCode.IO_Error));
      // has to be repeated because the op tracker returns failure if it sees 8/9 failures and the success target is 2
      serverErrorCodes.set(3, errorCode);
      serverErrorCodes.set(5, errorCode);
      Collections.shuffle(serverErrorCodes);
      setServerErrorCodes(serverErrorCodes, serverLayout);
      RouterErrorCode expected = errorCodeMap.getOrDefault(errorCode, RouterErrorCode.UnexpectedInternalError);
      executeOpAndVerify(blobIds, expected, false, true, true, false, false);
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
    executeOpAndVerify(blobIds, RouterErrorCode.OperationTimedOut, true, true, true, false, false);
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
      executeOpAndVerify(blobIds, RouterErrorCode.OperationTimedOut, true, true, true, false, false);
    }
  }

  /**
   * Checks that operations with duplicate blob Ids are rejected
   * @throws RouterException
   */
  @Test
  public void duplicateBlobIdsTest() throws RouterException {
    blobIds.add(blobIds.get(1));
    try {
      ttlUpdateManager.submitTtlUpdateOperation(blobIds.get(0), blobIds.subList(1, blobIds.size()), UPDATE_SERVICE_ID,
          Utils.Infinite_Time, new FutureResult<>(), new TestCallback<>(), quotaChargeCallback);
      fail("Should have failed to submit operation because the provided blob id list contains duplicates");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Test the case when some of the replicas in originating DC are unavailable, we should return AmbryUnavailable.
   * @throws Exception
   */
  @Test
  public void testOrigDcUnavailability() throws Exception {
    // Default all replicas to return not found.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not found from bootstrap, 2 not found from standby in originating dc
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(localDc)) {
        serversInLocalDc.add(mockServer);
      }
    });
    for (String blob : blobIds) {
      BlobId blobId = RouterUtils.getBlobIdFromString(blob, clusterMap);
      MockPartitionId partitionId = (MockPartitionId) blobId.getPartition();
      ReplicaId boostrapReplica = partitionId.replicaIds.stream()
          .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(localDc))
          .filter(replicaId -> replicaId.getDataNodeId().getHostname().equals(serversInLocalDc.get(0).getHostName()))
          .findFirst()
          .get();
      partitionId.setReplicaState(boostrapReplica, ReplicaState.BOOTSTRAP);
    }
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    executeOpAndVerify(blobIds, RouterErrorCode.AmbryUnavailable, false, true, true, false, false);
  }

  /**
   * Test the case when all of the replicas in originating DC return NotFound, we should return BlobNotFound.
   * @throws Exception
   */
  @Test
  public void testOrigDcNotFound() throws Exception {
    // Default all replicas to return IO error.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.IO_Error);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not found from bootstrap, 2 not found from standby in originating dc
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(localDc)) {
        serversInLocalDc.add(mockServer);
      }
    });
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);

    executeOpAndVerify(blobIds, RouterErrorCode.BlobDoesNotExist, false, true, true, false, false);
  }

  /**
   * Test the case when error precedence is maintained with unavailability in originating DC.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithOrigDcUnavailability() throws Exception {
    // Default all replicas to return not found.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not found from bootstrap, 2 not found from standby in originating dc
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(localDc)) {
        serversInLocalDc.add(mockServer);
      }
    });
    for (String blob : blobIds) {
      BlobId blobId = RouterUtils.getBlobIdFromString(blob, clusterMap);
      MockPartitionId partitionId = (MockPartitionId) blobId.getPartition();
      ReplicaId boostrapReplica = partitionId.replicaIds.stream()
          .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(localDc))
          .filter(replicaId -> replicaId.getDataNodeId().getHostname().equals(serversInLocalDc.get(0).getHostName()))
          .findFirst()
          .get();
      partitionId.setReplicaState(boostrapReplica, ReplicaState.BOOTSTRAP);
    }
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);

    // Set two remote servers to return Blob_Update_Not_Allowed
    List<MockServer> serversInRemoteDc = new ArrayList<>(serverLayout.getMockServers());
    serversInRemoteDc.removeAll(serversInLocalDc);
    serversInRemoteDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Update_Not_Allowed);
    serversInRemoteDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Update_Not_Allowed);

    executeOpAndVerify(blobIds, RouterErrorCode.BlobUpdateNotAllowed, false, true, true, false, false);
  }

  /**
   * Test the case when there is 2 success in originating data center, we should return success.
   * @throws Exception
   */
  @Test
  public void testOrigDcSuccess() throws Exception {
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not found from bootstrap, 2 not found from standby in originating dc
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(localDc)) {
        serversInLocalDc.add(mockServer);
      }
    });
    for (String blob : blobIds) {
      BlobId blobId = RouterUtils.getBlobIdFromString(blob, clusterMap);
      MockPartitionId partitionId = (MockPartitionId) blobId.getPartition();
      ReplicaId boostrapReplica = partitionId.replicaIds.stream()
          .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(localDc))
          .filter(replicaId -> replicaId.getDataNodeId().getHostname().equals(serversInLocalDc.get(0).getHostName()))
          .findFirst()
          .get();
      partitionId.setReplicaState(boostrapReplica, ReplicaState.BOOTSTRAP);
    }
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.No_Error);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.No_Error);
    executeOpAndVerify(blobIds, null, false, true, true, false, false);
  }

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
   * @param isQuotaRejected if {@code true}, return quota rejected ResponseInfo back to op.
   * @throws Exception
   */
  private void executeOpAndVerify(Collection<String> ids, RouterErrorCode expectedErrorCode, boolean advanceTime,
      boolean ignoreUnrecognizedRequests, boolean verifyNoNotificationsOnFailure, boolean verifyTtlAfterUpdate,
      boolean isQuotaRejected) throws Exception {
    FutureResult<Void> future = new FutureResult<>();
    TestCallback<Void> callback = new TestCallback<>();
    notificationSystem.reset();
    List<String> chunkIds = new ArrayList<>(ids);
    router.currentOperationsCount.addAndGet(ids.size() == 1 ? 1 : ids.size() - 1);
    ttlUpdateManager.close();
    ttlUpdateManager.submitTtlUpdateOperation(chunkIds.get(0), chunkIds.subList(1, chunkIds.size()), UPDATE_SERVICE_ID,
        Utils.Infinite_Time, future, callback, quotaChargeCallback);
    sendRequestsGetResponses(future, ttlUpdateManager, advanceTime, ignoreUnrecognizedRequests, isQuotaRejected);
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
      assertTtl(router, chunkIds, expectedTtlSecs);
    }
  }

  /**
   * Executes a ttl update operations and verifies results
   * @param ids the collection of ids to ttl update
   * @param expectedErrorCode the expected {@link RouterErrorCode} if failure is expected. {@code null} if expected to
   *                          succeed
   * @param advanceTime if {@code true}, advances time after each poll and handleResponse iteration
   * @param ignoreUnrecognizedRequests if {@code true}, doesn't throw an exception if a response is received for a
   *                                   request not sent in this execution of the function
   * @param verifyNoNotificationsOnFailure if {@code true}, verifies that there are no notifications on failure.
   * @param verifyTtlUpdatedList the collection of chunk ids for which ttl should be verified to have been applied.
   * @throws Exception
   */
  private void executeOpAndVerifyWithInfinity(Collection<String> ids, RouterErrorCode expectedErrorCode,
      boolean advanceTime, boolean ignoreUnrecognizedRequests, boolean verifyNoNotificationsOnFailure,
      Collection<String> verifyTtlUpdatedList) throws Exception {
    executeOpAndVerify(ids, expectedErrorCode, advanceTime, ignoreUnrecognizedRequests, verifyNoNotificationsOnFailure,
        false, false);
    assertTtl(router, verifyTtlUpdatedList, Utils.Infinite_Time);
  }

  // helpers
  // general

  /**
   * Sends all the requests that the {@code manager} may have ready
   * @param futureResult the {@link FutureResult} that tracks the operation
   * @param manager the {@link TtlUpdateManager} to poll for requests
   * @param advanceTime if {@code true}, advances time after each poll and handleResponse iteration
   * @param ignoreUnrecognizedRequests if {@code true}, doesn't throw an exception if a response is received for a
   *                                   request not sent in this execution of the function
   * @param isQuotaRejected if {@code true}, return quota rejected ResponseInfo back to op.
   */
  private void sendRequestsGetResponses(FutureResult<Void> futureResult, TtlUpdateManager manager, boolean advanceTime,
      boolean ignoreUnrecognizedRequests, boolean isQuotaRejected) {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    Set<Integer> requestsToDrop = new HashSet<>();
    Set<RequestInfo> requestAcks = new HashSet<>();
    List<RequestInfo> referenceRequestInfos = new ArrayList<>();
    while (!futureResult.isDone()) {
      manager.poll(requestInfoList, requestsToDrop);
      referenceRequestInfos.addAll(requestInfoList);
      List<ResponseInfo> responseInfoList = new ArrayList<>();
      if (isQuotaRejected) {
        responseInfoList = requestInfoList.stream()
            .map(requestInfo -> new ResponseInfo(requestInfo, true))
            .collect(Collectors.toList());
      } else {
        try {
          responseInfoList = networkClient.sendAndPoll(requestInfoList, requestsToDrop, AWAIT_TIMEOUT_MS);
        } catch (RuntimeException | Error e) {
          if (!advanceTime) {
            throw e;
          }
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
        RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
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
      responseInfoList.forEach(ResponseInfo::release);
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
    properties.setProperty("router.datacenter.name", localDc);
    properties.setProperty("router.ttl.update.success.target", Integer.toString(successTarget));
    properties.setProperty("router.ttl.update.request.parallelism", Integer.toString(parallelism));
    return properties;
  }

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
      if (mockServer.getDataCenter().equals(localDc)) {
        serversInLocalDc.add(mockServer);
      }
    });
    if (successfulResponsesCount > serversInLocalDc.size()) {
      throw new IllegalArgumentException(successfulResponsesCount + " > num servers: " + serverCount);
    }
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);
    if (successfulResponsesCount > 0) {
      for (int i = 0; i < successfulResponsesCount; i++) {
        serversInLocalDc.get(i).setServerErrorForAllRequests(errorCodeToReturn);
      }
      // Set error code from other local dc servers as Disk_Unavailable since it is not practical that we get 1 found
      // and 2 not found from healthy local dc servers.
      for (int i = successfulResponsesCount; i < serversInLocalDc.size(); i++) {
        serversInLocalDc.get(i).setServerErrorForAllRequests(ServerErrorCode.Disk_Unavailable);
      }
    }
    RouterErrorCode expectedErrorCode =
        (successfulResponsesCount > 0) ? RouterErrorCode.AmbryUnavailable : RouterErrorCode.BlobDoesNotExist;
    executeOpAndVerify(blobIds, shouldSucceed ? null : expectedErrorCode, false, true, true, false, false);
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  // fixedCountSuccessfulResponseTest() helpers

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
        new ArrayList<>(Collections.nCopies(serverCount, ServerErrorCode.IO_Error));
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
    expected.add(RouterErrorCode.UnexpectedInternalError);
    for (int i = 0; i < expected.size(); i++) {
      List<ServerErrorCode> shuffled = new ArrayList<>(serverErrorCodes);
      Collections.shuffle(shuffled);
      setServerErrorCodes(shuffled, serverLayout);
      executeOpAndVerify(blobIds, expected.get(i), false, true, true, false, false);
      if (i * 2 + 1 < serverErrorCodes.size()) {
        serverErrorCodes.set(i * 2, ServerErrorCode.IO_Error);
        serverErrorCodes.set(i * 2 + 1, ServerErrorCode.IO_Error);
      }
    }
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
    assertTtl(router, blobIds, TTL_SECS);
  }

  static {
    TestUtils.RANDOM.nextBytes(PUT_CONTENT);
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

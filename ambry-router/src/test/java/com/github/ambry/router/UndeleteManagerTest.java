/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.router.RouterTestHelpers.*;
import static org.junit.Assert.*;


/**
 * Unit test for {@link UndeleteManager} and {@link UndeleteOperation}.
 */
public class UndeleteManagerTest {
  private static final int DEFAULT_PARALLELISM = 3;
  // changing this may fail tests
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int ADVANCE_TIME_INCREMENT_MS = 1000;
  private static final byte[] PUT_CONTENT = new byte[1000];
  private static final int BLOBS_COUNT = 5;
  private static final String UNDELETE_SERVICE_ID = "undelete-service-id";
  private static final String LOCAL_DC = "DC1";
  private final NonBlockingRouter router;
  private final RouterConfig routerConfig;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
  private final MockClusterMap clusterMap = new MockClusterMap();
  private final MockServerLayout serverLayout = new MockServerLayout(clusterMap);
  private final MockTime time = new MockTime();
  private final List<String> blobIds = new ArrayList<>(BLOBS_COUNT);
  private final AccountService accountService = new InMemAccountService(true, false);
  private final MockNetworkClientFactory networkClientFactory;
  private final NonBlockingRouterMetrics metrics;
  private UndeleteManager undeleteManager;
  private SocketNetworkClient networkClient;

  /**
   * Constructor to setup UndeleteManagerTest
   */
  public UndeleteManagerTest() throws Exception {
    VerifiableProperties vProps = new VerifiableProperties(getNonBlockingRouterProperties(DEFAULT_PARALLELISM));
    routerConfig = new RouterConfig(vProps);
    metrics = new NonBlockingRouterMetrics(clusterMap, null);
    networkClientFactory = new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
        CHECKOUT_TIMEOUT_MS, serverLayout, time);
    NotificationSystem notificationSystem = new LoggingNotificationSystem();
    router =
        new NonBlockingRouter(routerConfig, metrics, networkClientFactory, notificationSystem, clusterMap, null, null,
            null, new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);
  }

  @Before
  public void setup() throws Exception {
    blobIds.clear();
    for (int i = 0; i < BLOBS_COUNT; i++) {
      ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(PUT_CONTENT));
      BlobProperties putBlobProperties =
          new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
              Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), false, null);
      String blobId = router.putBlob(putBlobProperties, new byte[0], putChannel, new PutBlobOptionsBuilder().build())
          .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      blobIds.add(blobId);
      // Make sure all the mock servers have this put
      BlobId id = new BlobId(blobId, clusterMap);
      for (MockServer server : serverLayout.getMockServers()) {
        if (!server.getBlobs().containsKey(blobId)) {
          server.send(
              new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
                  id, putBlobProperties, ByteBuffer.wrap(new byte[0]), Unpooled.wrappedBuffer(PUT_CONTENT),
                  PUT_CONTENT.length, BlobType.DataBlob, null)).release();
        }
      }
    }
    undeleteManager = new UndeleteManager(clusterMap, new ResponseHandler(clusterMap), new LoggingNotificationSystem(),
        accountService, routerConfig, metrics, time);
    networkClient = networkClientFactory.getNetworkClient();
  }

  /**
   * Closes the router and undelete manager and does some post verification.
   */
  @After
  public void cleanup() {
    undeleteManager.close();
    // Only first test would use router, so close it here.
    assertCloseCleanup(router);
  }

  /**
   * Basic test for an undelete through the {@link Router} (failure cases w.r.t interaction with Router in
   * {@link NonBlockingRouterTest}.
   * @throws Exception
   */
  @Test
  public void basicThroughRouterTest() throws Exception {
    // first make sure all the mock servers have the put
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
      // Undelete requires global quorum, so we have to make sure all the mock servers received a delete.
      router.undeleteBlob(blobId, UNDELETE_SERVICE_ID).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      verifyUndelete(blobId);
    }
  }

  /**
   * Tests single blob id through undelete manager and server returns no error.
   * @throws Exception
   */
  @Test
  public void singleBlobThroughManagerTest() throws Exception {
    for (String blobId : blobIds) {
      executeOpAndVerify(Collections.singleton(blobId), RouterErrorCode.BlobNotDeleted);
      deleteBlobInAllServer(blobId);
      executeOpAndVerify(Collections.singleton(blobId), null);
      // Already undeleted, should return BlobUndeleted.
      executeOpAndVerify(Collections.singleton(blobId), RouterErrorCode.BlobUndeleted);
    }
  }

  /**
   * Tests all blob ids at once through undelete manager and server returns no error.
   * @throws Exception
   */
  @Test
  public void batchBlobThroughManagerTest() throws Exception {
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
    }
    executeOpAndVerify(blobIds, null);
  }

  /**
   * Failure tests when at least two servers in a datacenter return different lifeVersion, which
   * causes lifeVersion conflict error.
   * @throws Exception
   */
  @Test
  public void lifeVersionConflictTest() throws Exception {
    HashMap<String, List<MockServer>> dcToMockServers = new HashMap<>();
    serverLayout.getMockServers()
        .forEach(server -> dcToMockServers.computeIfAbsent(server.getDataCenter(), k -> new ArrayList()).add(server));
    List<String> dcs = new ArrayList<>(dcToMockServers.keySet());
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
      // Randomly pick a datacenter and change the lifeVersion of first two mock server to 5.
      int dcIdx = TestUtils.RANDOM.nextInt(dcs.size());
      List<MockServer> servers = dcToMockServers.get(dcs.get(dcIdx));
      for (int i = 0; i < 2; i++) {
        StoredBlob blob = servers.get(i).getBlobs().get(blobId);
        blob.lifeVersion = 5;
      }
      executeOpAndVerify(Collections.singleton(blobId), RouterErrorCode.LifeVersionConflict);
    }
  }

  /**
   * Failure tests when two servers in a datacenter return blob_not_found, which breaks global quorum.
   * @throws Exception
   */
  @Test
  public void singleFailureTest() throws Exception {
    HashMap<String, List<MockServer>> dcToMockServers = new HashMap<>();
    serverLayout.getMockServers()
        .forEach(server -> dcToMockServers.computeIfAbsent(server.getDataCenter(), k -> new ArrayList()).add(server));
    String dc = new ArrayList<>(dcToMockServers.keySet()).get(0);
    List<MockServer> servers = dcToMockServers.get(dc);
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);

      // Only set the first two server's error code to be blob not found, since first two servers
      // are in the same datacenter, and undelete requires a global quorum.
      for (MockServer server : servers.subList(0, 2)) {
        server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }

      executeOpAndVerify(Collections.singleton(blobId), RouterErrorCode.BlobDoesNotExist);
      serverLayout.getMockServers().forEach(server -> server.resetServerErrors());
    }
  }

  /**
   * Failure tests when two servers in a datacenter return blob_not_found. It fails one undelete request and thus fails
   * all the blobs in the same batch.
   * @throws Exception
   */
  @Test
  public void singleFailureInBatchUndeleteTest() throws Exception {
    HashMap<String, List<MockServer>> dcToMockServers = new HashMap<>();
    serverLayout.getMockServers()
        .forEach(server -> dcToMockServers.computeIfAbsent(server.getDataCenter(), k -> new ArrayList()).add(server));
    String dc = new ArrayList<>(dcToMockServers.keySet()).get(0);
    List<MockServer> servers = dcToMockServers.get(dc);

    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
    }

    // Only set the first two server's error code to be blob not found, since first two servers
    // are in the same datacenter, and undelete requires a global quorum.
    for (MockServer server : servers.subList(0, 2)) {
      server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    }
    executeOpAndVerify(blobIds, RouterErrorCode.BlobDoesNotExist);
    serverLayout.getMockServers().forEach(server -> server.resetServerErrors());
  }

  /**
   * Tests when each one of the hosts in all three datacenter returns an error code but stil undelete has the global quorum.
   * @throws Exception
   */
  @Test
  public void successUndeleteWithSomeServerError() throws Exception {
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
    }
    HashMap<String, List<MockServer>> dcToMockServers = new HashMap<>();
    serverLayout.getMockServers()
        .forEach(server -> dcToMockServers.computeIfAbsent(server.getDataCenter(), k -> new ArrayList()).add(server));

    List<ServerErrorCode> errorCodes = new ArrayList<>();
    errorCodes.add(ServerErrorCode.Blob_Not_Found);
    errorCodes.add(ServerErrorCode.Blob_Not_Deleted);
    errorCodes.add(ServerErrorCode.Disk_Unavailable);

    int i = 0;
    for (Map.Entry<String, List<MockServer>> entry : dcToMockServers.entrySet()) {
      entry.getValue().get(0).setServerErrorForAllRequests(errorCodes.get(i++));
    }

    for (String blobId : blobIds) {
      executeOpAndVerify(Collections.singleton(blobId), null);
    }
  }

  /**
   * Tests for behavior on timeouts
   * @throws Exception
   */
  @Test
  public void responseTimeoutTest() throws Exception {
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
    }
    // configure servers to not respond to requests
    serverLayout.getMockServers().forEach(mockServer -> mockServer.setShouldRespond(false));
    executeOpAndVerify(blobIds, RouterErrorCode.OperationTimedOut, true);
  }

  /**
   * Test for behavior on errors in the network client and selector
   * @throws Exception
   */
  @Test
  public void networkClientAndSelectorErrorsTest() throws Exception {
    for (String blobId : blobIds) {
      deleteBlobInAllServer(blobId);
    }
    for (MockSelectorState state : MockSelectorState.values()) {
      if (state == MockSelectorState.Good) {
        continue;
      }
      mockSelectorState.set(state);
      executeOpAndVerify(Collections.singleton(blobIds.get(0)), RouterErrorCode.OperationTimedOut, true);
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
      undeleteManager.submitUndeleteOperation(blobIds, UNDELETE_SERVICE_ID, new FutureResult<>(), null);
      fail("Should have failed to submit operation because the provided blob id list contains duplicates");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Generates {@link Properties} that includes initial configuration.
   * @param parallelism undelete request parallelism.
   * @return Properties
   */
  private Properties getNonBlockingRouterProperties(int parallelism) {
    Properties properties = new Properties();
    properties.setProperty(RouterConfig.ROUTER_HOSTNAME, "localhost");
    properties.setProperty(RouterConfig.ROUTER_DATACENTER_NAME, LOCAL_DC);
    properties.setProperty(RouterConfig.ROUTER_UNDELETE_REQUEST_PARALLELISM, Integer.toString(parallelism));
    return properties;
  }

  private void assertDeleted(String blobId) throws Exception {
    try {
      router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      fail("blob " + blobId + " Should be deleted");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RouterException);
      assertEquals(RouterErrorCode.BlobDeleted, ((RouterException) e.getCause()).getErrorCode());
    }
  }

  private void assertNotDeleted(String blobId) throws Exception {
    router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  private void deleteBlobInAllServer(String blobId) throws Exception {
    // Delete this blob
    router.deleteBlob(blobId, "serviceid").get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    // Then make sure all the mock servers have the delete request.
    assertDeleted(blobId);
    BlobId id = new BlobId(blobId, clusterMap);
    for (MockServer server : serverLayout.getMockServers()) {
      if (!server.getBlobs().get(blobId).isDeleted()) {
        server.send(
            new DeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
                id, time.milliseconds())).release();
      }
    }
  }

  private void verifyUndelete(String blobId) throws Exception {
    assertNotDeleted(blobId);
    int numServersNotUndelete = 0;
    for (MockServer server : serverLayout.getMockServers()) {
      StoredBlob blob = server.getBlobs().get(blobId);
      if (blob.isUndeleted()) {
        assertEquals((short) 1, blob.lifeVersion);
        assertFalse(blob.isDeleted());
      } else {
        numServersNotUndelete++;
      }
    }
    assertTrue("Global quorum not reached, yet undelete succeeded", numServersNotUndelete <= 3);
  }

  private void executeOpAndVerify(Collection<String> ids, RouterErrorCode expectedErrorCode, boolean advanceTime)
      throws Exception {
    FutureResult<Void> future = new FutureResult<>();
    NonBlockingRouter.currentOperationsCount.addAndGet(ids.size());
    undeleteManager.submitUndeleteOperation(ids, UNDELETE_SERVICE_ID, future, future::done);
    sendRequestsGetResponse(future, undeleteManager, advanceTime);
    if (expectedErrorCode == null) {
      // Should return no error
      future.get(1, TimeUnit.MILLISECONDS);
      for (String blobId : ids) {
        verifyUndelete(blobId);
      }
    } else {
      try {
        future.get(1, TimeUnit.MILLISECONDS);
        fail("Operation should fail, exception is expected");
      } catch (Exception e) {
        assertEquals("RouterErrorCode should be " + expectedErrorCode + " (future)", expectedErrorCode,
            ((RouterException) e.getCause()).getErrorCode());
      }
    }
  }

  private void executeOpAndVerify(Collection<String> ids, RouterErrorCode expectedErrorCode) throws Exception {
    executeOpAndVerify(ids, expectedErrorCode, false);
  }

  private void sendRequestsGetResponse(FutureResult<Void> future, UndeleteManager undeleteManager,
      boolean advanceTime) {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    Set<Integer> requestsToDrop = new HashSet<>();
    Set<RequestInfo> requestAcks = new HashSet<>();
    List<RequestInfo> referenceRequestInfos = new ArrayList<>();
    while (!future.isDone()) {
      undeleteManager.poll(requestInfoList, requestsToDrop);
      referenceRequestInfos.addAll(requestInfoList);

      List<ResponseInfo> responseInfoList = new ArrayList<>();
      try {
        responseInfoList = networkClient.sendAndPoll(requestInfoList, requestsToDrop, AWAIT_TIMEOUT_MS);
      } catch (RuntimeException | Error e) {
        if (!advanceTime) {
          throw e;
        }
      }
      for (ResponseInfo responseInfo : responseInfoList) {
        RequestInfo requestInfo = responseInfo.getRequestInfo();
        assertNotNull("Request is null", requestInfo);
        if (!referenceRequestInfos.contains(requestInfo)) {
          throw new IllegalStateException("Received Response for unrecognized request");
        } else if (requestAcks.contains(requestInfo)) {
          throw new IllegalStateException("Received response more than once for a request");
        }
        requestAcks.add(requestInfo);
        RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
        RequestOrResponseType type = ((RequestOrResponse) routerRequestInfo.getRequest()).getRequestType();
        switch (type) {
          case UndeleteRequest:
            undeleteManager.handleResponse(responseInfo);
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
}

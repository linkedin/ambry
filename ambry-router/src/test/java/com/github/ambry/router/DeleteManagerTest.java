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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.router.RouterTestHelpers.*;
import static org.junit.Assert.*;


/**
 * Unit test for {@link DeleteManager} and {@link DeleteOperation}.
 */

public class DeleteManagerTest {
  private static final int AWAIT_TIMEOUT_SECONDS = 10;
  // Since PUTs are sent to local DC, it is same as originating DC during DELETEs.
  private String localDc;
  private Time mockTime;
  private AtomicReference<MockSelectorState> mockSelectorState;
  private MockClusterMap clusterMap;
  private MockServerLayout serverLayout;
  private NonBlockingRouter router;
  private BlobId blobId;
  private String blobIdString;
  private PartitionId partition;
  private Future<Void> future;
  private DeleteManager deleteManager;
  private SocketNetworkClient networkClient;
  private NonBlockingRouterMetrics routerMetrics;
  private final AccountService accountService = new InMemAccountService(true, false);
  private final LoggingNotificationSystem notificationSystem = new LoggingNotificationSystem();
  private final QuotaChargeCallback quotaChargeCallback = QuotaTestUtils.createTestQuotaChargeCallback();

  /**
   * A checker that either asserts that a delete operation succeeds or returns the specified error code.
   */
  private final ErrorCodeChecker deleteErrorCodeChecker = new ErrorCodeChecker() {
    @Override
    public void testAndAssert(RouterErrorCode expectedError) throws Exception {
      future = router.deleteBlob(blobIdString, null);
      if (expectedError == null) {
        future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } else {
        assertFailureAndCheckErrorCode(future, expectedError);
      }
    }
  };

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  // The maximum number of inflight requests for a single delete operation.
  private static final String DELETE_PARALLELISM = "3";

  /**
   * Initializes ClusterMap, Router, mock servers, and an {@code BlobId} to be deleted.
   */
  @Before
  public void init() throws Exception {
    mockTime = new MockTime();
    mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
    clusterMap = new MockClusterMap();
    serverLayout = new MockServerLayout(clusterMap);
    localDc = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    VerifiableProperties vProps = new VerifiableProperties(getNonBlockingRouterProperties());
    RouterConfig routerConfig = new RouterConfig(vProps);
    routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    router = new NonBlockingRouter(routerConfig, routerMetrics,
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    List<PartitionId> mockPartitions = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    partition = mockPartitions.get(ThreadLocalRandom.current().nextInt(mockPartitions.size()));
    blobId =
        new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), partition, false,
            BlobId.BlobDataType.DATACHUNK);
    blobIdString = blobId.getID();
    deleteManager =
        new DeleteManager(clusterMap, new ResponseHandler(clusterMap), accountService, notificationSystem, routerConfig,
            routerMetrics, new RouterCallback(null, new ArrayList<>()), mockTime, router);
    MockNetworkClientFactory networkClientFactory =
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime);
    networkClient = networkClientFactory.getNetworkClient();
  }

  /**
   * Closes the router and does some post verification.
   */
  @After
  public void cleanUp() {
    assertCloseCleanup(router);
  }

  /**
   * Test a basic delete operation that will succeed.
   */
  @Test
  public void testBasicDeletion() throws Exception {
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout, null,
        deleteErrorCodeChecker);
  }

  /**
   * Test that a bad user defined callback will not crash the router.
   * @throws Exception
   */
  @Test
  public void testBadCallback() throws Exception {
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout, null,
        new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError) throws Exception {
            final CountDownLatch callbackCalled = new CountDownLatch(1);
            List<Future> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
              if (i == 1) {
                futures.add(router.deleteBlob(blobIdString, null, new Callback<Void>() {
                  @Override
                  public void onCompletion(Void result, Exception exception) {
                    callbackCalled.countDown();
                    throw new RuntimeException("Throwing an exception in the user callback");
                  }
                }, quotaChargeCallback));
              } else {
                futures.add(router.deleteBlob(blobIdString, null));
              }
            }
            for (Future future : futures) {
              future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            long waitStart = SystemTime.getInstance().milliseconds();
            while (router.getBackgroundOperationsCount() != 0
                && SystemTime.getInstance().milliseconds() < waitStart + AWAIT_TIMEOUT_SECONDS * 1000) {
              Thread.sleep(1000);
            }
            Assert.assertTrue("Callback not called.", callbackCalled.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            Assert.assertEquals("All operations should be finished.", 0, router.getOperationsCount());
            Assert.assertTrue("Router should not be closed", router.isOpen());

            //Test that DeleteManager is still operational
            router.deleteBlob(blobIdString, null).get();
          }
        });
  }

  /**
   * Test the cases for invalid blobId strings.
   */
  @Test
  public void testBlobIdNotValid() throws Exception {
    String[] input = {"123", "abcd", "", "/"};
    for (String s : input) {
      future = router.deleteBlob(s, null);
      assertFailureAndCheckErrorCode(future, RouterErrorCode.InvalidBlobId);
    }
  }

  /**
   * Test the case when one server store responds with {@code Blob_Expired}, and other servers
   * respond with {@code Blob_Not_Found}. The delete operation should be able to resolve the
   * router error code as {@code Blob_Expired}. The order of received responses is the same as
   * defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobExpired() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Replica_Unavailable);
    serverErrorCodes[5] = ServerErrorCode.Blob_Expired;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.BlobExpired, deleteErrorCodeChecker,
        localDc);
  }

  /**
   * Test for the case when delete operation fails due to quota compliance.
   */
  @Test
  public void testQuotaRejected() throws Exception {
    // Create a router with QuotaRejectionOperationController for this test.
    router.close();
    Properties properties = getNonBlockingRouterProperties();
    properties.setProperty(RouterConfig.OPERATION_CONTROLLER,
        QuotaRejectingOperationController.class.getCanonicalName());
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    testWithQuotaRejection(deleteErrorCodeChecker);
  }

  /**
   * Test the case when one server store responds with {@code Blob_Authorization_Failure}, and other servers
   * respond with {@code Blob_Not_Found}. The delete operation should be able to resolve the
   * router error code as {@code Blob_Authorization_Failure}. The order of received responses is the same as
   * defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobAuthorizationFailure() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Blob_Not_Found);
    serverErrorCodes[0] = ServerErrorCode.Blob_Authorization_Failure;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.BlobAuthorizationFailure,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode}, and the {@link DeleteOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The delete operation should be able
   * to resolve the router error code as {@code Blob_Authorization_Failure}. The order of received responses
   * is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobAuthorizationFailureOverrideAll() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];

    Arrays.fill(serverErrorCodes, ServerErrorCode.Blob_Not_Found);
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.Blob_Authorization_Failure;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.BlobAuthorizationFailure,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * Tests to ensure that {@link RouterErrorCode}s are properly resolved based on precedence
   * @throws Exception
   */
  @Test
  public void routerErrorCodeResolutionFirstSetTest() throws Exception {
    LinkedHashMap<ServerErrorCode, RouterErrorCode> codesToSetAndTest = new LinkedHashMap<>();
    // test 4 codes
    codesToSetAndTest.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    codesToSetAndTest.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    codesToSetAndTest.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    doRouterErrorCodeResolutionTest(codesToSetAndTest);
  }

  /**
   * Tests to ensure that {@link RouterErrorCode}s are properly resolved based on precedence
   * @throws Exception
   */
  @Test
  public void routerErrorCodeResolutionSecondSetTest() throws Exception {
    LinkedHashMap<ServerErrorCode, RouterErrorCode> codesToSetAndTest = new LinkedHashMap<>();
    codesToSetAndTest.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    codesToSetAndTest.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    codesToSetAndTest.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    codesToSetAndTest.put(ServerErrorCode.Partition_Unknown, RouterErrorCode.UnexpectedInternalError);
    doRouterErrorCodeResolutionTest(codesToSetAndTest);
  }

  /**
   * Test if the {@link RouterErrorCode} is as expected for different {@link ServerErrorCode}.
   */
  @Test
  public void testVariousServerErrorCode() throws Exception {
    HashMap<ServerErrorCode, RouterErrorCode> map = new HashMap<>();
    map.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    map.put(ServerErrorCode.Blob_Not_Found, RouterErrorCode.BlobDoesNotExist);
    map.put(ServerErrorCode.Disk_Unavailable, RouterErrorCode.AmbryUnavailable);
    map.put(ServerErrorCode.Replica_Unavailable, RouterErrorCode.AmbryUnavailable);
    map.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    for (ServerErrorCode serverErrorCode : ServerErrorCode.values()) {
      if (serverErrorCode != ServerErrorCode.No_Error && serverErrorCode != ServerErrorCode.Blob_Deleted
          && !map.containsKey(serverErrorCode)) {
        map.put(serverErrorCode, RouterErrorCode.UnexpectedInternalError);
      }
    }
    for (Map.Entry<ServerErrorCode, RouterErrorCode> entity : map.entrySet()) {
      // Reset not-found cache after each test since we are using same blob ID for all error codes
      router.getNotFoundCache().invalidateAll();
      testWithErrorCodes(Collections.singletonMap(entity.getKey(), 9), serverLayout, entity.getValue(),
          deleteErrorCodeChecker);
    }
  }

  /**
   * Test the case when the blob cannot be found in store servers, though the last response is {@code IO_Error}.
   * The delete operation is expected to return {@link RouterErrorCode#BlobDoesNotExist}, since the delete operation will be completed
   * before the last response according to its {@link OperationTracker}. The order of received responses is the
   * same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobNotFoundWithLastResponseNotBlobNotFound() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Blob_Not_Found);
    serverErrorCodes[8] = ServerErrorCode.IO_Error;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.BlobDoesNotExist,
        deleteErrorCodeChecker, localDc);
  }

  @Test
  public void testBlobNotFoundReturnedWhenAllReplicasReturnNotFound() throws Exception {
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);
    RouterErrorCode expectedErrorCode = RouterErrorCode.BlobDoesNotExist;
    FutureResult<Void> future = new FutureResult<>();
    TestCallback<Void> callback = new TestCallback<>();
    deleteManager.submitDeleteBlobOperation(blobIdString, localDc, future, callback, quotaChargeCallback);

    router.incrementOperationsCount(1);
    sendRequestsGetResponses(future, deleteManager);
    assertFailureAndCheckErrorCode(future, expectedErrorCode);
    assertEquals("There should be an Blob Not Found error", routerMetrics.blobDoesNotExistErrorCount.getCount(), 1);
  }

  /**
   * Test the case when some of the replicas in originating DC are unavailable, we should return AmbryUnavailable.
   * @throws Exception
   */
  @Test
  public void testOrigDcUnavailability() throws Exception {
    router.close();
    Properties properties = getNonBlockingRouterProperties();
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    deleteManager =
        new DeleteManager(clusterMap, new ResponseHandler(clusterMap), accountService, notificationSystem, routerConfig,
            routerMetrics, new RouterCallback(null, new ArrayList<>()), mockTime, router);

    String originatingDcName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    MockPartitionId partitionId = (MockPartitionId) blobId.getPartition();
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(originatingDcName)) {
        serversInLocalDc.add(mockServer);
      }
    });

    // Default all replicas to return not found.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not-found from bootstrap, 1 notFound from standby and 1 success from standby in originating dc
    ReplicaId boostrapReplica = partitionId.replicaIds.stream()
        .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName))
        .filter(replicaId -> replicaId.getDataNodeId().getHostname().equals(serversInLocalDc.get(0).getHostName()))
        .findFirst()
        .get();
    partitionId.setReplicaState(boostrapReplica, ReplicaState.BOOTSTRAP);
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.No_Error);

    // Verify that final error is Ambry unavailable since one of the replica is in bootstrap state.
    deleteErrorCodeChecker.testAndAssert(RouterErrorCode.AmbryUnavailable);
  }

  /**
   * Test the case when all of the replicas in originating DC return NotFound, we should return BlobNotFound.
   * @throws Exception
   */
  @Test
  public void testOrigDcNotFound() throws Exception {
    router.close();
    Properties properties = getNonBlockingRouterProperties();
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    deleteManager =
        new DeleteManager(clusterMap, new ResponseHandler(clusterMap), accountService, notificationSystem, routerConfig,
            routerMetrics, new RouterCallback(null, new ArrayList<>()), mockTime, router);

    String originatingDcName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(originatingDcName)) {
        serversInLocalDc.add(mockServer);
      }
    });

    // Default all replicas to return IO error which has higher precedence than NotFound.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.IO_Error);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set NotFound error from all originating DC replicas.
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);

    // Verify that final error is BlobDoesNotExist.
    deleteErrorCodeChecker.testAndAssert(RouterErrorCode.BlobDoesNotExist);
  }

  /**
   * Test the case when error precedence is maintained with unavailability in originating DC.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithOrigDcUnavailability() throws Exception {
    router.close();
    Properties properties = getNonBlockingRouterProperties();
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    deleteManager =
        new DeleteManager(clusterMap, new ResponseHandler(clusterMap), accountService, notificationSystem, routerConfig,
            routerMetrics, new RouterCallback(null, new ArrayList<>()), mockTime, router);

    String originatingDcName = clusterMap.getDatacenterName(clusterMap.getLocalDatacenterId());
    MockPartitionId partitionId = (MockPartitionId) blobId.getPartition();
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(originatingDcName)) {
        serversInLocalDc.add(mockServer);
      }
    });

    // Default all replicas to return not found.
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);

    // Set 1 not-found from bootstrap, 1 notFound from standby and 1 success from standby in originating dc
    ReplicaId boostrapReplica = partitionId.replicaIds.stream()
        .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName))
        .filter(replicaId -> replicaId.getDataNodeId().getHostname().equals(serversInLocalDc.get(0).getHostName()))
        .findFirst()
        .get();
    partitionId.setReplicaState(boostrapReplica, ReplicaState.BOOTSTRAP);
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    serversInLocalDc.get(2).setServerErrorForAllRequests(ServerErrorCode.No_Error);

    // Set one server to return Authorization failure
    MockServer serverInRemoteDc = serverLayout.getMockServers()
        .stream()
        .filter(mockServer -> !mockServer.getDataCenter().equals(originatingDcName))
        .findAny()
        .get();
    serverInRemoteDc.setServerErrorForAllRequests(ServerErrorCode.Blob_Authorization_Failure);

    // Verify that final error is BlobAuthorizationFailure since it is higher precedence than unavailability.
    deleteErrorCodeChecker.testAndAssert(RouterErrorCode.BlobAuthorizationFailure);
  }

  /**
   * Test the case when there is 2 success and 1 not found in originating data center, we should return success.
   * @throws Exception
   */
  @Test
  public void testOrigDcSuccess() throws Exception {
    router.close();
    Properties properties = getNonBlockingRouterProperties();
    VerifiableProperties vProps = new VerifiableProperties(properties);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    deleteManager =
        new DeleteManager(clusterMap, new ResponseHandler(clusterMap), accountService, notificationSystem, routerConfig,
            routerMetrics, new RouterCallback(null, new ArrayList<>()), mockTime, router);
    String originatingDcName = clusterMap.getDatacenterName(blobId.getDatacenterId());
    List<MockServer> serversInLocalDc = new ArrayList<>();
    serverLayout.getMockServers().forEach(mockServer -> {
      if (mockServer.getDataCenter().equals(originatingDcName)) {
        serversInLocalDc.add(mockServer);
      }
    });
    int serverCount = serverLayout.getMockServers().size();
    List<ServerErrorCode> serverErrorCodes = Collections.nCopies(serverCount, ServerErrorCode.Blob_Not_Found);
    setServerErrorCodes(serverErrorCodes, serverLayout);
    serversInLocalDc.get(0).setServerErrorForAllRequests(ServerErrorCode.No_Error);
    serversInLocalDc.get(1).setServerErrorForAllRequests(ServerErrorCode.No_Error);
    deleteErrorCodeChecker.testAndAssert(null);
  }

  /**
   * Sends all the requests that the {@code manager} may have ready
   * @param futureResult the {@link FutureResult} that tracks the operation
   * @param manager the {@link DeleteManager} to poll for requests
   */
  private void sendRequestsGetResponses(FutureResult<Void> futureResult, DeleteManager manager) {
    List<RequestInfo> requestInfoList = new ArrayList<>();
    Set<Integer> requestsToDrop = new HashSet<>();
    Set<RequestInfo> requestAcks = new HashSet<>();
    while (!futureResult.isDone()) {
      manager.poll(requestInfoList, requestsToDrop);
      List<ResponseInfo> responseInfoList;
      responseInfoList = networkClient.sendAndPoll(requestInfoList, requestsToDrop, AWAIT_TIMEOUT_MS);
      for (ResponseInfo responseInfo : responseInfoList) {
        RequestInfo requestInfo = responseInfo.getRequestInfo();
        assertNotNull("RequestInfo is null", requestInfo);
        if (requestAcks.contains(requestInfo)) {
          throw new IllegalStateException("Received response more than once for a request");
        }
        requestAcks.add(requestInfo);
        RequestInfo routerRequestInfo = responseInfo.getRequestInfo();
        RequestOrResponseType type = ((RequestOrResponse) routerRequestInfo.getRequest()).getRequestType();
        if (type == RequestOrResponseType.DeleteRequest) {
          manager.handleResponse(responseInfo);
        } else {
          throw new IllegalStateException("Unrecognized request type: " + type);
        }
      }
      responseInfoList.forEach(ResponseInfo::release);
      requestInfoList.clear();
    }
  }

  /**
   * Test the case when the two server responses are {@code ServerErrorCode.Blob_Deleted}, one is in the middle
   * of the responses, and the other is the last response. In this case, we should return {@code Blob_Deleted},
   * as we treat {@code Blob_Deleted} as a successful response, and we have met the {@code successTarget}.
   * The order of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testBlobNotFoundWithTwoBlobDeleted() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.IO_Error);
    serverErrorCodes[5] = ServerErrorCode.Blob_Deleted;
    serverErrorCodes[8] = ServerErrorCode.Blob_Deleted;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, null, deleteErrorCodeChecker, localDc);
  }

  /**
   * In this test, there is only one server that returns {@code ServerErrorCode.Blob_Deleted}, which is
   * not sufficient to meet the success target, therefore a router exception should be expected. The order
   * of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testSingleBlobDeletedReturned() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.Unknown_Error);
    serverErrorCodes[7] = ServerErrorCode.Blob_Deleted;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.UnexpectedInternalError,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode}, and the {@link DeleteOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The {@link ServerErrorCode} tested
   * are those could be mapped to {@link RouterErrorCode#AmbryUnavailable}. The order of received responses
   * is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testVariousServerErrorCodes() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.No_Error;
    serverErrorCodes[6] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.Disk_Unavailable;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.AmbryUnavailable,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * The parallelism is set to 3 not 9.
   *
   * Test the case where servers return different {@link ServerErrorCode}, and the {@link DeleteOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The {link ServerErrorCode} tested
   * are those could be mapped to {@link RouterErrorCode#AmbryUnavailable}. The order of received responses
   * is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testVariousServerErrorCodesForThreeParallelism() throws Exception {
    assertCloseCleanup(router);
    Properties props = getNonBlockingRouterProperties();
    props.setProperty("router.delete.request.parallelism", "3");
    VerifiableProperties vProps = new VerifiableProperties(props);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.No_Error;
    serverErrorCodes[6] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.Disk_Unavailable;
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.AmbryUnavailable,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * Test the case when request gets expired before the corresponding store server sends
   * back a response. Set servers to not respond any requests, so {@link DeleteOperation}
   * can be "in flight" all the time. The order of received responses is the same as defined
   * in {@code serverErrorCodes}.
   */
  @Test
  public void testResponseTimeout() throws Exception {
    setServerResponse(false);
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout,
        RouterErrorCode.OperationTimedOut, new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError) throws Exception {
            CountDownLatch operationCompleteLatch = new CountDownLatch(1);
            future =
                router.deleteBlob(blobIdString, null, new ClientCallback(operationCompleteLatch), quotaChargeCallback);
            do {
              // increment mock time
              mockTime.sleep(1000);
            } while (!operationCompleteLatch.await(10, TimeUnit.MILLISECONDS));
            assertFailureAndCheckErrorCode(future, expectedError);
          }
        });
  }

  /**
   * Test the case when the {@link com.github.ambry.network.Selector} of {@link SocketNetworkClient}
   * experiences various exceptions. The order of received responses is the same as defined in {@code serverErrorCodes}.
   */
  @Test
  public void testSelectorError() throws Exception {
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.No_Error);
    HashMap<MockSelectorState, RouterErrorCode> errorCodeHashMap = new HashMap<>();
    errorCodeHashMap.put(MockSelectorState.DisconnectOnSend, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnAllPoll, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnConnect, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowExceptionOnSend, RouterErrorCode.OperationTimedOut);
    errorCodeHashMap.put(MockSelectorState.ThrowThrowableOnSend, RouterErrorCode.RouterClosed);
    for (MockSelectorState state : MockSelectorState.values()) {
      if (state == MockSelectorState.Good || state == MockSelectorState.FailConnectionInitiationOnPoll) {
        // FailConnectionInitiationOnPoll is temporarily used for warm up failure test, skip it here
        continue;
      }
      mockSelectorState.set(state);
      setServerErrorCodes(serverErrorCodes, partition, serverLayout, localDc);
      CountDownLatch operationCompleteLatch = new CountDownLatch(1);
      future = router.deleteBlob(blobIdString, null, new ClientCallback(operationCompleteLatch), quotaChargeCallback);
      do {
        // increment mock time
        mockTime.sleep(1000);
      } while (!operationCompleteLatch.await(10, TimeUnit.MILLISECONDS));
      assertFailureAndCheckErrorCode(future, errorCodeHashMap.get(state));
    }
  }

  /**
   * Test the case how a {@link DeleteManager} acts when a router is closed, and when there are inflight
   * operations. Setting servers to not respond any requests, so {@link DeleteOperation} can be "in flight".
   */
  @Test
  public void testRouterClosedDuringOperation() throws Exception {
    setServerResponse(false);
    testWithErrorCodes(Collections.singletonMap(ServerErrorCode.No_Error, 9), serverLayout,
        RouterErrorCode.RouterClosed, new ErrorCodeChecker() {
          @Override
          public void testAndAssert(RouterErrorCode expectedError) throws Exception {
            future = router.deleteBlob(blobIdString, null);
            router.close();
            assertFailureAndCheckErrorCode(future, expectedError);
          }
        });
  }

  /**
   * Test the case when getting NOT_FOUND error from origin DC while termination on NOT_FOUND is enabled.
   */
  @Test
  public void testOriginDcNotFoundError() throws Exception {
    assertCloseCleanup(router);
    Properties props = getNonBlockingRouterProperties();
    props.setProperty("router.delete.request.parallelism", "1");
    props.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
    props.setProperty("router.operation.tracker.check.all.originating.replicas.for.not.found", "false");
    VerifiableProperties vProps = new VerifiableProperties(props);
    RouterConfig routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(clusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), new LoggingNotificationSystem(), clusterMap, null, null, null,
        new InMemAccountService(false, true), mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
    blobId =
        new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE, clusterMap.getLocalDatacenterId(),
            Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), partition, false,
            BlobId.BlobDataType.DATACHUNK);
    blobIdString = blobId.getID();
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    Arrays.fill(serverErrorCodes, ServerErrorCode.IO_Error);
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Blob_Not_Found;
    // The first two responses are blob not found and they are from the local dc and originating dc.
    // So even if the rest of servers returns No_Error, router will not send any requests to them.
    testWithErrorCodes(serverErrorCodes, partition, serverLayout, RouterErrorCode.BlobDoesNotExist,
        deleteErrorCodeChecker, localDc);
  }

  /**
   * User callback that is called when the {@link DeleteOperation} is completed.
   */
  private class ClientCallback implements Callback<Void> {
    private final CountDownLatch operationCompleteLatch;

    ClientCallback(CountDownLatch operationCompleteLatch) {
      this.operationCompleteLatch = operationCompleteLatch;
    }

    @Override
    public void onCompletion(Void t, Exception e) {
      operationCompleteLatch.countDown();
    }
  }

  /**
   * Sets all the servers if they should respond requests or not.
   *
   * @param shouldRespond {@code true} if the servers should respond, otherwise {@code false}.
   */
  private void setServerResponse(boolean shouldRespond) {
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      MockServer server = serverLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort());
      server.setShouldRespond(shouldRespond);
    }
  }

  /**
   * Check that a delete operation has failed with a router exception with the specified error code.
   * @param future the {@link Future} for the delete operation
   * @param expectedError the expected {@link RouterErrorCode}
   */
  private void assertFailureAndCheckErrorCode(Future<Void> future, RouterErrorCode expectedError) {
    try {
      future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      fail("Deletion should be unsuccessful. Exception is expected.");
    } catch (Exception e) {
      assertEquals("RouterErrorCode should be " + expectedError, expectedError,
          ((RouterException) e.getCause()).getErrorCode());
    }
  }

  /**
   * Generates {@link Properties} that includes initial configuration.
   *
   * @return Properties
   */
  private Properties getNonBlockingRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", localDc);
    properties.setProperty("router.delete.request.parallelism", DELETE_PARALLELISM);
    return properties;
  }

  /**
   * Runs the router code resolution test based on the input
   * @param codesToSetAndTest a {@link LinkedHashMap} that defines the ordering of the router error codes and also
   *                          provides the server error codes that must be set and their equivalent router error codes.
   * @throws Exception
   */
  private void doRouterErrorCodeResolutionTest(LinkedHashMap<ServerErrorCode, RouterErrorCode> codesToSetAndTest)
      throws Exception {
    if (codesToSetAndTest.size() * 2 > serverLayout.getMockServers().size()) {
      throw new IllegalStateException("Cannot run test because there aren't enough servers for the given codes");
    }
    List<ServerErrorCode> serverErrorCodes =
        new ArrayList<>(Collections.nCopies(serverLayout.getMockServers().size(), ServerErrorCode.IO_Error));
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
      deleteErrorCodeChecker.testAndAssert(expected.get(i));
      if (i * 2 + 1 < serverErrorCodes.size()) {
        serverErrorCodes.set(i * 2, ServerErrorCode.IO_Error);
        serverErrorCodes.set(i * 2 + 1, ServerErrorCode.IO_Error);
      }
    }
    serverLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }
}

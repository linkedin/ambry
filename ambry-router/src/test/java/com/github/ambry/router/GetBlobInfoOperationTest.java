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
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.PutManagerTest.*;
import static org.junit.Assume.*;


/**
 * Tests for {@link GetBlobInfoOperation}
 */
@RunWith(Parameterized.class)
public class GetBlobInfoOperationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int BLOB_SIZE = 100;
  private static final int BLOB_USER_METADATA_SIZE = 10;

  private int requestParallelism = 2;
  private int successTarget = 1;
  private RouterConfig routerConfig;
  private NonBlockingRouterMetrics routerMetrics;
  private final MockClusterMap mockClusterMap;
  private final MockServerLayout mockServerLayout;
  private final int replicasCount;
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>();
  private final ResponseHandler responseHandler;
  private final MockNetworkClientFactory networkClientFactory;
  private final SocketNetworkClient networkClient;
  private final MockRouterCallback routerCallback;
  private final MockTime time = new MockTime();
  private final Map<Integer, GetOperation> correlationIdToGetOperation = new HashMap<>();
  private final NonBlockingRouter router;
  private final Random random = new Random();
  private final BlobId blobId;
  private final BlobProperties blobProperties;
  private final byte[] userMetadata;
  private final byte[] putContent;
  private final boolean testEncryption;
  private final String operationTrackerType;
  private final RequestRegistrationCallback<GetOperation> requestRegistrationCallback =
      new RequestRegistrationCallback<>(correlationIdToGetOperation);
  private final GetBlobOptionsInternal options;
  private String kmsSingleKey;
  private MockKeyManagementService kms = null;
  private MockCryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;
  private ReplicaId localReplica;
  private ReplicaId remoteReplica;
  private String localDcName;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker}, with and without encryption
   * @return an array of Pairs of {{@link SimpleOperationTracker}, Non-Encrypted}, {{@link SimpleOperationTracker}, Encrypted}
   * and {{@link AdaptiveOperationTracker}, Non-Encrypted}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{SimpleOperationTracker.class.getSimpleName(), false},
        {SimpleOperationTracker.class.getSimpleName(), true}, {AdaptiveOperationTracker.class.getSimpleName(), false}});
  }

  /**
   * @param operationTrackerType @param operationTrackerType the type of {@link OperationTracker} to use.
   * @param testEncryption {@code true} if blob needs to be encrypted. {@code false} otherwise
   * @throws Exception
   */
  public GetBlobInfoOperationTest(String operationTrackerType, boolean testEncryption) throws Exception {
    this.operationTrackerType = operationTrackerType;
    this.testEncryption = testEncryption;
    VerifiableProperties vprops = new VerifiableProperties(getNonBlockingRouterProperties(true));
    routerConfig = new RouterConfig(vprops);
    mockClusterMap = new MockClusterMap();
    localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    options = new GetBlobOptionsInternal(
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(), false,
        routerMetrics.ageAtGet);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    replicasCount =
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0).getReplicaIds().size();
    responseHandler = new ResponseHandler(mockClusterMap);
    networkClientFactory = new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
        CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    if (testEncryption) {
      kmsSingleKey = TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS);
      instantiateCryptoComponents(vprops);
    }
    router = new NonBlockingRouter(new RouterConfig(vprops), new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
        networkClientFactory, new LoggingNotificationSystem(), mockClusterMap, kms, cryptoService, cryptoJobHandler,
        new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);
    short accountId = Utils.getRandomShort(random);
    short containerId = Utils.getRandomShort(random);
    blobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time, accountId,
            containerId, testEncryption, null);
    userMetadata = new byte[BLOB_USER_METADATA_SIZE];
    random.nextBytes(userMetadata);
    putContent = new byte[BLOB_SIZE];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    String blobIdStr =
        router.putBlob(blobProperties, userMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    blobId = RouterUtils.getBlobIdFromString(blobIdStr, mockClusterMap);
    localReplica = RouterTestHelpers.getAnyReplica(blobId, true, localDcName);
    remoteReplica = RouterTestHelpers.getAnyReplica(blobId, false, localDcName);
    networkClient = networkClientFactory.getNetworkClient();
    router.close();
    routerCallback = new MockRouterCallback(networkClient, Collections.emptyList());
    if (testEncryption) {
      instantiateCryptoComponents(vprops);
    }
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    if (networkClient != null) {
      networkClient.close();
    }
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    if (cryptoJobHandler != null) {
      cryptoJobHandler.close();
    }
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Instantiates crypto components (kms, cryptoService and CryptoJobHandler)
   * @param vprops {@link VerifiableProperties} instance to use
   * @throws GeneralSecurityException
   */
  private void instantiateCryptoComponents(VerifiableProperties vprops) throws GeneralSecurityException {
    kms = new MockKeyManagementService(new KMSConfig(vprops), kmsSingleKey);
    cryptoService = new MockCryptoService(new CryptoServiceConfig(vprops));
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
  }

  /**
   * Test {@link GetBlobInfoOperation} instantiation and validate the get methods.
   */
  @Test
  public void testInstantiation() {
    BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, Utils.getRandomShort(random), Utils.getRandomShort(random),
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    Callback<GetBlobResultInternal> getOperationCallback = (result, exception) -> {
      // no op.
    };

    // test a good case
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options,
            getOperationCallback, routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    Assert.assertEquals("Callback must match", getOperationCallback, op.getCallback());
    Assert.assertEquals("Blob ids must match", blobId.getID(), op.getBlobIdStr());

    // test the case where the tracker type is bad
    Properties properties = getNonBlockingRouterProperties(true);
    properties.setProperty("router.get.operation.tracker.type", "NonExistentTracker");
    RouterConfig badConfig = new RouterConfig(new VerifiableProperties(properties));
    try {
      new GetBlobInfoOperation(badConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options,
          getOperationCallback, routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
      Assert.fail("Instantiation of GetBlobInfoOperation with an invalid tracker type must fail");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Test basic successful operation completion, by polling and handing over responses to the BlobInfo operation.
   * @throws Exception
   */
  @Test
  public void testPollAndResponseHandling() throws Exception {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    ArrayList<RequestInfo> requestListToFill = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestListToFill);
    op.poll(requestRegistrationCallback);
    Assert.assertEquals("There should only be as many requests at this point as requestParallelism", requestParallelism,
        correlationIdToGetOperation.size());

    CountDownLatch onPollLatch = new CountDownLatch(1);
    if (testEncryption) {
      routerCallback.setOnPollLatch(onPollLatch);
    }
    List<ResponseInfo> responses = sendAndWaitForResponses(requestListToFill);
    for (ResponseInfo responseInfo : responses) {
      GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(
          new NettyByteBufDataInputStream(responseInfo.content()), mockClusterMap) : null;
      op.handleResponse(responseInfo, getResponse);
      if (op.isOperationComplete()) {
        break;
      }
    }
    responses.forEach(ResponseInfo::release);
    if (testEncryption) {
      Assert.assertTrue("Latch should have been zeroed ", onPollLatch.await(500, TimeUnit.MILLISECONDS));
      op.poll(requestRegistrationCallback);
    }
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    assertSuccess(op);
    // poll again to make sure that counters aren't triggered again (check in @After)
    op.poll(requestRegistrationCallback);
  }

  /**
   * Test the case where all requests time out within the GetOperation.
   */
  @Test
  public void testRouterRequestTimeoutAllFailure() {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    op.poll(requestRegistrationCallback);
    int count = 0;
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
      ++count;
    }
    // At this time requests would have been created for all replicas, as none of them were delivered,
    // and cross-colo proxying is enabled by default.
    Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
        correlationIdToGetOperation.size());
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    RouterException routerException = (RouterException) op.getOperationException();
    Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
    // test that time out response shouldn't update the Histogram if exclude timeout is enabled
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getOperationTrackerInUse();
    Assert.assertEquals("Timed-out response shouldn't be counted into local colo latency histogram", 0,
        tracker.getLatencyHistogram(localReplica).getCount());
    Assert.assertEquals("Timed-out response shouldn't be counted into cross colo latency histogram", 0,
        tracker.getLatencyHistogram(remoteReplica).getCount());
  }

  /**
   * Test that timed out requests are allowed to update Histogram by default.
   */
  @Test
  public void testTimeoutRequestUpdateHistogramByDefault() {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    VerifiableProperties vprops = new VerifiableProperties(getNonBlockingRouterProperties(false));
    RouterConfig routerConfig = new RouterConfig(vprops);
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    op.poll(requestRegistrationCallback);
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
    }
    Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
        correlationIdToGetOperation.size());
    Assert.assertEquals(RouterErrorCode.OperationTimedOut,
        ((RouterException) op.getOperationException()).getErrorCode());
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getOperationTrackerInUse();
    Assert.assertEquals("Number of data points in local colo latency histogram is not expected", 3,
        tracker.getLatencyHistogram(localReplica).getCount());
    Assert.assertEquals("Number of data points in cross colo latency histogram is not expected", 6,
        tracker.getLatencyHistogram(remoteReplica).getCount());
  }

  /**
   * Test the case where all local replicas and one remote replica timed out. The rest remote replicas respond
   * with Blob_Not_Found.
   * @throws Exception
   */
  @Test
  public void testTimeoutAndBlobNotFoundLocalTimeout() throws Exception {
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    correlationIdToGetOperation.clear();
    for (MockServer server : mockServerLayout.getMockServers()) {
      if (!server.getDataCenter().equals(localDcName)) {
        server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }
    }
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getOperationTrackerInUse();

    op.poll(requestRegistrationCallback);
    Assert.assertEquals("There should only be as many requests at this point as requestParallelism", requestParallelism,
        correlationIdToGetOperation.size());
    int count = 0;
    while (!op.isOperationComplete()) {
      time.sleep(routerConfig.routerRequestTimeoutMs + 1);
      op.poll(requestRegistrationCallback);
      if (++count == 2) {
        // exit loop to let remote replicas complete the response.
        break;
      }
    }
    // 2 + 2 requests have been sent out and all of them timed out. Nest, complete operation on remaining replicas
    completeOp(op);
    RouterException routerException = (RouterException) op.getOperationException();
    // error code should be OperationTimedOut because it precedes BlobDoesNotExist
    Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
    Assert.assertEquals("The number of data points in local colo latency histogram is not expected", 0,
        tracker.getLatencyHistogram(localReplica).getCount());
    // the count of data points in Histogram should be 5 because 9(total replicas) - 4(# of timed out request) = 5
    Assert.assertEquals("The number of data points in cross colo latency histogram is not expected", 5,
        tracker.getLatencyHistogram(remoteReplica).getCount());
  }

  /**
   * Test the case where origin replicas return Blob_Not_found and the rest returns IO_Error.
   * @throws Exception
   */
  @Test
  public void testIOErrorAndBlobNotFoundInOriginDc() throws Exception {
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    correlationIdToGetOperation.clear();

    // Pick a remote DC as the new local DC.
    String newLocal = "DC1";
    String oldLocal = localDcName;
    Properties props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", newLocal);
    routerConfig = new RouterConfig(new VerifiableProperties(props));

    for (MockServer server : mockServerLayout.getMockServers()) {
      if (server.getDataCenter().equals(oldLocal)) {
        // for origin DC, always return Blob_Not_Found;
        server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      } else {
        // otherwise, return IO_Error.
        server.setServerErrorForAllRequests(ServerErrorCode.IO_Error);
      }
    }

    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getOperationTrackerInUse();

    completeOp(op);
    RouterException routerException = (RouterException) op.getOperationException();
    // error code should be OperationTimedOut because it precedes BlobDoesNotExist
    Assert.assertEquals(RouterErrorCode.BlobDoesNotExist, routerException.getErrorCode());
    // localReplica now becomes remote replica.
    Assert.assertEquals("The number of data points in remote colo latency histogram is not expected", 2,
        tracker.getLatencyHistogram(localReplica).getCount());

    props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", oldLocal);
    routerConfig = new RouterConfig(new VerifiableProperties(props));
  }

  /**
   * Test the case where origin replicas return Blob_Not_found and the rest times out.
   * @throws Exception
   */
  @Test
  public void testTimeoutAndBlobNotFoundInOriginDc() throws Exception {
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    correlationIdToGetOperation.clear();

    // Pick a remote DC as the new local DC.
    String newLocal = "DC1";
    String oldLocal = localDcName;
    Properties props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", newLocal);
    props.setProperty("router.get.request.parallelism", "3");
    props.setProperty("router.operation.tracker.max.inflight.requests", "3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));

    for (MockServer server : mockServerLayout.getMockServers()) {
      if (server.getDataCenter().equals(oldLocal)) {
        // for origin DC, always return Blob_Not_Found;
        server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      } else {
        // Randomly set something here, it will not be used.
        server.setServerErrorForAllRequests(ServerErrorCode.No_Error);
      }
    }

    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getOperationTrackerInUse();

    // First three requests would come from local datacenter and they will all time out.
    op.poll(requestRegistrationCallback);
    Assert.assertEquals("There should only be as many requests at this point as requestParallelism", 3,
        correlationIdToGetOperation.size());
    time.sleep(routerConfig.routerRequestTimeoutMs + 1);

    completeOp(op);
    RouterException routerException = (RouterException) op.getOperationException();
    // error code should be OperationTimedOut because it precedes BlobDoesNotExist
    Assert.assertEquals(RouterErrorCode.BlobDoesNotExist, routerException.getErrorCode());

    props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", oldLocal);
    props.setProperty("router.get.request.parallelism", Integer.toString(requestParallelism));
    props.setProperty("router.operation.tracker.max.inflight.requests", "2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
  }

  /**
   * Test the case where all requests time out within the SocketNetworkClient.
   * @throws Exception
   */
  @Test
  public void testNetworkClientTimeoutAllFailure() throws Exception {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    ArrayList<RequestInfo> requestListToFill = new ArrayList<>();
    requestRegistrationCallback.setRequestsToSend(requestListToFill);

    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      for (RequestInfo requestInfo : requestListToFill) {
        ResponseInfo fakeResponse = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
        op.handleResponse(fakeResponse, null);
        fakeResponse.release();
        if (op.isOperationComplete()) {
          break;
        }
      }
      requestListToFill.clear();
    }

    // At this time requests would have been created for all replicas, as none of them were delivered,
    // and cross-colo proxying is enabled by default.
    Assert.assertEquals("Must have attempted sending requests to all replicas", replicasCount,
        correlationIdToGetOperation.size());
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    RouterException routerException = (RouterException) op.getOperationException();
    Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
  }

  /**
   * Test the case where every server returns Blob_Not_Found. All servers must have been contacted,
   * due to cross-colo proxying.
   * @throws Exception
   */
  @Test
  public void testBlobNotFoundCase() throws Exception {
    mockServerLayout.getMockServers()
        .forEach(server -> server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found));
    assertOperationFailure(RouterErrorCode.BlobDoesNotExist);
    // Blob is created by putBlob function so the local datacenter will be the origin datecenter.
    // Thus only need two Blob_Not_Found to terminate the operation.
    Assert.assertEquals("Must have attempted sending requests to all replicas", 2, correlationIdToGetOperation.size());
  }

  /**
   * Test the case with Blob_Not_Found errors from most servers, and Blob_Deleted, Blob_Expired or
   * Blob_Authorization_Failure at just one server. The latter should be the exception received for the operation.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithSpecialCase() throws Exception {
    ServerErrorCode[] serverErrorCodesToTest =
        {ServerErrorCode.Blob_Deleted, ServerErrorCode.Blob_Expired, ServerErrorCode.Blob_Authorization_Failure};
    RouterErrorCode[] routerErrorCodesToExpect =
        {RouterErrorCode.BlobDeleted, RouterErrorCode.BlobExpired, RouterErrorCode.BlobAuthorizationFailure};
    for (int i = 0; i < serverErrorCodesToTest.length; i++) {
      int indexToSetCustomError = random.nextInt(replicasCount);
      ServerErrorCode[] serverErrorCodesInOrder = new ServerErrorCode[9];
      for (int j = 0; j < serverErrorCodesInOrder.length; j++) {
        if (j == indexToSetCustomError) {
          serverErrorCodesInOrder[j] = serverErrorCodesToTest[i];
        } else {
          serverErrorCodesInOrder[j] = ServerErrorCode.Replica_Unavailable;
        }
      }
      testErrorPrecedence(serverErrorCodesInOrder, routerErrorCodesToExpect[i]);
    }
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode} or success, and the {@link GetBlobInfoOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The get blob operation should be able
   * to resolve the router error code as {@code Blob_Authorization_Failure}.
   * @throws Exception
   */
  @Test
  public void testBlobAuthorizationFailureOverrideAll() throws Exception {
    successTarget = 2; // set it to 2 for more coverage
    Properties props = getNonBlockingRouterProperties(true);
    routerConfig = new RouterConfig(new VerifiableProperties(props));

    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.Blob_Authorization_Failure;
    serverErrorCodes[6] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.No_Error;
    testErrorPrecedence(serverErrorCodes, RouterErrorCode.BlobAuthorizationFailure);
  }

  /**
   * Tests the case where all servers return the same server level error code
   * @throws Exception
   */
  @Test
  public void testFailureOnServerErrors() throws Exception {
    // set the status to various server level errors (remove all partition level errors or non errors)
    EnumSet<ServerErrorCode> serverErrors = EnumSet.complementOf(
        EnumSet.of(ServerErrorCode.Blob_Deleted, ServerErrorCode.Blob_Expired, ServerErrorCode.No_Error,
            ServerErrorCode.Blob_Authorization_Failure, ServerErrorCode.Blob_Not_Found));
    for (ServerErrorCode serverErrorCode : serverErrors) {
      mockServerLayout.getMockServers().forEach(server -> server.setServerErrorForAllRequests(serverErrorCode));
      RouterErrorCode expectedRouterError;
      switch (serverErrorCode) {
        case Replica_Unavailable:
          expectedRouterError = RouterErrorCode.AmbryUnavailable;
          break;
        case Disk_Unavailable:
          // if all the disks are unavailable (which should be extremely rare), after replacing these disks, the blob is
          // definitely not present.
          expectedRouterError = RouterErrorCode.BlobDoesNotExist;
          break;
        default:
          expectedRouterError = RouterErrorCode.UnexpectedInternalError;
      }
      assertOperationFailure(expectedRouterError);
    }
  }

  /**
   * Test failure with KMS
   * @throws Exception
   */
  @Test
  public void testKMSFailure() throws Exception {
    if (testEncryption) {
      kms.exceptionToThrow.set(GSE);
      assertOperationFailure(RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Test failure with CryptoService
   * @throws Exception
   */
  @Test
  public void testCryptoServiceFailure() throws Exception {
    if (testEncryption) {
      cryptoService.exceptionOnDecryption.set(GSE);
      assertOperationFailure(RouterErrorCode.UnexpectedInternalError);
    }
  }

  /**
   * Help test error precedence.
   * @param serverErrorCodesInOrder the list of error codes to set the mock servers with.
   * @param expectedErrorCode the expected router error code for the operation.
   * @throws Exception
   */
  private void testErrorPrecedence(ServerErrorCode[] serverErrorCodesInOrder, RouterErrorCode expectedErrorCode)
      throws Exception {
    int i = 0;
    for (MockServer server : mockServerLayout.getMockServers()) {
      server.setServerErrorForAllRequests(serverErrorCodesInOrder[i++]);
    }
    assertOperationFailure(expectedErrorCode);
  }

  /**
   * Test the case with multiple errors (server level and partition level) from multiple servers,
   * with just one server returning a successful response. The operation should succeed.
   * @throws Exception
   */
  @Test
  public void testSuccessInThePresenceOfVariousErrors() throws Exception {
    // The put for the blob being requested happened.
    String dcWherePutHappened = routerConfig.routerDatacenterName;

    // test requests coming in from local dc as well as cross-colo.
    Properties props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC1");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);

    props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);

    props = getNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    testVariousErrors(dcWherePutHappened);
  }

  private void testVariousErrors(String dcWherePutHappened) throws Exception {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());

    ArrayList<MockServer> mockServers = new ArrayList<>(mockServerLayout.getMockServers());
    // set the status to various server level or partition level errors (not Blob_Deleted or Blob_Expired).
    mockServers.get(0).setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
    mockServers.get(1).setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);
    mockServers.get(2).setServerErrorForAllRequests(ServerErrorCode.IO_Error);
    mockServers.get(3).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    mockServers.get(4).setServerErrorForAllRequests(ServerErrorCode.Data_Corrupt);
    mockServers.get(5).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    mockServers.get(6).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    mockServers.get(7).setServerErrorForAllRequests(ServerErrorCode.Disk_Unavailable);
    mockServers.get(8).setServerErrorForAllRequests(ServerErrorCode.Unknown_Error);

    // clear the hard error in one of the servers in the datacenter where the put happened.
    for (MockServer mockServer : mockServers) {
      if (mockServer.getDataCenter().equals(dcWherePutHappened)) {
        mockServer.setServerErrorForAllRequests(ServerErrorCode.No_Error);
        break;
      }
    }
    completeOp(op);
    assertSuccess(op);
  }

  /**
   * Assert that operation fails with the expected error code
   * @param errorCode expected error code on failure
   * @throws IOException
   */
  private void assertOperationFailure(RouterErrorCode errorCode) throws IOException {
    correlationIdToGetOperation.clear();
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobInfoOperation op =
        new GetBlobInfoOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, null,
            routerCallback, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
    op.poll(requestRegistrationCallback);
    Assert.assertEquals("There should only be as many requests at this point as requestParallelism", requestParallelism,
        correlationIdToGetOperation.size());
    completeOp(op);
    RouterException routerException = (RouterException) op.getOperationException();
    Assert.assertEquals(errorCode, routerException.getErrorCode());
  }

  /**
   * Completes {@code op}
   * @param op the {@link GetBlobInfoOperation} to complete
   * @throws IOException
   */
  private void completeOp(GetBlobInfoOperation op) throws IOException {
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.getRequestsToSend());
      for (ResponseInfo responseInfo : responses) {
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(
            new NettyByteBufDataInputStream(responseInfo.content()), mockClusterMap) : null;
        op.handleResponse(responseInfo, getResponse);
        if (op.isOperationComplete()) {
          break;
        }
      }
      responses.forEach(ResponseInfo::release);
    }
  }

  /**
   * Submit all the requests that were handed over by the operation and wait until a response is received for every
   * one of them.
   * @param requestList the list containing the requests handed over by the operation.
   * @return the list of responses from the network client.
   */
  private List<ResponseInfo> sendAndWaitForResponses(List<RequestInfo> requestList) {
    int sendCount = requestList.size();
    Collections.shuffle(requestList);
    List<ResponseInfo> responseList =
        new ArrayList<>(networkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(networkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    }
    return responseList;
  }

  /**
   * Assert that the operation is complete and successful. Note that the future completion and callback invocation
   * happens outside of the GetOperation, so those are not checked here. But at this point, the operation result should
   * be ready.
   * @param op the {@link GetBlobInfoOperation} that should have completed.
   */
  private void assertSuccess(GetBlobInfoOperation op) {
    Assert.assertNull("Null expected", op.getOperationException());
    BlobInfo blobInfo = op.getOperationResult().getBlobResult.getBlobInfo();
    Assert.assertNull("Unexpected blob data channel in operation result",
        op.getOperationResult().getBlobResult.getBlobDataChannel());
    Assert.assertTrue("Blob properties must be the same",
        RouterTestHelpers.arePersistedFieldsEquivalent(blobProperties, blobInfo.getBlobProperties()));
    Assert.assertEquals("Blob size should in received blobProperties should be the same as actual", BLOB_SIZE,
        blobInfo.getBlobProperties().getBlobSize());
    Assert.assertArrayEquals("User metadata must be the same", userMetadata, blobInfo.getUserMetadata());
  }

  /**
   * Get the properties for the {@link NonBlockingRouter}.
   * @param excludeTimeout whether to exclude timed out request data point in Histogram.
   * @return the constructed properties.
   */
  private Properties getNonBlockingRouterProperties(boolean excludeTimeout) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC3");
    properties.setProperty("router.get.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.get.success.target", Integer.toString(successTarget));
    properties.setProperty("router.get.operation.tracker.type", operationTrackerType);
    properties.setProperty("router.request.timeout.ms", Integer.toString(20));
    properties.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(excludeTimeout));
    properties.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
    return properties;
  }
}

/**
 * Mocks {@link RouterCallback}
 */
class MockRouterCallback extends RouterCallback {

  private CountDownLatch onPollLatch;

  MockRouterCallback(SocketNetworkClient networkClient, List<BackgroundDeleteRequest> backgroundDeleteRequests) {
    super(networkClient, backgroundDeleteRequests);
  }

  @Override
  public void onPollReady() {
    super.onPollReady();
    if (onPollLatch != null) {
      onPollLatch.countDown();
    }
  }

  /**
   * Sets {@link CountDownLatch} to be counted down on {@link #onPollReady()}
   * @param onPollLatch {@link CountDownLatch} that needs to be set
   */
  void setOnPollLatch(CountDownLatch onPollLatch) {
    this.onPollLatch = onPollLatch;
  }
}


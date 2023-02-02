/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static com.github.ambry.router.RouterTestHelpers.*;
import static com.github.ambry.utils.TestUtils.*;


/**
 * Base class for {@link NonBlockingRouter} tests.
 */
public class NonBlockingRouterTestBase {
  protected static final int MAX_PORTS_PLAIN_TEXT = 3;
  protected static final int MAX_PORTS_SSL = 3;
  protected static final int CHECKOUT_TIMEOUT_MS = 1000;
  protected static final int REQUEST_TIMEOUT_MS = 1000;
  protected static final int NETWORK_TIMEOUT_MS = 500;
  protected static final int PUT_REQUEST_PARALLELISM = 3;
  protected static final int PUT_SUCCESS_TARGET = 2;
  protected static final int GET_REQUEST_PARALLELISM = 2;
  protected static final int GET_SUCCESS_TARGET = 1;
  protected static final int DELETE_REQUEST_PARALLELISM = 3;
  protected static final int DELETE_SUCCESS_TARGET = 2;
  protected static final int PUT_CONTENT_SIZE = 1000;
  protected static final int USER_METADATA_SIZE = 10;
  protected static final int NOT_FOUND_CACHE_TTL_MS = 1000;
  protected final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<>(MockSelectorState.Good);
  protected final MockTime mockTime;
  protected final MockKeyManagementService kms;
  protected final String singleKeyForKMS;
  protected final CryptoService cryptoService;
  protected final MockClusterMap mockClusterMap;
  protected final MockServerLayout mockServerLayout;
  protected final boolean testEncryption;
  protected final int metadataContentVersion;
  protected final boolean includeCloudDc;
  protected final InMemAccountService accountService;
  protected final Random random = new Random();
  protected NonBlockingRouter router;
  protected NonBlockingRouterMetrics routerMetrics;
  protected RouterConfig routerConfig;
  protected CryptoJobHandler cryptoJobHandler;
  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;
  protected int maxPutChunkSize = PUT_CONTENT_SIZE;
  protected PutManager putManager;
  protected GetManager getManager;
  protected DeleteManager deleteManager;
  protected final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();
  protected String localDcName;

  /**
   * Initialize parameters common to all tests. This constructor is exposed for use by {@link CloudRouterTest}.
   * @param testEncryption {@code true} to test with encryption enabled. {@code false} otherwise
   * @param metadataContentVersion the metadata content version to test with.
   * @param includeCloudDc {@code true} to make the local datacenter a cloud DC.
   * @throws Exception if initialization fails
   */
  protected NonBlockingRouterTestBase(boolean testEncryption, int metadataContentVersion, boolean includeCloudDc)
      throws Exception {
    this.testEncryption = testEncryption;
    this.metadataContentVersion = metadataContentVersion;
    this.includeCloudDc = includeCloudDc;
    mockTime = new MockTime();
    mockClusterMap = new MockClusterMap(false, true, 9, 3, 3, false, includeCloudDc, null);
    localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    mockServerLayout = new MockServerLayout(mockClusterMap);
    VerifiableProperties vProps = new VerifiableProperties(new Properties());
    singleKeyForKMS = TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS);
    kms = new MockKeyManagementService(new KMSConfig(vProps), singleKeyForKMS);
    cryptoService = new GCMCryptoService(new CryptoServiceConfig(vProps));
    cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    accountService = new InMemAccountService(false, true);
  }

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    if (router != null) {
      Assert.assertEquals("Current operations count should be 0", 0, router.currentOperationsCount.get());
      assertCloseCleanup(router);
    }
    nettyByteBufLeakHelper.afterTest();
    nettyByteBufLeakHelper.setDisabled(false);
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @return the created VerifiableProperties instance.
   */
  protected Properties getNonBlockingRouterProperties(String routerDataCenter) {
    return getNonBlockingRouterProperties(routerDataCenter, PUT_REQUEST_PARALLELISM, DELETE_REQUEST_PARALLELISM);
  }

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @param routerDataCenter the router's datacenter name
   * @param putParallelism put request parallelism
   * @param deleteParallelism delete request parallelism
   * @return the created VerifiableProperties instance.
   */
  protected Properties getNonBlockingRouterProperties(String routerDataCenter, int putParallelism,
      int deleteParallelism) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
    properties.setProperty("router.put.request.parallelism", Integer.toString(putParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(PUT_SUCCESS_TARGET));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxPutChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(GET_REQUEST_PARALLELISM));
    properties.setProperty("router.get.success.target", Integer.toString(GET_SUCCESS_TARGET));
    properties.setProperty("router.delete.request.parallelism", Integer.toString(deleteParallelism));
    properties.setProperty("router.delete.success.target", Integer.toString(DELETE_SUCCESS_TARGET));
    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.request.timeout.ms", Integer.toString(REQUEST_TIMEOUT_MS));
    properties.setProperty("router.request.network.timeout.ms", Integer.toString(NETWORK_TIMEOUT_MS));
    properties.setProperty("router.connections.local.dc.warm.up.percentage", Integer.toString(67));
    properties.setProperty("router.connections.remote.dc.warm.up.percentage", Integer.toString(34));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "dc1");
    properties.setProperty("clustermap.host.name", "localhost");
    properties.setProperty("kms.default.container.key", TestUtils.getRandomKey(128));
    properties.setProperty("router.metadata.content.version", String.valueOf(metadataContentVersion));
    properties.setProperty("router.not.found.cache.ttl.in.ms", String.valueOf(NOT_FOUND_CACHE_TTL_MS));
    properties.setProperty("router.get.eligible.replicas.by.state.enabled", "true");
    properties.setProperty("router.repair.with.replicate.blob.enabled", "true");
    properties.setProperty("router.repair.with.replicate.blob.on.delete.enabled", "true");

    return properties;
  }

  /**
   * Construct {@link Properties} and {@link MockServerLayout} and initialize and set the
   * router with them.
   */
  protected void setRouter() throws Exception {
    setRouter(getNonBlockingRouterProperties(mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId())),
        mockServerLayout, new LoggingNotificationSystem());
  }

  /**
   * Initialize and set the router with the given {@link Properties} and {@link MockServerLayout}
   * @param props the {@link Properties}
   * @param serverLayout the {@link MockServerLayout}.
   * @param notificationSystem the {@link NotificationSystem} to use.
   */
  protected void setRouter(Properties props, MockServerLayout serverLayout, NotificationSystem notificationSystem)
      throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    routerConfig = new RouterConfig(verifiableProperties);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    router = new NonBlockingRouter(routerConfig, routerMetrics,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, serverLayout, mockTime), notificationSystem, mockClusterMap, kms, cryptoService,
        cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS, null);
  }

  protected void setRouterWithMetadataCache(Properties props, AmbryCacheStats ambryCacheStats) throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    routerConfig = new RouterConfig(verifiableProperties);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    AmbryCacheWithStats ambryCacheWithStats =
        new AmbryCacheWithStats("AmbryCacheWithStats", true, routerConfig.routerMaxNumMetadataCacheEntries,
            routerMetrics.getMetricRegistry(), ambryCacheStats);
    router = new NonBlockingRouter(routerConfig, routerMetrics,
        new MockNetworkClientFactory(verifiableProperties, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap, kms,
        cryptoService, cryptoJobHandler, accountService, mockTime, MockClusterMap.DEFAULT_PARTITION_CLASS,
        ambryCacheWithStats);
  }

  /**
   * Setup test suite to perform a {@link Router#putBlob} call using the constant {@link #PUT_CONTENT_SIZE}
   */
  protected void setOperationParams() {
    setOperationParams(PUT_CONTENT_SIZE, TTL_SECS);
  }

  /**
   * Setup test suite to perform a {@link Router#putBlob} call using random account and container ids.
   */
  protected void setOperationParams(int putContentSize, long ttlSecs) {
    setOperationParams(putContentSize, ttlSecs, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM));
  }

  /**
   * Setup test suite to perform a {@link Router#putBlob} call.
   * @param putContentSize the size of the content to put
   * @param ttlSecs the TTL in seconds for the blob.
   * @param accountId account id for the blob.
   * @param containerId container id for the blob.
   */
  protected void setOperationParams(int putContentSize, long ttlSecs, short accountId, short containerId) {
    putBlobProperties =
        new BlobProperties(-1, "serviceId", "memberId", "contentType", false, ttlSecs, accountId, containerId,
            testEncryption, null, null, null);
    putUserMetadata = new byte[USER_METADATA_SIZE];
    random.nextBytes(putUserMetadata);
    putContent = new byte[putContentSize];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Ensure that Put request for given blob id reaches to all the mock servers in the {@link MockServerLayout}.
   * @param blobId The blob id of which Put request will be created.
   * @param serverLayout The mock server layout.
   * @throws IOException
   */
  protected void ensurePutInAllServers(String blobId, MockServerLayout serverLayout) throws IOException {
    // Make sure all the mock servers have this put
    BlobId id = new BlobId(blobId, mockClusterMap);
    for (MockServer server : serverLayout.getMockServers()) {
      if (!server.getBlobs().containsKey(blobId)) {
        PutRequest putRequest =
            new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname, id,
                putBlobProperties, ByteBuffer.wrap(putUserMetadata), Unpooled.wrappedBuffer(putContent),
                putContent.length, BlobType.DataBlob, null);
        server.send(putRequest).release();
        putRequest.release();
      }
    }
  }

  /**
   * Ensure that Stitch requests for given blob id reaches to all the mock servees in the {@link MockServerLayout}.
   * @param blobId The blob id of which stitch request will be created.
   * @param serverLayout The mock server layout.
   * @param chunksToStitch The list of {@link ChunkInfo} to stitch.
   * @param singleBlobSize The size of each chunk
   * @throws IOException
   */
  protected void ensureStitchInAllServers(String blobId, MockServerLayout serverLayout, List<ChunkInfo> chunksToStitch,
      int singleBlobSize) throws IOException {
    TreeMap<Integer, Pair<StoreKey, Long>> indexToChunkIdsAndChunkSizes = new TreeMap<>();
    int i = 0;
    for (ChunkInfo chunkInfo : chunksToStitch) {
      indexToChunkIdsAndChunkSizes.put(i,
          new Pair<>(new BlobId(chunkInfo.getBlobId(), mockClusterMap), chunkInfo.getChunkSizeInBytes()));
      i++;
    }
    ByteBuffer serializedContent;
    int totalSize = singleBlobSize * chunksToStitch.size();
    if (routerConfig.routerMetadataContentVersion == MessageFormatRecord.Metadata_Content_Version_V2) {
      serializedContent = MetadataContentSerDe.serializeMetadataContentV2(singleBlobSize, totalSize,
          indexToChunkIdsAndChunkSizes.values().stream().map(Pair::getFirst).collect(Collectors.toList()));
    } else {
      List<Pair<StoreKey, Long>> orderedChunkIdList = new ArrayList<>(indexToChunkIdsAndChunkSizes.values());
      serializedContent = MetadataContentSerDe.serializeMetadataContentV3(totalSize, orderedChunkIdList);
    }
    BlobId id = new BlobId(blobId, mockClusterMap);
    for (MockServer server : serverLayout.getMockServers()) {
      if (!server.getBlobs().containsKey(blobId)) {
        PutRequest putRequest =
            new PutRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname, id,
                putBlobProperties, ByteBuffer.wrap(putUserMetadata), Unpooled.wrappedBuffer(serializedContent),
                serializedContent.remaining(), BlobType.MetadataBlob, null);
        server.send(putRequest).release();
        putRequest.release();
      }
    }
  }

  /**
   * Ensure that Delete request for given blob is reaches to all the  mock servers in the {@link MockServerLayout}.
   * @param blobId The blob id of which Delete request will be created.
   * @param serverLayout The mock server layout.
   * @throws IOException
   */
  protected void ensureDeleteInAllServers(String blobId, MockServerLayout serverLayout) throws IOException {
    BlobId id = new BlobId(blobId, mockClusterMap);
    for (MockServer server : serverLayout.getMockServers()) {
      if (!server.getBlobs().get(blobId).isDeleted()) {
        DeleteRequest deleteRequest =
            new DeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
                id, mockTime.milliseconds());
        server.send(deleteRequest).release();
        deleteRequest.release();
      }
    }
  }

  /**
   * Ensure that Undelete request for given blob is reaches to all the  mock servers in the {@link MockServerLayout}.
   * @param blobId The blob id of which Undelete request will be created.
   * @param serverLayout The mock server layout.
   * @throws IOException
   */
  protected void ensureUndeleteInAllServers(String blobId, MockServerLayout serverLayout) throws IOException {
    BlobId id = new BlobId(blobId, mockClusterMap);
    for (MockServer server : serverLayout.getMockServers()) {
      if (!server.getBlobs().get(blobId).isUndeleted()) {
        UndeleteRequest undeleteRequest =
            new UndeleteRequest(NonBlockingRouter.correlationIdGenerator.incrementAndGet(), routerConfig.routerHostname,
                id, mockTime.milliseconds());
        server.send(undeleteRequest).release();
        undeleteRequest.release();
      }
    }
  }

  /**
   * Test that failure detector is correctly notified for all responses regardless of the order in which successful
   * and failed responses arrive.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link SocketNetworkClient}
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
  protected void testFailureDetectorNotification(OperationHelper opHelper, SocketNetworkClient networkClient,
      List<ReplicaId> failedReplicaIds, BlobId blobId, AtomicInteger successfulResponseCount,
      AtomicBoolean invalidResponse, int indexToFail) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    Set<Integer> allDropped = new HashSet<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests, allDropped);
    }
    ReplicaId replicaIdToFail = indexToFail == -1 ? null : allRequests.get(indexToFail).getReplicaId();
    for (RequestInfo requestInfo : allRequests) {
      ResponseInfo responseInfo;
      if (replicaIdToFail != null && replicaIdToFail.equals(requestInfo.getReplicaId())) {
        responseInfo = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
        requestInfo.getRequest().release();
      } else {
        List<RequestInfo> requestInfoListToSend = new ArrayList<>();
        requestInfoListToSend.add(requestInfo);
        List<ResponseInfo> responseInfoList;
        loopStartTimeMs = SystemTime.getInstance().milliseconds();
        do {
          if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
            Assert.fail("Waited too long for the response.");
          }
          responseInfoList = networkClient.sendAndPoll(requestInfoListToSend, Collections.emptySet(), 10);
          requestInfoListToSend.clear();
        } while (responseInfoList.size() == 0);
        responseInfo = responseInfoList.get(0);
      }
      opHelper.handleResponse(responseInfo);
      responseInfo.release();
    }
    // Poll once again so that the operation gets a chance to complete.
    allRequests.clear();
    if (testEncryption) {
      opHelper.awaitOpCompletionOrTimeOut(futureResult);
    } else {
      opHelper.pollOpManager(allRequests, allDropped);
    }
    futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    Assert.assertEquals(0, allDropped.size());
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
  protected void testNoResponseNoNotification(OperationHelper opHelper, List<ReplicaId> failedReplicaIds, BlobId blobId,
      AtomicInteger successfulResponseCount, AtomicBoolean invalidResponse) throws Exception {
    failedReplicaIds.clear();
    successfulResponseCount.set(0);
    invalidResponse.set(false);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    List<RequestInfo> allRequests = new ArrayList<>();
    Set<Integer> allDropped = new HashSet<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (!futureResult.isDone()) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests, allDropped);
      mockTime.sleep(REQUEST_TIMEOUT_MS + 1);
    }
    System.out.println(allDropped);
    Assert.assertEquals("Successful notification should not have arrived for replicas that were up", 0,
        successfulResponseCount.get());
    Assert.assertEquals("Failure detector should not have been notified", 0, failedReplicaIds.size());
    Assert.assertFalse("There should be no notifications of any other kind", invalidResponse.get());
    Set<Integer> allCorrelationIds = allRequests.stream()
        .map(requestInfo -> requestInfo.getRequest().getCorrelationId())
        .collect(Collectors.toSet());
    Assert.assertEquals("Timed out requests should be dropped", allCorrelationIds, new HashSet<>(allDropped));
    allRequests.forEach(r -> r.getRequest().release());
  }

  /**
   * Test that operations succeed even in the presence of responses that are corrupt and fail to deserialize.
   * @param opHelper the {@link OperationHelper}
   * @param networkClient the {@link SocketNetworkClient}
   * @param blobId the id of the blob to get/delete. For puts, this will be null.
   * @throws Exception
   */
  protected void testResponseDeserializationError(OperationHelper opHelper, SocketNetworkClient networkClient,
      BlobId blobId) throws Exception {
    mockSelectorState.set(MockSelectorState.Good);
    FutureResult futureResult = opHelper.submitOperation(blobId);
    int requestParallelism = opHelper.requestParallelism;
    List<RequestInfo> allRequests = new ArrayList<>();
    Set<Integer> allDropped = new HashSet<>();
    long loopStartTimeMs = SystemTime.getInstance().milliseconds();
    while (allRequests.size() < requestParallelism) {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for requests.");
      }
      opHelper.pollOpManager(allRequests, allDropped);
    }
    List<ResponseInfo> responseInfoList = new ArrayList<>();
    loopStartTimeMs = SystemTime.getInstance().milliseconds();
    do {
      if (loopStartTimeMs + AWAIT_TIMEOUT_MS < SystemTime.getInstance().milliseconds()) {
        Assert.fail("Waited too long for the response.");
      }
      responseInfoList.addAll(networkClient.sendAndPoll(allRequests, allDropped, 10));
      allRequests.clear();
    } while (responseInfoList.size() < requestParallelism);
    // corrupt the first response.
    ByteBuf response = responseInfoList.get(0).content();
    byte b = response.getByte(response.writerIndex() - 1);
    response.setByte(response.writerIndex() - 1, (byte) ~b);
    for (ResponseInfo responseInfo : responseInfoList) {
      opHelper.handleResponse(responseInfo);
    }
    responseInfoList.forEach(ResponseInfo::release);
    allRequests.clear();
    if (testEncryption) {
      opHelper.awaitOpCompletionOrTimeOut(futureResult);
    } else {
      opHelper.pollOpManager(allRequests, allDropped);
    }
    try {
      futureResult.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Assert.fail("Operation should have succeeded with one corrupt response");
    }
  }

  /**
   * Assert that the number of ChunkFiller and RequestResponseHandler threads running are as expected.
   * @param expectedRequestResponseHandlerCount the expected number of ChunkFiller and RequestResponseHandler threads.
   * @param expectedChunkFillerCount the expected number of ChunkFiller threads.
   */
  protected void assertExpectedThreadCounts(int expectedRequestResponseHandlerCount, int expectedChunkFillerCount) {
    Assert.assertEquals("Number of RequestResponseHandler threads running should be as expected",
        expectedRequestResponseHandlerCount, TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    Assert.assertEquals("Number of chunkFiller threads running should be as expected", expectedChunkFillerCount,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    if (expectedRequestResponseHandlerCount == 0) {
      Assert.assertFalse("Router should be closed if there are no worker threads running", router.isOpen());
      Assert.assertEquals("All operations should have completed if the router is closed", 0,
          router.getOperationsCount());
    }
  }

  /**
   * Assert that submission after closing the router returns a future that is already done and an appropriate
   * exception.
   */
  protected void assertClosed() {
    Future<String> future =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build());
    Assert.assertTrue(future.isDone());
    try {
      ((CompletableFuture<?>) future).join();
      Assert.fail("Excepting failure");
    } catch (Exception e) {
      RouterException routerException = (RouterException) Utils.extractFutureExceptionCause(e);
      Assert.assertEquals(routerException.getErrorCode(), RouterErrorCode.RouterClosed);
    }
  }

  /**
   * Does the TTL update test by putting a blob, checking its TTL, updating TTL and then rechecking the TTL again.
   * @param numChunks the number of chunks required when the blob is put. Has to divide {@link #PUT_CONTENT_SIZE}
   *                  perfectly for test to work.
   * @throws Exception
   */
  protected void doTtlUpdateTest(int numChunks) throws Exception {
    Assert.assertEquals("This test works only if the number of chunks is a perfect divisor of PUT_CONTENT_SIZE", 0,
        PUT_CONTENT_SIZE % numChunks);
    maxPutChunkSize = PUT_CONTENT_SIZE / numChunks;
    String updateServiceId = "update-service";
    TtlUpdateNotificationSystem notificationSystem = new TtlUpdateNotificationSystem();
    setRouter(getNonBlockingRouterProperties("DC1"), new MockServerLayout(mockClusterMap), notificationSystem);
    setOperationParams();
    Assert.assertFalse("The original ttl should not be infinite for this test to work",
        putBlobProperties.getTimeToLiveInSeconds() == Utils.Infinite_Time);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build())
        .get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertTtl(router, Collections.singleton(blobId), TTL_SECS);
    router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    // if more than one chunk is created, also account for metadata blob
    notificationSystem.checkNotifications(numChunks == 1 ? 1 : numChunks + 1, updateServiceId, Utils.Infinite_Time);
    assertTtl(router, Collections.singleton(blobId), Utils.Infinite_Time);
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    if (numChunks == 1) {
      Assert.assertEquals("Get should have been skipped", 1, routerMetrics.skippedGetBlobCount.getCount());
    } else {
      Assert.assertEquals("Get should NOT have been skipped", 0, routerMetrics.skippedGetBlobCount.getCount());
    }
    router.close();
    // check that ttl update won't work after router close
    CompletableFuture<Void> future = router.updateBlobTtl(blobId, updateServiceId, Utils.Infinite_Time);
    Assert.assertTrue(future.isDone());
    try {
      future.join();
      Assert.fail("Expecting a failure");
    } catch (Exception e) {
      RouterException routerException = (RouterException) Utils.extractFutureExceptionCause(e);
      Assert.assertEquals(routerException.getErrorCode(), RouterErrorCode.RouterClosed);
    }
  }

  /**
   * Enum for the three operation types.
   */
  protected enum OperationType {
    PUT, GET, DELETE,
  }

  /**
   * A helper class to abstract away the details about specific operation manager.
   */
  protected class OperationHelper {
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
     * @throws RouterException if the blobIdStr is invalid.
     */
    FutureResult submitOperation(BlobId blobId) throws RouterException {
      FutureResult futureResult = null;
      switch (opType) {
        case PUT:
          futureResult = new FutureResult<String>();
          ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
          putManager.submitPutBlobOperation(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT,
              futureResult, null, null);
          break;
        case GET:
          final FutureResult<GetBlobResult> getFutureResult = new FutureResult<>();
          getManager.submitGetBlobOperation(blobId.getID(), new GetBlobOptionsInternal(
              new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(), false,
              routerMetrics.ageAtGet), getFutureResult::done, null);
          futureResult = getFutureResult;
          break;
        case DELETE:
          futureResult = new FutureResult<Void>();
          deleteManager.submitDeleteBlobOperation(blobId.getID(), null, futureResult, null, null);
          break;
      }
      router.currentOperationsCount.incrementAndGet();
      return futureResult;
    }

    /**
     * Poll the associated operation manager.
     * @param requestsToSend the list of {@link RequestInfo} to send to pass into the poll call.
     * @param requestsToDrop the list of correlation IDs to drop to pass into the poll call.
     */
    void pollOpManager(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop) {
      switch (opType) {
        case PUT:
          putManager.poll(requestsToSend, requestsToDrop);
          break;
        case GET:
          getManager.poll(requestsToSend, requestsToDrop);
          break;
        case DELETE:
          deleteManager.poll(requestsToSend, requestsToDrop);
          break;
      }
    }

    /**
     * Polls all managers at regular intervals until the operation is complete or timeout is reached
     * @param futureResult {@link FutureResult} that needs to be tested for completion
     * @throws InterruptedException
     */
    protected void awaitOpCompletionOrTimeOut(FutureResult futureResult) throws InterruptedException {
      int timer = 0;
      List<RequestInfo> allRequests = new ArrayList<>();
      Set<Integer> allDropped = new HashSet<>();
      while (timer < AWAIT_TIMEOUT_MS / 2 && !futureResult.completed()) {
        pollOpManager(allRequests, allDropped);
        Thread.sleep(50);
        timer += 50;
        allRequests.clear();
        allDropped.clear();
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

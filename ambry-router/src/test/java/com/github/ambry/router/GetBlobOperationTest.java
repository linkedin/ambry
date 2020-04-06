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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobAll;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.router.RouterTestHelpers.*;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
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
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.PutManagerTest.*;
import static com.github.ambry.router.RouterTestHelpers.*;
import static org.junit.Assume.*;


/**
 * Tests for {@link GetBlobOperation}
 * This class creates a {@link NonBlockingRouter} with a {@link MockServer} and does puts through it. The gets,
 * however are done directly by the tests - that is, the tests create {@link GetBlobOperation} and get requests from
 * it and then use a {@link SocketNetworkClient} directly to send requests to and get responses from the {@link MockServer}.
 * Since the {@link SocketNetworkClient} used by the router and the test are different, and since the
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
  private static final String LOCAL_DC = "DC3";
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
  private String localDcName;

  // Certain tests recreate the routerConfig with different properties.
  private RouterConfig routerConfig;
  private int blobSize;

  // Parameters for puts which are also used to verify the gets.
  private String blobIdStr;
  private BlobId blobId;
  private BlobProperties blobProperties;
  private byte[] userMetadata;
  private byte[] putContent;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  // Options which are passed into GetBlobOperations
  private GetBlobOptionsInternal options;

  private final RequestRegistrationCallback<GetOperation> requestRegistrationCallback =
      new RequestRegistrationCallback<>(correlationIdToGetOperation);

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

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    router.close();
    Assert.assertEquals("All operations should have completed", 0, router.getOperationsCount());
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker} with and without encryption
   * @return an array of Pairs of {{@link SimpleOperationTracker}, Non-Encrypted}, {{@link AdaptiveOperationTracker}, Encrypted}
   * and {{@link AdaptiveOperationTracker}, Non-Encrypted}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{SimpleOperationTracker.class.getSimpleName(), false},
        {AdaptiveOperationTracker.class.getSimpleName(), false},
        {AdaptiveOperationTracker.class.getSimpleName(), true}});
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
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(true));
    routerConfig = new RouterConfig(vprops);
    mockClusterMap = new MockClusterMap();
    localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    blobIdFactory = new BlobIdFactory(mockClusterMap);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    replicasCount =
        mockClusterMap.getRandomWritablePartition(MockClusterMap.DEFAULT_PARTITION_CLASS, null).getReplicaIds().size();
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
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
        networkClientFactory, new LoggingNotificationSystem(), mockClusterMap, kms, cryptoService, cryptoJobHandler,
        new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);
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
        Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption, null);
    userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    blobIdStr = router.putBlob(blobProperties, userMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    blobId = RouterUtils.getBlobIdFromString(blobIdStr, mockClusterMap);
  }

  /**
   * Do a put directly to the mock servers. This allows for blobs with malformed properties to be constructed.
   * @param blobType the {@link BlobType} for the blob to upload.
   * @param blobContent the raw content for the blob to upload (i.e. this can be serialized composite blob metadata or
   *                    an encrypted blob).
   */
  private void doDirectPut(BlobType blobType, ByteBuf blobContent) throws Exception {
    List<PartitionId> writablePartitionIds = mockClusterMap.getWritablePartitionIds(null);
    PartitionId partitionId = writablePartitionIds.get(random.nextInt(writablePartitionIds.size()));
    blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
        partitionId, blobProperties.isEncrypted(),
        blobType == BlobType.MetadataBlob ? BlobId.BlobDataType.METADATA : BlobId.BlobDataType.DATACHUNK);
    blobIdStr = blobId.getID();
    Iterator<MockServer> servers = partitionId.getReplicaIds()
        .stream()
        .map(ReplicaId::getDataNodeId)
        .map(dataNodeId -> mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort()))
        .iterator();

    ByteBuffer blobEncryptionKey = null;
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadata);
    if (blobProperties.isEncrypted()) {
      FutureResult<EncryptJob.EncryptJobResult> futureResult = new FutureResult<>();
      cryptoJobHandler.submitJob(new EncryptJob(blobProperties.getAccountId(), blobProperties.getContainerId(),
          blobType == BlobType.MetadataBlob ? null : blobContent.retainedDuplicate(), userMetadataBuf.duplicate(),
          kms.getRandomKey(), cryptoService, kms, new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
          futureResult::done));
      EncryptJob.EncryptJobResult result = futureResult.get(5, TimeUnit.SECONDS);
      blobEncryptionKey = result.getEncryptedKey();
      if (blobType != BlobType.MetadataBlob) {
        blobContent.release();
        blobContent = result.getEncryptedBlobContent();
      }
      userMetadataBuf = result.getEncryptedUserMetadata();
    }
    while (servers.hasNext()) {
      MockServer server = servers.next();
      PutRequest request =
          new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
              blobContent.retainedDuplicate(), blobContent.readableBytes(), blobType,
              blobEncryptionKey == null ? null : blobEncryptionKey.duplicate());
      // Make sure we release the BoundedNettyByteBufReceive.
      server.send(request).release();
    }
    blobContent.release();
  }

  /**
   * Test {@link GetBlobOperation} instantiation and validate the get methods.
   */
  @Test
  public void testInstantiation() {
    Callback<GetBlobResultInternal> getRouterCallback = new Callback<GetBlobResultInternal>() {
      @Override
      public void onCompletion(GetBlobResultInternal result, Exception exception) {
        // no op.
      }
    };

    blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM),
        mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    blobIdStr = blobId.getID();
    // test a good case
    // operationCount is not incremented here as this operation is not taken to completion.
    GetBlobOperation op = new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId,
        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet),
        getRouterCallback, routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false);
    Assert.assertEquals("Callbacks must match", getRouterCallback, op.getCallback());
    Assert.assertEquals("Blob ids must match", blobIdStr, op.getBlobIdStr());

    // test the case where the tracker type is bad
    Properties properties = getDefaultNonBlockingRouterProperties(true);
    properties.setProperty("router.get.operation.tracker.type", "NonExistentTracker");
    RouterConfig badConfig = new RouterConfig(new VerifiableProperties(properties));
    try {
      new GetBlobOperation(badConfig, routerMetrics, mockClusterMap, responseHandler, blobId,
          new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet),
          getRouterCallback, routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false);
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
      GetBlobOptions.OperationType operationType;
      switch (i % 3) {
        case 0:
          operationType = GetBlobOptions.OperationType.All;
          break;
        case 1:
          operationType = GetBlobOptions.OperationType.Data;
          break;
        default:
          operationType = GetBlobOptions.OperationType.BlobInfo;
          break;
      }
      options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(operationType).build(), false,
          routerMetrics.ageAtGet);
      getAndAssertSuccess();
    }
  }

  /**
   * Test gets of simple blob in raw mode.
   */
  @Test
  public void testSimpleBlobRawMode() throws Exception {
    blobSize = maxChunkSize;
    doPut();
    options = new GetBlobOptionsInternal(
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).rawMode(true).build(), false,
        routerMetrics.ageAtGet);
    if (testEncryption) {
      getAndAssertSuccess();
    } else {
      GetBlobOperation op = createOperationAndComplete(null);
      Assert.assertEquals(IllegalStateException.class, op.getOperationException().getClass());
    }
  }

  /**
   * Test gets of simple blob with getChunkIdsOnly being true.
   */
  @Test
  public void testSimpleBlobGetChunkIdsOnly() throws Exception {
    blobSize = maxChunkSize;
    doPut();
    options =
        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).build(),
            true, routerMetrics.ageAtGet);
    getAndAssertSuccess();
  }

  /**
   * Test gets of composite blob in raw mode.
   */
  @Test
  public void testCompositeBlobRawMode() throws Exception {
    options = new GetBlobOptionsInternal(
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).rawMode(true).build(), false,
        routerMetrics.ageAtGet);

    for (int numChunks = 2; numChunks < 10; numChunks++) {
      blobSize = numChunks * maxChunkSize;
      doPut();
      if (testEncryption) {
        getAndAssertSuccess();
        ByteBuffer payload = getBlobBuffer();
        // extract chunk ids
        BlobAll blobAll =
            MessageFormatRecord.deserializeBlobAll(new ByteArrayInputStream(payload.array()), blobIdFactory);
        ByteBuf metadataBuffer = blobAll.getBlobData().content();
        try {
          CompositeBlobInfo compositeBlobInfo =
              MetadataContentSerDe.deserializeMetadataContentRecord(metadataBuffer.nioBuffer(), blobIdFactory);
          Assert.assertEquals("Total size didn't match", blobSize, compositeBlobInfo.getTotalSize());
          Assert.assertEquals("Chunk count didn't match", numChunks, compositeBlobInfo.getKeys().size());
        } finally {
          metadataBuffer.release();
        }

        // TODO; test raw get on each chunk (needs changes to test framework)
      } else {
        // Only supported for encrypted blobs now
      }
    }
  }

  /**
   * Test gets of composite blob with getChunkIdsOnly being true.
   */
  @Test
  public void testCompositeBlobGetChunkIdsOnly() throws Exception {
    options =
        new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.All).build(),
            true, routerMetrics.ageAtGet);

    for (int numChunks = 2; numChunks < 10; numChunks++) {
      blobSize = numChunks * maxChunkSize;
      doPut();
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
      GetBlobOptions.OperationType operationType;
      switch (i % 3) {
        case 0:
          operationType = GetBlobOptions.OperationType.All;
          break;
        case 1:
          operationType = GetBlobOptions.OperationType.Data;
          break;
        default:
          operationType = GetBlobOptions.OperationType.BlobInfo;
          break;
      }
      options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().operationType(operationType).build(), false,
          routerMetrics.ageAtGet);
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
    GetBlobOperation op = createOperation(routerConfig, null);
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

    // test that timed out response won't update latency histogram if exclude timeout is enabled.
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getFirstChunkOperationTrackerInUse();
    Histogram localColoTracker =
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, true, localDcName));
    Histogram crossColoTracker =
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, false, localDcName));
    Assert.assertEquals("Timed-out response shouldn't be counted into local colo latency histogram", 0,
        localColoTracker.getCount());
    Assert.assertEquals("Timed-out response shouldn't be counted into cross colo latency histogram", 0,
        crossColoTracker.getCount());
  }

  /**
   * Test that timed out requests are allowed to update Histogram by default.
   * @throws Exception
   */
  @Test
  public void testTimeoutRequestUpdateHistogramByDefault() throws Exception {
    doPut();
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(false));
    RouterConfig routerConfig = new RouterConfig(vprops);
    GetBlobOperation op = createOperation(routerConfig, null);
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
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getFirstChunkOperationTrackerInUse();

    Assert.assertEquals("Number of data points in local colo latency histogram is not expected", 3,
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, true, localDcName)).getCount());
    Assert.assertEquals("Number of data points in cross colo latency histogram is not expected", 6,
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, false, localDcName)).getCount());
  }

  /**
   * Test the case where 2 local replicas timed out. The remaining one local replica and rest remote replicas respond
   * with Blob_Not_Found.
   * @throws Exception
   */
  @Test
  public void testRequestTimeoutAndBlobNotFoundLocalTimeout() throws Exception {
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    doPut();
    GetBlobOperation op = createOperation(routerConfig, null);
    AdaptiveOperationTracker tracker = (AdaptiveOperationTracker) op.getFirstChunkOperationTrackerInUse();
    correlationIdToGetOperation.clear();
    for (MockServer server : mockServerLayout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    }
    op.poll(requestRegistrationCallback);
    time.sleep(routerConfig.routerRequestTimeoutMs + 1);

    // 2 requests have been sent out and both of them timed out. Nest, complete operation on remaining replicas
    // The request should have response from one local replica and all remote replicas.
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.getRequestsToSend());
      for (ResponseInfo responseInfo : responses) {
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(
            new NettyByteBufDataInputStream(responseInfo.content()), mockClusterMap) : null;
        op.handleResponse(responseInfo, getResponse);
        responseInfo.release();
      }
    }

    RouterException routerException = (RouterException) op.getOperationException();
    // error code should be OperationTimedOut because it precedes BlobDoesNotExist
    Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
    Histogram localColoTracker =
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, true, localDcName));
    Histogram crossColoTracker =
        tracker.getLatencyHistogram(RouterTestHelpers.getAnyReplica(blobId, false, localDcName));
    // the count of data points in local colo Histogram should be 1, because first 2 request timed out
    Assert.assertEquals("The number of data points in local colo latency histogram is not expected", 1,
        localColoTracker.getCount());
    // the count of data points in cross colo Histogram should be 6 because all remote replicas respond with proper error code
    Assert.assertEquals("The number of data points in cross colo latency histogram is not expected", 6,
        crossColoTracker.getCount());
  }

  /**
   * Test the case where origin replicas return Blob_Not_found and the rest times out.
   * @throws Exception
   */
  @Test
  public void testTimeoutAndBlobNotFoundInOriginDc() throws Exception {
    assumeTrue(operationTrackerType.equals(AdaptiveOperationTracker.class.getSimpleName()));
    doPut();

    // Pick a remote DC as the new local DC.
    String newLocal = "DC1";
    String oldLocal = localDcName;
    Properties props = getDefaultNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", newLocal);
    props.setProperty("router.get.request.parallelism", "3");
    props.setProperty("router.operation.tracker.max.inflight.requests", "3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));

    GetBlobOperation op = createOperation(routerConfig, null);
    correlationIdToGetOperation.clear();
    for (MockServer server : mockServerLayout.getMockServers()) {
      server.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
    }
    op.poll(requestRegistrationCallback);
    time.sleep(routerConfig.routerRequestTimeoutMs + 1);

    // 3 requests have been sent out and all of them timed out. Nest, complete operation on remaining replicas
    // The request should have response from all remote replicas.
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.getRequestsToSend());
      for (ResponseInfo responseInfo : responses) {
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(
            new NettyByteBufDataInputStream(responseInfo.content()), mockClusterMap) : null;
        op.handleResponse(responseInfo, getResponse);
        responseInfo.release();
      }
    }

    RouterException routerException = (RouterException) op.getOperationException();
    // error code should be OperationTimedOut because it precedes BlobDoesNotExist
    Assert.assertEquals(RouterErrorCode.BlobDoesNotExist, routerException.getErrorCode());

    props = getDefaultNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", oldLocal);
    props.setProperty("router.get.request.parallelism", "2");
    props.setProperty("router.operation.tracker.max.inflight.requests", "2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
  }

  /**
   * Test the case where all requests time out within the SocketNetworkClient.
   * @throws Exception
   */
  @Test
  public void testNetworkClientTimeoutAllFailure() throws Exception {
    doPut();
    GetBlobOperation op = createOperation(routerConfig, null);
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      for (RequestInfo requestInfo : requestRegistrationCallback.getRequestsToSend()) {
        ResponseInfo fakeResponse = new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null);
        op.handleResponse(fakeResponse, null);
        fakeResponse.release();
        if (op.isOperationComplete()) {
          break;
        }
      }
      requestRegistrationCallback.getRequestsToSend().clear();
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
            // Local dc is the origin DC, only two NOT_FOUND would terminate the operation.
            Assert.assertEquals("Must have attempted sending requests to all replicas", 2,
                correlationIdToGetOperation.size());
            assertFailureAndCheckErrorCode(op, expectedError);
          }
        });
  }

  /**
   * Test the case with Blob_Not_Found errors from most servers, and Blob_Deleted, Blob_Expired or
   * Blob_Authorization_Failure at just one server. The latter should be the exception received for the operation.
   * @throws Exception
   */
  @Test
  public void testErrorPrecedenceWithSpecialCase() throws Exception {
    doPut();
    Map<ServerErrorCode, RouterErrorCode> serverErrorToRouterError = new HashMap<>();
    serverErrorToRouterError.put(ServerErrorCode.Blob_Deleted, RouterErrorCode.BlobDeleted);
    serverErrorToRouterError.put(ServerErrorCode.Blob_Expired, RouterErrorCode.BlobExpired);
    serverErrorToRouterError.put(ServerErrorCode.Blob_Authorization_Failure, RouterErrorCode.BlobAuthorizationFailure);
    for (Map.Entry<ServerErrorCode, RouterErrorCode> entry : serverErrorToRouterError.entrySet()) {
      Map<ServerErrorCode, Integer> errorCounts = new HashMap<>();
      errorCounts.put(ServerErrorCode.Replica_Unavailable, replicasCount - 1);
      errorCounts.put(entry.getKey(), 1);
      testWithErrorCodes(errorCounts, mockServerLayout, entry.getValue(), getErrorCodeChecker);
    }
  }

  /**
   * Test the case where originating dc returns 2 Disk_Unavailable and 1 Not_Found and rest replicas return Not_Found.
   * In this case, result of GET operation will be Not_Found.
   * @throws Exception
   */
  @Test
  public void testCombinedDiskDownAndNotFoundCase() throws Exception {
    doPut();
    List<MockServer> localDcServers = mockServerLayout.getMockServers()
        .stream()
        .filter(s -> s.getDataCenter().equals(LOCAL_DC))
        .collect(Collectors.toList());
    mockServerLayout.getMockServers().forEach(s -> {
      if (!localDcServers.contains(s)) {
        s.setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }
    });
    for (int i = 0; i < 3; ++i) {
      if (i < 2) {
        localDcServers.get(i).setServerErrorForAllRequests(ServerErrorCode.Disk_Unavailable);
      } else {
        localDcServers.get(i).setServerErrorForAllRequests(ServerErrorCode.Blob_Not_Found);
      }
    }
    getErrorCodeChecker.testAndAssert(RouterErrorCode.BlobDoesNotExist);
    mockServerLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Test the case new nodes/partitions are dynamically added and router should be able to route quests to new replicas
   * and add new nodes into router metrics (if they are not present previously)
   */
  @Test
  public void testGetBlobFromNewAddedNode() throws Exception {
    // create 3 new nodes in mock clustermap and place new partitions on these nodes.
    List<MockDataNodeId> newNodes = mockClusterMap.createNewDataNodes(3, LOCAL_DC);
    // make all existing partitions READ_ONLY to force router to route PUT against new partitions
    mockClusterMap.getAllPartitionIds(null)
        .forEach(p -> ((MockPartitionId) p).setPartitionState(PartitionState.READ_ONLY));
    // create 3 new partitions on new nodes
    for (int i = 0; i < 3; ++i) {
      mockClusterMap.createNewPartition(newNodes);
    }
    // add new nodes to mock server layout
    mockServerLayout.addMockServers(newNodes, mockClusterMap);
    // put a composite blob which should go to new partitions on new nodes only
    doPut();
    // make sure get blob from new partitions on new nodes succeeds
    getAndAssertSuccess();
  }

  /**
   * Test the case where servers return different {@link ServerErrorCode} or success, and the {@link GetBlobOperation}
   * is able to resolve and conclude the correct {@link RouterErrorCode}. The get operation should be able
   * to resolve the router error code as {@code Blob_Authorization_Failure}.
   * @throws Exception
   */
  @Test
  public void testAuthorizationFailureOverrideAll() throws Exception {
    doPut();
    ServerErrorCode[] serverErrorCodes = new ServerErrorCode[9];
    serverErrorCodes[0] = ServerErrorCode.Blob_Not_Found;
    serverErrorCodes[1] = ServerErrorCode.Data_Corrupt;
    serverErrorCodes[2] = ServerErrorCode.IO_Error;
    serverErrorCodes[3] = ServerErrorCode.Partition_Unknown;
    serverErrorCodes[4] = ServerErrorCode.Disk_Unavailable;
    serverErrorCodes[5] = ServerErrorCode.Blob_Authorization_Failure;
    serverErrorCodes[6] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[7] = ServerErrorCode.Unknown_Error;
    serverErrorCodes[8] = ServerErrorCode.Blob_Authorization_Failure;
    Map<ServerErrorCode, Integer> errorCounts = new HashMap<>();
    for (int i = 0; i < 9; i++) {
      if (!errorCounts.containsKey(serverErrorCodes[i])) {
        errorCounts.put(serverErrorCodes[i], 0);
      }
      errorCounts.put(serverErrorCodes[i], errorCounts.get(serverErrorCodes[i]) + 1);
    }
    testWithErrorCodes(errorCounts, mockServerLayout, RouterErrorCode.BlobAuthorizationFailure, getErrorCodeChecker);
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
    Properties props = getDefaultNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC1");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(dcWherePutHappened);

    props = getDefaultNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(dcWherePutHappened);

    props = getDefaultNonBlockingRouterProperties(true);
    props.setProperty("router.datacenter.name", "DC3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(dcWherePutHappened);
  }

  /**
   * Tests the case where all servers return the same server level error code
   * @throws Exception
   */
  @Test
  public void testFailureOnServerErrors() throws Exception {
    doPut();
    // set the status to various server level errors (remove all partition level errors or non errors)
    EnumSet<ServerErrorCode> serverErrors = EnumSet.complementOf(
        EnumSet.of(ServerErrorCode.Blob_Deleted, ServerErrorCode.Blob_Expired, ServerErrorCode.No_Error,
            ServerErrorCode.Blob_Authorization_Failure, ServerErrorCode.Blob_Not_Found));
    for (ServerErrorCode serverErrorCode : serverErrors) {
      mockServerLayout.getMockServers().forEach(server -> server.setServerErrorForAllRequests(serverErrorCode));
      GetBlobOperation op = createOperationAndComplete(null);
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
      assertFailureAndCheckErrorCode(op, expectedRouterError);
    }
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
  private void doTestSuccessInThePresenceOfVariousErrors(String dcWherePutHappened) throws Exception {
    ArrayList<MockServer> mockServers = new ArrayList<>(mockServerLayout.getMockServers());
    ArrayList<ServerErrorCode> serverErrors = new ArrayList<>(Arrays.asList(ServerErrorCode.values()));
    // set the status to various server level or partition level errors (not Blob_Deleted or Blob_Expired - as they
    // are final), except for one of the servers in the datacenter where the put happened (we do this as puts only go
    // to the local dc, whereas gets go cross colo).
    serverErrors.remove(ServerErrorCode.Blob_Deleted);
    serverErrors.remove(ServerErrorCode.Blob_Expired);
    serverErrors.remove(ServerErrorCode.No_Error);
    serverErrors.remove(ServerErrorCode.Blob_Authorization_Failure);
    boolean goodServerMarked = false;
    boolean notFoundSetInOriginalDC = false;
    for (MockServer mockServer : mockServers) {
      ServerErrorCode code = serverErrors.get(random.nextInt(serverErrors.size()));
      // make sure in the original dc, we don't set Blob_Not_Found twice.
      if (mockServer.getDataCenter().equals(dcWherePutHappened)) {
        if (!goodServerMarked) {
          mockServer.setServerErrorForAllRequests(ServerErrorCode.No_Error);
          goodServerMarked = true;
        } else {
          if (!notFoundSetInOriginalDC) {
            mockServer.setServerErrorForAllRequests(code);
            notFoundSetInOriginalDC = code == ServerErrorCode.Blob_Not_Found;
          } else {
            while (code == ServerErrorCode.Blob_Not_Found) {
              code = serverErrors.get(random.nextInt(serverErrors.size()));
            }
            mockServer.setServerErrorForAllRequests(code);
          }
        }
      } else {
        mockServer.setServerErrorForAllRequests(code);
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
   * A past issue with replication logic resulted in the blob size listed in the blob properties reflecting the size
   * of a chunk's content buffer instead of the plaintext size of the entire blob. This issue affects composite blobs
   * and simple encrypted blob. This test tests the router's ability to replace the incorrect blob size field in the
   * blob properties with the inferred correct size.
   * @throws Exception
   */
  @Test
  public void testBlobSizeReplacement() throws Exception {
    userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    options = new GetBlobOptionsInternal(
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(), false,
        routerMetrics.ageAtGet);

    // test simple blob case
    blobSize = maxChunkSize;
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    blobProperties =
        new BlobProperties(blobSize + 20, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption, null);
    ByteBuf putContentBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    putContentBuf.writeBytes(putContent);
    doDirectPut(BlobType.DataBlob, putContentBuf.retainedDuplicate());
    putContentBuf.release();
    Counter sizeMismatchCounter = (testEncryption ? routerMetrics.simpleEncryptedBlobSizeMismatchCount
        : routerMetrics.simpleUnencryptedBlobSizeMismatchCount);
    long startCount = sizeMismatchCounter.getCount();
    getAndAssertSuccess();
    long endCount = sizeMismatchCounter.getCount();
    Assert.assertEquals("Wrong number of blob size mismatches", 1, endCount - startCount);

    // test composite blob case
    int numChunks = 3;
    blobSize = maxChunkSize;
    List<StoreKey> storeKeys = new ArrayList<>(numChunks);
    for (int i = 0; i < numChunks; i++) {
      doPut();
      storeKeys.add(blobId);
    }
    blobSize = maxChunkSize * numChunks;
    ByteBuffer metadataContent = MetadataContentSerDe.serializeMetadataContentV2(maxChunkSize, blobSize, storeKeys);
    metadataContent.flip();
    blobProperties =
        new BlobProperties(blobSize - 20, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption, null);
    ByteBuf metadataContentBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(metadataContent.remaining());
    metadataContentBuf.writeBytes(metadataContent.duplicate());
    doDirectPut(BlobType.MetadataBlob, metadataContentBuf.retainedDuplicate());
    metadataContentBuf.release();
    startCount = routerMetrics.compositeBlobSizeMismatchCount.getCount();
    getAndAssertSuccess();
    endCount = routerMetrics.compositeBlobSizeMismatchCount.getCount();
    Assert.assertEquals("Wrong number of blob size mismatches", 1, endCount - startCount);
  }

  /**
   * Test that gets work for blobs with the old blob format (V1).
   * @throws Exception
   */
  @Test
  public void testLegacyBlobGetSuccess() throws Exception {
    mockServerLayout.getMockServers()
        .forEach(mockServer -> mockServer.setBlobFormatVersion(MessageFormatRecord.Blob_Version_V1));
    for (int i = 0; i < 10; i++) {
      // blobSize in the range [1, maxChunkSize]
      blobSize = random.nextInt(maxChunkSize) + 1;
      doPut();
      getAndAssertSuccess();
    }
    mockServerLayout.getMockServers()
        .forEach(mockServer -> mockServer.setBlobFormatVersion(MessageFormatRecord.Blob_Version_V2));
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
    // Last blobSize + 1 bytes
    testRangeRequestLastNBytes(blobSize + 1, true);
    // Range over the end of the blob
    testRangeRequestOffsetRange(random.nextInt(blobSize), blobSize + 5, true);
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
    // Last blobSize + 1 bytes
    testRangeRequestLastNBytes(blobSize + 1, true);
    // Range over the end of the blob
    testRangeRequestOffsetRange(random.nextInt(blobSize), blobSize + 5, true);
    // Ranges that start past the end of the blob (should not succeed)
    testRangeRequestFromStartOffset(blobSize, false);
    testRangeRequestOffsetRange(blobSize, blobSize + 20, false);
    // 1 byte ranges
    testRangeRequestOffsetRange(0, 0, true);
    testRangeRequestOffsetRange(blobSize / 2, blobSize / 2, true);
    testRangeRequestOffsetRange(blobSize - 1, blobSize - 1, true);
    testRangeRequestFromStartOffset(blobSize - 1, true);
    testRangeRequestLastNBytes(1, true);

    blobSize = maxChunkSize * 2 + random.nextInt(maxChunkSize) + 1;
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
    GetBlobOperation op = createOperation(routerConfig, null);
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
                while (writableChannel.getNextChunk(100) != null) {
                  writableChannel.resolveOldestChunk(null);
                }
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
        .range(ByteRanges.fromOffsetRange(startOffset, endOffset))
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
        .range(ByteRanges.fromStartOffset(startOffset))
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
        .range(ByteRanges.fromLastNBytes(lastNBytes))
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
    mockServerLayout.getMockServers().forEach(mockServer -> mockServer.setGetErrorOnDataBlobOnly(true));
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
    final AtomicReference<Throwable> readCompleteThrowable = new AtomicReference<>(null);
    final AtomicLong readCompleteResult = new AtomicLong(0);
    final AtomicReference<Exception> operationException = new AtomicReference<>(null);
    final int numChunks = ((blobSize + maxChunkSize - 1) / maxChunkSize) + (blobSize > maxChunkSize ? 1 : 0);
    mockNetworkClient.resetProcessedResponseCount();
    Callback<GetBlobResultInternal> callback = (result, exception) -> {
      if (exception != null) {
        operationException.set(exception);
        readCompleteLatch.countDown();
      } else {
        try {
          if (options.getChunkIdsOnly) {
            Assert.assertNull("Unexpected blob result when getChunkIdsOnly", result.getBlobResult);
            if (blobSize > maxChunkSize) {
              // CompositeBlob
              Assert.assertNotNull("CompositeBlob should return a list of blob ids when getting chunk ids",
                  result.storeKeys);
              Assert.assertEquals(result.storeKeys.size(), (blobSize + maxChunkSize - 1) / maxChunkSize);
            } else {
              // SimpleBlob
              Assert.assertNull("Unexpected list of blob id when getChunkIdsOnly is true on a simple blob",
                  result.storeKeys);
            }
            readCompleteLatch.countDown();
            return;
          }
          BlobInfo blobInfo;
          switch (options.getBlobOptions.getOperationType()) {
            case All:
              if (!options.getBlobOptions.isRawMode()) {
                blobInfo = result.getBlobResult.getBlobInfo();
                Assert.assertTrue("Blob properties must be the same",
                    RouterTestHelpers.arePersistedFieldsEquivalent(blobProperties, blobInfo.getBlobProperties()));
                Assert.assertEquals("Blob size should in received blobProperties should be the same as actual",
                    blobSize, blobInfo.getBlobProperties().getBlobSize());
                Assert.assertArrayEquals("User metadata must be the same", userMetadata, blobInfo.getUserMetadata());
              }
              break;
            case Data:
              Assert.assertNull("Unexpected blob info in operation result", result.getBlobResult.getBlobInfo());
              break;
            case BlobInfo:
              blobInfo = result.getBlobResult.getBlobInfo();
              Assert.assertTrue("Blob properties must be the same",
                  RouterTestHelpers.arePersistedFieldsEquivalent(blobProperties, blobInfo.getBlobProperties()));
              Assert.assertEquals("Blob size should in received blobProperties should be the same as actual", blobSize,
                  blobInfo.getBlobProperties().getBlobSize());
              Assert.assertNull("Unexpected blob data in operation result", result.getBlobResult.getBlobDataChannel());
          }
        } catch (Throwable e) {
          readCompleteThrowable.set(e);
        }

        if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo) {
          final ByteBufferAsyncWritableChannel asyncWritableChannel = new ByteBufferAsyncWritableChannel();
          final Future<Long> preSetReadIntoFuture =
              initiateReadBeforeChunkGet ? result.getBlobResult.getBlobDataChannel()
                  .readInto(asyncWritableChannel, null) : null;
          Utils.newThread(() -> {
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
                readCompleteThrowable);
          }, false).start();
        } else {
          readCompleteLatch.countDown();
        }
      }
    };

    GetBlobOperation op = createOperationAndComplete(callback);

    readCompleteLatch.await();
    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    if (operationException.get() != null) {
      throw operationException.get();
    }
    if (readCompleteThrowable.get() != null) {
      throw new IllegalStateException(readCompleteThrowable.get());
    }
    // Ensure that a ChannelClosed exception is not set when the ReadableStreamChannel is closed correctly.
    Assert.assertNull("Callback operation exception should be null", op.getOperationException());
    if (options.getBlobOptions.getOperationType() != GetBlobOptions.OperationType.BlobInfo
        && !options.getBlobOptions.isRawMode() && !options.getChunkIdsOnly) {
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
    GetBlobOperation op = createOperation(routerConfig, callback);
    while (!op.isOperationComplete()) {
      op.poll(requestRegistrationCallback);
      List<ResponseInfo> responses = sendAndWaitForResponses(requestRegistrationCallback.getRequestsToSend());
      for (ResponseInfo responseInfo : responses) {
        DataInputStream dis = new NettyByteBufDataInputStream(responseInfo.content());
        GetResponse getResponse = responseInfo.getError() == null ? GetResponse.readFrom(dis, mockClusterMap) : null;
        op.handleResponse(responseInfo, getResponse);
        responseInfo.release();
      }
    }
    return op;
  }

  /**
   * Create a getBlob operation with the specified callback
   * @param routerConfig the routerConfig used to instantiate GetBlobOperation.
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @return the operation
   */
  private GetBlobOperation createOperation(RouterConfig routerConfig, Callback<GetBlobResultInternal> callback) {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, callback,
            routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());
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
   * @param readCompleteThrowable This will contain any exceptions encountered during the read.
   */
  private void assertBlobReadSuccess(GetBlobOptions options, Future<Long> readIntoFuture,
      ByteBufferAsyncWritableChannel asyncWritableChannel, ReadableStreamChannel readableStreamChannel,
      CountDownLatch readCompleteLatch, AtomicLong readCompleteResult,
      AtomicReference<Throwable> readCompleteThrowable) {
    try {
      ByteBuffer putContentBuf;
      if (options != null && options.isRawMode()) {
        putContentBuf = getBlobBuffer();
        Assert.assertNotNull("Did not find server with blob: " + blobIdStr, putContentBuf);
      } else {
        putContentBuf = ByteBuffer.wrap(putContent);
        // If a range is set, compare the result against the specified byte range.
        if (options != null && options.getRange() != null) {
          ByteRange range = options.getRange().toResolvedByteRange(blobSize);
          putContentBuf = ByteBuffer.wrap(putContent, (int) range.getStartOffset(), (int) range.getRangeSize());
        }
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
    } catch (Throwable e) {
      readCompleteThrowable.set(e);
    } finally {
      readCompleteLatch.countDown();
    }
  }

  /**
   * @return the ByteBuffer for the blob contents on a server that hosts the partition.
   * @throws IOException
   */
  private ByteBuffer getBlobBuffer() throws IOException {
    // Find server with the blob
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      MockServer server =
          mockServerLayout.getMockServer(replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort());
      if (server.getBlobs().containsKey(blobId.getID())) {
        return getBlobBufferFromServer(server);
      }
    }
    return null;
  }

  /**
   * @param server the server hosting the blob.
   * @return the ByteBuffer for the blob contents on the specified server.
   * @throws IOException
   */
  private ByteBuffer getBlobBufferFromServer(MockServer server) throws IOException {
    PartitionRequestInfo requestInfo =
        new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
    GetRequest getRequest =
        new GetRequest(1, "assertBlobReadSuccess", MessageFormatFlags.All, Collections.singletonList(requestInfo),
            GetOption.None);
    GetResponse getResponse = server.makeGetResponse(getRequest, ServerErrorCode.No_Error);

    // simulating server sending response over the wire
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) getResponse.sizeInBytes()));
    getResponse.writeTo(channel);

    // simulating the Router receiving the data from the wire
    ByteBuffer data = channel.getBuffer();
    data.flip();
    DataInputStream stream = new DataInputStream(new ByteBufferInputStream(data));

    // read off the size because GetResponse.readFrom() expects that this be read off
    stream.readLong();
    // construct a GetResponse as the Router would have
    getResponse = GetResponse.readFrom(stream, mockClusterMap);
    byte[] blobData = Utils.readBytesFromStream(getResponse.getInputStream(),
        (int) getResponse.getPartitionResponseInfoList().get(0).getMessageInfoList().get(0).getSize());
    // set the put content buf to the data in the stream
    return ByteBuffer.wrap(blobData);
  }

  /**
   * Submit all the requests that were handed over by the operation and wait until a response is received for every
   * one of them.
   * @param requestList the list containing the requests handed over by the operation.
   * @return the list of responses from the network client.
   */
  private List<ResponseInfo> sendAndWaitForResponses(List<RequestInfo> requestList) {
    int sendCount = requestList.size();
    // Shuffle the replicas to introduce randomness in the order in which responses arrive.
    Collections.shuffle(requestList);
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    requestList.clear();
    while (responseList.size() < sendCount) {
      responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    }
    return responseList;
  }

  /**
   * Get the default {@link Properties} for the {@link NonBlockingRouter}.
   * @param excludeTimeout whether to exclude timed out request in Histogram.
   * @return the constructed {@link Properties}
   */
  private Properties getDefaultNonBlockingRouterProperties(boolean excludeTimeout) {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", LOCAL_DC);
    properties.setProperty("router.put.request.parallelism", Integer.toString(3));
    properties.setProperty("router.put.success.target", Integer.toString(2));
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(maxChunkSize));
    properties.setProperty("router.get.request.parallelism", Integer.toString(2));
    properties.setProperty("router.get.success.target", Integer.toString(1));
    properties.setProperty("router.get.operation.tracker.type", operationTrackerType);
    properties.setProperty("router.request.timeout.ms", Integer.toString(20));
    properties.setProperty("router.operation.tracker.exclude.timeout.enabled", Boolean.toString(excludeTimeout));
    properties.setProperty("router.operation.tracker.terminate.on.not.found.enabled", "true");
    properties.setProperty("router.get.blob.operation.share.memory", "true");
    return properties;
  }
}

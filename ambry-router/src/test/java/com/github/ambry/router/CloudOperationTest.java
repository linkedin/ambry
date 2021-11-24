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

import com.github.ambry.account.InMemAccountService;
import com.github.ambry.cloud.CloudDestination;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestinationFactory;
import com.github.ambry.cloud.LatchBasedInMemoryCloudDestination;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.network.CompositeNetworkClientFactory;
import com.github.ambry.network.LocalNetworkClientFactory;
import com.github.ambry.network.LocalRequestResponseChannel;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.CompositeNetworkClient;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.RequestHandlerPool;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufDataInputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.io.DataInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link GetBlobOperation}
 * This class creates a {@link NonBlockingRouter} with a {@link MockServer} and
 * a {@link LatchBasedInMemoryCloudDestination} and does puts through it.
 * The gets, are done directly by the tests - that is, the tests create {@link GetBlobOperation} and get requests from
 * it and then use a {@link CompositeNetworkClient} directly to send requests to and get responses
 * from the {@link MockServer} and the {@link LatchBasedInMemoryCloudDestination}.
 */
@RunWith(Parameterized.class)
public class CloudOperationTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final String LOCAL_DC = "DC3";
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
  private final MockCompositeNetworkClient mockNetworkClient;
  private final RouterCallback routerCallback;
  private final String operationTrackerType;
  private final boolean testEncryption;
  private MockKeyManagementService kms = null;
  private MockCryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;

  // Certain tests recreate the routerConfig with different properties.
  private RouterConfig routerConfig;

  // Parameters for puts which are also used to verify the gets.
  private BlobProperties blobProperties;
  private NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  // Options which are passed into GetBlobOperations
  private GetBlobOptionsInternal options;

  private final RequestRegistrationCallback<GetOperation> requestRegistrationCallback =
      new RequestRegistrationCallback<>(correlationIdToGetOperation);
  private final QuotaChargeCallback quotaChargeCallback = QuotaTestUtils.createDummyQuotaChargeEventListener();

  /**
   * Running for both {@link SimpleOperationTracker} and {@link AdaptiveOperationTracker} with and without encryption,
   * also include one Cloud Backed Data Center.
   * @return an array of {{@link SimpleOperationTracker}, Non-Encrypted, includeCloudDC},
   * {{@link AdaptiveOperationTracker}, Encrypted, includeCloudDC}
   * and {{@link AdaptiveOperationTracker}, Non-Encrypted, includeCloudDC}
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {SimpleOperationTracker.class.getSimpleName(), false},
        {AdaptiveOperationTracker.class.getSimpleName(), false},
        {AdaptiveOperationTracker.class.getSimpleName(), true}
    });
  }

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
   * Instantiate a router.
   * @param operationTrackerType the type of {@link OperationTracker} to use.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public CloudOperationTest(final String operationTrackerType, final boolean testEncryption) throws Exception {
    this.operationTrackerType = operationTrackerType;
    this.testEncryption = testEncryption;
    // Defaults. Tests may override these and do new puts as appropriate.
    maxChunkSize = random.nextInt(1024 * 1024) + 1;

    mockSelectorState.set(MockSelectorState.Good);
    VerifiableProperties vprops = new VerifiableProperties(getDefaultNonBlockingRouterProperties(true, LOCAL_DC));
    routerConfig = new RouterConfig(vprops);
    mockClusterMap = new MockClusterMap(false, true, 9, 3, 3, false, true, LOCAL_DC);
    String localDcName = mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId());
    Assert.assertEquals("Local DC Name is same as the one we set.", LOCAL_DC, localDcName);

    blobIdFactory = new BlobIdFactory(mockClusterMap);
    routerMetrics = new NonBlockingRouterMetrics(mockClusterMap, routerConfig);
    options = new GetBlobOptionsInternal(new GetBlobOptionsBuilder().build(), false, routerMetrics.ageAtGet);
    mockServerLayout = new MockServerLayout(mockClusterMap);
    responseHandler = new ResponseHandler(mockClusterMap);

    if (testEncryption) {
      kms = new MockKeyManagementService(new KMSConfig(vprops),
          TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
      cryptoService = new MockCryptoService(new CryptoServiceConfig(vprops));
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    }

    CloudConfig cloudConfig = new CloudConfig(vprops);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, vprops, mockClusterMap.getMetricRegistry(),
            mockClusterMap);
    CloudDestination cloudDestination = cloudDestinationFactory.getCloudDestination();
    RequestHandlerPool requestHandlerPool =
        CloudRouterFactory.getRequestHandlerPool(vprops, mockClusterMap, cloudDestination, cloudConfig);

    Map<ReplicaType, NetworkClientFactory> childFactories = new EnumMap<>(ReplicaType.class);
    LocalNetworkClientFactory cloudClientFactory = new LocalNetworkClientFactory((LocalRequestResponseChannel) requestHandlerPool.getChannel(),
        new NetworkConfig(vprops), new NetworkMetrics(routerMetrics.getMetricRegistry()), time);
    childFactories.put(ReplicaType.CLOUD_BACKED, cloudClientFactory);

    MockNetworkClientFactory diskClientFactory = new MockNetworkClientFactory(vprops, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
        CHECKOUT_TIMEOUT_MS, mockServerLayout, time);
    childFactories.put(ReplicaType.DISK_BACKED, diskClientFactory);

    NetworkClientFactory networkClientFactory = new CompositeNetworkClientFactory(childFactories);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
        networkClientFactory, new LoggingNotificationSystem(), mockClusterMap, kms, cryptoService, cryptoJobHandler,
        new InMemAccountService(false, true), time, MockClusterMap.DEFAULT_PARTITION_CLASS);

    NetworkClient compNetworkClient = networkClientFactory.getNetworkClient();
    mockNetworkClient = new MockCompositeNetworkClient(compNetworkClient);
    routerCallback = new RouterCallback(mockNetworkClient, new ArrayList<BackgroundDeleteRequest>());
  }

  /**
   * Does a single put of the content based on provided user metadata, put content.
   * @param userMetadata the user meta data
   * @param blobSize blob size
   * @param putContent the raw content for the blob to upload
   * @return the blob id
   * @throws Exception
   */
  private BlobId doPut(byte[] userMetadata, int blobSize, byte[] putContent) throws Exception {
    blobProperties = new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption, null, null, null);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    // TODO fix null quota charge event listener
    String blobIdStr = router.putBlob(blobProperties, userMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    return RouterUtils.getBlobIdFromString(blobIdStr, mockClusterMap);
  }

  /**
   * Do a put directly to the mock servers. This allows for blobs with malformed properties to be constructed.
   * @param blobType the {@link BlobType} for the blob to upload.
   * @param blobContent the raw content for the blob to upload (i.e. this can be serialized composite blob metadata or
   *                    an encrypted blob).
   */
  private BlobId doDirectPut(BlobType blobType, ByteBuf blobContent) throws Exception {
    List<PartitionId> writablePartitionIds = mockClusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    PartitionId partitionId = writablePartitionIds.get(random.nextInt(writablePartitionIds.size()));
    BlobId blobId = new BlobId(routerConfig.routerBlobidCurrentVersion, BlobId.BlobIdType.NATIVE,
        mockClusterMap.getLocalDatacenterId(), blobProperties.getAccountId(), blobProperties.getContainerId(),
        partitionId, blobProperties.isEncrypted(),
        blobType == BlobType.MetadataBlob ? BlobId.BlobDataType.METADATA : BlobId.BlobDataType.DATACHUNK);
    Iterator<MockServer> servers = partitionId.getReplicaIds()
        .stream()
        .map(ReplicaId::getDataNodeId)
        .map(dataNodeId -> mockServerLayout.getMockServer(dataNodeId.getHostname(), dataNodeId.getPort()))
        .iterator();

    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    ByteBuffer blobEncryptionKey = null;
    ByteBuffer userMetadataBuf = ByteBuffer.wrap(userMetadata);
    if (blobProperties.isEncrypted()) {
      FutureResult<EncryptJob.EncryptJobResult> futureResult = new FutureResult<>();
      cryptoJobHandler.submitJob(new EncryptJob(blobProperties.getAccountId(), blobProperties.getContainerId(),
          blobType == BlobType.MetadataBlob ? null : blobContent.retainedDuplicate(), userMetadataBuf.duplicate(),
          kms.getRandomKey(), cryptoService, kms, null, new CryptoJobMetricsTracker(routerMetrics.encryptJobMetrics),
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
      request.release();
    }

    // send to Cloud destinations.
    PutRequest request =
        new PutRequest(random.nextInt(), "clientId", blobId, blobProperties, userMetadataBuf.duplicate(),
            blobContent.retainedDuplicate(), blobContent.readableBytes(), blobType,
            blobEncryptionKey == null ? null : blobEncryptionKey.duplicate());
    // Get the cloud replica.
    ReplicaId replica = partitionId.getReplicaIds().get(0);
    String hostname = replica.getDataNodeId().getHostname();
    Port port = new Port(-1, PortType.PLAINTEXT);

    List<RequestInfo> requestList = new ArrayList<>();
    RequestInfo requestInfo = new RequestInfo(hostname, port, request, replica, null);
    requestList.add(requestInfo);
    List<ResponseInfo> responseList = new ArrayList<>();
    responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100)); // Sent with mockNetworkClient
    requestList.clear();
    while (responseList.size() < 1) {
      responseList.addAll(mockNetworkClient.sendAndPoll(requestList, Collections.emptySet(), 100));
    }
    request.release();
    blobContent.release();
    return blobId;
  }

  /**
   * Create a getBlob operation with the specified blob id and callback,  nd poll until completion.
   * @param blobId the id of the blob to get
   * @param callback the callback to run after completion of the operation, or {@code null} if no callback.
   * @return the operation
   * @throws Exception
   */
  private GetBlobOperation createGetBlobOperationAndComplete(BlobId blobId, Callback<GetBlobResultInternal> callback)
      throws Exception {
    NonBlockingRouter.currentOperationsCount.incrementAndGet();
    GetBlobOperation op =
        new GetBlobOperation(routerConfig, routerMetrics, mockClusterMap, responseHandler, blobId, options, callback,
            routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false, quotaChargeCallback);
    requestRegistrationCallback.setRequestsToSend(new ArrayList<>());

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
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   * @param blobId id of the blob to get
   * @param getChunksBeforeRead {@code true} if all chunks should be cached by the router before reading from the
   *                            stream.
   * @param initiateReadBeforeChunkGet Whether readInto() should be initiated immediately before data chunks are
   *                                   fetched by the router to simulate chunk arrival delay.
   * @param expectedLifeVersion the expected lifeVersion from get operation.
   * @param expectedBlobSize the expected blob size
   * @param expectedUserMetadata the expected user meta data
   * @param expectPutContent the expected blob content
   */
  private void getBlobAndAssertSuccess(final BlobId blobId, final boolean getChunksBeforeRead, final boolean initiateReadBeforeChunkGet,
      final short expectedLifeVersion,
      final int expectedBlobSize, final byte[] expectedUserMetadata, final byte[] expectPutContent) throws Exception {
    final CountDownLatch readCompleteLatch = new CountDownLatch(1);
    final AtomicReference<Throwable> readCompleteThrowable = new AtomicReference<>(null);
    final AtomicLong readCompleteResult = new AtomicLong(0);
    final AtomicReference<Exception> operationException = new AtomicReference<>(null);
    final int numChunks = ((expectedBlobSize + maxChunkSize - 1) / maxChunkSize) + (expectedBlobSize > maxChunkSize ? 1 : 0);
    mockNetworkClient.resetProcessedResponseCount();
    Callback<GetBlobResultInternal> callback = (result, exception) -> {
      if (exception != null) {
        operationException.set(exception);
        readCompleteLatch.countDown();
      } else {
        try {
          if (options.getChunkIdsOnly) {
            Assert.assertNull("Unexpected blob result when getChunkIdsOnly", result.getBlobResult);
            if (expectedBlobSize > maxChunkSize) {
              // CompositeBlob
              Assert.assertNotNull("CompositeBlob should return a list of blob ids when getting chunk ids",
                  result.storeKeys);
              Assert.assertEquals(result.storeKeys.size(), (expectedBlobSize + maxChunkSize - 1) / maxChunkSize);
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
                    expectedBlobSize, blobInfo.getBlobProperties().getBlobSize());
                Assert.assertArrayEquals("User metadata must be the same", expectedUserMetadata, blobInfo.getUserMetadata());
                Assert.assertEquals("LifeVersion mismatch", expectedLifeVersion, blobInfo.getLifeVersion());
              }
              break;
            case Data:
              Assert.assertNull("Unexpected blob info in operation result", result.getBlobResult.getBlobInfo());
              break;
            case BlobInfo:
              blobInfo = result.getBlobResult.getBlobInfo();
              Assert.assertTrue("Blob properties must be the same",
                  RouterTestHelpers.arePersistedFieldsEquivalent(blobProperties, blobInfo.getBlobProperties()));
              Assert.assertEquals("Blob size should in received blobProperties should be the same as actual", expectedBlobSize,
                  blobInfo.getBlobProperties().getBlobSize());
              Assert.assertNull("Unexpected blob data in operation result", result.getBlobResult.getBlobDataChannel());
              Assert.assertEquals("LifeVersion mismatch", expectedLifeVersion, blobInfo.getLifeVersion());
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
            assertBlobReadSuccess(blobId, options.getBlobOptions, readIntoFuture, asyncWritableChannel,
                result.getBlobResult.getBlobDataChannel(), readCompleteLatch, readCompleteResult,
                readCompleteThrowable, expectedBlobSize, expectPutContent);
          }, false).start();
        } else {
          readCompleteLatch.countDown();
        }
      }
    };

    GetBlobOperation op = createGetBlobOperationAndComplete(blobId, callback);

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
      int sizeWritten = expectedBlobSize;
      if (options.getBlobOptions.getRange() != null) {
        ByteRange range = options.getBlobOptions.getRange()
            .toResolvedByteRange(expectedBlobSize, options.getBlobOptions.resolveRangeOnEmptyBlob());
        sizeWritten = (int) range.getRangeSize();
      }
      Assert.assertEquals("Size read must equal size written", sizeWritten, readCompleteResult.get());
    }
  }

  /**
   * @param blobId id of the blob
   * @return the ByteBuffer for the blob contents on a server that hosts the partition.
   * @throws IOException
   */
  private ByteBuffer getBlobBuffer(BlobId blobId) throws IOException {
    // Find server with the blob
    for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
      MockServer server =
          mockServerLayout.getMockServer(replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort());
      if (server.getBlobs().containsKey(blobId.getID())) {
        return getBlobBufferFromServer(server, blobId);
      }
    }
    return null;
  }

  /**
   * @param server the server hosting the blob.
   * @param blobId id of the blob
   * @return the ByteBuffer for the blob contents on the specified server.
   * @throws IOException
   */
  private ByteBuffer getBlobBufferFromServer(MockServer server, BlobId blobId) throws IOException {
    PartitionRequestInfo requestInfo =
        new PartitionRequestInfo(blobId.getPartition(), Collections.singletonList(blobId));
    GetRequest getRequest =
        new GetRequest(1, "assertBlobReadSuccess", MessageFormatFlags.All, Collections.singletonList(requestInfo),
            GetOption.None);
    GetResponse getResponse = server.makeGetResponse(getRequest, ServerErrorCode.No_Error);
    getRequest.release();

    // simulating server sending response over the wire
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) getResponse.sizeInBytes()));
    getResponse.writeTo(channel);
    getResponse.release();

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
   * Assert that the operation is complete and successful. Note that the future completion and callback invocation
   * happens outside of the GetOperation, so those are not checked here. But at this point, the operation result should
   * be ready.
   * @param blobId id of the blob
   * @param options The {@link GetBlobOptions} for the operation to check.
   * @param readIntoFuture The future associated with the read on the {@link ReadableStreamChannel} result of the
   *                       operation.
   * @param asyncWritableChannel The {@link ByteBufferAsyncWritableChannel} to which bytes will be written by the
   *                             operation.
   * @param readableStreamChannel The {@link ReadableStreamChannel} that bytes are read from in the operation.
   * @param readCompleteLatch The latch to count down once the read is completed.
   * @param readCompleteResult This will contain the bytes written on return.
   * @param readCompleteThrowable This will contain any exceptions encountered during the read.
   * @param blobSize size of the blob
   * @param putContent expected content of the blob
   */
  private void assertBlobReadSuccess(BlobId blobId, GetBlobOptions options, Future<Long> readIntoFuture,
      ByteBufferAsyncWritableChannel asyncWritableChannel, ReadableStreamChannel readableStreamChannel,
      CountDownLatch readCompleteLatch, AtomicLong readCompleteResult,
      AtomicReference<Throwable> readCompleteThrowable, final int blobSize, final byte[] putContent) {
    try {
      ByteBuffer putContentBuf;
      if (options != null && options.isRawMode()) {
        putContentBuf = getBlobBuffer(blobId);
        Assert.assertNotNull("Did not find server with blob: " + blobId.getID(), putContentBuf);
      } else {
        // If a range is set, compare the result against the specified byte range.
        if (options != null && options.getRange() != null) {
          ByteRange range = options.getRange().toResolvedByteRange(blobSize, options.resolveRangeOnEmptyBlob());
          putContentBuf = ByteBuffer.wrap(putContent, (int) range.getStartOffset(), (int) range.getRangeSize());
        } else {
          putContentBuf = ByteBuffer.wrap(putContent);
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
   * Helper method to simulate errors from the servers. Only one node in the datacenter where the put happened will
   * return success. No matter what order the servers are contacted, as long as one of them returns success, the whole
   * operation should succeed.
   * @param blobId id of the blob
   * @param dcWherePutHappened the datacenter where the put happened.
   * @param userMetadata the expected blob size
   * @param blobSize the expected user meta data
   * @param putContent the expected blob content
   */
  private void doTestSuccessInThePresenceOfVariousErrors(BlobId blobId, String dcWherePutHappened,
      byte[] userMetadata, int blobSize, byte[] putContent) throws Exception {
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
    getBlobAndAssertSuccess(blobId, false, false, (short)0, blobSize, userMetadata, putContent);
  }

  /**
   * Disk backed DC returns either Disk Down or Not Found.
   * Cloud backed DC returns Not Found.
   */
  @Test
  public void testCombinedDiskDownAndNotFoundCase() throws Exception {
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    BlobId blobId = doPut(userMetadata, blobSize, putContent);
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

    GetBlobOperation op = createGetBlobOperationAndComplete(blobId, null);

    Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
    RouterException routerException = (RouterException) op.getOperationException();
    if (routerException == null) {
      Assert.fail("Expected getBlobOperation to fail");
    }
    Assert.assertEquals(RouterErrorCode.BlobDoesNotExist, routerException.getErrorCode());

    mockServerLayout.getMockServers().forEach(MockServer::resetServerErrors);
  }

  /**
   * Disk backed DC hit server failure while Cloud backed DC returns Not Found.
   */
  @Test
  public void testFailureOnServerErrors() throws Exception {
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    BlobId blobId = doPut(userMetadata, blobSize, putContent);

    // set the status to various server level errors (remove all partition level errors or non errors)
    EnumSet<ServerErrorCode> serverErrors = EnumSet.complementOf(
        EnumSet.of(ServerErrorCode.Blob_Deleted, ServerErrorCode.Blob_Expired, ServerErrorCode.No_Error,
            ServerErrorCode.Blob_Authorization_Failure, ServerErrorCode.Blob_Not_Found));
    for (ServerErrorCode serverErrorCode : serverErrors) {
      mockServerLayout.getMockServers().forEach(server -> server.setServerErrorForAllRequests(serverErrorCode));
      GetBlobOperation op = createGetBlobOperationAndComplete(blobId, null);
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

      Assert.assertTrue("Operation should be complete at this time", op.isOperationComplete());
      RouterException routerException = (RouterException) op.getOperationException();
      if (routerException == null) {
        Assert.fail("Expected getBlobOperation to fail");
      }
      Assert.assertEquals(expectedRouterError, routerException.getErrorCode());
    }
  }

  /**
   * Disk backed DC returns different kinds of errors while cloud backed DC returns Not Found.
   */
  @Test
  public void testSuccessInThePresenceOfVariousErrors() throws Exception {
    byte[] userMetadata = new byte[10];
    random.nextBytes(userMetadata);
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);
    BlobId blobId = doPut(userMetadata, blobSize, putContent);
    // The put for the blob being requested happened.
    String dcWherePutHappened = routerConfig.routerDatacenterName;

    // test requests coming in from local dc as well as cross-colo.
    Properties props = getDefaultNonBlockingRouterProperties(true, "DC1");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(blobId, dcWherePutHappened, userMetadata, blobSize, putContent);

    props = getDefaultNonBlockingRouterProperties(true, "DC2");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(blobId, dcWherePutHappened, userMetadata, blobSize, putContent);

    props = getDefaultNonBlockingRouterProperties(true, "DC3");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    doTestSuccessInThePresenceOfVariousErrors(blobId, dcWherePutHappened, userMetadata, blobSize, putContent);
  }

  /**
   * Disk backed DC all returns failure but cloud backed DC returns the Blob information successfully.
   */
  @Test
  public void testFailoverToAzure() throws Exception {
    byte[] userMetadata = new byte[10];
    // a blob size that is greater than the maxChunkSize and is not a multiple of it. Will result in a composite blob.
    int blobSize = maxChunkSize * random.nextInt(10) + random.nextInt(maxChunkSize - 1) + 1;
    random.nextBytes(userMetadata);
    options = new GetBlobOptionsInternal(
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(), false,
        routerMetrics.ageAtGet);

    byte[] putContent = new byte[blobSize];
    random.nextBytes(putContent);

    blobProperties =
        new BlobProperties(blobSize + 20, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
            Utils.getRandomShort(random), Utils.getRandomShort(random), testEncryption, null, null, null);
    ByteBuf putContentBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(blobSize);
    putContentBuf.writeBytes(putContent);
    BlobId blobId = doDirectPut(BlobType.DataBlob, putContentBuf.retainedDuplicate());
    putContentBuf.release();
    Counter sizeMismatchCounter = (testEncryption ? routerMetrics.simpleEncryptedBlobSizeMismatchCount
        : routerMetrics.simpleUnencryptedBlobSizeMismatchCount);
    long startCount = sizeMismatchCounter.getCount();
    getBlobAndAssertSuccess(blobId, false, false, (short)0, blobSize, userMetadata, putContent);
    long endCount = sizeMismatchCounter.getCount();
    Assert.assertEquals("Wrong number of blob size mismatches", 1, endCount - startCount);

    // test requests coming in from local dc as well as cross-colo.
    Properties props = getDefaultNonBlockingRouterProperties(true, "DC1");
    routerConfig = new RouterConfig(new VerifiableProperties(props));
    ArrayList<MockServer> mockServers = new ArrayList<>(mockServerLayout.getMockServers());
    ArrayList<ServerErrorCode> serverErrors = new ArrayList<>(Arrays.asList(ServerErrorCode.values()));
    // set the disk backed server status to various server level or partition level errors (not Blob_Deleted or Blob_Expired - as they
    // are final)
    serverErrors.remove(ServerErrorCode.Blob_Deleted);
    serverErrors.remove(ServerErrorCode.Blob_Expired);
    serverErrors.remove(ServerErrorCode.No_Error);
    serverErrors.remove(ServerErrorCode.Blob_Authorization_Failure);
    for (MockServer mockServer : mockServers) {
      ServerErrorCode code = serverErrors.get(random.nextInt(serverErrors.size()));
      mockServer.setServerErrorForAllRequests(code);
    }
    getBlobAndAssertSuccess(blobId, false, false, (short)0, blobSize, userMetadata, putContent);

    // Jing TODO: test composite blob case?
  }


  /**
   * Get the default {@link Properties} for the {@link NonBlockingRouter}.
   * @param excludeTimeout whether to exclude timed out request in Histogram.
   * @return the constructed {@link Properties}
   * @param routerDataCenter the local data center
   */
  private Properties getDefaultNonBlockingRouterProperties(boolean excludeTimeout, String routerDataCenter) {
    Properties properties = new Properties();

    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", routerDataCenter);
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

    properties.setProperty("router.connection.checkout.timeout.ms", Integer.toString(CHECKOUT_TIMEOUT_MS));
    properties.setProperty("router.connections.local.dc.warm.up.percentage", Integer.toString(67));
    properties.setProperty("router.connections.remote.dc.warm.up.percentage", Integer.toString(34));
    properties.setProperty("clustermap.cluster.name", "test");
    properties.setProperty("clustermap.datacenter.name", "dc1");
    properties.setProperty("clustermap.host.name", "localhost");

    properties.setProperty("clustermap.port", "1666");
    properties.setProperty("clustermap.default.partition.class", MockClusterMap.DEFAULT_PARTITION_CLASS);
    properties.setProperty("clustermap.resolve.hostnames", "false");
    properties.setProperty(CloudConfig.CLOUD_DESTINATION_FACTORY_CLASS,
        LatchBasedInMemoryCloudDestinationFactory.class.getName());
    properties.setProperty(CloudConfig.VCR_MIN_TTL_DAYS, "0");

    properties.setProperty("kms.default.container.key", "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");
    return properties;
  }
}

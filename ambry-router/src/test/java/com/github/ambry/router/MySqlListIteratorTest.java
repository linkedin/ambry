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
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.QuotaConfig;
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
import com.github.ambry.quota.QuotaTestUtils;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
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
public class MySqlListIteratorTest {
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
  private final QuotaTestUtils.TestQuotaChargeCallback quotaChargeCallback;

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
    return Arrays.asList(new Object[][]{{SimpleOperationTracker.class.getSimpleName(), false, false},
        {SimpleOperationTracker.class.getSimpleName(), false, true},
        {AdaptiveOperationTracker.class.getSimpleName(), false, false},
        {AdaptiveOperationTracker.class.getSimpleName(), false, true},
        {AdaptiveOperationTracker.class.getSimpleName(), true, false}});
  }

  /**
   * Instantiate a router, perform a put, close the router. The blob that was put will be saved in the MockServer,
   * and can be queried by the getBlob operations in the test.
   * @param operationTrackerType the type of {@link OperationTracker} to use.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   * @param isBandwidthThrottlingEnabled value for
   * {@link com.github.ambry.config.QuotaConfig#bandwidthThrottlingFeatureEnabled}.
   */
  public MySqlListIteratorTest(String operationTrackerType, boolean testEncryption, boolean isBandwidthThrottlingEnabled)
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(QuotaConfig.BANDWIDTH_THROTTLING_FEATURE_ENABLED,
        String.valueOf(isBandwidthThrottlingEnabled));
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(properties));
    quotaChargeCallback = QuotaTestUtils.createTestQuotaChargeCallback(quotaConfig);
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

  @Test
  public void testHasNext() {
    GetBlobOperation op = createOperation(routerConfig, null);
    List<CompositeBlobInfo.ChunkMetadata> parentList = new ArrayList<>();
    String accountName = "unknown-account";
    String containerName = "unknown-container";
    String blobName = "blob-name";
    GetBlobOperation.MySqlListItr listItr = op.new MySqlListItr(parentList, accountName, containerName, blobName, -1);
    Assert.assertEquals(false, listItr.hasNext());
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   */
  private void getAndAssertSuccess() throws Exception {
    getAndAssertSuccess(false, false, (short) 0);
  }

  /**
   * Construct GetBlob operations with appropriate callbacks, then poll those operations until they complete,
   * and ensure that the whole blob data is read out and the contents match.
   * @param getChunksBeforeRead {@code true} if all chunks should be cached by the router before reading from the
   *                            stream.
   * @param initiateReadBeforeChunkGet Whether readInto() should be initiated immediately before data chunks are
   *                                   fetched by the router to simulate chunk arrival delay.
   * @param expectedLifeVersion the expected lifeVersion from get operation.
   */
  private void getAndAssertSuccess(final boolean getChunksBeforeRead, final boolean initiateReadBeforeChunkGet,
      short expectedLifeVersion) throws Exception {
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
              Assert.assertEquals("Blob size should in received blobProperties should be the same as actual", blobSize,
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
        ByteRange range = options.getBlobOptions.getRange()
            .toResolvedByteRange(blobSize, options.getBlobOptions.resolveRangeOnEmptyBlob());
        sizeWritten = (int) range.getRangeSize();
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
            routerCallback, blobIdFactory, kms, cryptoService, cryptoJobHandler, time, false, quotaChargeCallback);
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

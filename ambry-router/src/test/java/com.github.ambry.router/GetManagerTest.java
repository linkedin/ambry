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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class GetManagerTest {
  private final MockServerLayout mockServerLayout;
  private final MockTime mockTime = new MockTime();
  private final MockClusterMap mockClusterMap;
  private final Random random = new Random();
  // this is a reference to the state used by the mockSelector. just allows tests to manipulate the state.
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<MockSelectorState>();
  private NonBlockingRouter router;
  private final boolean testEncryption;
  private final int metadataContentVersion;
  private KeyManagementService kms = null;
  private CryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;
  private RouterConfig routerConfig;
  private int requestParallelism;
  private int successTarget;
  // Request params;
  private long blobSize;
  private BlobProperties putBlobProperties;
  private byte[] putUserMetadata;
  private byte[] putContent;
  private ReadableStreamChannel putChannel;
  private GetBlobOptions options = new GetBlobOptionsBuilder().build();
  private List<ChunkInfo> chunkInfos;
  private static final int CHUNK_SIZE = new Random().nextInt(1024 * 1024) + 8191;
  private static final int LARGE_BLOB_SIZE = CHUNK_SIZE * 6 + 11;
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private static final int NUM_STITCHED_CHUNKS = 10;

  /**
   * Running for both regular and encrypted blobs, and versions 2 and 3 of MetadataContent
   * @return an array with all four different choices
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false, MessageFormatRecord.Metadata_Content_Version_V2},
        {false, MessageFormatRecord.Metadata_Content_Version_V3},
        {true, MessageFormatRecord.Metadata_Content_Version_V2},
        {true, MessageFormatRecord.Metadata_Content_Version_V3}});
  }

  /**
   * Pre-initialization common to all tests.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public GetManagerTest(boolean testEncryption, int metadataContentVersion) throws Exception {
    this.testEncryption = testEncryption;
    this.metadataContentVersion = metadataContentVersion;
    requestParallelism = 3;
    successTarget = 2;
    mockSelectorState.set(MockSelectorState.Good);
    mockClusterMap = new MockClusterMap();
    mockServerLayout = new MockServerLayout(mockClusterMap);
    resetEncryptionObjects();
  }

  /**
   * Every test in this class should leave the router closed in the end. Some tests do additional checks after
   * closing the router. This is just a guard to ensure that the tests are not broken (which helped when developing
   * these tests).
   */
  @After
  public void postCheck() {
    Assert.assertFalse("Router should be closed at the end of each test", router.isOpen());
    Assert.assertEquals("Router operations count must be zero", 0, router.getOperationsCount());
    if (cryptoJobHandler != null) {
      cryptoJobHandler.close();
    }
  }

  /**
   * Tests getBlobInfo() and getBlob() of simple blobs
   * @throws Exception
   */
  @Test
  public void testSimpleBlobGetSuccess() throws Exception {
    testGetSuccess(CHUNK_SIZE, new GetBlobOptionsBuilder().build());
  }

  /**
   * Tests getBlobInfo() and getBlob() of composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetSuccess() throws Exception {
    testGetSuccess(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().build());
  }

  /**
   * Tests getBlobInfo() and getBlob() of stitched composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetSuccessStitchDifferentSizedBlobs() throws Exception {
    if (metadataContentVersion > MessageFormatRecord.Metadata_Content_Version_V2) {
      testGetSuccessStitch(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().build());
    } else {
      router = getNonBlockingRouter();
      router.close();
    }
  }

  /**
   * Tests getBlob() for segments of stitched composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetSuccessStitchDifferentSizedBlobsSegments() throws Exception {
    if (metadataContentVersion > MessageFormatRecord.Metadata_Content_Version_V2) {
      //Test grabbing every segment
      for (int i = 0; i < NUM_STITCHED_CHUNKS; i++) {
        testGetSuccessStitch(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().blobSegment(i).build());
      }
    } else {
      router = getNonBlockingRouter();
      router.close();
    }
  }

  /**
   * Tests the router range request interface.
   * @throws Exception
   */
  @Test
  public void testRangeRequest() throws Exception {
    testGetSuccess(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data)
        .range(ByteRanges.fromOffsetRange(CHUNK_SIZE * 2 + 3, CHUNK_SIZE * 5 + 4))
        .build());
  }

  /**
   * Tests the router range request interface on stitched blobs.
   * @throws Exception
   */
  @Test
  public void testRangeRequestStitchDifferentSizedBlobs() throws Exception {
    if (metadataContentVersion > 2) {
      testGetSuccessStitch(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data)
          .range(ByteRanges.fromOffsetRange(CHUNK_SIZE * 2 + 3, CHUNK_SIZE * 5 + 4))
          .build());
    } else {
      router = getNonBlockingRouter();
      router.close();
    }
  }

  /**
   * Tests the router range request interface on stitched blob segments.
   * @throws Exception
   */
  @Test
  public void testRangeRequestStitchDifferentSizedBlobsSegments() throws Exception {
    if (metadataContentVersion > MessageFormatRecord.Metadata_Content_Version_V2) {
      router = getNonBlockingRouter();
      String blobId = createStitchBlob(LARGE_BLOB_SIZE);
      //Test grabbing every segment with various offset ranges
      for (int i = 0; i < NUM_STITCHED_CHUNKS; i++) {
        int chunkSize = (int) chunkInfos.get(i).getChunkSizeInBytes();
        // entire chunk, [0, chunkSize-1]
        this.options =
            new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromOffsetRange(0, chunkSize - 1)).build();
        getBlobAndCompareContent(blobId);

        // overflow: [0, chunkSize]
        this.options =
            new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromOffsetRange(0, chunkSize)).build();
        getBlobAndCompareContent(blobId);

        // more overflow: [0, chunkSize + 100]
        this.options =
            new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromOffsetRange(0, chunkSize + 100)).build();
        getBlobAndCompareContent(blobId);

        // last N bytes: [chunkSize-1-N, chunkSize-1]
        this.options = new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromLastNBytes(100)).build();
        getBlobAndCompareContent(blobId);

        // range: [chunkSize/2, chunkSize/2 + Random number]
        int end = new Random().nextInt(chunkSize / 2) + chunkSize / 2;
        this.options =
            new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromOffsetRange(chunkSize / 2, end)).build();
        getBlobAndCompareContent(blobId);

        this.options = new GetBlobOptionsBuilder().blobSegment(i).range(ByteRanges.fromOffsetRange(0, 0)).build();
        getBlobAndCompareContent(blobId);
      }
      this.options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build();
      getBlobAndCompareContent(blobId);
      router.close();
    } else {
      router = getNonBlockingRouter();
      router.close();
    }
  }

  /**
   * Test a get request.
   * @param blobSize the size of the blob to put/get.
   * @param options the {@link GetBlobOptions} for the get request.
   */
  private void testGetSuccess(int blobSize, GetBlobOptions options) throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(blobSize, options);
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    getBlobAndCompareContent(blobId);
    // Test GetBlobInfoOperation, regardless of options passed in.
    this.options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build();
    getBlobAndCompareContent(blobId);
    router.close();
  }

  private String createStitchBlob(int blobSize) throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.allocate(blobSize);
    //Divide blob into NUM_STITCHED_CHUNKS chunks to be stitched
    int chunkSize = blobSize / NUM_STITCHED_CHUNKS;
    List<String> stitchBlobsIds = new ArrayList<>();
    chunkInfos = new ArrayList<>();
    int curBlobSize = blobSize;
    for (int i = 0; i < NUM_STITCHED_CHUNKS; i++) {
      //Give each chunk a different size
      int curChunkSize = Math.min(curBlobSize, chunkSize + i * 5);
      setOperationParams(curChunkSize, options);
      byteBuffer.put(putContent);
      curBlobSize -= curChunkSize;
      stitchBlobsIds.add(
          router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get());
      chunkInfos.add(new ChunkInfo(stitchBlobsIds.get(i), curChunkSize, -1L));
    }
    setOperationParams(blobSize, null);
    this.options = new GetBlobOptionsBuilder().build();
    putContent = byteBuffer.array();
    return router.stitchBlob(putBlobProperties, putUserMetadata, chunkInfos).get();
  }

  /**
   * Test a get request on a stitched blob.
   * @param blobSize the size of the blob to put/get.
   * @param options the {@link GetBlobOptions} for the get request.
   */
  private void testGetSuccessStitch(int blobSize, GetBlobOptions options) throws Exception {
    router = getNonBlockingRouter();
    String blobId = createStitchBlob(blobSize);
    this.options = options;
    getBlobAndCompareContent(blobId);
    // Test GetBlobInfoOperation, regardless of options passed in.
    this.options = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build();
    getBlobAndCompareContent(blobId);
    router.close();
  }

  /**
   * Test that an exception thrown in a user defined callback will not crash the router
   * @throws Exception
   */
  @Test
  public void testCallbackRuntimeException() throws Exception {
    final CountDownLatch getBlobCallbackCalled = new CountDownLatch(1);
    testBadCallback(new Callback<GetBlobResult>() {
      @Override
      public void onCompletion(GetBlobResult result, Exception exception) {
        getBlobCallbackCalled.countDown();
        throw new RuntimeException("Throwing an exception in the user callback");
      }
    }, getBlobCallbackCalled, true);
  }

  /**
   * Test the case where async write results in an exception. Read should be notified,
   * operation should get completed.
   */
  @Test
  public void testAsyncWriteException() throws Exception {
    final CountDownLatch getBlobCallbackCalled = new CountDownLatch(1);
    testBadCallback(new Callback<GetBlobResult>() {
      @Override
      public void onCompletion(final GetBlobResult result, final Exception exception) {
        getBlobCallbackCalled.countDown();
        AsyncWritableChannel asyncWritableChannel = new AsyncWritableChannel() {
          boolean open = true;

          @Override
          public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
            throw new RuntimeException("This will be thrown when the channel is written to.");
          }

          @Override
          public boolean isOpen() {
            return open;
          }

          @Override
          public void close() throws IOException {
            open = false;
          }
        };
        result.getBlobDataChannel().readInto(asyncWritableChannel, null);
      }
    }, getBlobCallbackCalled, false);
  }

  /**
   * Test that a bad user defined callback will not crash the router.
   * @param getBlobCallback User defined callback to be called after getBlob operation.
   * @param getBlobCallbackCalled This latch should be at 0 after {@code getBlobCallback} has been called.
   * @param checkBadCallbackBlob {@code true} if the blob contents provided by the getBlob operation with the bad
   *                             callback should be inspected for correctness.
   * @throws Exception
   */
  private void testBadCallback(Callback<GetBlobResult> getBlobCallback, CountDownLatch getBlobCallbackCalled,
      Boolean checkBadCallbackBlob) throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(LARGE_BLOB_SIZE, new GetBlobOptionsBuilder().build());
    final CountDownLatch getBlobInfoCallbackCalled = new CountDownLatch(1);
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    List<Future<GetBlobResult>> getBlobInfoFutures = new ArrayList<>();
    List<Future<GetBlobResult>> getBlobDataFutures = new ArrayList<>();
    GetBlobOptions infoOptions =
        new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build();
    GetBlobOptions dataOptions = new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data).build();
    for (int i = 0; i < 5; i++) {
      if (i == 1) {
        getBlobInfoFutures.add(router.getBlob(blobId, infoOptions, new Callback<GetBlobResult>() {
          @Override
          public void onCompletion(GetBlobResult result, Exception exception) {
            getBlobInfoCallbackCalled.countDown();
            throw new RuntimeException("Throwing an exception in the user callback");
          }
        }));
        getBlobDataFutures.add(router.getBlob(blobId, dataOptions, getBlobCallback));
      } else {
        getBlobInfoFutures.add(router.getBlob(blobId, infoOptions));
        getBlobDataFutures.add(router.getBlob(blobId, dataOptions));
      }
    }
    options = dataOptions;
    for (int i = 0; i < getBlobDataFutures.size(); i++) {
      if (i != 1 || checkBadCallbackBlob) {
        compareContent(getBlobDataFutures.get(i).get().getBlobDataChannel());
      }
    }
    options = infoOptions;
    for (Future<GetBlobResult> future : getBlobInfoFutures) {
      compareBlobInfo(future.get().getBlobInfo());
    }
    Assert.assertTrue("getBlobInfo callback not called.", getBlobInfoCallbackCalled.await(2, TimeUnit.SECONDS));
    Assert.assertTrue("getBlob callback not called.", getBlobCallbackCalled.await(2, TimeUnit.SECONDS));
    Assert.assertEquals("All operations should be finished.", 0, router.getOperationsCount());
    Assert.assertTrue("Router should not be closed", router.isOpen());

    // Test that GetManager is still operational
    setOperationParams(CHUNK_SIZE, new GetBlobOptionsBuilder().build());
    blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    getBlobAndCompareContent(blobId);
    this.options = infoOptions;
    getBlobAndCompareContent(blobId);
    router.close();
  }

  /**
   * Tests the failure case where poll throws and closes the router. This also tests the case where the GetManager
   * gets closed with active operations, and ensures that operations get completed with the appropriate error.
   * @throws Exception
   */
  @Test
  public void testFailureOnAllPollThatSends() throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(CHUNK_SIZE, new GetBlobOptionsBuilder().build());
    String blobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, new PutBlobOptionsBuilder().build()).get();
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnSend);
    Future future;
    try {
      future = router.getBlob(blobId,
          new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build());
      while (!future.isDone()) {
        mockTime.sleep(routerConfig.routerRequestTimeoutMs + 1);
        Thread.yield();
      }
      future.get();
      Assert.fail("operation should have thrown");
    } catch (ExecutionException e) {
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
    }

    try {
      future = router.getBlob(blobId, options);
      while (!future.isDone()) {
        mockTime.sleep(routerConfig.routerRequestTimeoutMs + 1);
        Thread.yield();
      }
      future.get();
      Assert.fail("operation should have thrown");
    } catch (ExecutionException e) {
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals(RouterErrorCode.OperationTimedOut, routerException.getErrorCode());
    }
    router.close();
  }

  /**
   * Tests Bad blobId for different get operations
   * @throws Exception
   */
  @Test
  public void testBadBlobId() throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(CHUNK_SIZE, new GetBlobOptionsBuilder().build());
    String[] badBlobIds = {"", "abc", "123", "invalid_id", "[],/-"};
    for (String blobId : badBlobIds) {
      for (GetBlobOptions.OperationType opType : GetBlobOptions.OperationType.values()) {
        getBlobAndAssertFailure(blobId, new GetBlobOptionsBuilder().operationType(opType).build(),
            RouterErrorCode.InvalidBlobId);
      }
    }
    router.close();
  }

  /**
   * Do a getBlob on the given blob id and ensure that all the data is fetched and is correct.
   * @param blobId the id of the blob (simple or composite) that needs to be fetched and compared.
   * @throws Exception
   */
  private void getBlobAndCompareContent(String blobId) throws Exception {
    GetBlobResult result = router.getBlob(blobId, options).get();

    switch (options.getOperationType()) {
      case All:
        compareBlobInfo(result.getBlobInfo());
        compareContent(result.getBlobDataChannel());
        break;
      case Data:
        compareContent(result.getBlobDataChannel());
        break;
      case BlobInfo:
        compareBlobInfo(result.getBlobInfo());
        Assert.assertNull("Unexpected blob data channel in result", result.getBlobDataChannel());
        break;
    }
  }

  /**
   * Do a getBlob on the given blob id and ensure that exception is thrown with the expected error code
   * @param blobId the id of the blob (simple or composite) that needs to be fetched and compared.
   * @param options {@link GetBlobOptions} to be used in the get call
   * @param expectedErrorCode expected {@link RouterErrorCode}
   * @throws Exception
   */
  private void getBlobAndAssertFailure(String blobId, GetBlobOptions options, RouterErrorCode expectedErrorCode)
      throws Exception {
    try {
      Future future = router.getBlob(blobId, options);
      future.get(routerConfig.routerRequestTimeoutMs + 1, TimeUnit.MILLISECONDS);
      Assert.fail("operation should have thrown");
    } catch (ExecutionException e) {
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals(expectedErrorCode, routerException.getErrorCode());
    }
  }

  /**
   * Compare and assert that the properties and user metadata in the given {@link BlobInfo} is exactly the same as
   * the original put properties and metadata.
   * @param blobInfo the {@link ReadableStreamChannel} that is the candidate for comparison.
   */
  private void compareBlobInfo(BlobInfo blobInfo) {
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.arePersistedFieldsEquivalent(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertEquals("Blob size in received blobProperties should be the same as actual", blobSize,
        blobInfo.getBlobProperties().getBlobSize());
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
  }

  /**
   * Compare and assert that the content in the given {@link ReadableStreamChannel} is exactly the same as
   * the original put content.
   * @param readableStreamChannel the {@link ReadableStreamChannel} that is the candidate for comparison.
   */
  private void compareContent(ReadableStreamChannel readableStreamChannel) throws Exception {
    ByteRange byteRange = options.getRange();
    if (options.hasBlobSegmentIdx()) {
      long offset = 0;
      long size = chunkInfos.get(0).getChunkSizeInBytes();
      for (int i = 0; i < options.getBlobSegmentIdx(); i++) {
        size = chunkInfos.get(i + 1).getChunkSizeInBytes();
        offset += chunkInfos.get(i).getChunkSizeInBytes();
      }
      if (options.getRange() == null) {
        byteRange = ByteRanges.fromOffsetRange(offset, offset + size - 1);
      } else {
        byteRange = byteRange.toResolvedByteRange(chunkInfos.get(options.getBlobSegmentIdx()).getChunkSizeInBytes());
        byteRange = ByteRanges.fromOffsetRange(byteRange.getStartOffset() + offset, byteRange.getEndOffset() + offset);
      }
    }
    RouterTestHelpers.compareContent(putContent, byteRange, readableStreamChannel);
  }

  /**
   * @return Return a {@link NonBlockingRouter} created with default {@link VerifiableProperties}
   */
  private NonBlockingRouter getNonBlockingRouter() throws IOException, GeneralSecurityException {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(CHUNK_SIZE));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    properties.setProperty("router.metadata.content.version", String.valueOf(metadataContentVersion));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap, routerConfig),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap, kms,
        cryptoService, cryptoJobHandler, new InMemAccountService(false, true), mockTime,
        MockClusterMap.DEFAULT_PARTITION_CLASS);
    resetEncryptionObjects();
    return router;
  }

  /**
   * Set operation parameters for the blob that will be put and got.
   * @param blobSize the blob size for the blob that will be put and got.
   * @param options the options for the get request
   */
  private void setOperationParams(int blobSize, GetBlobOptions options) {
    this.blobSize = blobSize;
    putBlobProperties = new BlobProperties(-1, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time,
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), testEncryption, null);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    this.options = options;
  }

  /**
   * Resets objects related to encryption testing
   * @throws GeneralSecurityException
   */
  private void resetEncryptionObjects() throws GeneralSecurityException {
    if (testEncryption) {
      VerifiableProperties vProps = new VerifiableProperties(new Properties());
      kms = new SingleKeyManagementService(new KMSConfig(vProps),
          TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
      cryptoService = new GCMCryptoService(new CryptoServiceConfig(vProps));
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    }
  }
}


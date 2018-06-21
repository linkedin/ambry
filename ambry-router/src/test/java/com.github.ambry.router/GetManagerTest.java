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
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
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
  private KeyManagementService kms = null;
  private CryptoService cryptoService = null;
  private CryptoJobHandler cryptoJobHandler = null;
  private RouterConfig routerConfig;
  private int chunkSize;
  private int requestParallelism;
  private int successTarget;
  // Request params;
  private long blobSize;
  private BlobProperties putBlobProperties;
  private byte[] putUserMetadata;
  private byte[] putContent;
  private ReadableStreamChannel putChannel;
  private GetBlobOptions options = new GetBlobOptionsBuilder().build();
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  /**
   * Running for both regular and encrypted blobs
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  /**
   * Pre-initialization common to all tests.
   * @param testEncryption {@code true} if blobs need to be tested w/ encryption. {@code false} otherwise
   */
  public GetManagerTest(boolean testEncryption) throws Exception {
    this.testEncryption = testEncryption;
    // random chunkSize in the range [1, 1 MB]
    chunkSize = random.nextInt(1024 * 1024) + 1;
    requestParallelism = 3;
    successTarget = 2;
    mockSelectorState.set(MockSelectorState.Good);
    mockClusterMap = new MockClusterMap();
    mockServerLayout = new MockServerLayout(mockClusterMap);
    if (testEncryption) {
      VerifiableProperties vProps = new VerifiableProperties(new Properties());
      kms = new SingleKeyManagementService(new KMSConfig(vProps),
          TestUtils.getRandomKey(SingleKeyManagementServiceTest.DEFAULT_KEY_SIZE_CHARS));
      cryptoService = new GCMCryptoService(new CryptoServiceConfig(vProps));
      cryptoJobHandler = new CryptoJobHandler(CryptoJobHandlerTest.DEFAULT_THREAD_COUNT);
    }
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
    testGetSuccess(chunkSize, new GetBlobOptionsBuilder().build());
  }

  /**
   * Tests getBlobInfo() and getBlob() of composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetSuccess() throws Exception {
    testGetSuccess(chunkSize * 6 + 11, new GetBlobOptionsBuilder().build());
  }

  /**
   * Tests the router range request interface.
   * @throws Exception
   */
  @Test
  public void testRangeRequest() throws Exception {
    testGetSuccess(chunkSize * 6 + 11, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.Data)
        .range(ByteRange.fromOffsetRange(chunkSize * 2 + 3, chunkSize * 5 + 4))
        .build());
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
    setOperationParams(chunkSize * 6 + 11, new GetBlobOptionsBuilder().build());
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
    setOperationParams(chunkSize, new GetBlobOptionsBuilder().build());
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
    setOperationParams(chunkSize, new GetBlobOptionsBuilder().build());
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
    setOperationParams(chunkSize, new GetBlobOptionsBuilder().build());
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
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
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
    ByteBuffer putContentBuf = ByteBuffer.wrap(putContent);
    // If a range is set, compare the result against the specified byte range.
    if (options.getRange() != null) {
      ByteRange range = options.getRange().toResolvedByteRange(putContent.length);
      putContentBuf = ByteBuffer.wrap(putContent, (int) range.getStartOffset(), (int) range.getRangeSize());
    }
    ByteBufferAsyncWritableChannel getChannel = new ByteBufferAsyncWritableChannel();
    Future<Long> readIntoFuture = readableStreamChannel.readInto(getChannel, null);
    final int bytesToRead = putContentBuf.remaining();
    int readBytes = 0;
    do {
      ByteBuffer buf = getChannel.getNextChunk();
      int bufLength = buf.remaining();
      Assert.assertTrue("total content read should not be greater than length of put content",
          readBytes + bufLength <= bytesToRead);
      while (buf.hasRemaining()) {
        Assert.assertEquals("Get and Put blob content should match", putContentBuf.get(), buf.get());
        readBytes++;
      }
      getChannel.resolveOldestChunk(null);
    } while (readBytes < bytesToRead);
    Assert.assertEquals("the returned length in the future should be the length of data written", (long) readBytes,
        (long) readIntoFuture.get());
    Assert.assertNull("There should be no more data in the channel", getChannel.getNextChunk(0));
  }

  /**
   * @return Return a {@link NonBlockingRouter} created with default {@link VerifiableProperties}
   */
  private NonBlockingRouter getNonBlockingRouter() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    routerConfig = new RouterConfig(vProps);
    router = new NonBlockingRouter(routerConfig, new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap, kms,
        cryptoService, cryptoJobHandler, new InMemAccountService(false, true), mockTime,
        MockClusterMap.DEFAULT_PARTITION_CLASS);
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
        Utils.getRandomShort(TestUtils.RANDOM), Utils.getRandomShort(TestUtils.RANDOM), testEncryption);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    this.options = options;
  }
}


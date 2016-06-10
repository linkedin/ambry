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

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferAsyncWritableChannel;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
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


public class GetManagerTest {
  private final MockServerLayout mockServerLayout;
  private final MockTime mockTime = new MockTime();
  private final MockClusterMap mockClusterMap;
  private final Random random = new Random();
  // this is a reference to the state used by the mockSelector. just allows tests to manipulate the state.
  private final AtomicReference<MockSelectorState> mockSelectorState = new AtomicReference<MockSelectorState>();
  private NonBlockingRouter router;
  private int chunkSize;
  private int requestParallelism;
  private int successTarget;
  // Request params;
  private BlobProperties putBlobProperties;
  private byte[] putUserMetadata;
  private byte[] putContent;
  private ReadableStreamChannel putChannel;

  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;

  /**
   * Pre-initialization common to all tests.
   */
  public GetManagerTest()
      throws Exception {
    // random chunkSize in the range [1, 1 MB]
    chunkSize = random.nextInt(1024 * 1024) + 1;
    requestParallelism = 3;
    successTarget = 2;
    mockSelectorState.set(MockSelectorState.Good);
    mockClusterMap = new MockClusterMap();
    mockServerLayout = new MockServerLayout(mockClusterMap);
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
  }

  /**
   * Tests getBlobInfo() and getBlob() of simple blobs
   * @throws Exception
   */
  @Test
  public void testSimpleBlobGetSuccess()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    BlobInfo blobInfo = router.getBlobInfo(blobId).get();
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
    getBlobAndCompareContent(blobId);
    router.close();
  }

  /**
   * Tests getBlobInfo() and getBlob() of composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetSuccess()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize * 6 + 11);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    BlobInfo blobInfo = router.getBlobInfo(blobId).get();
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
    getBlobAndCompareContent(blobId);
    router.close();
  }

  /**
   * Test that a bad user defined callback will not crash the router.
   * @throws Exception
   */
  @Test
  public void testBadCallback()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize * 6 + 11);
    final CountDownLatch callbackCalled = new CountDownLatch(1);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    router.getBlobInfo(blobId, new Callback<BlobInfo>() {
      @Override
      public void onCompletion(BlobInfo result, Exception exception) {
        callbackCalled.countDown();
        throw new RuntimeException("Throwing an exception in the user callback");
      }
    }).get();
    router.getBlob(blobId, new Callback<ReadableStreamChannel>() {
      @Override
      public void onCompletion(ReadableStreamChannel result, Exception exception) {
        callbackCalled.countDown();
        throw new RuntimeException("Throwing an exception in the user callback");
      }
    }).get();
    Assert.assertTrue("Callback not called.", callbackCalled.await(2, TimeUnit.SECONDS));
    Assert.assertEquals("All operations should be finished.", 0, router.getOperationsCount());
    Assert.assertTrue("Router should not be closed", router.isOpen());

    // Test that GetManager is still operational
    setOperationParams(chunkSize);
    blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    BlobInfo blobInfo = router.getBlobInfo(blobId).get();
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
    getBlobAndCompareContent(blobId);
    router.close();
  }

  /**
   * Tests the failure case where poll throws and closes the router. This also tests the case where the GetManager
   * gets closed with active operations, and ensures that operations get completed with the appropriate error.
   * @throws Exception
   */
  @Test
  public void testFailureOnAllPollThatSends()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    mockSelectorState.set(MockSelectorState.ThrowExceptionOnSend);
    try {
      router.getBlobInfo(blobId).get();
      Assert.fail("operation should have thrown");
    } catch (ExecutionException e) {
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals("Exception received should be router closed error", RouterErrorCode.RouterClosed,
          routerException.getErrorCode());
    }

    try {
      router.getBlob(blobId).get();
      Assert.fail("operation should have thrown");
    } catch (ExecutionException e) {
      RouterException routerException = (RouterException) e.getCause();
      Assert.assertEquals("Exception received should be router closed error", RouterErrorCode.RouterClosed,
          routerException.getErrorCode());
    }
  }

  /**
   * Do a getBlob on the given blob id and ensure that all the data is fetched and is correct.
   * @param blobId the id of the blob (simple or composite) that needs to be fetched and compared.
   * @throws Exception
   */
  private void getBlobAndCompareContent(String blobId)
      throws Exception {
    ByteBufferAsyncWritableChannel getChannel = new ByteBufferAsyncWritableChannel();
    Future<Long> readIntoFuture = router.getBlob(blobId).get().readInto(getChannel, null);
    int readBytes = 0;
    do {
      ByteBuffer buf = getChannel.getNextChunk();
      int bufLength = buf.remaining();
      Assert.assertTrue("total content read should not be greater than length of put content",
          readBytes + bufLength <= putContent.length);
      while (buf.hasRemaining()) {
        Assert.assertEquals("Get and Put blob content should match", putContent[readBytes++], buf.get());
      }
      getChannel.resolveOldestChunk(null);
    } while (readBytes < putContent.length);
    Assert.assertEquals("the returned length in the future should be the length of data written", (long) readBytes,
        (long) readIntoFuture.get());
    Assert.assertNull("There should be no more data in the channel", getChannel.getNextChunk(0));
  }

  /**
   * @return Return a {@link NonBlockingRouter} created with default {@link VerifiableProperties}
   */
  private NonBlockingRouter getNonBlockingRouter()
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    properties.setProperty("router.max.put.chunk.size.bytes", Integer.toString(chunkSize));
    properties.setProperty("router.put.request.parallelism", Integer.toString(requestParallelism));
    properties.setProperty("router.put.success.target", Integer.toString(successTarget));
    VerifiableProperties vProps = new VerifiableProperties(properties);
    router = new NonBlockingRouter(new RouterConfig(vProps),
        new NonBlockingRouterMetrics(mockClusterMap),
        new MockNetworkClientFactory(vProps, mockSelectorState, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, mockServerLayout, mockTime), new LoggingNotificationSystem(), mockClusterMap,
        mockTime);
    return router;
  }

  /**
   * Set operation parameters for the blob that will be put and got.
   * @param blobSize the blob size for the blob that will be put and got.
   */
  private void setOperationParams(int blobSize) {
    putBlobProperties =
        new BlobProperties(blobSize, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[blobSize];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }
}


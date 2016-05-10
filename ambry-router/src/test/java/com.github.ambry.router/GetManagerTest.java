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
import java.util.concurrent.ExecutionException;
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
    Assert.assertEquals("Router operations count must be zero", 0, NonBlockingRouter.getOperationsCount());
  }

  /**
   * Tests get blob info of simple blobs
   * @throws Exception
   */
  @Test
  public void testSimpleBlobGetBlobInfoSuccess()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    BlobInfo blobInfo = router.getBlobInfo(blobId).get();
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
    router.close();
  }

  /**
   * Tests get blob info of composite blobs
   * @throws Exception
   */
  @Test
  public void testCompositeBlobGetBlobInfoSuccess()
      throws Exception {
    router = getNonBlockingRouter();
    setOperationParams(chunkSize * 6 + 11);
    String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    BlobInfo blobInfo = router.getBlobInfo(blobId).get();
    Assert.assertTrue("Blob properties should match",
        RouterTestHelpers.haveEquivalentFields(putBlobProperties, blobInfo.getBlobProperties()));
    Assert.assertArrayEquals("User metadata should match", putUserMetadata, blobInfo.getUserMetadata());
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
        new NonBlockingRouterMetrics(mockClusterMap.getMetricRegistry()),
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


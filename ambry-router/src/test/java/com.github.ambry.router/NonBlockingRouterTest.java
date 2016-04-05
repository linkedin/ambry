/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;


/**
 * Class to test the {@link NonBlockingRouter}
 */
public class NonBlockingRouterTest {
  private static final int MAX_PORTS_PLAIN_TEXT = 3;
  private static final int MAX_PORTS_SSL = 3;
  private static final int CHECKOUT_TIMEOUT_MS = 1000;
  private final Random random = new Random();
  private NonBlockingRouter router;

  // Request params;
  BlobProperties putBlobProperties;
  byte[] putUserMetadata;
  byte[] putContent;
  ReadableStreamChannel putChannel;

  /**
   * Constructs and returns a VerifiableProperties instance with the defaults required for instantiating
   * the {@link NonBlockingRouter}.
   * @return the created VerifiableProperties instance.
   */
  private Properties getNonBlockingRouterProperties() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "DC1");
    return properties;
  }

  private void setOperationParams() {
    putBlobProperties = new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    putContent = new byte[100];
    random.nextBytes(putContent);
    putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
  }

  /**
   * Test the {@link NonBlockingRouterFactory}
   */
  @Test
  public void testNonBlockingRouterFactory()
      throws Exception {
    Properties props = getNonBlockingRouterProperties();
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem()).getRouter();
    assertExpectedThreadCounts(1);
    router.close();
    assertExpectedThreadCounts(0);
  }

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic()
      throws Exception {
    Properties props = getNonBlockingRouterProperties();
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties),
        new NonBlockingRouterMetrics(new MetricRegistry()),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, mockTime);

    assertExpectedThreadCounts(1);

    setOperationParams();

    // More extensive test for puts present elsewhere - these statements are here just to exercise the flow within the
    // NonBlockingRouter class, and to ensure that operations submitted to a router eventually completes.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    // @todo to be enabled when these operation managers are implemented.
    // router.getBlob("nonExistentBlobId");
    // router.getBlobInfo("nonExistentBlobid");
    // router.deleteBlob("nonExistentBlobId");
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    assertClosed();
  }

  /**
   * Test that multiple scaling units can be instantiated, exercised and closed.
   */
  @Test
  public void testMultipleScalingUnit()
      throws Exception {
    final int SCALING_UNITS = 3;
    Properties props = getNonBlockingRouterProperties();
    props.setProperty("router.scaling.unit.count", Integer.toString(SCALING_UNITS));
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    MockTime mockTime = new MockTime();
    router = new NonBlockingRouter(new RouterConfig(verifiableProperties),
        new NonBlockingRouterMetrics(new MetricRegistry()),
        new MockNetworkClientFactory(verifiableProperties, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockServerLayout(mockClusterMap), mockTime), new LoggingNotificationSystem(),
        mockClusterMap, mockTime);

    assertExpectedThreadCounts(SCALING_UNITS);

    // Submit a few jobs so that all the scaling units get exercised.
    for (int i = 0; i < SCALING_UNITS * 10; i++) {
      setOperationParams();
      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    }
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    setOperationParams();
    assertClosed();
  }

  /**
   * Assert that the number of ChunkFiller and RequestResponseHandler threads running are as expected.
   * @param expectedCount the expected number of ChunkFiller and RequestResponseHandler threads.
   */
  private void assertExpectedThreadCounts(int expectedCount) {
    Assert.assertEquals("Number of chunkFiller threads running should be as expected", expectedCount,
        TestUtils.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("Number of RequestResponseHandler threads running should be as expected", expectedCount,
        TestUtils.numThreadsByThisName("RequestResponseHandlerThread"));
    if (expectedCount == 0) {
      Assert.assertFalse("Router should be closed if there are no worker threads running", router.isOpen());
      Assert
          .assertEquals("All operations should have completed if the router is closed", 0, router.getOperationsCount());
    }
  }

  /**
   * Assert that submission after closing the router returns a future that is already done and an appropriate
   * exception.
   */
  private void assertClosed() {
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }
}

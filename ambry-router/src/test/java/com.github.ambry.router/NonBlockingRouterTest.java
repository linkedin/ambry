package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
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

    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

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
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }

  /**
   * Test that multiple scaling units can be instantiated, closed, and that closing one will close the router.
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
      BlobProperties putBlobProperties =
          new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
      byte[] putUserMetadata = new byte[10];
      random.nextBytes(putUserMetadata);
      byte[] putContent = new byte[100];
      random.nextBytes(putContent);
      ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

      router.putBlob(putBlobProperties, putUserMetadata, putChannel).get();
    }
    router.close();
    assertExpectedThreadCounts(0);

    //submission after closing should return a future that is already done.
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }

  private void assertExpectedThreadCounts(int expectedCount) {
    Assert.assertEquals("Number of chunkFiller threads running should be as expected", expectedCount,
        UtilsTest.numThreadsByThisName("ChunkFillerThread"));
    Assert.assertEquals("Number of RequestResponseHandler threads running should be as expected", expectedCount,
        UtilsTest.numThreadsByThisName("RequestResponseHandlerThread"));
    if (expectedCount == 0) {
      Assert.assertFalse("Router should be closed if there are no worker threads running", router.isOpen());
      Assert
          .assertEquals("All operations should have completed if the router is closed", 0, router.getOperationsCount());
    }
  }
}

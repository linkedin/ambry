package com.github.ambry.router;

import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
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
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic()
      throws Exception {
    Properties props = getNonBlockingRouterProperties();
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    Router router =
        new NonBlockingRouterFactory(verifiableProperties, mockClusterMap, new LoggingNotificationSystem()).getRouter();

    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    router.getBlob("nonExistentBlobId");
    router.getBlobInfo("nonExistentBlobid");
    router.deleteBlob("nonExistentBlobId");
    router.close();

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
    Properties props = getNonBlockingRouterProperties();
    props.setProperty("router.scaling.unit.count", "3");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    NonBlockingRouter router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem()).getRouter();

    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    router.close();

    //submission after closing should return a future that is already done.
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
    RouterException e = (RouterException) ((FutureResult<String>) future).error();
    Assert.assertEquals(e.getErrorCode(), RouterErrorCode.RouterClosed);
  }
}

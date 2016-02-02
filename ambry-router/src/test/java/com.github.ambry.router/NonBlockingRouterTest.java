package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Class to test the {@link ConnectionManager}
 */
public class NonBlockingRouterTest {

  /**
   * Tests if {@link NonBlockingRouterFactory} instantiation and functionality.
   * @throws IOException
   * @throws InstantiationException
   */
  @Test
  public void testNonBlockingRouterFactoryTest() {
    try {
      Properties props = new Properties();
      props.setProperty("router.hostname", "localhost");
      props.setProperty("router.datacenter.name", "DC1");
      VerifiableProperties verifiableProperties = new VerifiableProperties((props));
      RouterFactory routerFactory =
          new NonBlockingRouterFactory(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem(),
              new MockTime());
      Router router = routerFactory.getRouter();
      Assert.assertEquals(NonBlockingRouter.class.getCanonicalName(), router.getClass().getCanonicalName());
    } catch (Exception e) {
      Assert.assertTrue("Received exception " + e.getMessage(), false);
    }
  }

  /**
   * Tests the ConnectionManager. Checks out and checks in connections and ensures that the pool limit is honored.
   * @throws IOException
   */
  @Test
  public void testConnectionManager()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.max.connections.per.port.ssl", "3");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    MockRequestResponseHandler mockRequestResponseHandler = new MockRequestResponseHandler();
    ConnectionManager connectionManager = new ConnectionManager(mockRequestResponseHandler, routerConfig);

    //When no connections were ever made to a host:port, connectionManager should return null, but
    //initiate connections.
    Port port1 = new Port(100, PortType.PLAINTEXT);
    String conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNull(conn11);
    String conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNull(conn12);
    String conn13 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNull(conn13);
    String conn14 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNull(conn14);

    Assert.assertEquals(3, mockRequestResponseHandler.count());

    Port port2 = new Port(200, PortType.SSL);
    String conn21 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn21);
    String conn22 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn22);
    String conn23 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn23);
    String conn24 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn24);

    Assert.assertEquals(6, mockRequestResponseHandler.count());
    //Assume a connection was made.

    ListIterator<String> iter = mockRequestResponseHandler.getConnectionIds().listIterator();
    while (iter.hasNext()) {
      connectionManager.checkInConnection(iter.next());
      iter.remove();
    }

    conn21 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn21);
    //Check this connection id back in. This should be returned in a future
    //checkout.
    connectionManager.checkInConnection(conn21);
    conn22 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn22);
    conn23 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn23);
    Assert.assertEquals(conn21, connectionManager.checkOutConnection("host2", port2));
    //Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    Assert.assertEquals(0, mockRequestResponseHandler.count());

    conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    //Remove a checked out connection.
    connectionManager.destroyConnection(conn12);
    connectionManager.checkInConnection(conn11);
    Assert.assertNotNull(connectionManager.checkOutConnection("host1", port1));
    Assert.assertNotNull(connectionManager.checkOutConnection("host1", port1));
    Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    //One new connection should have been initialized.
    Assert.assertEquals(1, mockRequestResponseHandler.count());
  }

  /**
   * Test OperationController with single scaling unit
   */
  @Test
  public void testOperationController()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.count", "1");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    //Instantiation test
    Router router = null;
    try {
      router = new NonBlockingRouterFactory(verifiableProperties, mockClusterMap, new LoggingNotificationSystem(),
          new MockTime()).getRouter();
    } catch (Exception e) {
      Assert.assertTrue("Received exception " + e.getMessage(), false);
    }

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    router.close();
    //submission after closing should return a future that is already done.
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
  }

  /**
   * Test that multiple {@link OperationController} can be instantiated, can be closed, and that
   * closing one will close the router.
   * @throws IOException
   */
  @Test
  public void testOperationControllerMultiple()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.count", "3");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    //Instantiation test
    TestNonBlockingRouter router = null;
    try {
      router = new TestNonBlockingRouterFactory(verifiableProperties, mockClusterMap, new LoggingNotificationSystem(),
          new MockTime()).getRouter();
    } catch (Exception e) {
      Assert.assertTrue("Received exception " + e.getMessage(), false);
    }

    for (OperationController oc : router.getOperationControllers()) {
      Assert.assertTrue(oc.isRunning());
    }

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);

    //test closing down flow when the operation controller closes by itself.
    router.closeOperationController();
    for (OperationController oc : router.getOperationControllers()) {
      Assert.assertFalse(oc.isRunning());
    }

    Assert.assertFalse(router.isRunning());

    //Ensure the router completes the future
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());

    //Ensure router close just works.
    router.close();
  }
}

/**
 * A factory that utilizes {@link NonBlockingRouterFactory} to return a {@link TestNonBlockingRouter}
 */
class TestNonBlockingRouterFactory extends NonBlockingRouterFactory {
  public TestNonBlockingRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, Time time)
      throws Exception {
    super(verifiableProperties, clusterMap, notificationSystem, time);
  }

  public TestNonBlockingRouter getRouter()
      throws InstantiationException {
    try {
      return new TestNonBlockingRouter(routerConfig, routerMetrics, networkConfig, networkMetrics, sslFactory,
          notificationSystem, clusterMap, time);
    } catch (Exception e) {
      throw new InstantiationException("Could not instantiate TestRouter");
    }
  }
}

/**
 * A {@link Router} that used for testing a {@link NonBlockingRouter}.
 */
class TestNonBlockingRouter extends NonBlockingRouter {
  TestNonBlockingRouter(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkConfig networkConfig, NetworkMetrics networkMetrics, SSLFactory sslFactory,
      NotificationSystem notificationSystem, ClusterMap clusterMap, Time time)
      throws Exception {
    super(routerConfig, routerMetrics, networkConfig, networkMetrics, sslFactory, notificationSystem, clusterMap,
        time);
  }

  void closeOperationController() {
    super.onOperationControllerClose();
  }
}

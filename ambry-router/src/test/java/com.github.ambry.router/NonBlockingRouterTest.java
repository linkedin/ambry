package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ListIterator;
import java.util.Properties;
import java.util.Random;
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
          new NonBlockingRouterFactory(verifiableProperties, new MockClusterMap(), new LoggingNotificationSystem());
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
      connectionManager.addToAvailablePool(iter.next());
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
   * Test OperationController
   */
  @Test
  public void testOperationController()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.count", "1");
    MetricRegistry metricRegistry = new MetricRegistry();
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
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
    NonBlockingRouter router = null;
    try {
      router =
          new NonBlockingRouter(routerConfig, metricRegistry, new LoggingNotificationSystem(), mockClusterMap);
    } catch (Exception e) {
      Assert.assertTrue("Received exception " + e.getMessage(), false);
    }

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
  }
}

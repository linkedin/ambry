package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests the {@link NonBlockingConnectionManager}. Constructs one using a {@link MockSelector}. Checks out and
 * checks in connections and ensures that the pool limit is honored. Also tests sending, receiving,
 * destroying and closing the connection manager.
 * @throws IOException
 */
public class NonBlockingConnectionManagerTest {
  MockSelector selector;
  NonBlockingConnectionManager connectionManager;
  VerifiableProperties verifiableProperties;
  RouterConfig routerConfig;
  NetworkConfig networkConfig;
  Time time;

  @Before
  public void initialize()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.max.connections.per.port.ssl", "2");
    verifiableProperties = new VerifiableProperties((props));
    routerConfig = new RouterConfig(verifiableProperties);
    networkConfig = new NetworkConfig(verifiableProperties);
    time = new MockTime();
    selector = new MockSelector();
  }

  /**
   * Test successful instantiation with good inputs.
   */
  @Test
  public void testNonBlockingConnectionManagerInstantiation() {
    connectionManager =
        new NonBlockingConnectionManager(selector, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
            routerConfig.routerMaxConnectionsPerPortSsl, time);
  }

  /**
   * Test instantiation failure with bad inputs.
   */
  @Test
  public void testNonBlockingConnectionManagerInstantiationBadInputs() {
    try {
      connectionManager =
          new NonBlockingConnectionManager(null, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
              routerConfig.routerMaxConnectionsPerPortSsl, time);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      connectionManager =
          new NonBlockingConnectionManager(selector, null, routerConfig.routerMaxConnectionsPerPortPlainText,
              routerConfig.routerMaxConnectionsPerPortSsl, time);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests honoring of pool limits.
   * @throws Exception
   */
  @Test
  public void testNonBlockingConnectionManager()
      throws Exception {
    connectionManager =
        new NonBlockingConnectionManager(selector, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
            routerConfig.routerMaxConnectionsPerPortSsl, time);
    // When no connections were ever made to a host:port, connectionManager should return null, but
    // initiate connections.
    Port port1 = new Port(100, PortType.PLAINTEXT);
    for (int i = 0; i < 4; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    }
    Assert.assertEquals(3, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(0, connectionManager.getAvailableConnectionsCount());

    Port port2 = new Port(200, PortType.SSL);
    for (int i = 0; i < 4; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    }
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(0, connectionManager.getAvailableConnectionsCount());

    selector.poll(0, null);
    for (String conn : selector.connected()) {
      connectionManager.checkInConnection(conn);
    }
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(5, connectionManager.getAvailableConnectionsCount());

    String conn = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn);
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(4, connectionManager.getAvailableConnectionsCount());

    // Check this connection id back in. This should be returned in a future
    // checkout.
    connectionManager.checkInConnection(conn);
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(5, connectionManager.getAvailableConnectionsCount());

    String checkedInConn = conn;
    Set<String> connIds = new HashSet<String>();
    for (int i = 0; i < 2; i++) {
      conn = connectionManager.checkOutConnection("host2", port2);
      Assert.assertNotNull(conn);
      connIds.add(conn);
    }
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(3, connectionManager.getAvailableConnectionsCount());

    // Make sure that one of the checked out connection is the same as the previously checked in connection.
    Assert.assertTrue(connIds.contains(checkedInConn));

    // Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(3, connectionManager.getAvailableConnectionsCount());

    // test invalid connectionId
    try {
      connectionManager.checkInConnection("invalid");
      Assert.assertFalse(true);
    } catch (Exception e) {
    }

    // destroying an invalid connection does not throw an exception (as the selector just adds these to a metric),
    // but should not affect the counts or the pool limit;
    connectionManager.destroyConnection("invalid");

    // test destroy
    String conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    String conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    connectionManager.checkInConnection(conn11);
    // Remove a checked out connection.
    connectionManager.destroyConnection(conn12);
    // Remove a checked in connection.
    connectionManager.destroyConnection(conn11);

    // Remove the same connection again, which should work. We need this behavior as connections can get disconnected
    // on their own.
    connectionManager.destroyConnection(conn11);
    Assert.assertEquals(3, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(1, connectionManager.getAvailableConnectionsCount());

    for (int i = 0; i < 1; i++) {
      Assert.assertNotNull(connectionManager.checkOutConnection("host1", port1));
    }
    for (int i = 0; i < 2; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    }
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(0, connectionManager.getAvailableConnectionsCount());
    connectionManager.close();
  }
}


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
 * checks in connections and ensures that the pool totalConnectionsCount is honored. Also tests removing connections and closing the
 * connection manager.
 */
public class NonBlockingConnectionManagerTest {
  private MockSelector selector;
  private NonBlockingConnectionManager connectionManager;
  private VerifiableProperties verifiableProperties;
  private RouterConfig routerConfig;
  private NetworkConfig networkConfig;
  private Time time;

  @Before
  public void initialize()
      throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.scaling.unit.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.max.connections.per.port.ssl", "2");
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
    connectionManager = new NonBlockingConnectionManager(selector, networkConfig,
        routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, time);
  }

  /**
   * Test instantiation failure with bad inputs.
   */
  @Test
  public void testNonBlockingConnectionManagerInstantiationBadInputs() {
    try {
      connectionManager = new NonBlockingConnectionManager(null, networkConfig,
          routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
          routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, time);
      Assert.fail("NonBlockingConnectionManager should not get constructed with a null Selector");
    } catch (IllegalArgumentException e) {
    }

    try {
      connectionManager =
          new NonBlockingConnectionManager(selector, null, routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
              routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, time);
      Assert.fail("NonBlockingConnectionManager should not get constructed with a null NetworkConfig");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests honoring of pool totalConnectionsCounts.
   * @throws IOException
   */
  @Test
  public void testNonBlockingConnectionManager()
      throws IOException {
    connectionManager = new NonBlockingConnectionManager(selector, networkConfig,
        routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl, time);
    // When no connections were ever made to a host:port, connectionManager should return null, but
    // initiate connections.
    int totalConnectionsCount = 0;
    int availableCount = 0;
    Port port1 = new Port(100, PortType.PLAINTEXT);
    for (int i = 0; i < routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText + 1; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    }
    totalConnectionsCount = routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText;
    assertCounts(totalConnectionsCount, availableCount);
    Port port2 = new Port(200, PortType.SSL);
    for (int i = 0; i < routerConfig.routerScalingUnitMaxConnectionsPerPortSsl + 2; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    }
    totalConnectionsCount += routerConfig.routerScalingUnitMaxConnectionsPerPortSsl;
    assertCounts(totalConnectionsCount, availableCount);

    selector.poll(0, null);
    for (String conn : selector.connected()) {
      connectionManager.checkInConnection(conn);
      availableCount++;
    }
    assertCounts(totalConnectionsCount, availableCount);

    String conn = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn);
    availableCount--;
    assertCounts(totalConnectionsCount, availableCount);

    // Check this connection id back in. This should be returned in a future
    // checkout.
    connectionManager.checkInConnection(conn);
    availableCount++;
    assertCounts(totalConnectionsCount, availableCount);

    String checkedInConn = conn;
    Set<String> connIds = new HashSet<String>();
    for (int i = 0; i < routerConfig.routerScalingUnitMaxConnectionsPerPortSsl; i++) {
      conn = connectionManager.checkOutConnection("host2", port2);
      Assert.assertNotNull(conn);
      connIds.add(conn);
      availableCount--;
    }
    assertCounts(totalConnectionsCount, availableCount);

    // Make sure that one of the checked out connection is the same as the previously checked in connection.
    Assert.assertTrue(connIds.contains(checkedInConn));

    // Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    assertCounts(totalConnectionsCount, availableCount);

    // test invalid connectionId
    try {
      connectionManager.checkInConnection("invalid");
      Assert.fail("Invalid connections should not get checked in.");
    } catch (IllegalArgumentException e) {
    }

    try {
      connectionManager.removeConnection("invalid");
      Assert.fail("Removing invalid connections should not succeed.");
    } catch (IllegalArgumentException e) {
    }

    // test connection removal.
    String conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    availableCount--;
    String conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    availableCount--;
    connectionManager.checkInConnection(conn11);
    availableCount++;
    // Remove a checked out connection.
    connectionManager.removeConnection(conn12);
    totalConnectionsCount--;
    // Remove a checked in connection.
    connectionManager.removeConnection(conn11);
    totalConnectionsCount--;
    availableCount--;

    // Remove the same connection again, which should throw.
    try {
      connectionManager.removeConnection(conn11);
      Assert.fail("Removing the same connection twice should not succeed.");
    } catch (IllegalArgumentException e) {
    }
    assertCounts(totalConnectionsCount, availableCount);

    Assert.assertNotNull(connectionManager.checkOutConnection("host1", port1));
    availableCount--;
    for (int i = 0; i < 2; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
      totalConnectionsCount++;
    }
    assertCounts(totalConnectionsCount, availableCount);

    connectionManager.close();
    try {
      connectionManager.checkOutConnection("host1", port1);
      Assert.fail("Attempting an operation on a closed connection manager should throw");
    } catch (IllegalStateException e) {
    }
  }

  private void assertCounts(int totalConnectionsCount, int availableCount) {
    Assert.assertEquals(totalConnectionsCount, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(availableCount, connectionManager.getAvailableConnectionsCount());
  }
}


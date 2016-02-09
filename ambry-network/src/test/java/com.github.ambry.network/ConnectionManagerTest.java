package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class ConnectionManagerTest {
  /**
   * Tests the ConnectionManager. Checks out and checks in connections and ensures that the pool limit is honored.
   * @throws IOException
   */
  @Test
  public void testConnectionManager()
      throws Exception {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.max.connections.per.port.ssl", "3");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    NetworkConfig networkConfig = new NetworkConfig(verifiableProperties);
    MockSelector mockSelector = new MockSelector(new NetworkMetrics(new MetricRegistry()), new MockTime());
    ConnectionManager connectionManager =
        new ConnectionManager(mockSelector, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
            routerConfig.routerMaxConnectionsPerPortSsl);

    //When no connections were ever made to a host:port, connectionManager should return null, but
    //initiate connections.
    Port port1 = new Port(100, PortType.PLAINTEXT);
    for (int i = 0; i < 4; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    }

    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host1", port1.getPort()));

    Port port2 = new Port(200, PortType.SSL);
    for (int i = 0; i < 4; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    }

    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host1", port1.getPort()));
    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host2", port2.getPort()));

    //Assume connections were made.
    for (String connectionId : mockSelector.getConnections()) {
      connectionManager.checkInConnection(connectionId);
    }

    mockSelector.resetConnections();

    String conn = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn);
    //Check this connection id back in. This should be returned in a future
    //checkout.
    connectionManager.checkInConnection(conn);
    String checkedInConn = conn;

    Set<String> connIds = new HashSet<String>();
    for (int i = 0; i < 3; i++) {
      conn = connectionManager.checkOutConnection("host2", port2);
      Assert.assertNotNull(conn);
      connIds.add(conn);
    }

    // Make sure that one of the checked out connection is the same as the previously checked in connection.
    Assert.assertTrue(connIds.contains(checkedInConn));

    //Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    Assert.assertEquals(0, mockSelector.getActiveConnectionCount("host2", port2.getPort()));

    //testDestroyConnection
    String conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    String conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    //Remove a checked out connection.
    connectionManager.destroyConnection(conn12);
    connectionManager.checkInConnection(conn11);

    for (int i = 0; i < 2; i++) {
      Assert.assertNotNull(connectionManager.checkOutConnection("host1", port1));
    }
    for (int i = 0; i < 2; i++) {
      Assert.assertNull(connectionManager.checkOutConnection("host1", port1));
    }
    //One new connection should have been initialized.
    Assert.assertEquals(1, mockSelector.getActiveConnectionCount("host1", port1.getPort()));
  }

  /**
   * Overrides the connect() method, so it does not make actual connections, just tracks the calls.
   */
  class MockSelector extends Selector {
    private HashMap<String, Integer> connectionIdToCount;
    private ArrayList<String> connections;

    public MockSelector(NetworkMetrics metrics, Time time)
        throws IOException {
      super(metrics, time, null);
      connectionIdToCount = new HashMap<String, Integer>();
      connections = new ArrayList<String>();
    }

    /**
     * Mocks the connect by simply keeping track of the connection requests to a (host, port)
     * @param address The address to connect to
     * @param sendBufferSize not used.
     * @param receiveBufferSize not used.
     * @param portType {@PortType} which represents the type of connection to establish
     * @return
     * @throws IOException
     */
    @Override
    public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType)
        throws IOException {
      String hostPortString = address.getHostString() + address.getPort();
      Integer count = connectionIdToCount.get(hostPortString);
      if (count != null) {
        count++;
      } else {
        count = 1;
      }
      connectionIdToCount.put(hostPortString, count);
      String connectionId = hostPortString + count;
      connections.add(connectionId);
      return connectionId;
    }

    /**
     * Returns the number of connection requests made to the given (host, port)
     * @param host the host to connect to
     * @param port the port on the host to connect to
     * @return number of connection requests made to the given (host, port)
     */
    public int getActiveConnectionCount(String host, int port) {
      String hostPortString = host + port;
      Integer i = connectionIdToCount.get(hostPortString);
      if (i == null) {
        i = 0;
      }
      return i;
    }

    /**
     * Returns a list of connection id strings for the connection requests made.
     * @return
     */
    public ArrayList<String> getConnections() {
      return connections;
    }

    /**
     * Reinitializes the MockSelector.
     */
    public void resetConnections() {
      connectionIdToCount.clear();
      connections.clear();
    }
  }
}

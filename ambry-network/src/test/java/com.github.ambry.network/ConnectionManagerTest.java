package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
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
    SSLConfig sslConfig = new SSLConfig(verifiableProperties);
    SSLFactory sslFactory = sslConfig.sslEnabledDatacenters.length() > 0 ? new SSLFactory(sslConfig) : null;
    MockSelector mockSelector = new MockSelector(new NetworkMetrics(new MetricRegistry()), new MockTime(), sslFactory);
    ConnectionManager connectionManager =
        new ConnectionManager(mockSelector, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
            routerConfig.routerMaxConnectionsPerPortSsl);

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

    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host1", port1.getPort()));

    Port port2 = new Port(200, PortType.SSL);
    String conn21 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn21);
    String conn22 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn22);
    String conn23 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn23);
    String conn24 = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNull(conn24);

    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host1", port1.getPort()));
    Assert.assertEquals(3, mockSelector.getActiveConnectionCount("host2", port2.getPort()));

    //Assume connections were made.
    for (String connectionId : mockSelector.getConnections()) {
      connectionManager.checkInConnection(connectionId);
    }

    mockSelector.resetConnections();

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
    Assert.assertEquals(0, mockSelector.getActiveConnectionCount("host2", port2.getPort()));

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
    Assert.assertEquals(1, mockSelector.getActiveConnectionCount("host1", port1.getPort()));
  }

  /**
   * Overrides the connect() method, so it does not make actual connections, just tracks the calls.
   */
  class MockSelector extends Selector {
    private HashMap<String, Integer> connectionIdToCount;
    private ArrayList<String> connections;
    int index = 0;

    public MockSelector(NetworkMetrics metrics, Time time, SSLFactory sslFactory)
        throws IOException {
      super(metrics, time, sslFactory);
      connectionIdToCount = new HashMap<String, Integer>();
      connections = new ArrayList<String>();
    }

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
      String connectionId = hostPortString + index++;
      connections.add(connectionId);
      return connectionId;
    }

    public int getActiveConnectionCount(String host, int port) {
      String hostPortString = host + port;
      Integer i = connectionIdToCount.get(hostPortString);
      if (i == null) {
        i = 0;
      }
      return i;
    }

    public ArrayList<String> getConnections() {
      return connections;
    }

    public void resetConnections() {
      connectionIdToCount.clear();
      connections.clear();
    }
  }
}

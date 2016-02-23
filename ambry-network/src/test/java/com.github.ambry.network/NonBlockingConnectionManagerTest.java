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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


public class NonBlockingConnectionManagerTest {

  /**
   * Tests the {@link NonBlockingConnectionManagerFactory}
   */
  @Test
  public void testNonBlockingConnectionManagerFactory()
      throws Exception {
    NetworkMetrics networkMetrics = new NetworkMetrics(new MetricRegistry());
    VerifiableProperties vprops = new VerifiableProperties(new Properties());
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    SSLConfig sslConfig = new SSLConfig(vprops);
    MockTime mockTime = new MockTime();

    ConnectionManagerFactory connectionManagerFactory =
        new NonBlockingConnectionManagerFactory(networkMetrics, networkConfig, sslConfig, 3, 3, mockTime);
    ConnectionManager connectionManager = connectionManagerFactory.getConnectionManager();
  }

  /**
   * Tests the {@link NonBlockingConnectionManager}. Checks out and checks in connections and ensures that the pool
   * limit is honored.
   * @throws IOException
   */
  @Test
  public void testNonBlockingConnectionManager()
      throws Exception {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.max.connections.per.port.plain.text", "3");
    props.setProperty("router.max.connections.per.port.ssl", "2");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    RouterConfig routerConfig = new RouterConfig(verifiableProperties);
    NetworkConfig networkConfig = new NetworkConfig(verifiableProperties);
    Time time = new MockTime();
    MockSelector mockSelector = new MockSelector(new NetworkMetrics(new MetricRegistry()), time);
    ConnectionManager connectionManager =
        new NonBlockingConnectionManager(mockSelector, networkConfig, routerConfig.routerMaxConnectionsPerPortPlainText,
            routerConfig.routerMaxConnectionsPerPortSsl, time);

    //When no connections were ever made to a host:port, connectionManager should return null, but
    //initiate connections.
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

    do {
      connectionManager.sendAndPoll(10, null);
    } while (connectionManager.getAvailableConnectionsCount() < 5);
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(5, connectionManager.getAvailableConnectionsCount());

    String conn = connectionManager.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn);
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(4, connectionManager.getAvailableConnectionsCount());

    //Check this connection id back in. This should be returned in a future
    //checkout.
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

    //Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionManager.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(3, connectionManager.getAvailableConnectionsCount());

    //test invalid connectionId
    try {
      connectionManager.checkInConnection("invalid");
      Assert.assertFalse(true);
    } catch (Exception e) {
    }

    // destroying an invalid connection does not throw an exception (as the selector just adds these to a metric),
    // but should not affect the counts or the pool limit;
    connectionManager.destroyConnection("invalid");

    //test send and receive
    String conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    String conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);

    List<NetworkSend> sends = new ArrayList<NetworkSend>();
    sends.add(new NetworkSend(conn11, new MockSend(), null, time));
    sends.add(new NetworkSend(conn12, new MockSend(), null, time));
    ConnectionManagerPollResponse response = connectionManager.sendAndPoll(10, sends);
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(1, connectionManager.getAvailableConnectionsCount());

    for (NetworkReceive receive : response.getCompletedReceives()) {
      connectionManager.checkInConnection(receive.getConnectionId());
    }
    Assert.assertEquals(5, connectionManager.getTotalConnectionsCount());
    Assert.assertEquals(3, connectionManager.getAvailableConnectionsCount());

    //test destroy
    conn11 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    conn12 = connectionManager.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    //Remove a checked out connection.
    connectionManager.destroyConnection(conn12);
    connectionManager.checkInConnection(conn11);
    //Remove a checked in connection.
    connectionManager.destroyConnection(conn11);

    do {
      connectionManager.sendAndPoll(10, null);
    } while (connectionManager.getTotalConnectionsCount() > 3);
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

  /**
   * Overrides the connect() method, so it does not make actual connections, just tracks the calls.
   */
  class MockSelector extends Selector {
    int index;
    private Set<String> connectionIds = new HashSet<String>();
    private List<String> connected = new ArrayList<String>();
    private List<String> disconnected = new ArrayList<String>();
    private List<NetworkSend> sends;
    private List<NetworkReceive> receives;

    public MockSelector(NetworkMetrics metrics, Time time)
        throws IOException {
      super(metrics, time, null);
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
      String hostPortString = address.getHostString() + address.getPort() + index++;
      connected.add(hostPortString);
      connectionIds.add(hostPortString);
      return hostPortString;
    }

    @Override
    public void poll(long timeoutMs, List<NetworkSend> sends) {
      this.sends = sends;
      if (sends != null) {
        for (NetworkSend send : sends) {
          receives.add(new MockReceive(send.getConnectionId()));
        }
      }
    }

    @Override
    public List<String> connected() {
      List<String> toReturn = connected;
      connected = new ArrayList<String>();
      return toReturn;
    }

    @Override
    public List<String> disconnected() {
      List<String> toReturn = disconnected;
      disconnected = new ArrayList<String>();
      return toReturn;
    }

    @Override
    public List<NetworkSend> completedSends() {
      List<NetworkSend> toReturn = sends;
      sends = new ArrayList<NetworkSend>();
      return toReturn;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
      List<NetworkReceive> toReturn = receives;
      receives = new ArrayList<NetworkReceive>();
      return toReturn;
    }

    @Override
    public void close(String conn) {
      if (connectionIds.contains(conn)) {
        disconnected.add(conn);
      }
    }

    @Override
    public void close() {

    }
  }

  class MockSend implements Send {
    private ByteBuffer buf;
    private int index;
    private int size;

    public MockSend() {
      buf = ByteBuffer.allocate(16);
      size = 16;
    }

    @Override
    public long writeTo(WritableByteChannel channel)
        throws IOException {
      long written = channel.write(buf);
      return written;
    }

    @Override
    public boolean isSendComplete() {
      return buf.remaining() == 0;
    }

    @Override
    public long sizeInBytes() {
      return size;
    }
  }

  class MockReceive extends NetworkReceive {
    String connectionId;

    public MockReceive(String connectionId) {
      super(connectionId, new BoundedByteBufferReceive(), new MockTime());
      this.connectionId = connectionId;
    }

    @Override
    public String getConnectionId() {
      return connectionId;
    }
  }
}


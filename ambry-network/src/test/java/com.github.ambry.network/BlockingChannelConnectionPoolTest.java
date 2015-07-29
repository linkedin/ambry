package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for the blocking channel connection pool
 */

public class BlockingChannelConnectionPoolTest {

  private SocketServer server1 = null;
  private SocketServer server2 = null;
  private SocketServer server3 = null;

  public BlockingChannelConnectionPoolTest()
      throws InterruptedException, IOException {
    Properties props = new Properties();
    props.setProperty("port", "6667");
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(6667, PortType.PLAINTEXT));
    ports.add(new Port(7667, PortType.SSL));
    server1 = new SocketServer(config, new MetricRegistry(), ports);
    server1.start();
    props.setProperty("port", "6668");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    ports = new ArrayList<Port>();
    ports.add(new Port(6668, PortType.PLAINTEXT));
    ports.add(new Port(7668, PortType.SSL));
    server2 = new SocketServer(config, new MetricRegistry(), ports);
    server2.start();
    props.setProperty("port", "6669");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    ports = new ArrayList<Port>();
    ports.add(new Port(6669, PortType.PLAINTEXT));
    ports.add(new Port(7669, PortType.SSL));
    server3 = new SocketServer(config, new MetricRegistry(), ports);
    server3.start();
  }

  @After
  public void cleanup() {
    server1.shutdown();
    server2.shutdown();
    server3.shutdown();
  }

  class BlockingChannelInfoThread implements Runnable {

    private AtomicInteger channelCount;
    private BlockingChannelInfo channelInfo;
    private CountDownLatch releaseLatch;
    private boolean destroyConnection;

    public BlockingChannelInfoThread(AtomicInteger channelCount, BlockingChannelInfo channelInfo,
        CountDownLatch releaseLatch, boolean destroyConnection) {
      this.channelCount = channelCount;
      this.channelInfo = channelInfo;
      this.releaseLatch = releaseLatch;
      this.destroyConnection = destroyConnection;
    }

    @Override
    public void run() {
      try {
        BlockingChannel channel = channelInfo.getBlockingChannel(1000);
        channelCount.decrementAndGet();
        releaseLatch.await();
        if (destroyConnection) {
          channelInfo.destroyBlockingChannel(channel);
        } else {
          channelInfo.addBlockingChannel(channel);
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.assertFalse(true);
      }
    }
  }

  @Test
  public void testBlockingChannelInfoForPlainText()
      throws Exception {
    testBlockingChannelInfo("127.0.0.1", new Port(6667, PortType.PLAINTEXT), 5, 2);
  }

  @Test
  public void testBlockingChannelInfoForSSL()
      throws Exception {
    testBlockingChannelInfo("127.0.0.1", new Port(7667, PortType.SSL), 2, 5);
  }

  private void testBlockingChannelInfo(String host, Port port, int maxConnectionsPerPortPlainText,
      int maxConnectionsPerPortSSL)
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "" + maxConnectionsPerPortPlainText);
    props.put("connectionpool.max.connections.per.port.ssl", "" + maxConnectionsPerPortSSL);
    int maxConnectionsPerChannel =
        (port.getPortType() == PortType.PLAINTEXT) ? maxConnectionsPerPortPlainText : maxConnectionsPerPortSSL;
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), host, port.getPort(),
            new MetricRegistry(), port.getPortType());
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    BlockingChannel blockingChannel = null;
    try {
      blockingChannel = channelInfo.getBlockingChannel(1000);
    } catch (ConnectionPoolTimeoutException e) {
      Assert.assertTrue(false);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
    channelInfo.addBlockingChannel(blockingChannel);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
    AtomicInteger channelCount = new AtomicInteger(2 * maxConnectionsPerChannel);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < 2 * maxConnectionsPerChannel; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, false);
      Thread t = new Thread(infoThread);
      t.start();
    }
    while (channelCount.get() != maxConnectionsPerChannel) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerChannel);
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerChannel);
    channelInfo.cleanup();
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    channelCount = new AtomicInteger(2 * maxConnectionsPerChannel);
    releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < 2 * maxConnectionsPerChannel; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, true);
      Thread t = new Thread(infoThread);
      t.start();
    }
    while (channelCount.get() != maxConnectionsPerChannel) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerChannel);
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerChannel);
    channelInfo.cleanup();
    channelCount = new AtomicInteger(maxConnectionsPerChannel / 2);
    releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < (maxConnectionsPerChannel / 2); i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, true);
      Thread t = new Thread(infoThread);
      t.start();
    }
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), (maxConnectionsPerChannel / 2));
    channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), (maxConnectionsPerChannel / 2));
    channelInfo.cleanup();
  }

  class ConnectionPoolThread implements Runnable {

    private AtomicReference<Exception> exception;
    private Map<String, AtomicInteger> channelCount;
    private ConnectionPool connectionPool;
    private boolean destroyConnection;
    private CountDownLatch releaseConnection;
    private Map<String, Port> channelToPortMap;

    public ConnectionPoolThread(Map<String, AtomicInteger> channelCount, Map<String, Port> channelToPortMap,
        ConnectionPool connectionPool, boolean destroyConnection, CountDownLatch releaseConnection,
        AtomicReference<Exception> e) {
      this.channelCount = channelCount;
      this.channelToPortMap = channelToPortMap;
      this.connectionPool = connectionPool;
      this.destroyConnection = destroyConnection;
      this.releaseConnection = releaseConnection;
      this.exception = e;
    }

    @Override
    public void run() {
      try {
        List<ConnectedChannel> connectedChannels = new ArrayList<ConnectedChannel>();
        for (String channelStr : channelCount.keySet()) {
          Port port = channelToPortMap.get(channelStr);
          ConnectedChannel channel =
              connectionPool.checkOutConnection("localhost", new Port(port.getPort(), port.getPortType()), 1000);
          connectedChannels.add(channel);
          channelCount.get(channelStr).incrementAndGet();
        }
        releaseConnection.await();
        for (ConnectedChannel channel : connectedChannels) {
          if (destroyConnection) {
            connectionPool.destroyConnection(channel);
          } else {
            connectionPool.checkInConnection(channel);
          }
        }
      } catch (Exception e) {
        exception.set(e);
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testBlockingChannelConnectionPool()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "5");
    props.put("connectionpool.max.connections.per.port.ssl", "5");
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(props)),
            new MetricRegistry());
    connectionPool.start();

    CountDownLatch releaseConnection = new CountDownLatch(1);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, AtomicInteger> channelCount = new HashMap<String, AtomicInteger>();
    channelCount.put("localhost" + 6667, new AtomicInteger(0));
    channelCount.put("localhost" + 6668, new AtomicInteger(0));
    channelCount.put("localhost" + 6669, new AtomicInteger(0));
    Map<String, Port> channelToPortMap = new HashMap<String, Port>();
    channelToPortMap.put("localhost" + 6667, new Port(6667, PortType.PLAINTEXT));
    channelToPortMap.put("localhost" + 6668, new Port(6668, PortType.PLAINTEXT));
    channelToPortMap.put("localhost" + 6669, new Port(6669, PortType.PLAINTEXT));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, channelToPortMap, connectionPool, false, releaseConnection, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6668, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 5);

    releaseConnection.countDown();
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6668, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 10);
    connectionPool.shutdown();
  }

  @Test
  public void testBlockingChannelConnectionPoolForSSL()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "5");
    props.put("connectionpool.max.connections.per.port.ssl", "5");
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(props)),
            new MetricRegistry());
    connectionPool.start();

    CountDownLatch releaseConnection = new CountDownLatch(1);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, AtomicInteger> channelCount = new HashMap<String, AtomicInteger>();
    channelCount.put("localhost" + 6667, new AtomicInteger(0));
    channelCount.put("localhost" + 6668, new AtomicInteger(0));
    channelCount.put("localhost" + 6669, new AtomicInteger(0));
    Map<String, Port> channelToPortMap = new HashMap<String, Port>();
    channelToPortMap.put("localhost" + 6667, new Port(6667, PortType.SSL));
    channelToPortMap.put("localhost" + 6668, new Port(6668, PortType.SSL));
    channelToPortMap.put("localhost" + 6669, new Port(6669, PortType.SSL));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, channelToPortMap, connectionPool, false, releaseConnection, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6668, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 5);

    releaseConnection.countDown();
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6668, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 10);
    connectionPool.shutdown();
  }

  @Test
  public void testBlockingChannelConnectionPoolMixOfPlainTextAndSSL()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "5");
    props.put("connectionpool.max.connections.per.port.ssl", "5");
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(props)),
            new MetricRegistry());
    connectionPool.start();

    CountDownLatch releaseConnection = new CountDownLatch(1);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, AtomicInteger> channelCount = new HashMap<String, AtomicInteger>();
    channelCount.put("localhost" + 6667, new AtomicInteger(0));
    channelCount.put("localhost" + 7668, new AtomicInteger(0));
    channelCount.put("localhost" + 6669, new AtomicInteger(0));
    Map<String, Port> channelToPortMap = new HashMap<String, Port>();
    channelToPortMap.put("localhost" + 6667, new Port(6667, PortType.PLAINTEXT));
    channelToPortMap.put("localhost" + 7668, new Port(7668, PortType.SSL));
    channelToPortMap.put("localhost" + 6669, new Port(6669, PortType.PLAINTEXT));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, channelToPortMap, connectionPool, false, releaseConnection, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 7668, 5);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 5);

    releaseConnection.countDown();
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6667, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 7668, 10);
    waitForConditionAndCheckForException(channelCount, exception, "localhost", 6669, 10);
    connectionPool.shutdown();
  }

  private void waitForConditionAndCheckForException(Map<String, AtomicInteger> channelCount,
      AtomicReference<Exception> exception, String host, int port, int expectedChannelCount)
      throws Exception {
    while (channelCount.get(host + port).get() != expectedChannelCount && exception.get() == null) {
      Thread.sleep(2);
    }
    if (exception.get() != null) {
      throw exception.get();
    }
  }
}

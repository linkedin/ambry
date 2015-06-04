package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.Arrays;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Test for the blocking channel connection pool
 */

@RunWith(Parameterized.class)
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
    server1 = new SocketServer(config, new MetricRegistry());
    server1.start();
    props.setProperty("port", "6668");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    server2 = new SocketServer(config, new MetricRegistry());
    server2.start();
    props.setProperty("port", "6669");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    server3 = new SocketServer(config, new MetricRegistry());
    server3.start();
  }

  @After
  public void cleanup() {
    server1.shutdown();
    server2.shutdown();
    server3.shutdown();
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[2][0]);
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
  public void testBlockingChannelInfo()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.host", "5");
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), "127.0.0.1", 6667,
            new MetricRegistry());
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
    AtomicInteger channelCount = new AtomicInteger(10);
    CountDownLatch releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < 10; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, false);
      Thread t = new Thread(infoThread);
      t.start();
    }
    while (channelCount.get() != 5) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    channelInfo.cleanup();
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    channelCount = new AtomicInteger(10);
    releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < 10; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, true);
      Thread t = new Thread(infoThread);
      t.start();
    }
    while (channelCount.get() != 5) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    channelInfo.cleanup();
    channelCount = new AtomicInteger(2);
    releaseLatch = new CountDownLatch(1);
    for (int i = 0; i < 2; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelCount, channelInfo, releaseLatch, true);
      Thread t = new Thread(infoThread);
      t.start();
    }
    releaseLatch.countDown();
    while (channelCount.get() != 0) {
      Thread.sleep(2);
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 2);
    channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 2);
    channelInfo.cleanup();
  }

  class ConnectionPoolThread implements Runnable {

    private AtomicReference<Exception> exception;
    private Map<String, AtomicInteger> channelCount;
    private ConnectionPool connectionPool;
    private boolean destroyConnection;
    private CountDownLatch releaseConnection;

    public ConnectionPoolThread(Map<String, AtomicInteger> channelCount, ConnectionPool connectionPool,
        boolean destroyConnection, CountDownLatch releaseConnection, AtomicReference<Exception> e) {
      this.channelCount = channelCount;
      this.connectionPool = connectionPool;
      this.destroyConnection = destroyConnection;
      this.releaseConnection = releaseConnection;
      this.exception = e;
    }

    @Override
    public void run() {
      try {
        ConnectedChannel channel1 = connectionPool.checkOutConnection("localhost", 6667, 1000);
        channelCount.get("localhost" + 6667).incrementAndGet();
        ConnectedChannel channel2 = connectionPool.checkOutConnection("localhost", 6668, 1000);
        channelCount.get("localhost" + 6668).incrementAndGet();
        ConnectedChannel channel3 = connectionPool.checkOutConnection("localhost", 6669, 1000);
        channelCount.get("localhost" + 6669).incrementAndGet();
        releaseConnection.await();
        if (destroyConnection) {
          connectionPool.destroyConnection(channel1);
        } else {
          connectionPool.checkInConnection(channel1);
        }
        if (destroyConnection) {
          connectionPool.destroyConnection(channel2);
        } else {
          connectionPool.checkInConnection(channel2);
        }
        if (destroyConnection) {
          connectionPool.destroyConnection(channel3);
        } else {
          connectionPool.checkInConnection(channel3);
        }
      } catch (Exception e) {
        exception.set(e);
        e.printStackTrace();
        Assert.assertFalse(true);
      }
    }
  }

  @Test
  public void testBlockingChannelConnectionPool()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.host", "5");
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
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, connectionPool, false, releaseConnection, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    waitForConditionAndCheckForException(channelCount, exception, 6667, 5);
    waitForConditionAndCheckForException(channelCount, exception, 6668, 5);
    waitForConditionAndCheckForException(channelCount, exception, 6669, 5);

    releaseConnection.countDown();
    waitForConditionAndCheckForException(channelCount, exception, 6667, 10);
    waitForConditionAndCheckForException(channelCount, exception, 6668, 10);
    waitForConditionAndCheckForException(channelCount, exception, 6669, 10);
    connectionPool.shutdown();
  }

  private void waitForConditionAndCheckForException(Map<String, AtomicInteger> channelCount,
      AtomicReference<Exception> exception, int port, int exceptedChannelCount)
      throws Exception {
    while (channelCount.get("localhost" + port).get() != exceptedChannelCount && exception.get() == null) {
      Thread.sleep(2);
    }
    if (exception.get() != null) {
      throw exception.get();
    }
  }
}

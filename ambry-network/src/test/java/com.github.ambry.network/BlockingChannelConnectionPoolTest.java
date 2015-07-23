package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

  class BlockingChannelInfoThread implements Runnable {
    private final BlockingChannelInfo channelInfo;
    private final CountDownLatch channelCount;
    private final CountDownLatch shouldRelease;
    private final CountDownLatch releaseComplete;
    private final boolean destroyConnection;
    private final AtomicReference<Exception> exception;

    public BlockingChannelInfoThread(BlockingChannelInfo channelInfo, CountDownLatch channelCount,
        CountDownLatch shouldRelease, CountDownLatch releaseComplete, boolean destroyConnection,
        AtomicReference<Exception> exception) {
      this.channelInfo = channelInfo;
      this.channelCount = channelCount;
      this.shouldRelease = shouldRelease;
      this.releaseComplete = releaseComplete;
      this.destroyConnection = destroyConnection;
      this.exception = exception;
    }

    @Override
    public void run() {
      try {
        BlockingChannel channel = channelInfo.getBlockingChannel(1000);
        channelCount.countDown();
        if (shouldRelease.await(1000, TimeUnit.MILLISECONDS)) {
          if (destroyConnection) {
            channelInfo.destroyBlockingChannel(channel);
          } else {
            channelInfo.releaseBlockingChannel(channel);
          }
        } else if (exception.get() == null) {
          exception.set(new Exception("Timed out waiting for signal to release connections"));
        }
      } catch (Exception e) {
        exception.set(e);
      } finally {
        releaseComplete.countDown();
      }
    }
  }

  @Test
  public void testBlockingChannelInfo()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.host", "5");
    createAndReleaseSingleChannelTest(props);
    overSubscriptionTest(props, true);
    overSubscriptionTest(props, false);
    underSubscriptionTest(props);
  }

  private void createAndReleaseSingleChannelTest(Properties props)
      throws InterruptedException, ConnectionPoolTimeoutException {
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), "127.0.0.1", 6667,
            new MetricRegistry());
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    BlockingChannel blockingChannel = channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
    channelInfo.releaseBlockingChannel(blockingChannel);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
  }

  private void overSubscriptionTest(Properties props, boolean destroyConnection)
      throws Exception {
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), "127.0.0.1", 6667,
            new MetricRegistry());

    CountDownLatch channelCount = new CountDownLatch(5);
    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(10);
    for (int i = 0; i < 5; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, destroyConnection,
              exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    awaitCountdown(channelCount, 1000, exception, "Timed out while waiting for channel count to reach 5");
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);

    // try 5 more connections
    channelCount = new CountDownLatch(5);
    for (int i = 0; i < 5; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, destroyConnection,
              exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    shouldRelease.countDown();
    awaitCountdown(channelCount, 1000, exception, "Timed out while waiting for channel count to reach 5");
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 5);
    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    channelInfo.cleanup();
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
  }

  private void underSubscriptionTest(Properties props)
      throws Exception {
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), "127.0.0.1", 6667,
            new MetricRegistry());
    CountDownLatch channelCount = new CountDownLatch(2);
    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(2);
    for (int i = 0; i < 2; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, true, exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    shouldRelease.countDown();
    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 2);
    channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 2);
    channelInfo.cleanup();
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
  }

  class ConnectionPoolThread implements Runnable {

    private final AtomicReference<Exception> exception;
    private final Map<String, CountDownLatch> channelCount;
    private final ConnectionPool connectionPool;
    private final boolean destroyConnection;
    private final CountDownLatch shouldRelease;
    private final CountDownLatch releaseComplete;

    public ConnectionPoolThread(Map<String, CountDownLatch> channelCount, ConnectionPool connectionPool,
        boolean destroyConnection, CountDownLatch shouldRelease, CountDownLatch releaseComplete,
        AtomicReference<Exception> e) {
      this.channelCount = channelCount;
      this.connectionPool = connectionPool;
      this.destroyConnection = destroyConnection;
      this.shouldRelease = shouldRelease;
      this.releaseComplete = releaseComplete;
      this.exception = e;
    }

    @Override
    public void run() {
      try {
        ConnectedChannel channel1 = connectionPool.checkOutConnection("localhost", 6667, 1000);
        channelCount.get("localhost" + 6667).countDown();
        ConnectedChannel channel2 = connectionPool.checkOutConnection("localhost", 6668, 1000);
        channelCount.get("localhost" + 6668).countDown();
        ConnectedChannel channel3 = connectionPool.checkOutConnection("localhost", 6669, 1000);
        channelCount.get("localhost" + 6669).countDown();
        if (shouldRelease.await(5000, TimeUnit.MILLISECONDS)) {
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
        } else if (exception.get() == null) {
          exception.set(new Exception("Timed out waiting for signal to release connections"));
        }
      } catch (Exception e) {
        exception.set(e);
      } finally {
        releaseComplete.countDown();
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

    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(10);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, CountDownLatch> channelCount = new HashMap<String, CountDownLatch>();
    channelCount.put("localhost" + 6667, new CountDownLatch(5));
    channelCount.put("localhost" + 6668, new CountDownLatch(5));
    channelCount.put("localhost" + 6669, new CountDownLatch(5));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, connectionPool, false, shouldRelease, releaseComplete, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    awaitCountdown(channelCount.get("localhost" + 6667), 1000, exception,
        "Timed out waiting for channel count to reach 5");
    awaitCountdown(channelCount.get("localhost" + 6668), 1000, exception,
        "Timed out waiting for channel count to reach 5");
    awaitCountdown(channelCount.get("localhost" + 6668), 1000, exception,
        "Timed out waiting for channel count to reach 5");

    // reset
    channelCount.put("localhost" + 6667, new CountDownLatch(5));
    channelCount.put("localhost" + 6668, new CountDownLatch(5));
    channelCount.put("localhost" + 6669, new CountDownLatch(5));

    shouldRelease.countDown();
    awaitCountdown(channelCount.get("localhost" + 6667), 1000, exception,
        "Timed out waiting for channel count to reach 5");
    awaitCountdown(channelCount.get("localhost" + 6668), 1000, exception,
        "Timed out waiting for channel count to reach 5");
    awaitCountdown(channelCount.get("localhost" + 6668), 1000, exception,
        "Timed out waiting for channel count to reach 5");
    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    connectionPool.shutdown();
  }

  private void awaitCountdown(CountDownLatch countDownLatch, long timeoutMs, AtomicReference<Exception> exception,
      String errMsg)
      throws Exception {
    if (!countDownLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      if (exception.get() == null) {
        exception.set(new Exception(errMsg));
      }
      throw exception.get();
    }
  }
}

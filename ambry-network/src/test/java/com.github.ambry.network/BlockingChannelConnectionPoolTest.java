/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test for the blocking channel connection pool
 */
public class BlockingChannelConnectionPoolTest {
  private SocketServer server1 = null;
  private SocketServer server2 = null;
  private SocketServer server3 = null;
  private static File trustStoreFile = null;
  private static SSLFactory sslFactory;
  private static SSLConfig sslConfig;
  private static SSLConfig serverSSLConfig1;
  private static SSLConfig serverSSLConfig2;
  private static SSLConfig serverSSLConfig3;
  private static SSLSocketFactory sslSocketFactory;

  /**
   * Run only once for all tests
   */
  @BeforeClass
  public static void initializeTests()
      throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    serverSSLConfig1 = TestSSLUtils.createSSLConfig("DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server1");
    serverSSLConfig2 = TestSSLUtils.createSSLConfig("DC1,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server2");
    serverSSLConfig3 = TestSSLUtils.createSSLConfig("DC1,DC2", SSLFactory.Mode.SERVER, trustStoreFile, "server3");
    sslConfig = TestSSLUtils.createSSLConfig("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
    sslFactory = new SSLFactory(sslConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    sslSocketFactory = sslContext.getSocketFactory();
  }

  public BlockingChannelConnectionPoolTest()
      throws Exception {
    Properties props = new Properties();
    props.setProperty("port", "6667");
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(6667, PortType.PLAINTEXT));
    ports.add(new Port(7667, PortType.SSL));
    server1 = new SocketServer(config, serverSSLConfig1, new MetricRegistry(), ports);
    server1.start();
    props.setProperty("port", "6668");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    ports = new ArrayList<Port>();
    ports.add(new Port(6668, PortType.PLAINTEXT));
    ports.add(new Port(7668, PortType.SSL));
    server2 = new SocketServer(config, serverSSLConfig2, new MetricRegistry(), ports);
    server2.start();
    props.setProperty("port", "6669");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    ports = new ArrayList<Port>();
    ports.add(new Port(6669, PortType.PLAINTEXT));
    ports.add(new Port(7669, PortType.SSL));
    server3 = new SocketServer(config, serverSSLConfig3, new MetricRegistry(), ports);
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
  public void testBlockingChannelInfoForPlainText()
      throws Exception {
    testBlockingChannelInfo("127.0.0.1", new Port(6667, PortType.PLAINTEXT), 5, 5);
  }

  @Test
  public void testBlockingChannelInfoForSSL()
      throws Exception {
    testBlockingChannelInfo("127.0.0.1", new Port(7667, PortType.SSL), 5, 5);
  }

  private void testBlockingChannelInfo(String host, Port port, int maxConnectionsPerPortPlainText,
      int maxConnectionsPerPortSSL)
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "" + maxConnectionsPerPortPlainText);
    props.put("connectionpool.max.connections.per.port.ssl", "" + maxConnectionsPerPortSSL);
    int maxConnectionsPerHost =
        (port.getPortType() == PortType.PLAINTEXT) ? maxConnectionsPerPortPlainText : maxConnectionsPerPortSSL;
    createAndReleaseSingleChannelTest(props, host, port);
    overSubscriptionTest(props, host, port, maxConnectionsPerHost, true);
    overSubscriptionTest(props, host, port, maxConnectionsPerHost, false);
    underSubscriptionTest(props, host, port, (maxConnectionsPerHost / 2));
  }

  private void createAndReleaseSingleChannelTest(Properties props, String host, Port port)
      throws InterruptedException, ConnectionPoolTimeoutException {
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), host, port,
            new MetricRegistry(), sslSocketFactory, sslConfig);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
    BlockingChannel blockingChannel = channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
    channelInfo.releaseBlockingChannel(blockingChannel);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 1);
  }

  private void overSubscriptionTest(Properties props, String host, Port port, int maxConnectionsPerHost,
      boolean destroyConnection)
      throws Exception {
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), host, port,
            new MetricRegistry(), sslSocketFactory, sslConfig);

    CountDownLatch channelCount = new CountDownLatch(maxConnectionsPerHost);
    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(2 * maxConnectionsPerHost);
    for (int i = 0; i < maxConnectionsPerHost; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, destroyConnection,
              exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    awaitCountdown(channelCount, 1000, exception,
        "Timed out while waiting for channel count to reach " + maxConnectionsPerHost);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerHost);

    // try "maxConnectionsPerHost" more connections
    channelCount = new CountDownLatch(maxConnectionsPerHost);
    for (int i = 0; i < maxConnectionsPerHost; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, destroyConnection,
              exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerHost);
    shouldRelease.countDown();
    awaitCountdown(channelCount, 1000, exception,
        "Timed out while waiting for channel count to reach " + maxConnectionsPerHost);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), maxConnectionsPerHost);
    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    channelInfo.cleanup();
    Assert.assertEquals(channelInfo.getNumberOfConnections(), 0);
  }

  private void underSubscriptionTest(Properties props, String host, Port port, int underSubscriptionCount)
      throws Exception {
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    BlockingChannelInfo channelInfo =
        new BlockingChannelInfo(new ConnectionPoolConfig(new VerifiableProperties(props)), host, port,
            new MetricRegistry(), sslSocketFactory, sslConfig);
    CountDownLatch channelCount = new CountDownLatch(underSubscriptionCount);
    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(underSubscriptionCount);
    for (int i = 0; i < underSubscriptionCount; i++) {
      BlockingChannelInfoThread infoThread =
          new BlockingChannelInfoThread(channelInfo, channelCount, shouldRelease, releaseComplete, true, exception);
      Thread t = new Thread(infoThread);
      t.start();
    }
    shouldRelease.countDown();
    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    Assert.assertEquals(channelInfo.getNumberOfConnections(), underSubscriptionCount);
    channelInfo.getBlockingChannel(1000);
    Assert.assertEquals(channelInfo.getNumberOfConnections(), underSubscriptionCount);
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
    private Map<String, Port> channelToPortMap;

    public ConnectionPoolThread(Map<String, CountDownLatch> channelCount, Map<String, Port> channelToPortMap,
        ConnectionPool connectionPool, boolean destroyConnection, CountDownLatch shouldRelease,
        CountDownLatch releaseComplete, AtomicReference<Exception> e) {
      this.channelCount = channelCount;
      this.channelToPortMap = channelToPortMap;
      this.connectionPool = connectionPool;
      this.destroyConnection = destroyConnection;
      this.shouldRelease = shouldRelease;
      this.releaseComplete = releaseComplete;
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
          channelCount.get(channelStr).countDown();
        }
        if (shouldRelease.await(5000, TimeUnit.MILLISECONDS)) {
          for (ConnectedChannel channel : connectedChannels) {
            if (destroyConnection) {
              connectionPool.destroyConnection(channel);
            } else {
              connectionPool.checkInConnection(channel);
            }
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
    props.put("connectionpool.max.connections.per.port.plain.text", "5");
    props.put("connectionpool.max.connections.per.port.ssl", "5");
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(props)), sslConfig,
            new MetricRegistry());
    connectionPool.start();

    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(10);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, CountDownLatch> channelCount = new HashMap<String, CountDownLatch>();
    channelCount.put("localhost" + 6667, new CountDownLatch(5));
    channelCount.put("localhost" + 6668, new CountDownLatch(5));
    channelCount.put("localhost" + 6669, new CountDownLatch(5));
    Map<String, Port> channelToPortMap = new HashMap<String, Port>();
    channelToPortMap.put("localhost" + 6667, new Port(6667, PortType.PLAINTEXT));
    channelToPortMap.put("localhost" + 6668, new Port(6668, PortType.PLAINTEXT));
    channelToPortMap.put("localhost" + 6669, new Port(6669, PortType.PLAINTEXT));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, channelToPortMap, connectionPool, false, shouldRelease,
              releaseComplete, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    for (String channelStr : channelCount.keySet()) {
      awaitCountdown(channelCount.get(channelStr), 1000, exception, "Timed out waiting for channel count to reach 5");
    }
    // reset
    for (String channelStr : channelCount.keySet()) {
      channelCount.put(channelStr, new CountDownLatch(5));
    }
    shouldRelease.countDown();
    for (String channelStr : channelCount.keySet()) {
      awaitCountdown(channelCount.get(channelStr), 1000, exception, "Timed out waiting for channel count to reach 5");
    }

    awaitCountdown(releaseComplete, 2000, exception, "Timed out while waiting for channels to be released");
    connectionPool.shutdown();
  }

  @Test
  public void testSSLBlockingChannelConnectionPool()
      throws Exception {
    Properties props = new Properties();
    props.put("connectionpool.max.connections.per.port.plain.text", "5");
    props.put("connectionpool.max.connections.per.port.ssl", "5");
    ConnectionPool connectionPool =
        new BlockingChannelConnectionPool(new ConnectionPoolConfig(new VerifiableProperties(props)), sslConfig,
            new MetricRegistry());
    connectionPool.start();

    CountDownLatch shouldRelease = new CountDownLatch(1);
    CountDownLatch releaseComplete = new CountDownLatch(10);
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    Map<String, CountDownLatch> channelCount = new HashMap<String, CountDownLatch>();
    channelCount.put("localhost" + 7667, new CountDownLatch(5));
    channelCount.put("localhost" + 7668, new CountDownLatch(5));
    channelCount.put("localhost" + 7669, new CountDownLatch(5));
    Map<String, Port> channelToPortMap = new HashMap<String, Port>();
    channelToPortMap.put("localhost" + 7667, new Port(7667, PortType.SSL));
    channelToPortMap.put("localhost" + 7668, new Port(7668, PortType.SSL));
    channelToPortMap.put("localhost" + 7669, new Port(7669, PortType.SSL));
    for (int i = 0; i < 10; i++) {
      ConnectionPoolThread connectionPoolThread =
          new ConnectionPoolThread(channelCount, channelToPortMap, connectionPool, false, shouldRelease,
              releaseComplete, exception);
      Thread t = new Thread(connectionPoolThread);
      t.start();
    }
    for (String channelStr : channelCount.keySet()) {
      awaitCountdown(channelCount.get(channelStr), 1000, exception, "Timed out waiting for channel count to reach 5");
    }
    // reset
    for (String channelStr : channelCount.keySet()) {
      channelCount.put(channelStr, new CountDownLatch(5));
    }
    shouldRelease.countDown();
    for (String channelStr : channelCount.keySet()) {
      awaitCountdown(channelCount.get(channelStr), 1000, exception, "Timed out waiting for channel count to reach 5");
    }

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

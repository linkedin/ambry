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
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.commons.TestSSLUtils;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.conscrypt.Conscrypt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.*;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class SSLSelectorTest {

  private static final int DEFAULT_SOCKET_BUF_SIZE = 4 * 1024;
  private final SSLFactory clientSSLFactory;
  private final int applicationBufferSize;
  private final EchoServer server;
  private Selector selector;
  private final File trustStoreFile;
  private final NetworkConfig networkConfig;
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    List<String> supportedProviders = new ArrayList<>();
    supportedProviders.add("SunJSSE");
    if (Conscrypt.isAvailable()) {
      supportedProviders.add("Conscrypt");
    }
    for (String provider : supportedProviders) {
      for (int poolSize : new int[]{0, 2}) {
        for (boolean useDirectBuffers : TestUtils.BOOLEAN_VALUES) {
          params.add(new Object[]{provider, poolSize, useDirectBuffers});
        }
      }
    }
    return params;
  }

  /**
   * @param sslContextProvider the name of the SSL library provider to use.
   * @param poolSize the size of the worker pool for reading/writing/connecting to sockets.
   * @param useDirectBuffers true to allocate direct buffers in {@link SSLTransmission}.
   * @throws Exception
   */
  public SSLSelectorTest(String sslContextProvider, int poolSize, boolean useDirectBuffers) throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    Properties serverProps = new Properties();
    TestSSLUtils.addSSLProperties(serverProps, "DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server",
        sslContextProvider);
    SSLConfig sslConfig = new SSLConfig(new VerifiableProperties(serverProps));
    Properties clientProps = new Properties();
    TestSSLUtils.addSSLProperties(clientProps, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client",
        sslContextProvider);
    SSLConfig clientSSLConfig = new SSLConfig(new VerifiableProperties(clientProps));
    SSLFactory serverSSLFactory = SSLFactory.getNewInstance(sslConfig);
    clientSSLFactory = SSLFactory.getNewInstance(clientSSLConfig);
    server = new EchoServer(serverSSLFactory, 18383);
    server.start();
    applicationBufferSize = clientSSLFactory.createSSLEngine("localhost", server.port, SSLFactory.Mode.CLIENT)
        .getSession()
        .getApplicationBufferSize();
    Properties props = new Properties();
    props.setProperty("selector.executor.pool.size", Integer.toString(poolSize));
    props.setProperty("selector.use.direct.buffers", Boolean.toString(useDirectBuffers));
    VerifiableProperties vprops = new VerifiableProperties(props);
    networkConfig = new NetworkConfig(vprops);
    selector = new Selector(new NetworkMetrics(new MetricRegistry()), SystemTime.getInstance(), clientSSLFactory,
        networkConfig);
  }

  @After
  public void teardown() throws Exception {
    selector.close();
    server.close();
    trustStoreFile.delete();
  }

  /**
   * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
   */
  @Test
  public void testServerDisconnect() throws Exception {
    // connect and do a simple request
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    assertEquals("hello", blockingRequest(connectionId, "hello"));

    // disconnect
    server.closeConnections();
    while (!selector.disconnected().contains(connectionId)) {
      selector.poll(1000L);
    }

    // reconnect and do another request
    connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    assertEquals("hello", blockingRequest(connectionId, "hello"));
  }

  /**
   * Validate that the client can intentionally disconnect and reconnect
   */
  @Test
  public void testClientDisconnect() throws Exception {
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    selector.disconnect(connectionId);
    selector.poll(10, asList(SelectorTest.createSend(connectionId, "hello1")));
    assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
    assertEquals("There should be a disconnect", 1, selector.disconnected().size());
    assertTrue("The disconnect should be from our node", selector.disconnected().contains(connectionId));
    connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    assertEquals("hello2", blockingRequest(connectionId, "hello2"));
  }

  /**
   * Sending a request with one already in flight should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithInProgress() throws Exception {
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    selector.poll(1000L,
        asList(SelectorTest.createSend(connectionId, "test1"), SelectorTest.createSend(connectionId, "test2")));
  }

  /**
   * Sending a request to a node with a bad hostname should result in an exception during connect
   */
  @Test(expected = IOException.class)
  public void testNoRouteToHost() throws Exception {
    selector.connect(new InetSocketAddress("asdf.asdf.dsc", server.port), DEFAULT_SOCKET_BUF_SIZE,
        DEFAULT_SOCKET_BUF_SIZE, PortType.SSL);
  }

  /**
   * Sending a request to a node not listening on that port should result in disconnection
   */
  @Test
  public void testConnectionRefused() throws Exception {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", 6668), DEFAULT_SOCKET_BUF_SIZE, DEFAULT_SOCKET_BUF_SIZE,
            PortType.SSL);
    while (selector.disconnected().contains(connectionId)) {
      selector.poll(1000L);
    }
  }

  /**
   * Send multiple requests to several connections in parallel. Validate that responses are received in the order that
   * requests were sent.
   */
  @Test
  public void testNormalOperation() throws Exception {
    int conns = 5;

    // create connections
    ArrayList<String> connectionIds = new ArrayList<String>();
    for (int i = 0; i < conns; i++) {
      connectionIds.add(blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE));
    }

    // send echo requests and receive responses
    int responseCount = 0;
    List<NetworkSend> sends = new ArrayList<NetworkSend>();
    for (int i = 0; i < conns; i++) {
      String connectionId = connectionIds.get(i);
      sends.add(SelectorTest.createSend(connectionId, connectionId + "&" + 0));
    }

    // loop until we complete all requests
    while (responseCount < conns) {
      // do the i/o
      selector.poll(0L, sends);
      Thread.sleep(100);

      assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

      // handle any responses we may have gotten
      for (NetworkReceive receive : selector.completedReceives()) {
        ByteBuf payload = receive.getReceivedBytes().content();
        String[] pieces = SelectorTest.asString(payload).split("&");
        assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
        assertEquals("Check the source", receive.getConnectionId(), pieces[0]);
        assertTrue("Received connectionId is as expected ", connectionIds.contains(receive.getConnectionId()));
        assertEquals("Check the request counter", 0, Integer.parseInt(pieces[1]));
        responseCount++;
        receive.getReceivedBytes().release();
      }

      // prepare new sends for the next round
      sends.clear();
      for (NetworkSend send : selector.completedSends()) {
        String dest = send.getConnectionId();
        sends.add(SelectorTest.createSend(dest, dest + "&" + 0));
      }
    }
  }

  /**
   * Validate that we can send and receive a message larger than the receive and send buffer size
   */
  @Test
  public void testSendLargeRequest() throws Exception {
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    String big = SelectorTest.randomString(10 * DEFAULT_SOCKET_BUF_SIZE, new Random());
    assertEquals(big, blockingRequest(connectionId, big));
  }

  /**
   * Test sending an empty string
   */
  @Test
  public void testEmptyRequest() throws Exception {
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    assertEquals("", blockingRequest(connectionId, ""));
  }

  @Test
  public void testSSLConnect() throws IOException {
    String connectionId = selector.connect(new InetSocketAddress("localhost", server.port), DEFAULT_SOCKET_BUF_SIZE,
        DEFAULT_SOCKET_BUF_SIZE, PortType.SSL);
    while (!selector.connected().contains(connectionId)) {
      selector.poll(10000L);
    }
    Assert.assertTrue("Channel should have been ready by now ", selector.isChannelReady(connectionId));
  }

  @Test
  public void testCloseAfterConnectCall() throws IOException {
    String connectionId = selector.connect(new InetSocketAddress("localhost", server.port), DEFAULT_SOCKET_BUF_SIZE,
        DEFAULT_SOCKET_BUF_SIZE, PortType.SSL);
    selector.close(connectionId);
    selector.poll(0);
    Assert.assertTrue("Channel should have been added to disconnected list",
        selector.disconnected().contains(connectionId));
  }

  /**
   * selector.poll() should be able to fetch more data than netReadBuffer from the socket.
   */
  @Test
  public void testSelectorPollReadSize() throws Exception {
    for (int socketBufSize : new int[]{applicationBufferSize, applicationBufferSize * 3, applicationBufferSize - 10}) {
      String connectionId = blockingSSLConnect(socketBufSize);
      String message =
          SelectorTest.randomString(socketBufSize * 2 + TestUtils.RANDOM.nextInt(socketBufSize), TestUtils.RANDOM);

      // Did not count the exact number of polls to completion since its hard to make that test deterministic.
      // i.e. EchoServer could respond slower than expected.
      assertEquals("Wrong echoed response", message, blockingRequest(connectionId, message));
    }
  }

  /**
   * Tests handling of BUFFER_UNDERFLOW during unwrap when network read buffer is smaller than SSL session packet buffer
   * size.
   */
  @Test
  public void testNetReadBufferResize() throws Exception {
    useCustomBufferSizeSelector(10, null, null);
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    String message = SelectorTest.randomString(5 * applicationBufferSize, new Random());
    assertEquals("Wrong echoed response", message, blockingRequest(connectionId, message));
  }

  /**
   * Tests handling of BUFFER_OVERFLOW during wrap when network write buffer is smaller than SSL session packet buffer
   * size.
   */
  @Test
  public void testNetWriteBufferResize() throws Exception {
    useCustomBufferSizeSelector(null, 10, null);
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    String message = SelectorTest.randomString(10, new Random());
    assertEquals("Wrong echoed response", message, blockingRequest(connectionId, message));
  }

  /**
   * Tests handling of BUFFER_OVERFLOW during unwrap when application read buffer is smaller than SSL session
   * application buffer size.
   */
  @Test
  public void testAppReadBufferResize() throws Exception {
    useCustomBufferSizeSelector(null, null, 10);
    String connectionId = blockingSSLConnect(DEFAULT_SOCKET_BUF_SIZE);
    String message = SelectorTest.randomString(5 * applicationBufferSize, new Random());
    assertEquals("Wrong echoed response", message, blockingRequest(connectionId, message));
  }

  /**
   * Make a request to the echo server and wait for the full response to come.
   * @param connectionId the ID of an established connection.
   * @param s the data to send.
   * @return the data received back from the echo server.
   * @throws Exception
   */
  private String blockingRequest(String connectionId, String s) throws Exception {
    selector.poll(1000L, Collections.singletonList(SelectorTest.createSend(connectionId, s)));
    while (true) {
      selector.poll(1000L);
      for (NetworkReceive receive : selector.completedReceives()) {
        if (receive.getConnectionId().equals(connectionId)) {
          ByteBuf payload = receive.getReceivedBytes().content();
          try {
            return SelectorTest.asString(payload);
          } finally {
            payload.release();
          }
        }
      }
    }
  }

  /**
   * Connect and wait for the connection to complete.
   * @param socketBufSize the size for the socket send and receive buffers.
   * @return the connection ID.
   * @throws IOException
   */
  private String blockingSSLConnect(int socketBufSize) throws IOException {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", server.port), socketBufSize, socketBufSize, PortType.SSL);
    while (!selector.connected().contains(connectionId)) {
      selector.poll(10000L);
    }
    return connectionId;
  }

  /**
   * Replace the {@link #selector} instance with that overrides buffer sizing logic to induce BUFFER_OVERFLOW and
   * BUFFER_UNDERFLOW cases. This overrides the methods used to get the new size for the buffers used by
   * {@link javax.net.ssl.SSLEngine}, doubling the size of the buffer each time until the size recommended by the
   * engine is reached.
   * @param netReadBufSizeStart if non-null, initialize the net read buffer with this size.
   * @param netWriteBufSizeStart if non-null, initialize the net write buffer with this size.
   * @param appReadBufSizeStart if non-null, initialize the app buffer with this size.
   */
  private void useCustomBufferSizeSelector(final Integer netReadBufSizeStart, final Integer netWriteBufSizeStart,
      final Integer appReadBufSizeStart) throws Exception {
    // close existing selector
    selector.close();
    NetworkMetrics metrics = new NetworkMetrics(new MetricRegistry());
    Time time = SystemTime.getInstance();
    selector = new Selector(metrics, time, clientSSLFactory, networkConfig) {
      @Override
      protected Transmission createTransmission(String connectionId, SelectionKey key, String hostname, int port,
          PortType portType, SSLFactory.Mode mode) throws IOException {
        AtomicReference<Integer> netReadBufSizeOverride = new AtomicReference<>(netReadBufSizeStart);
        AtomicReference<Integer> netWriteBufSizeOverride = new AtomicReference<>(netWriteBufSizeStart);
        AtomicReference<Integer> appReadBufSizeOverride = new AtomicReference<>(appReadBufSizeStart);
        return new SSLTransmission(clientSSLFactory, connectionId, (SocketChannel) key.channel(), key, hostname, port,
            time, metrics, mode, networkConfig) {
          @Override
          protected int netReadBufferSize() {
            // netReadBufferSize() is invoked in SSLTransportLayer.read() prior to the read
            // operation. To avoid the read buffer being expanded too early, increase buffer size
            // only when read buffer is full. This ensures that BUFFER_UNDERFLOW is always
            // triggered in testNetReadBufferResize().
            boolean updateAllowed = netReadBuffer() != null && !netReadBuffer().hasRemaining();
            return getAndUpdateSize(super.netReadBufferSize(), netReadBufSizeOverride, updateAllowed);
          }

          @Override
          protected int netWriteBufferSize() {
            return getAndUpdateSize(super.netWriteBufferSize(), netWriteBufSizeOverride, true);
          }

          @Override
          protected int appReadBufferSize() {
            return getAndUpdateSize(super.appReadBufferSize(), appReadBufSizeOverride, true);
          }

          /**
           * Helper method for incrementing the recommended buffer size for buffer expansion testing.
           * @param defaultSize the default size to use if an override is not present.
           * @param sizeOverride a reference to the current override value, the reference can be null if the override
           *                     is not enabled.
           * @param updateAllowed if the override size should be increased. If {@code true}, the size returned will
           *                      be double the last, capping out at {@code defaultSize}. Otherwise, the size returned
           *                      will be the same as last.
           * @return the recommended buffer size to return.
           */
          private int getAndUpdateSize(int defaultSize, AtomicReference<Integer> sizeOverride, boolean updateAllowed) {
            return sizeOverride.get() == null ? defaultSize
                : sizeOverride.updateAndGet(i -> updateAllowed ? Math.min(i * 2, defaultSize) : i);
          }
        };
      }
    };
  }
}

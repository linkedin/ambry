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
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.SystemTime;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.*;
import static org.junit.Assert.*;


/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
@RunWith(Parameterized.class)
public class SelectorTest {
  private static final int BUFFER_SIZE = 4 * 1024;
  private EchoServer server;
  private Selector selector;
  private int selectorExecutorPoolSize;
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
    return Arrays.asList(new Object[][]{{0}, {2}});
  }

  public SelectorTest(int poolSize) {
    selectorExecutorPoolSize = poolSize;
  }

  @Before
  public void setup() throws Exception {
    this.server = new EchoServer(18283);
    this.server.start();
    Properties props = new Properties();
    props.setProperty("selector.executor.pool.size", Integer.toString(selectorExecutorPoolSize));
    VerifiableProperties vprops = new VerifiableProperties(props);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    this.selector =
        new Selector(new NetworkMetrics(new MetricRegistry()), SystemTime.getInstance(), null, networkConfig);
  }

  @After
  public void teardown() throws Exception {
    this.selector.close();
    this.server.close();
  }

  /**
   * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
   */
  @Test
  public void testServerDisconnect() throws Exception {
    // connect and do a simple request
    String connectionId = blockingConnect();
    assertEquals("hello", blockingRequest(connectionId, "hello"));

    // disconnect
    this.server.closeConnections();
    while (!selector.disconnected().contains(connectionId)) {
      selector.poll(1000L);
    }

    // reconnect and do another request
    connectionId = blockingConnect();
    assertEquals("hello", blockingRequest(connectionId, "hello"));
  }

  /**
   * Validate that the client can intentionally disconnect and reconnect
   */
  @Test
  public void testClientDisconnect() throws Exception {
    String connectionId = blockingConnect();
    selector.disconnect(connectionId);
    selector.poll(10, asList(createSend(connectionId, "hello1")));
    assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
    assertEquals("There should be a disconnect", 1, selector.disconnected().size());
    assertTrue("The disconnect should be from our node", selector.disconnected().contains(connectionId));
    connectionId = blockingConnect();
    assertEquals("hello2", blockingRequest(connectionId, "hello2"));
  }

  /**
   * Validate that a closed connectionId is returned via disconnected list after close
   */
  @Test
  public void testDisconnectedListOnClose() throws Exception {
    String connectionId = blockingConnect();
    assertEquals("Disconnect list should be empty", 0, selector.disconnected().size());
    selector.close(connectionId);
    selector.poll(0);
    assertEquals("There should be a disconnect", 1, selector.disconnected().size());
    assertTrue("Expected connectionId " + connectionId + " missing from selector's disconnected list ",
        selector.disconnected().contains(connectionId));
    // make sure that the connection id is not returned via disconnected list after another poll()
    selector.poll(0);
    assertEquals("Disconnect list should be empty", 0, selector.disconnected().size());
  }

  /**
   * Sending a request with one already in flight should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithInProgress() throws Exception {
    String connectionId = blockingConnect();
    selector.poll(1000L, asList(createSend(connectionId, "test1"), createSend(connectionId, "test2")));
  }

  /**
   * Sending a request to a node without an existing connection should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithoutConnecting() throws Exception {
    selector.poll(1000L, asList(createSend("testCantSendWithoutConnecting_test", "test")));
  }

  /**
   * Sending a request to a node with a bad hostname should result in an exception during connect
   */
  @Test(expected = IOException.class)
  public void testNoRouteToHost() throws Exception {
    selector.connect(new InetSocketAddress("asdf.asdf.dsc", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.PLAINTEXT);
  }

  /**
   * Sending a request to a node not listening on that port should result in disconnection
   */
  @Test
  public void testConnectionRefused() throws Exception {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", 6668), BUFFER_SIZE, BUFFER_SIZE, PortType.PLAINTEXT);
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
    int reqs = 500;

    // create connections
    InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
    ArrayList<String> connectionIds = new ArrayList<String>();
    for (int i = 0; i < conns; i++) {
      String connectionId = selector.connect(addr, BUFFER_SIZE, BUFFER_SIZE, PortType.PLAINTEXT);
      connectionIds.add(connectionId);
    }

    // send echo requests and receive responses
    int[] requests = new int[conns];
    int[] responses = new int[conns];
    int responseCount = 0;
    List<NetworkSend> sends = new ArrayList<NetworkSend>();
    for (int i = 0; i < conns; i++) {
      String connectionId = connectionIds.get(i);
      sends.add(createSend(connectionId, connectionId + "&" + 0));
    }

    // loop until we complete all requests
    while (responseCount < conns * reqs) {
      // do the i/o
      selector.poll(100L, sends);

      assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

      // handle any responses we may have gotten
      for (NetworkReceive receive : selector.completedReceives()) {
        ByteBuf payload = receive.getReceivedBytes().content();
        String[] pieces = asString(payload).split("&");
        assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
        assertEquals("Check the source", receive.getConnectionId(), pieces[0]);
        int index = Integer.parseInt(receive.getConnectionId().split("_")[1]);
        assertEquals("Check the request counter", responses[index], Integer.parseInt(pieces[1]));
        responses[index]++; // increment the expected counter
        responseCount++;
        receive.getReceivedBytes().release();
      }

      // prepare new sends for the next round
      sends.clear();
      for (NetworkSend send : selector.completedSends()) {
        String dest = send.getConnectionId();
        String[] pieces = dest.split("_");
        int index = Integer.parseInt(pieces[1]);
        requests[index]++;
        if (requests[index] < reqs) {
          sends.add(createSend(dest, dest + "&" + requests[index]));
        }
      }
    }
  }

  /**
   * Validate that we can send and receive a message larger than the receive and send buffer size
   */
  @Test
  public void testSendLargeRequest() throws Exception {
    String connectionId = blockingConnect();
    String big = randomString(10 * BUFFER_SIZE, new Random());
    assertEquals(big, blockingRequest(connectionId, big));
  }

  /**
   * Test sending an empty string
   */
  @Test
  public void testEmptyRequest() throws Exception {
    String connectionId = blockingConnect();
    assertEquals("", blockingRequest(connectionId, ""));
  }

  private String blockingRequest(String connectionId, String s) throws Exception {
    selector.poll(1000L, asList(createSend(connectionId, s)));
    while (true) {
      selector.poll(1000L);
      for (NetworkReceive receive : selector.completedReceives()) {
        if (receive.getConnectionId() == connectionId) {
          ByteBuf payload = receive.getReceivedBytes().content();
          try {
            return asString(payload);
          } finally {
            receive.getReceivedBytes().release();
          }
        }
      }
    }
  }

  /* connect and wait for the connection to complete */
  private String blockingConnect() throws IOException {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.PLAINTEXT);
    while (!selector.connected().contains(connectionId)) {
      selector.poll(10000L);
    }
    return connectionId;
  }

  static NetworkSend createSend(String connectionId, String s) {
    ByteBuffer buf = ByteBuffer.allocate(8 + s.getBytes().length);
    buf.putLong(s.getBytes().length + 8);
    buf.put(s.getBytes());
    buf.flip();
    return new NetworkSend(connectionId, new BoundedByteBufferSend(buf), null, SystemTime.getInstance());
  }

  static String asString(ByteBuf payload) {
    ByteBuffer buffer = ByteBuffer.allocate(payload.readableBytes());
    payload.readBytes(buffer);
    buffer.flip();
    return new String(buffer.array(), buffer.arrayOffset());
  }

  /**
   * Generate a random string of letters and digits of the given length
   *
   * @param len The length of the string
   * @return The random string
   */
  static String randomString(int len, Random random) {
    String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    String DIGITS = "0123456789";
    String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    StringBuilder b = new StringBuilder();
    for (int i = 0; i < len; i++) {
      b.append(LETTERS_AND_DIGITS.charAt(random.nextInt(LETTERS_AND_DIGITS.length())));
    }
    return b.toString();
  }
}

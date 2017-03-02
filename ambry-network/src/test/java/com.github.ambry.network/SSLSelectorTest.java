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
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.SystemTime;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.*;
import static org.junit.Assert.*;


public class SSLSelectorTest {

  private static final int BUFFER_SIZE = 4 * 1024;
  private EchoServer server;
  private Selector selector;
  private File trustStoreFile;

  @Before
  public void setup() throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig sslConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    SSLConfig clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));
    SSLFactory serverSSLFactory = new SSLFactory(sslConfig);
    SSLFactory clientSSLFactory = new SSLFactory(clientSSLConfig);
    server = new EchoServer(serverSSLFactory, 18383);
    server.start();
    selector = new Selector(new NetworkMetrics(new MetricRegistry()), SystemTime.getInstance(), clientSSLFactory);
  }

  @After
  public void teardown() throws Exception {
    selector.close();
    server.close();
  }

  /**
   * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
   */
  @Test
  public void testServerDisconnect() throws Exception {
    // connect and do a simple request
    String connectionId = blockingSSLConnect();
    assertEquals("hello", blockingRequest(connectionId, "hello"));

    // disconnect
    server.closeConnections();
    while (!selector.disconnected().contains(connectionId)) {
      selector.poll(1000L);
    }

    // reconnect and do another request
    connectionId = blockingSSLConnect();
    assertEquals("hello", blockingRequest(connectionId, "hello"));
  }

  /**
   * Validate that the client can intentionally disconnect and reconnect
   */
  @Test
  public void testClientDisconnect() throws Exception {
    String connectionId = blockingSSLConnect();
    selector.disconnect(connectionId);
    selector.poll(10, asList(SelectorTest.createSend(connectionId, "hello1")));
    assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
    assertEquals("There should be a disconnect", 1, selector.disconnected().size());
    assertTrue("The disconnect should be from our node", selector.disconnected().contains(connectionId));
    connectionId = blockingSSLConnect();
    assertEquals("hello2", blockingRequest(connectionId, "hello2"));
  }

  /**
   * Sending a request with one already in flight should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithInProgress() throws Exception {
    String connectionId = blockingSSLConnect();
    selector.poll(1000L,
        asList(SelectorTest.createSend(connectionId, "test1"), SelectorTest.createSend(connectionId, "test2")));
  }

  /**
   * Sending a request to a node with a bad hostname should result in an exception during connect
   */
  @Test(expected = IOException.class)
  public void testNoRouteToHost() throws Exception {
    selector.connect(new InetSocketAddress("asdf.asdf.dsc", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.SSL);
  }

  /**
   * Sending a request to a node not listening on that port should result in disconnection
   */
  @Test
  public void testConnectionRefused() throws Exception {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", 6668), BUFFER_SIZE, BUFFER_SIZE, PortType.SSL);
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
      connectionIds.add(blockingSSLConnect());
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

      assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

      // handle any responses we may have gotten
      for (NetworkReceive receive : selector.completedReceives()) {
        String[] pieces = SelectorTest.asString(receive).split("&");
        assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
        assertEquals("Check the source", receive.getConnectionId(), pieces[0]);
        assertEquals("Check that the receive has kindly been rewound", 0,
            receive.getReceivedBytes().getPayload().position());
        assertTrue("Received connectionId is as expected ", connectionIds.contains(receive.getConnectionId()));
        assertEquals("Check the request counter", 0, Integer.parseInt(pieces[1]));
        responseCount++;
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
    String connectionId = blockingSSLConnect();
    String big = SelectorTest.randomString(10 * BUFFER_SIZE, new Random());
    assertEquals(big, blockingRequest(connectionId, big));
  }

  /**
   * Test sending an empty string
   */
  @Test
  public void testEmptyRequest() throws Exception {
    String connectionId = blockingSSLConnect();
    assertEquals("", blockingRequest(connectionId, ""));
  }

  @Test
  public void testSSLConnect() throws IOException {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.SSL);
    while (!selector.connected().contains(connectionId)) {
      selector.poll(10000L);
    }
    Assert.assertTrue("Channel should have been ready by now ", selector.isChannelReady(connectionId));
  }

  @Test
  public void testCloseAfterConnectCall() throws IOException {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.SSL);
    selector.close(connectionId);
    selector.poll(0);
    Assert.assertTrue("Channel should have been added to disconnected list",
        selector.disconnected().contains(connectionId));
  }

  private String blockingRequest(String connectionId, String s) throws Exception {
    selector.poll(1000L, asList(SelectorTest.createSend(connectionId, s)));
    while (true) {
      selector.poll(1000L);
      for (NetworkReceive receive : selector.completedReceives()) {
        if (receive.getConnectionId() == connectionId) {
          return SelectorTest.asString(receive);
        }
      }
    }
  }

  /* connect and wait for the connection to complete */
  private String blockingSSLConnect() throws IOException {
    String connectionId =
        selector.connect(new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE, PortType.SSL);
    while (!selector.connected().contains(connectionId)) {
      selector.poll(10000L);
    }
    return connectionId;
  }
}

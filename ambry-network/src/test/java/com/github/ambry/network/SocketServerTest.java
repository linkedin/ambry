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
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class SocketServerTest {
  private static SSLFactory clientSSLFactory;
  private static SSLSocketFactory clientSSLSocketFactory;
  private static SSLConfig clientSSLConfig;
  private static SSLConfig serverSSLConfig;
  private SocketServer server = null;

  /**
   * Run only once for all tests
   */
  @BeforeClass
  public static void initializeTests() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    serverSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));
    clientSSLFactory = SSLFactory.getNewInstance(clientSSLConfig);
    SSLContext sslContext = clientSSLFactory.getSSLContext();
    clientSSLSocketFactory = sslContext.getSocketFactory();
  }

  public SocketServerTest() throws Exception {
    Properties props = new Properties();
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(config.port, PortType.PLAINTEXT));
    ports.add(new Port(config.port + 1000, PortType.SSL));
    server = new SocketServer(config, serverSSLConfig, new MetricRegistry(), ports);
    server.start();
  }

  @After
  public void cleanup() {
    server.shutdown();
  }

  @Test
  public void simpleRequest() throws IOException, InterruptedException {
    simpleRequest(new Port(server.getPort(), PortType.PLAINTEXT));
  }

  @Test
  public void simpleSSLRequest() throws IOException, InterruptedException {
    simpleRequest(new Port(server.getSSLPort(), PortType.SSL));
  }

  private void simpleRequest(Port targetPort) throws IOException, InterruptedException {
    byte[] bytesToSend = new byte[1028];
    new Random().nextBytes(bytesToSend);
    ByteBuffer byteBufferToSend = ByteBuffer.wrap(bytesToSend);
    byteBufferToSend.putLong(0, 1028);
    BoundedByteBufferSend bufferToSend = new BoundedByteBufferSend(byteBufferToSend);
    BlockingChannel channel = null;
    if (targetPort.getPortType() == PortType.SSL) {
      channel =
          new SSLBlockingChannel("localhost", targetPort.getPort(), new MetricRegistry(), 10000, 10000, 1000, 2000,
              clientSSLSocketFactory, clientSSLConfig);
    } else {
      channel = new BlockingChannel("localhost", targetPort.getPort(), 10000, 10000, 1000, 2000);
    }
    channel.connect();
    channel.send(bufferToSend);
    RequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
    NetworkRequest request = requestResponseChannel.receiveRequest();
    DataInputStream requestStream = new DataInputStream(request.getInputStream());
    byte[] outputBytes = new byte[1020];
    requestStream.readFully(outputBytes);
    for (int i = 0; i < 1020; i++) {
      Assert.assertEquals(bytesToSend[8 + i], outputBytes[i]);
    }

    // send response back and ensure response is received
    byte[] responseBytes = new byte[2048];
    new Random().nextBytes(responseBytes);
    ByteBuffer byteBufferToSendResponse = ByteBuffer.wrap(responseBytes);
    byteBufferToSendResponse.putLong(0, 2048);
    BoundedByteBufferSend responseToSend = new BoundedByteBufferSend(byteBufferToSendResponse);
    requestResponseChannel.sendResponse(responseToSend, request, null);
    InputStream streamResponse = channel.receive().getInputStream();
    byte[] responseBytesReceived = new byte[2040];
    streamResponse.read(responseBytesReceived);
    for (int i = 0; i < 2040; i++) {
      Assert.assertEquals(responseBytes[8 + i], responseBytesReceived[i]);
    }
    channel.disconnect();
  }

  /**
   * Choose a number of random available ports
   */
  ArrayList<Integer> choosePorts(int count) throws IOException {
    ArrayList<Integer> sockets = new ArrayList<Integer>();
    for (int i = 0; i < count; i++) {
      ServerSocket socket = new ServerSocket(0);
      sockets.add(socket.getLocalPort());
      socket.close();
    }
    return sockets;
  }

  /**
   * Choose an available port
   */
  public int choosePort() throws IOException {
    return choosePorts(1).get(0);
  }
}

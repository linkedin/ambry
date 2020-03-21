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
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


public class SSLBlockingChannelTest {
  private static SSLFactory sslFactory;
  private static SSLConfig clientSSLConfig;
  private static SSLSocketFactory sslSocketFactory;
  private static EchoServer sslEchoServer;
  private static String hostName = "localhost";
  private static int sslPort = 18284;

  /**
   * Run only once for all tests
   */
  @BeforeClass
  public static void initializeTests() throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig sslConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));

    sslFactory = SSLFactory.getNewInstance(sslConfig);
    sslEchoServer = new EchoServer(sslFactory, sslPort);
    sslEchoServer.start();

    //client
    sslFactory = SSLFactory.getNewInstance(clientSSLConfig);
    SSLContext sslContext = sslFactory.getSSLContext();
    sslSocketFactory = sslContext.getSocketFactory();
  }

  /**
   * Run only once for all tests
   */
  @AfterClass
  public static void finalizeTests() throws Exception {
    int serverExceptionCount = sslEchoServer.getExceptionCount();
    assertEquals(serverExceptionCount, 0);
    sslEchoServer.close();
  }

  @Before
  public void setup() throws Exception {
  }

  @After
  public void teardown() throws Exception {
  }

  @Test
  public void testSendAndReceive() throws Exception {
    BlockingChannel channel =
        new SSLBlockingChannel(hostName, sslPort, new MetricRegistry(), 10000, 10000, 10000, 2000, sslSocketFactory,
            clientSSLConfig);
    sendAndReceive(channel);
    channel.disconnect();
  }

  @Test
  public void testRenegotiation() throws Exception {
    BlockingChannel channel =
        new SSLBlockingChannel(hostName, sslPort, new MetricRegistry(), 10000, 10000, 10000, 2000, sslSocketFactory,
            clientSSLConfig);
    sendAndReceive(channel);
    sslEchoServer.renegotiate();
    sendAndReceive(channel);
    channel.disconnect();
  }

  @Test
  public void testWrongPortConnection() throws Exception {
    BlockingChannel channel =
        new SSLBlockingChannel(hostName, sslPort + 1, new MetricRegistry(), 10000, 10000, 10000, 2000, sslSocketFactory,
            clientSSLConfig);
    try {
      // send request
      channel.connect();
      fail("should have thrown!");
    } catch (IOException e) {
    }
  }

  private void sendAndReceive(BlockingChannel channel) throws Exception {
    long blobSize = 1028;
    byte[] bytesToSend = new byte[(int) blobSize];
    new Random().nextBytes(bytesToSend);
    ByteBuffer byteBufferToSend = ByteBuffer.wrap(bytesToSend);
    byteBufferToSend.putLong(0, blobSize);
    BoundedByteBufferSend bufferToSend = new BoundedByteBufferSend(byteBufferToSend);
    // send request
    channel.connect();
    channel.send(bufferToSend);
    // receive response
    InputStream streamResponse = channel.receive().getInputStream();
    DataInputStream input = new DataInputStream(streamResponse);
    byte[] bytesReceived = new byte[(int) blobSize - 8];
    input.readFully(bytesReceived);
    for (int i = 0; i < blobSize - 8; i++) {
      Assert.assertEquals(bytesToSend[8 + i], bytesReceived[i]);
    }
  }
}

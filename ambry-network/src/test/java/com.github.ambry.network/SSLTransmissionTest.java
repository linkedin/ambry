/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.junit.Test;


public class SSLTransmissionTest {
  private static final int DEFAULT_BUFFER_SIZE = 4 * 1024;
  private static final NetworkMetrics METRICS = new NetworkMetrics(new MetricRegistry());
  private final int applicationBufferSize;
  private final EchoServer server;
  private final InetSocketAddress serverAddress;
  private final SSLFactory clientSSLFactory;
  private final File trustStoreFile;

  public SSLTransmissionTest() throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig sslConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server"));
    SSLConfig clientSSLConfig =
        new SSLConfig(TestSSLUtils.createSslProps("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client"));
    SSLFactory serverSSLFactory = SSLFactory.getNewInstance(sslConfig);
    clientSSLFactory = SSLFactory.getNewInstance(clientSSLConfig);
    server = new EchoServer(serverSSLFactory, 18383);
    server.start();
    serverAddress = new InetSocketAddress("localhost", server.port);
    applicationBufferSize = clientSSLFactory.createSSLEngine("localhost", server.port, SSLFactory.Mode.CLIENT)
        .getSession()
        .getApplicationBufferSize();
  }

  @Test
  public void testBuffer() throws Exception {
    Selector nioSelector = Selector.open();
    SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(false);
    Socket socket = channel.socket();
    socket.setKeepAlive(true);
    socket.setSendBufferSize(applicationBufferSize);
    socket.setReceiveBufferSize(applicationBufferSize);
    socket.setTcpNoDelay(true);
    channel.connect(serverAddress);
    SelectionKey key = channel.register(nioSelector, SelectionKey.OP_CONNECT);
    Transmission transmission =
        new SSLTransmission(clientSSLFactory, "connectionId", channel, key, serverAddress.getHostName(),
            serverAddress.getPort(), SystemTime.getInstance(), METRICS, SSLFactory.Mode.CLIENT);
    transmission.setNetworkSend(SelectorTest.createSend("connectionId","abc"));
    boolean sendComplete = false;
    while (!sendComplete) {
      System.out.println("1a");
      if (key.isConnectable()) {
        System.out.println("1b");
        transmission.finishConnect();
      }
      /* if channel is not ready, finish prepare */
      if (transmission.isConnected() && !transmission.ready()) {
        System.out.println("1c");
        transmission.prepare();
      } else if (key.isReadable() && transmission.ready()) {
        System.out.println("1d");
        boolean readComplete = transmission.read();
        if (readComplete) {
          transmission.onReceiveComplete();
        }
      } else if (key.isWritable() && transmission.ready()) {
        System.out.println("1e");
        sendComplete = transmission.write();
        if (sendComplete) {
          transmission.onSendComplete();
          transmission.clearSend();
          key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        }
      } else if (!key.isValid()) {
        System.out.println("1f");
        transmission.close();
      }
    }
    System.out.println(SelectorTest.asString(transmission.getNetworkReceive()));
  }
}

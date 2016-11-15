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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.Channels;
import java.util.ArrayList;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;


/**
 * A blocking channel that is used to communicate with a server using SSL
 */
public class SSLBlockingChannel extends BlockingChannel {
  private SSLSocket sslSocket = null;
  private final SSLSocketFactory sslSocketFactory;
  private final SSLConfig sslConfig;
  public final Counter sslClientHandshakeErrorCount;
  public final Counter sslClientHandshakeCount;

  public SSLBlockingChannel(String host, int port, MetricRegistry registry, int readBufferSize, int writeBufferSize,
      int readTimeoutMs, int connectTimeoutMs, SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs);
    if (sslSocketFactory == null) {
      throw new IllegalArgumentException("sslSocketFactory is null when creating SSLBlockingChannel");
    }
    this.sslSocketFactory = sslSocketFactory;
    this.sslConfig = sslConfig;
    sslClientHandshakeErrorCount =
        registry.counter(MetricRegistry.name(SSLBlockingChannel.class, "SslClientHandshakeErrorCount"));
    sslClientHandshakeCount =
        registry.counter(MetricRegistry.name(SSLBlockingChannel.class, "SslClientHandshakeCount"));
  }

  @Override
  public void connect() throws IOException {
    synchronized (lock) {
      if (!connected) {
        Socket socket = new Socket();
        socket.setSoTimeout(readTimeoutMs);
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        if (readBufferSize > 0) {
          socket.setReceiveBufferSize(readBufferSize);
        }
        if (writeBufferSize > 0) {
          socket.setSendBufferSize(writeBufferSize);
        }
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        sslSocket = (SSLSocket) sslSocketFactory.createSocket(socket, host, port, true);

        ArrayList<String> protocolsList = Utils.splitString(sslConfig.sslEnabledProtocols, ",");
        if (protocolsList != null && protocolsList.size() > 0) {
          String[] enabledProtocols = protocolsList.toArray(new String[protocolsList.size()]);
          sslSocket.setEnabledProtocols(enabledProtocols);
        }

        ArrayList<String> cipherSuitesList = Utils.splitString(sslConfig.sslCipherSuites, ",");
        if (cipherSuitesList != null && cipherSuitesList.size() > 0 && !(cipherSuitesList.size() == 1
            && cipherSuitesList.get(0).equals(""))) {
          String[] cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
          sslSocket.setEnabledCipherSuites(cipherSuites);
        }

        // handshake in a blocking way
        try {
          sslSocket.startHandshake();
          sslClientHandshakeCount.inc();
        } catch (IOException e) {
          sslClientHandshakeErrorCount.inc();
          throw e;
        }
        writeChannel = Channels.newChannel(sslSocket.getOutputStream());
        readChannel = sslSocket.getInputStream();
        connected = true;
        logger.debug(
            "Created socket with SO_TIMEOUT = {} (requested {}), SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})",
            sslSocket.getSoTimeout(), readTimeoutMs, sslSocket.getReceiveBufferSize(), readBufferSize,
            sslSocket.getSendBufferSize(), writeBufferSize);
      }
    }
  }

  @Override
  public void disconnect() {
    synchronized (lock) {
      try {
        if (connected || sslSocket != null) {
          // closing the main socket channel *should* close the read channel
          // but let's do it to be sure.
          sslSocket.close();
          if (readChannel != null) {
            readChannel.close();
            readChannel = null;
          }
          if (writeChannel != null) {
            writeChannel.close();
            writeChannel = null;
          }
          sslSocket = null;
          connected = false;
        }
      } catch (Exception e) {
        logger.error("error while disconnecting {}", e);
      }
    }
  }
}

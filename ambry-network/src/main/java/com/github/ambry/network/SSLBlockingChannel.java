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
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;


/**
 * A blocking channel that is used to communicate with a server using SSL
 */
public class SSLBlockingChannel extends BlockingChannel {
  private final SSLSocketFactory sslSocketFactory;
  private final SSLConfig sslConfig;
  public final Counter sslClientHandshakeErrorCount;
  public final Counter sslClientHandshakeCount;

  public SSLBlockingChannel(String host, int port, MetricRegistry registry, int readBufferSize, int writeBufferSize,
      int readTimeoutMs, int connectTimeoutMs, boolean enableTcpNoDelay, SSLSocketFactory sslSocketFactory,
      SSLConfig sslConfig) {
    super(host, port, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs, enableTcpNoDelay);
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

  public SSLBlockingChannel(String host, int port, MetricRegistry registry, int readBufferSize, int writeBufferSize,
      int readTimeoutMs, int connectTimeoutMs, SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    this(host, port, registry, readBufferSize, writeBufferSize, readTimeoutMs, connectTimeoutMs, true, sslSocketFactory,
        sslConfig);
  }

  public SSLBlockingChannel(String host, int port, MetricRegistry registry, ConnectionPoolConfig config,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    this(host, port, registry, config.connectionPoolReadBufferSizeBytes, config.connectionPoolWriteBufferSizeBytes,
        config.connectionPoolReadTimeoutMs, config.connectionPoolConnectTimeoutMs,
        config.connectionPoolSocketEnableTcpNoDelay, sslSocketFactory, sslConfig);
  }

  /**
   * Calls BlockingChannel's createSocket() to create and connect a new TCP socket,
   * then wraps it inside an SSLSocket
   * @return Socket
   * @throws IOException
   */
  @Override
  public Socket createSocket() throws IOException {
    Socket tcpSocket = super.createSocket();

    SSLSocket sslSocket = null;

    try {
      sslSocket = (SSLSocket) sslSocketFactory.createSocket(tcpSocket, host, port, true);

      ArrayList<String> protocolsList = Utils.splitString(sslConfig.sslEnabledProtocols, ",");
      if (!protocolsList.isEmpty()) {
        String[] enabledProtocols = protocolsList.toArray(new String[protocolsList.size()]);
        sslSocket.setEnabledProtocols(enabledProtocols);
      }

      ArrayList<String> cipherSuitesList = Utils.splitString(sslConfig.sslCipherSuites, ",");
      if (!cipherSuitesList.isEmpty()) {
        String[] cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
        sslSocket.setEnabledCipherSuites(cipherSuites);
      }

      // handshake in a blocking way
      sslSocket.startHandshake();
      sslClientHandshakeCount.inc();
    } catch (IOException e) {
      sslClientHandshakeErrorCount.inc();
      tcpSocket.setSoLinger(true, 0);
      tcpSocket.close();
      throw e;
    }

    logger.debug(
        "Converted socket to SSL with enabled protocols {} and ciphers {}",
        sslSocket.getEnabledProtocols(),
        sslSocket.getEnabledCipherSuites()
    );

    return sslSocket;
  }
}

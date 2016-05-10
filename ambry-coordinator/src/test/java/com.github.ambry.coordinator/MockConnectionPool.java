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
package com.github.ambry.coordinator;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.io.IOException;


/**
 *
 */
public class MockConnectionPool implements ConnectionPool {
  private final int readBufferSizeBytes;
  private final int writeBufferSizeBytes;
  private final int readTimeoutMs;
  private final int connectTimeoutMs;

  // Need static instance of MockCluster so that all connection pools share common MockCluster.
  public static MockCluster mockCluster = null;

  public MockConnectionPool(ConnectionPoolConfig config) {
    this.readBufferSizeBytes = config.connectionPoolReadBufferSizeBytes;
    this.writeBufferSizeBytes = config.connectionPoolWriteBufferSizeBytes;
    this.readTimeoutMs = config.connectionPoolReadTimeoutMs;
    this.connectTimeoutMs = config.connectionPoolConnectTimeoutMs;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, Port port, long timeout)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException {

    BlockingChannel blockingChannel = null;
    if (port.getPortType() == PortType.PLAINTEXT) {
      blockingChannel =
          new MockBlockingChannel(mockCluster.getMockDataNode(host, port.getPort()), host, port.getPort(),
              readBufferSizeBytes, writeBufferSizeBytes, readTimeoutMs, connectTimeoutMs);
    } else {
      blockingChannel =
          new MockSSLBlockingChannel(mockCluster.getMockDataNode(host, port.getPort()), host, port.getPort(),
              readBufferSizeBytes, writeBufferSizeBytes, readTimeoutMs, connectTimeoutMs);
    }
    blockingChannel.connect();
    return blockingChannel;
  }

  @Override
  public void checkInConnection(ConnectedChannel connectedChannel) {
    destroyConnection(connectedChannel);
  }

  @Override
  public void destroyConnection(ConnectedChannel connectedChannel) {
    if (connectedChannel != null) {
      MockBlockingChannel channel = (MockBlockingChannel) connectedChannel;
      channel.disconnect();
    }
  }
}

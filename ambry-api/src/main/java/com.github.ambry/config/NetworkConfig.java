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
package com.github.ambry.config;

/**
 * The configs for network layer
 */
public class NetworkConfig {

  /**
   * The number of io threads that the server uses for carrying out network requests
   */
  @Config("num.io.threads")
  @Default("8")
  public final int numIoThreads;

  /**
   * The number of queued requests allowed before blocking the network threads
   */
  @Config("queued.max.requests")
  @Default("500")
  public final int queuedMaxRequests;

  /**
   * The port to listen and accept connections on
   */
  @Config("port")
  @Default("6667")
  public final int port;

  /**
   * Hostname of server. If this is set, it will only bind to this address. If this is not set,
   * it will bind to all interfaces, and publish one to ZK
   */
  @Config("host.name")
  @Default("localhost")
  public final String hostName;

  /**
   * The SO_SNDBUFF buffer of the socket sever sockets
   */
  @Config("socket.send.buffer.bytes")
  @Default("1048576")
  public final int socketSendBufferBytes;

  /**
   * The SO_RCVBUFF buffer of the socket sever sockets
   */
  @Config("socket.receive.buffer.bytes")
  @Default("1048576")
  public final int socketReceiveBufferBytes;

  /**
   * The maximum number of bytes in a socket request
   */
  @Config("socket.request.max.bytes")
  @Default("104857600")
  public final int socketRequestMaxBytes;

  /**
   * Whether the client should attempt to replenish connections when the number of connections to a host drops below
   * a minimum number of active connections.
   */
  @Config("network.client.enable.connection.replenishment")
  @Default("false")
  public final boolean networkClientEnableConnectionReplenishment;

  /**
   * The size of the pool if selector executor pool is employed. When size is 0, executor pool won't be used.
   */
  @Config("selector.executor.pool.size")
  @Default("4")
  public final int selectorExecutorPoolSize;

  /**
   * The max number of ready keys can be processed in a selector.poll() call. No limitation if -1 is used.
   */
  @Config("selector.max.key.to.process")
  @Default("-1")
  public final int selectorMaxKeyToProcess;

  public NetworkConfig(VerifiableProperties verifiableProperties) {

    numIoThreads = verifiableProperties.getIntInRange("num.io.threads", 8, 1, Integer.MAX_VALUE);
    port = verifiableProperties.getInt("port", 6667);
    hostName = verifiableProperties.getString("host.name", "localhost");
    socketSendBufferBytes = verifiableProperties.getInt("socket.send.buffer.bytes", 1 * 1024 * 1024);
    socketReceiveBufferBytes = verifiableProperties.getInt("socket.receive.buffer.bytes", 1 * 1024 * 1024);
    socketRequestMaxBytes =
        verifiableProperties.getIntInRange("socket.request.max.bytes", 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
    queuedMaxRequests = verifiableProperties.getIntInRange("queued.max.requests", 500, 1, Integer.MAX_VALUE);
    networkClientEnableConnectionReplenishment =
        verifiableProperties.getBoolean("network.client.enable.connection.replenishment", false);
    selectorExecutorPoolSize =
        verifiableProperties.getIntInRange("selector.executor.pool.size", 4, 0, Integer.MAX_VALUE);
    selectorMaxKeyToProcess =
        verifiableProperties.getIntInRange("selector.max.key.to.process", -1, -1, Integer.MAX_VALUE);
  }
}

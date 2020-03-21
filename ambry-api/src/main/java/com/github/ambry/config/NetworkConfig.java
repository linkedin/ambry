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

  public static final String NUM_IO_THREADS = "num.io.threads";
  public static final String QUEUED_MAX_REQUESTS = "queued.max.requests";
  public static final String PORT = "port";
  public static final String HOST_NAME = "host.name";
  public static final String SOCKET_SEND_BUFFER_BYTES = "socket.send.buffer.bytes";
  public static final String SOCKET_RECEIVE_BUFFER_BYTES = "socket.receive.buffer.bytes";
  public static final String SOCKET_REQUEST_MAX_BYTES = "socket.request.max.bytes";
  public static final String NETWORK_CLIENT_ENABLE_CONNECTION_REPLENISHMENT =
      "network.client.enable.connection.replenishment";
  public static final String NETWORK_CLIENT_MAX_REPLENISHMENT_PER_HOST_PER_SECOND =
      "network.client.max.replenishment.per.host.per.second";
  public static final String SELECTOR_EXECUTOR_POOL_SIZE = "selector.executor.pool.size";
  public static final String SELECTOR_MAX_KEY_TO_PROCESS = "selector.max.key.to.process";
  public static final String SELECTOR_USE_DIRECT_BUFFERS = "selector.use.direct.buffers";

  /**
   * The number of io threads that the server uses for carrying out network requests
   */
  @Config(NUM_IO_THREADS)
  @Default("8")
  public final int numIoThreads;

  /**
   * The number of queued requests allowed before blocking the network threads
   */
  @Config(QUEUED_MAX_REQUESTS)
  @Default("500")
  public final int queuedMaxRequests;

  /**
   * The port to listen and accept connections on
   */
  @Config(PORT)
  @Default("6667")
  public final int port;

  /**
   * Hostname of server. If this is set, it will only bind to this address. If this is not set,
   * it will bind to all interfaces, and publish one to ZK
   */
  @Config(HOST_NAME)
  @Default("localhost")
  public final String hostName;

  /**
   * The SO_SNDBUFF buffer of the socket sever sockets
   */
  @Config(SOCKET_SEND_BUFFER_BYTES)
  @Default("1048576")
  public final int socketSendBufferBytes;

  /**
   * The SO_RCVBUFF buffer of the socket sever sockets
   */
  @Config(SOCKET_RECEIVE_BUFFER_BYTES)
  @Default("1048576")
  public final int socketReceiveBufferBytes;

  /**
   * The maximum number of bytes in a socket request
   */
  @Config(SOCKET_REQUEST_MAX_BYTES)
  @Default("104857600")
  public final int socketRequestMaxBytes;

  /**
   * Whether the client should attempt to replenish connections when the number of connections to a host drops below
   * a minimum number of active connections.
   */
  @Config(NETWORK_CLIENT_ENABLE_CONNECTION_REPLENISHMENT)
  @Default("false")
  public final boolean networkClientEnableConnectionReplenishment;

  /**
   * The max number of new connections to a remote host that should be created per second when replenishing connections.
   */
  @Config(NETWORK_CLIENT_MAX_REPLENISHMENT_PER_HOST_PER_SECOND)
  @Default("1")
  public final int networkClientMaxReplenishmentPerHostPerSecond;

  /**
   * The size of the pool if selector executor pool is employed. When size is 0, executor pool won't be used.
   */
  @Config(SELECTOR_EXECUTOR_POOL_SIZE)
  @Default("4")
  public final int selectorExecutorPoolSize;

  /**
   * The max number of ready keys can be processed in a selector.poll() call. No limitation if -1 is used.
   */
  @Config(SELECTOR_MAX_KEY_TO_PROCESS)
  @Default("-1")
  public final int selectorMaxKeyToProcess;

  /**
   * True to allocate direct buffers within the selector (for things like SSL work).
   */
  @Config(SELECTOR_USE_DIRECT_BUFFERS)
  @Default("false")
  public final boolean selectorUseDirectBuffers;

  public NetworkConfig(VerifiableProperties verifiableProperties) {
    numIoThreads = verifiableProperties.getIntInRange(NUM_IO_THREADS, 8, 1, Integer.MAX_VALUE);
    queuedMaxRequests = verifiableProperties.getIntInRange(QUEUED_MAX_REQUESTS, 500, 1, Integer.MAX_VALUE);
    port = verifiableProperties.getInt(PORT, 6667);
    hostName = verifiableProperties.getString(HOST_NAME, "localhost");
    socketSendBufferBytes = verifiableProperties.getInt(SOCKET_SEND_BUFFER_BYTES, 1 * 1024 * 1024);
    socketReceiveBufferBytes = verifiableProperties.getInt(SOCKET_RECEIVE_BUFFER_BYTES, 1 * 1024 * 1024);
    socketRequestMaxBytes =
        verifiableProperties.getIntInRange(SOCKET_REQUEST_MAX_BYTES, 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
    networkClientEnableConnectionReplenishment =
        verifiableProperties.getBoolean(NETWORK_CLIENT_ENABLE_CONNECTION_REPLENISHMENT, false);
    networkClientMaxReplenishmentPerHostPerSecond =
        verifiableProperties.getIntInRange(NETWORK_CLIENT_MAX_REPLENISHMENT_PER_HOST_PER_SECOND, 1, 1,
            Integer.MAX_VALUE);
    selectorExecutorPoolSize = verifiableProperties.getIntInRange(SELECTOR_EXECUTOR_POOL_SIZE, 4, 0, Integer.MAX_VALUE);
    selectorMaxKeyToProcess =
        verifiableProperties.getIntInRange(SELECTOR_MAX_KEY_TO_PROCESS, -1, -1, Integer.MAX_VALUE);
    selectorUseDirectBuffers = verifiableProperties.getBoolean(SELECTOR_USE_DIRECT_BUFFERS, false);
  }
}

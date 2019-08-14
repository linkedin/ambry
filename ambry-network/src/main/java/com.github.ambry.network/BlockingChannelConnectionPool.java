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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A connection pool that uses BlockingChannel as the underlying connection.
 * It is responsible for all the connection management. It helps to
 * checkout a new connection, checkin an existing connection that has been
 * checked out and destroy a connection in the case of an error
 */
public final class BlockingChannelConnectionPool implements ConnectionPool {
  private static final Logger logger = LoggerFactory.getLogger(BlockingChannelConnectionPool.class);
  private final Map<String, BlockingChannelInfo> connections;
  private final ConnectionPoolConfig config;
  private final MetricRegistry registry;
  private final Timer connectionCheckOutTime;
  private final Timer connectionCheckInTime;
  private final Timer connectionDestroyTime;
  private final AtomicInteger requestsWaitingToCheckoutConnectionCount;
  private SSLSocketFactory sslSocketFactory;
  private final SSLConfig sslConfig;
  // Represents the total number to nodes connectedTo, i.e. if the blockingchannel has atleast 1 connection
  private Gauge<Integer> totalNumberOfNodesConnectedTo;
  // Represents the total number of connections, in other words, aggregate of the connections from all nodes
  public Gauge<Integer> totalNumberOfConnections;
  // Represents the number of requests waiting to checkout a connection
  public Gauge<Integer> requestsWaitingToCheckoutConnection;
  // Represents the number of sslSocketFactory Initializations by client
  public Counter sslSocketFactoryClientInitializationCount;
  // Represents the number of sslSocketFactory Initialization Error by client
  public Counter sslSocketFactoryClientInitializationErrorCount;

  public BlockingChannelConnectionPool(ConnectionPoolConfig config, SSLConfig sslConfig,
      ClusterMapConfig clusterMapConfig, MetricRegistry registry) throws Exception {
    connections = new ConcurrentHashMap<String, BlockingChannelInfo>();
    this.config = config;
    this.registry = registry;
    this.sslConfig = sslConfig;
    connectionCheckOutTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionCheckOutTime"));
    connectionCheckInTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionCheckInTime"));
    connectionDestroyTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionDestroyTime"));

    totalNumberOfNodesConnectedTo = () -> {
      int noOfNodesConnectedTo = 0;
      for (BlockingChannelInfo blockingChannelInfo : connections.values()) {
        if (blockingChannelInfo.getNumberOfConnections() > 0) {
          noOfNodesConnectedTo++;
        }
      }
      return noOfNodesConnectedTo;
    };
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "totalNumberOfNodesConnectedTo"),
        totalNumberOfNodesConnectedTo);

    totalNumberOfConnections = () -> {
      int noOfConnections = 0;
      for (BlockingChannelInfo blockingChannelInfo : connections.values()) {
        noOfConnections += blockingChannelInfo.getNumberOfConnections();
      }
      return noOfConnections;
    };
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "totalNumberOfConnections"),
        totalNumberOfConnections);
    requestsWaitingToCheckoutConnectionCount = new AtomicInteger(0);
    requestsWaitingToCheckoutConnection = requestsWaitingToCheckoutConnectionCount::get;
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "requestsWaitingToCheckoutConnection"),
        requestsWaitingToCheckoutConnection);
    sslSocketFactoryClientInitializationCount = registry.counter(
        MetricRegistry.name(BlockingChannelConnectionPool.class, "SslSocketFactoryClientInitializationCount"));
    sslSocketFactoryClientInitializationErrorCount = registry.counter(
        MetricRegistry.name(BlockingChannelConnectionPool.class, "SslSocketFactoryClientInitializationErrorCount"));

    if (clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0) {
      initializeSSLSocketFactory();
    } else {
      this.sslSocketFactory = null;
    }
  }

  @Override
  public void start() {
    logger.info("BlockingChannelConnectionPool started");
  }

  @Override
  public void shutdown() {
    logger.info("Shutting down the BlockingChannelConnectionPool");
    for (Map.Entry<String, BlockingChannelInfo> channels : connections.entrySet()) {
      channels.getValue().cleanup();
    }
  }

  private void initializeSSLSocketFactory() throws Exception {
    try {
      SSLFactory sslFactory = SSLFactory.getNewInstance(sslConfig);
      SSLContext sslContext = sslFactory.getSSLContext();
      this.sslSocketFactory = sslContext.getSocketFactory();
      this.sslSocketFactoryClientInitializationCount.inc();
    } catch (Exception e) {
      this.sslSocketFactoryClientInitializationErrorCount.inc();
      logger.error("SSLSocketFactory Client Initialization Error ", e);
      throw e;
    }
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, Port port, long timeoutInMs)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException {
    final Timer.Context context = connectionCheckOutTime.time();
    try {
      requestsWaitingToCheckoutConnectionCount.incrementAndGet();
      BlockingChannelInfo blockingChannelInfo = connections.get(host + port.getPort());
      if (blockingChannelInfo == null) {
        synchronized (this) {
          blockingChannelInfo = connections.get(host + port.getPort());
          if (blockingChannelInfo == null) {
            logger.trace("Creating new blocking channel info for host {} and port {}", host, port.getPort());
            blockingChannelInfo = new BlockingChannelInfo(config, host, port, registry, sslSocketFactory, sslConfig);
            connections.put(host + port.getPort(), blockingChannelInfo);
          } else {
            logger.trace("Using already existing BlockingChannelInfo for " + host + ":" + port.getPort()
                + " in synchronized block");
          }
        }
      } else {
        logger.trace("Using already existing BlockingChannelInfo for " + host + ":" + port.getPort());
      }
      return blockingChannelInfo.getBlockingChannel(timeoutInMs);
    } finally {
      requestsWaitingToCheckoutConnectionCount.decrementAndGet();
      context.stop();
    }
  }

  @Override
  public void checkInConnection(ConnectedChannel connectedChannel) {
    final Timer.Context context = connectionCheckInTime.time();
    try {
      BlockingChannelInfo blockingChannelInfo =
          connections.get(connectedChannel.getRemoteHost() + connectedChannel.getRemotePort());
      if (blockingChannelInfo == null) {
        logger.error("Unexpected state in connection pool. Host {} and port {} not found to checkin connection",
            connectedChannel.getRemoteHost(), connectedChannel.getRemotePort());
        throw new IllegalArgumentException("Connection does not belong to the pool");
      }
      blockingChannelInfo.releaseBlockingChannel((BlockingChannel) connectedChannel);
      logger.trace("Checking in connection for host {} and port {}", connectedChannel.getRemoteHost(),
          connectedChannel.getRemotePort());
    } finally {
      context.stop();
    }
  }

  @Override
  public void destroyConnection(ConnectedChannel connectedChannel) {
    final Timer.Context context = connectionDestroyTime.time();
    try {
      BlockingChannelInfo blockingChannelInfo =
          connections.get(connectedChannel.getRemoteHost() + connectedChannel.getRemotePort());
      if (blockingChannelInfo == null) {
        logger.error("Unexpected state in connection pool. Host {} and port {} not found to checkin connection",
            connectedChannel.getRemoteHost(), connectedChannel.getRemotePort());
        throw new IllegalArgumentException("Connection does not belong to the pool");
      }
      blockingChannelInfo.destroyBlockingChannel((BlockingChannel) connectedChannel);
      logger.trace("Destroying connection for host {} and port {}", connectedChannel.getRemoteHost(),
          connectedChannel.getRemotePort());
    } finally {
      context.stop();
    }
  }
}

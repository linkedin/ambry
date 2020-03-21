/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class BlockingChannelInfo {
  private static final Logger logger = LoggerFactory.getLogger(BlockingChannelInfo.class);
  private final ArrayBlockingQueue<BlockingChannel> blockingChannelAvailableConnections;
  private final ArrayBlockingQueue<BlockingChannel> blockingChannelActiveConnections;
  private final AtomicInteger numberOfConnections;
  private final ConnectionPoolConfig config;
  private final ReadWriteLock rwlock;
  private final Object lock;
  private final String host;
  private final Port port;
  protected Gauge<Integer> availableConnections;
  private Gauge<Integer> activeConnections;
  private Gauge<Integer> totalNumberOfConnections;
  private int maxConnectionsPerHostPerPort;
  private final SSLSocketFactory sslSocketFactory;
  private final SSLConfig sslConfig;
  private final MetricRegistry registry;

  BlockingChannelInfo(ConnectionPoolConfig config, String host, Port port, MetricRegistry registry,
      SSLSocketFactory sslSocketFactory, SSLConfig sslConfig) {
    this.config = config;
    this.port = port;
    this.registry = registry;
    if (port.getPortType() == PortType.SSL) {
      maxConnectionsPerHostPerPort = config.connectionPoolMaxConnectionsPerPortSSL;
    } else {
      maxConnectionsPerHostPerPort = config.connectionPoolMaxConnectionsPerPortPlainText;
    }
    this.blockingChannelAvailableConnections = new ArrayBlockingQueue<BlockingChannel>(maxConnectionsPerHostPerPort);
    this.blockingChannelActiveConnections = new ArrayBlockingQueue<BlockingChannel>(maxConnectionsPerHostPerPort);
    this.numberOfConnections = new AtomicInteger(0);
    this.rwlock = new ReentrantReadWriteLock();
    this.lock = new Object();
    this.host = host;
    this.sslSocketFactory = sslSocketFactory;
    this.sslConfig = sslConfig;

    availableConnections = blockingChannelAvailableConnections::size;
    registry.register(
        MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port.getPort() + "-availableConnections"),
        availableConnections);

    activeConnections = blockingChannelActiveConnections::size;
    registry.register(
        MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port.getPort() + "-activeConnections"),
        activeConnections);

    totalNumberOfConnections = numberOfConnections::intValue;
    registry.register(
        MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port.getPort() + "-totalNumberOfConnections"),
        totalNumberOfConnections);

    logger.info("Starting blocking channel info for host {} and port {}", host, port.getPort());
  }

  void releaseBlockingChannel(BlockingChannel blockingChannel) {
    rwlock.readLock().lock();
    try {
      if (blockingChannelActiveConnections.remove(blockingChannel)) {
        blockingChannelAvailableConnections.add(blockingChannel);
        logger.trace(
            "Adding connection to {}:{} back to pool. Current available connections {} Current active connections {}",
            blockingChannel.getRemoteHost(), blockingChannel.getRemotePort(),
            blockingChannelAvailableConnections.size(), blockingChannelActiveConnections.size());
      } else {
        logger.error("Tried to add invalid connection. Channel does not belong in the active queue. Host {} port {}"
                + " channel host {} channel port {}", host, port.getPort(), blockingChannel.getRemoteHost(),
            blockingChannel.getRemotePort());
      }
    } finally {
      rwlock.readLock().unlock();
    }
  }

  BlockingChannel getBlockingChannel(long timeoutInMs) throws InterruptedException, ConnectionPoolTimeoutException {
    rwlock.readLock().lock();
    try {
      // check if the max connections for this queue has reached or if there are any connections available
      // in the available queue. The check in available queue is approximate and it could not have any
      // connections when polled. In this case we just depend on an existing connection being placed back in
      // the available pool
      if (numberOfConnections.get() == maxConnectionsPerHostPerPort || blockingChannelAvailableConnections.size() > 0) {
        BlockingChannel channel = blockingChannelAvailableConnections.poll(timeoutInMs, TimeUnit.MILLISECONDS);
        if (channel != null) {
          blockingChannelActiveConnections.add(channel);
          logger.trace("Returning connection to " + channel.getRemoteHost() + ":" + channel.getRemotePort());
          return channel;
        } else if (numberOfConnections.get() == maxConnectionsPerHostPerPort) {
          logger.error("Timed out trying to get a connection for host {} and port {}", host, port.getPort());
          throw new ConnectionPoolTimeoutException(
              "Could not get a connection to host " + host + " and port " + port.getPort());
        }
      }
      synchronized (lock) {
        // if the number of connections created for this host and port is less than the max allowed
        // connections, we create a new one and add it to the available queue
        if (numberOfConnections.get() < maxConnectionsPerHostPerPort) {
          logger.trace("Planning to create a new connection for host {} and port {} ", host, port.getPort());
          BlockingChannel channel = getBlockingChannelBasedOnPortType(host, port.getPort());
          channel.connect();
          numberOfConnections.incrementAndGet();
          logger.trace("Created a new connection for host {} and port {}. Number of connections {}", host, port,
              numberOfConnections.get());
          blockingChannelActiveConnections.add(channel);
          return channel;
        }
      }
      BlockingChannel channel = blockingChannelAvailableConnections.poll(timeoutInMs, TimeUnit.MILLISECONDS);
      if (channel == null) {
        logger.error("Timed out trying to get a connection for host {} and port {}", host, port);
        throw new ConnectionPoolTimeoutException(
            "Could not get a connection to host " + host + " and port " + port.getPort());
      }
      blockingChannelActiveConnections.add(channel);
      return channel;
    } catch (SocketException e) {
      logger.error("Socket exception when trying to connect to remote host {} and port {}", host, port.getPort());
      throw new ConnectionPoolTimeoutException(
          "Socket exception when trying to connect to remote host " + host + " port " + port.getPort(), e);
    } catch (IOException e) {
      logger.error("IOException when trying to connect to the remote host {} and port {}", host, port.getPort());
      throw new ConnectionPoolTimeoutException(
          "IOException when trying to connect to remote host " + host + " port " + port.getPort(), e);
    } finally {
      rwlock.readLock().unlock();
    }
  }

  /**
   * Returns BlockingChannel or SSLBlockingChannel depending on whether the port type is PlainText or SSL
   * @param host upon which connection has to be established
   * @param port upon which connection has to be established
   * @return BlockingChannel
   */
  private BlockingChannel getBlockingChannelBasedOnPortType(String host, int port) {
    BlockingChannel channel = null;
    if (this.port.getPortType() == PortType.PLAINTEXT) {
      channel = new BlockingChannel(host, port, config);
    } else if (this.port.getPortType() == PortType.SSL) {
      channel = new SSLBlockingChannel(host, port, registry, config, sslSocketFactory, sslConfig);
    }
    return channel;
  }

  void destroyBlockingChannel(BlockingChannel blockingChannel) {
    rwlock.readLock().lock();
    try {
      boolean changed = blockingChannelActiveConnections.remove(blockingChannel);
      if (!changed) {
        logger.error("Invalid connection being destroyed. "
                + "Channel does not belong to this queue. queue host {} port {} channel host {} port {}", host,
            port.getPort(), blockingChannel.getRemoteHost(), blockingChannel.getRemotePort());
        throw new IllegalArgumentException("Invalid connection. Channel does not belong to this queue");
      }
      blockingChannel.disconnect();
      // we ensure we maintain the current count of connections to the host to avoid synchronization across threads
      // to create the connection
      BlockingChannel channel =
          getBlockingChannelBasedOnPortType(blockingChannel.getRemoteHost(), blockingChannel.getRemotePort());
      channel.connect();
      logger.trace("Destroying connection and adding new connection for host {} port {}", host, port.getPort());
      blockingChannelAvailableConnections.add(channel);
    } catch (Exception e) {
      logger.error("Connection failure to remote host {} and port {} when destroying and recreating the connection",
          host, port.getPort());
      synchronized (lock) {
        // decrement the number of connections to the host and port. we were not able to maintain the count
        numberOfConnections.decrementAndGet();
        // at this point we are good to clean up the available connections since re-creation failed
        do {
          BlockingChannel channel = blockingChannelAvailableConnections.poll();
          if (channel == null) {
            break;
          }
          if (config.connectionPoolSocketResetOnError) {
            channel.reset();
          } else {
            channel.disconnect();
          }
          numberOfConnections.decrementAndGet();
        } while (true);
      }
    } finally {
      rwlock.readLock().unlock();
    }
  }

  /**
   * @return the number of connections with this BlockingChannelInfo
   */
  int getNumberOfConnections() {
    return this.numberOfConnections.intValue();
  }

  void cleanup() {
    rwlock.writeLock().lock();
    logger.info("Cleaning all active and available connections for host {} and port {}", host, port.getPort());
    try {
      for (BlockingChannel channel : blockingChannelActiveConnections) {
        channel.disconnect();
      }
      blockingChannelActiveConnections.clear();
      for (BlockingChannel channel : blockingChannelAvailableConnections) {
        channel.disconnect();
      }
      blockingChannelAvailableConnections.clear();
      numberOfConnections.set(0);
      logger.info("Cleaning completed for all active and available connections for host {} and port {}", host,
          port.getPort());
    } finally {
      rwlock.writeLock().unlock();
    }
  }
}

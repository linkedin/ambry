package com.github.ambry.network;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.config.ConnectionPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class BlockingChannelInfo {
  private final ArrayBlockingQueue<BlockingChannel> blockingChannelAvailableConnections;
  private final ArrayBlockingQueue<BlockingChannel> blockingChannelActiveConnections;
  private final AtomicInteger numberOfConnections;
  private final ConnectionPoolConfig config;
  private final ReadWriteLock rwlock;
  private final Object lock;
  private final String host;
  private final int port;
  private final PortType portType;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private Gauge<Integer> availableConnections;
  private Gauge<Integer> activeConnections;
  private Gauge<Integer> totalNumberOfConnections;

  public BlockingChannelInfo(ConnectionPoolConfig config, String host, int port, MetricRegistry registry,
      PortType portType) {
    this.config = config;

    this.portType = portType;
    int blockingQueueSize;
    if (portType == PortType.PLAINTEXT) {
      blockingQueueSize = config.connectionPoolMaxConnectionsPerPortPlainText;
    } else if (portType == PortType.SSL) {
      blockingQueueSize = config.connectionPoolMaxConnectionsPerPortSSL;
    } else {
      blockingQueueSize = config.connectionPoolMaxConnectionsPerHost;
    }
    this.blockingChannelAvailableConnections = new ArrayBlockingQueue<BlockingChannel>(blockingQueueSize);
    this.blockingChannelActiveConnections = new ArrayBlockingQueue<BlockingChannel>(blockingQueueSize);
    this.numberOfConnections = new AtomicInteger(0);
    this.rwlock = new ReentrantReadWriteLock();
    this.lock = new Object();
    this.host = host;
    this.port = port;

    availableConnections = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return blockingChannelAvailableConnections.size();
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port + "-availableConnections"),
        availableConnections);

    activeConnections = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return blockingChannelActiveConnections.size();
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port + "-activeConnections"),
        activeConnections);

    totalNumberOfConnections = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return numberOfConnections.intValue();
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelInfo.class, host + "-" + port + "-totalNumberOfConnections"),
        totalNumberOfConnections);

    logger.info("Starting blocking channel info for host {} and port {}", host, port);
  }

  public void addBlockingChannel(BlockingChannel blockingChannel) {
    rwlock.readLock().lock();
    try {
      blockingChannelActiveConnections.remove(blockingChannel);
      blockingChannelAvailableConnections.add(blockingChannel);
      logger.trace(
          "Adding connection to {}:{} back to pool. Current available connections {} Current active connections {}",
          blockingChannel.getRemoteHost(), blockingChannel.getRemotePort(), blockingChannelAvailableConnections.size(),
          blockingChannelActiveConnections.size());
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public BlockingChannel getBlockingChannel(long timeoutInMs)
      throws InterruptedException, ConnectionPoolTimeoutException {
    rwlock.readLock().lock();
    try {
      // check if the max connections for this queue has reached or if there are any connections available
      // in the available queue. The check in available queue is approximate and it could not have any
      // connections when polled. In this case we just depend on an existing connection being placed back in
      // the available pool
      if (numberOfConnections.get() == config.connectionPoolMaxConnectionsPerHost
          || blockingChannelAvailableConnections.size() > 0) {
        BlockingChannel channel = blockingChannelAvailableConnections.poll(timeoutInMs, TimeUnit.MILLISECONDS);
        if (channel == null) {
          logger.error("Timed out trying to get a connection for host {} and port {}", host, port);
          throw new ConnectionPoolTimeoutException("Could not get a connection to host " + host + " and port " + port);
        }
        blockingChannelActiveConnections.add(channel);
        logger.trace("Returning connection to " + channel.getRemoteHost() + ":" + channel.getRemotePort());
        return channel;
      }
      synchronized (lock) {
        // if the number of connections created for this host and port is less than the max allowed
        // connections, we create a new one and add it to the available queue
        if (numberOfConnections.get() < config.connectionPoolMaxConnectionsPerHost) {
          logger.trace("Planning to create a new connection for host {} and port {} ", host, port);
          // will create SSLBlockingChannel incase portType is SSL
          BlockingChannel channel = new BlockingChannel(host, port, config.connectionPoolReadBufferSizeBytes,
              config.connectionPoolWriteBufferSizeBytes, config.connectionPoolReadTimeoutMs,
              config.connectionPoolConnectTimeoutMs);
          channel.connect();
          blockingChannelAvailableConnections.add(channel);
          numberOfConnections.incrementAndGet();
          logger.trace("Created a new connection for host {} and port {}. Number of connections {}", host, port,
              numberOfConnections.get());
        }
      }
      BlockingChannel channel = blockingChannelAvailableConnections.poll(timeoutInMs, TimeUnit.MILLISECONDS);
      if (channel == null) {
        logger.error("Timed out trying to get a connection for host {} and port {}", host, port);
        throw new ConnectionPoolTimeoutException("Could not get a connection to host " + host + " and port " + port);
      }
      blockingChannelActiveConnections.add(channel);
      return channel;
    } catch (SocketException e) {
      logger.error("Socket exception when trying to connect to remote host {} and port {}", host, port);
      throw new ConnectionPoolTimeoutException(
          "Socket exception when trying to connect to remote host " + host + " port " + port, e);
    } catch (IOException e) {
      logger.error("IOException when trying to connect to the remote host {} and port {}", host, port);
      throw new ConnectionPoolTimeoutException(
          "IOException when trying to connect to remote host " + host + " port " + port, e);
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public void destroyBlockingChannel(BlockingChannel blockingChannel) {
    rwlock.readLock().lock();
    try {
      boolean changed = blockingChannelActiveConnections.remove(blockingChannel);
      if (!changed) {
        logger.error("Invalid connection being destroyed. "
            + "Channel does not belong to this queue. queue host {} port {} channel host {} port {}", host, port,
            blockingChannel.getRemoteHost(), blockingChannel.getRemotePort());
        throw new IllegalArgumentException("Invalid connection. Channel does not belong to this queue");
      }
      blockingChannel.disconnect();
      // we ensure we maintain the current count of connections to the host to avoid synchronization across threads
      // to create the connection
      // will create SSLBlockingChannel incase the portType is SSL
      BlockingChannel channel = new BlockingChannel(blockingChannel.getRemoteHost(), blockingChannel.getRemotePort(),
          config.connectionPoolReadBufferSizeBytes, config.connectionPoolWriteBufferSizeBytes,
          config.connectionPoolReadTimeoutMs, config.connectionPoolConnectTimeoutMs);
      channel.connect();
      logger.trace("Destroying connection and adding new connection for host {} port {}", host, port);
      blockingChannelAvailableConnections.add(channel);
    } catch (Exception e) {
      logger
          .error("Connection failure to remote host {} and port {} when destroying and recreating the connection", host,
              port);
      synchronized (lock) {
        // decrement the number of connections to the host and port. we were not able to maintain the count
        numberOfConnections.decrementAndGet();
      }
    } finally {
      rwlock.readLock().unlock();
    }
  }

  /**
   * Returns the number of connections with this BlockingChannelInfo
   * @return
   */
  public int getNumberOfConnections() {
    return this.numberOfConnections.intValue();
  }

  public void cleanup() {
    rwlock.writeLock().lock();
    logger.info("Cleaning all active and available connections for host {} and port {}", host, port);
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
      logger.info("Cleaning completed for all active and available connections for host {} and port {}", host, port);
    } finally {
      rwlock.writeLock().unlock();
    }
  }
}

/**
 * A connection pool that uses BlockingChannel as the underlying connection.
 * It is responsible for all the connection management. It helps to
 * checkout a new connection, checkin an existing connection that has been
 * checked out and destroy a connection in the case of an error
 */
public final class BlockingChannelConnectionPool implements ConnectionPool {

  private final Map<String, BlockingChannelInfo> connections;
  private final ConnectionPoolConfig config;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final MetricRegistry registry;
  private final Timer connectionCheckOutTime;
  private final Timer connectionCheckInTime;
  private final Timer connectionDestroyTime;
  private final AtomicInteger requestsWaitingToCheckoutConnectionCount;
  // Represents the total number to nodes connectedTo, i.e. if the blockingchannel has atleast 1 connection
  private Gauge<Integer> totalNumberOfNodesConnectedTo;
  // Represents the total number of connections, in other words, aggregate of the connections from all nodes
  public Gauge<Integer> totalNumberOfConnections;
  // Represents the number of requests waiting to checkout a connection
  public Gauge<Integer> requestsWaitingToCheckoutConnection;

  public BlockingChannelConnectionPool(ConnectionPoolConfig config, MetricRegistry registry) {
    connections = new ConcurrentHashMap<String, BlockingChannelInfo>();
    this.config = config;
    this.registry = registry;
    connectionCheckOutTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionCheckOutTime"));
    connectionCheckInTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionCheckInTime"));
    connectionDestroyTime =
        registry.timer(MetricRegistry.name(BlockingChannelConnectionPool.class, "connectionDestroyTime"));

    totalNumberOfNodesConnectedTo = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        int noOfNodesConnectedTo = 0;
        for (BlockingChannelInfo blockingChannelInfo : connections.values()) {
          if (blockingChannelInfo.getNumberOfConnections() > 0) {
            noOfNodesConnectedTo++;
          }
        }
        return noOfNodesConnectedTo;
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "totalNumberOfNodesConnectedTo"),
        totalNumberOfNodesConnectedTo);

    totalNumberOfConnections = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        int noOfConnections = 0;
        for (BlockingChannelInfo blockingChannelInfo : connections.values()) {
          noOfConnections += blockingChannelInfo.getNumberOfConnections();
        }
        return noOfConnections;
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "totalNumberOfConnections"),
        totalNumberOfConnections);
    requestsWaitingToCheckoutConnectionCount = new AtomicInteger(0);
    requestsWaitingToCheckoutConnection = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return requestsWaitingToCheckoutConnectionCount.get();
      }
    };
    registry.register(MetricRegistry.name(BlockingChannelConnectionPool.class, "requestsWaitingToCheckoutConnection"),
        requestsWaitingToCheckoutConnection);
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

  @Override
  public ConnectedChannel checkOutConnection(String host, int port, PortType portType, long timeoutInMs)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException {
    final Timer.Context context = connectionCheckOutTime.time();
    try {
      requestsWaitingToCheckoutConnectionCount.incrementAndGet();
      BlockingChannelInfo blockingChannelInfo = connections.get(host + port);
      if (blockingChannelInfo == null) {
        synchronized (this) {
          blockingChannelInfo = connections.get(host + port);
          if (blockingChannelInfo == null) {
            logger.trace("Creating new blocking channel info for host {} and port {}", host, port);
            blockingChannelInfo = new BlockingChannelInfo(config, host, port, registry, portType);
            connections.put(host + port, blockingChannelInfo);
          } else {
            logger.trace(
                "Using already existing BlockingChannelInfo for " + host + ":" + port + " in synchronized block");
          }
        }
      } else {
        logger.trace("Using already existing BlockingChannelInfo for " + host + ":" + port);
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
      blockingChannelInfo.addBlockingChannel((BlockingChannel) connectedChannel);
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
    } finally {
      context.stop();
    }
  }
}

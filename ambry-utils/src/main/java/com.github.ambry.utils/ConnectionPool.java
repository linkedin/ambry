package com.github.ambry.utils;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.shared.BlockingChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// TODO: Should this connection pool be in shared? It pools BlockingChannel which are in shared.

// TODO: Clean up connection pool:
// * Decide on better names. BlockingChannel for interface and Connection for implementation is not intuitive.
// * Add bound on connection #s to each DataNode
// * Remove dependency on "cluster map objects" (e.g., DataNodeId)
// * Add connection lifecycle management? E.g., destroy a connection that has been checked out for minutes or build up
//   and shrink number of established connections based on queue depth waiting to check out connections.

/**
 * Pools connections to DataNodes. This permits connections to be re-used thus avoiding connection establishment. Blocks
 * on connection establishment to a specific node. This throttles connection establishments outstanding to one. Upon
 * shutdown, checked in connections are closed but checked out connections are leaked. There are no limits on number of
 * connections that can be established...
 */
public class ConnectionPool implements BlockingChannelPool {
  private final int readBufferSizeBytes;
  private final int writeBufferSizeBytes;
  private final int readTimeoutMs;

  private ConcurrentMap<DataNodeId, DataNodePool> dataNodePools;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public class DataNodePool {
    private DataNodeId dataNodeId;
    private Queue<BlockingChannel> openBlockingChannels;

    DataNodePool(DataNodeId dataNodeId) {
      this.dataNodeId = dataNodeId;
      this.openBlockingChannels = new LinkedList<BlockingChannel>();
    }

    public synchronized BlockingChannel checkout() throws IOException {
      if (openBlockingChannels.isEmpty()) {
        logger.debug("Establish new connection to {} started.", dataNodeId);
        BlockingChannel blockingChannel = new BlockingChannel(dataNodeId.getHostname(), dataNodeId.getPort(),
                                                              readBufferSizeBytes, writeBufferSizeBytes, readTimeoutMs);
        blockingChannel.connect();
        logger.debug("Establish new connection to {} completed.", dataNodeId);
        return blockingChannel;
      }
      else {
        return openBlockingChannels.remove();
      }
    }

    public synchronized void checkin(BlockingChannel blockingChannel) {
      openBlockingChannels.add(blockingChannel);
    }
  }

  public ConnectionPool(ConnectionPoolConfig config) {
    this.readBufferSizeBytes = config.readBufferSizeBytes;
    this.writeBufferSizeBytes = config.writeBufferSizeBytes;
    this.readTimeoutMs = config.readTimeoutMs;

    this.dataNodePools = new ConcurrentHashMap<DataNodeId, DataNodePool>();
  }

  @Override
  public void start() {
    logger.info("start started");
    // Could pre-establish one connections to each local datanode.
    logger.info("start completed");
  }

  @Override
  public void shutdown() {
    logger.info("shutdown started");
    // Could disconnect all connections, including those currently checked out.
    logger.info("shutdown completed");
  }

  @Override
  public BlockingChannel checkOutConnection(DataNodeId dataNodeId) throws IOException {
    logger.debug("Checking out connection for {}", dataNodeId);
    if (!dataNodePools.containsKey(dataNodeId)) {
      dataNodePools.putIfAbsent(dataNodeId, new DataNodePool(dataNodeId));
    }
    return dataNodePools.get(dataNodeId).checkout();
  }

  @Override
  public void checkInConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel) {
    logger.debug("Checking in connection for {}", dataNodeId);
    if (blockingChannel != null) {
      DataNodePool dataNodePool = dataNodePools.get(dataNodeId);
      dataNodePool.checkin(blockingChannel);
    }
  }

  @Override
  public void destroyConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel) {
    logger.debug("Destroying connection for {}", dataNodeId);
    if (blockingChannel != null) {
      blockingChannel.disconnect();
    }
  }
}



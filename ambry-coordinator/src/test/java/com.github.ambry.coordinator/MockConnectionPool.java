package com.github.ambry.coordinator;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.shared.BlockingChannel;
import com.github.ambry.shared.BlockingChannelPool;

import java.io.IOException;

/**
 *
 */
class MockConnectionPool implements BlockingChannelPool {
  private final int readBufferSizeBytes;
  private final int writeBufferSizeBytes;
  private final int readTimeoutMs;

  // Need static instance of MockCluster so that all connection pools share common MockCluster.
  private static MockCluster mockCluster = new MockCluster();

  public MockConnectionPool(ConnectionPoolConfig config) {
    this.readBufferSizeBytes = config.readBufferSizeBytes;
    this.writeBufferSizeBytes = config.writeBufferSizeBytes;
    this.readTimeoutMs = config.readTimeoutMs;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public BlockingChannel checkOutConnection(DataNodeId dataNodeId) throws IOException {
    BlockingChannel blockingChannel = new MockBlockingChannel(mockCluster.getMockDataNode(dataNodeId),
                                                              dataNodeId.getHostname(), dataNodeId.getPort(),
                                                              readBufferSizeBytes, writeBufferSizeBytes, readTimeoutMs);
    blockingChannel.connect();
    return blockingChannel;
  }

  @Override
  public void checkInConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel) {
    destroyConnection(dataNodeId, blockingChannel);
  }

  @Override
  public void destroyConnection(DataNodeId dataNodeId, BlockingChannel blockingChannel) {
    if (blockingChannel != null) {
      blockingChannel.disconnect();
    }
  }
}

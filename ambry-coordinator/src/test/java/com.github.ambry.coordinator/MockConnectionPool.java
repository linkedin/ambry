package com.github.ambry.coordinator;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.network.BlockingChannel;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;

import java.io.IOException;


/**
 *
 */
public class MockConnectionPool implements ConnectionPool {
  private final int readBufferSizeBytes;
  private final int writeBufferSizeBytes;
  private final int readTimeoutMs;

  // Need static instance of MockCluster so that all connection pools share common MockCluster.
  public static MockCluster mockCluster = null;

  public MockConnectionPool(ConnectionPoolConfig config) {
    this.readBufferSizeBytes = config.connectionPoolReadBufferSizeBytes;
    this.writeBufferSizeBytes = config.connectionPoolWriteBufferSizeBytes;
    this.readTimeoutMs = config.connectionPoolReadTimeoutMs;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, int port, long timeout)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException {
    BlockingChannel blockingChannel =
        new MockBlockingChannel(mockCluster.getMockDataNode(host, port), host, port, readBufferSizeBytes,
            writeBufferSizeBytes, readTimeoutMs);
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

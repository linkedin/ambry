package com.github.ambry.coordinator;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.shared.BlockingChannelPool;
import com.github.ambry.shared.BlockingChannelPoolFactory;

public class MockConnectionPoolFactory implements BlockingChannelPoolFactory {
  private ConnectionPoolConfig config;

  public MockConnectionPoolFactory(ConnectionPoolConfig config) {
    this.config = config;
  }

  @Override
  public BlockingChannelPool getBlockingChannelPool() {
    return new MockConnectionPool(config);
  }
}

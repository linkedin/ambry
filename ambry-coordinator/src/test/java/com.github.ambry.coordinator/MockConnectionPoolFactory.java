package com.github.ambry.coordinator;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.utils.BlockingChannelPool;
import com.github.ambry.utils.BlockingChannelPoolFactory;

public class MockConnectionPoolFactory implements BlockingChannelPoolFactory {
  ConnectionPoolConfig config;

  public MockConnectionPoolFactory(ConnectionPoolConfig config) {
    this.config = config;
  }

  public BlockingChannelPool getBlockingChannelPool() {
    return new MockConnectionPool(config);
  }
}

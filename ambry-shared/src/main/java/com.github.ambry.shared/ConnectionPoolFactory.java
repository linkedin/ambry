package com.github.ambry.shared;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.shared.BlockingChannelPool;
import com.github.ambry.shared.BlockingChannelPoolFactory;
import com.github.ambry.shared.ConnectionPool;

/**
 *  Factory to create BlockingChannelPool.
 */
public class ConnectionPoolFactory implements BlockingChannelPoolFactory {
  ConnectionPoolConfig config;

  public ConnectionPoolFactory(ConnectionPoolConfig config) {
    this.config = config;
  }

  public BlockingChannelPool getBlockingChannelPool() {
    return new ConnectionPool(config);
  }
}

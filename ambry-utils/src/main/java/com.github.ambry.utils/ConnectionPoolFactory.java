package com.github.ambry.utils;

import com.github.ambry.config.ConnectionPoolConfig;

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

package com.github.ambry.shared;

import com.github.ambry.config.ConnectionPoolConfig;


/**
 * A connection pool factory that creates a blocking channel pool
 */
public final class BlockingChannelConnectionPoolFactory implements ConnectionPoolFactory {

  private final ConnectionPoolConfig config;

  public BlockingChannelConnectionPoolFactory(ConnectionPoolConfig config) {
    this.config = config;
  }

  @Override
  public ConnectionPool getConnectionPool() {
    return new BlockingChannelConnectionPool(config);
  }
}

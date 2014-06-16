package com.github.ambry.shared;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;


/**
 * A connection pool factory that creates a blocking channel pool
 */
public final class BlockingChannelConnectionPoolFactory implements ConnectionPoolFactory {

  private final ConnectionPoolConfig config;
  private final MetricRegistry registry;

  public BlockingChannelConnectionPoolFactory(ConnectionPoolConfig config, MetricRegistry registry) {
    this.config = config;
    this.registry = registry;
  }

  @Override
  public ConnectionPool getConnectionPool() {
    return new BlockingChannelConnectionPool(config, registry);
  }
}

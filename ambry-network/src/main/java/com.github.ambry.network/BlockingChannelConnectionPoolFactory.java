package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import javax.net.ssl.SSLSocketFactory;


/**
 * A connection pool factory that creates a blocking channel pool
 */
public final class BlockingChannelConnectionPoolFactory implements ConnectionPoolFactory {
  private final ConnectionPoolConfig config;
  private final MetricRegistry registry;
  private final SSLSocketFactory sslSocketFactory;

  public BlockingChannelConnectionPoolFactory(ConnectionPoolConfig config, MetricRegistry registry,
      SSLSocketFactory sslSocketFactory) {
    this.config = config;
    this.registry = registry;
    this.sslSocketFactory = sslSocketFactory;
  }

  @Override
  public ConnectionPool getConnectionPool() {
    return new BlockingChannelConnectionPool(config, registry, sslSocketFactory);
  }
}

package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import javax.net.ssl.SSLSocketFactory;


/**
 * A connection pool factory that creates a blocking channel pool
 */
public final class BlockingChannelConnectionPoolFactory implements ConnectionPoolFactory {
  private final ConnectionPoolConfig connectionPoolConfig;
  private final SSLConfig sslConfig;
  private final MetricRegistry registry;

  public BlockingChannelConnectionPoolFactory(ConnectionPoolConfig connectionPoolConfig, SSLConfig sslConfig,
      MetricRegistry registry) {
    this.connectionPoolConfig = connectionPoolConfig;
    this.sslConfig = sslConfig;
    this.registry = registry;
  }

  @Override
  public ConnectionPool getConnectionPool()
      throws Exception {
    return new BlockingChannelConnectionPool(connectionPoolConfig, sslConfig, registry);
  }
}

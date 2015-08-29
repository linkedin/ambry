package com.github.ambry.coordinator;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolFactory;


public class MockConnectionPoolFactory implements ConnectionPoolFactory {
  private final ConnectionPoolConfig connectionPoolConfig;
  private final SSLConfig sslConfig;
  private final MetricRegistry registry;

  public MockConnectionPoolFactory(ConnectionPoolConfig connectionPoolConfig, SSLConfig sslConfig,
      MetricRegistry registry) {
    this.connectionPoolConfig = connectionPoolConfig;
    this.sslConfig = sslConfig;
    this.registry = registry;
  }

  public ConnectionPool getConnectionPool() {
    return new MockConnectionPool(connectionPoolConfig);
  }
}

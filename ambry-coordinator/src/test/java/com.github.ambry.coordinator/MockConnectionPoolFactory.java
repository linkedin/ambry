package com.github.ambry.coordinator;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolFactory;


public class MockConnectionPoolFactory implements ConnectionPoolFactory {
  ConnectionPoolConfig config;
  private final MetricRegistry registry;

  public MockConnectionPoolFactory(ConnectionPoolConfig config, MetricRegistry registry) {
    this.config = config;
    this.registry = registry;
  }

  public ConnectionPool getConnectionPool() {
    return new MockConnectionPool(config);
  }
}

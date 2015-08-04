package com.github.ambry.coordinator;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolFactory;
import javax.net.ssl.SSLSocketFactory;


public class MockConnectionPoolFactory implements ConnectionPoolFactory {
  ConnectionPoolConfig config;
  private final MetricRegistry registry;
  private final SSLSocketFactory sslSocketFactory;

  public MockConnectionPoolFactory(ConnectionPoolConfig config, MetricRegistry registry,
      SSLSocketFactory sslSocketFactory) {
    this.config = config;
    this.registry = registry;
    this.sslSocketFactory = sslSocketFactory;
  }

  public ConnectionPool getConnectionPool() {
    return new MockConnectionPool(config);
  }
}

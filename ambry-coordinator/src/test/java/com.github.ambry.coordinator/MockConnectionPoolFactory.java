package com.github.ambry.coordinator;

import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.ConnectionPoolFactory;


public class MockConnectionPoolFactory implements ConnectionPoolFactory {
  ConnectionPoolConfig config;

  public MockConnectionPoolFactory(ConnectionPoolConfig config) {
    this.config = config;
  }

  public ConnectionPool getConnectionPool() {
    return new MockConnectionPool(config);
  }
}

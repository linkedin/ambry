/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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

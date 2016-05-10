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

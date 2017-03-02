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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import java.io.IOException;


/**
 * A factory class used to get new instances of a {@link NetworkClient}
 */
public class NetworkClientFactory {
  protected final NetworkMetrics networkMetrics;
  protected final NetworkConfig networkConfig;
  protected final SSLFactory sslFactory;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
  private final int connectionCheckoutTimeoutMs;
  private final Time time;

  /**
   * Construct a factory using the given parameters.
   * @param networkMetrics the metrics for the Network layer.
   * @param networkConfig the configs for the Network layer.
   * @param sslFactory the sslFactory used for SSL connections.
   * @param maxConnectionsPerPortPlainText the max number of ports per plain text port for this connection manager.
   * @param maxConnectionsPerPortSsl the max number of ports per ssl port for this connection manager.
   * @param time the Time instance to use.
   */
  public NetworkClientFactory(NetworkMetrics networkMetrics, NetworkConfig networkConfig, SSLFactory sslFactory,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl, int connectionCheckoutTimeoutMs, Time time) {
    this.networkMetrics = networkMetrics;
    this.networkConfig = networkConfig;
    this.sslFactory = sslFactory;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
    this.connectionCheckoutTimeoutMs = connectionCheckoutTimeoutMs;
    this.time = time;
  }

  /**
   * Construct and return a new {@link NetworkClient}
   * @return return a new {@link NetworkClient}
   * @throws IOException if the {@link Selector} could not be instantiated.
   */
  public NetworkClient getNetworkClient() throws IOException {
    Selector selector = new Selector(networkMetrics, time, sslFactory);
    return new NetworkClient(selector, networkConfig, networkMetrics, maxConnectionsPerPortPlainText,
        maxConnectionsPerPortSsl, connectionCheckoutTimeoutMs, time);
  }
}


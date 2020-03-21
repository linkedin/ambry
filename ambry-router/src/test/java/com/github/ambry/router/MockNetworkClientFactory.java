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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.SocketNetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.SocketNetworkClientFactory;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A class that mocks {@link NetworkClientFactory} and returns a {@link SocketNetworkClient} created using a {@link
 * MockSelector}
 */
class MockNetworkClientFactory extends SocketNetworkClientFactory {
  private final Time time;
  private AtomicReference<MockSelectorState> state;
  private MockServerLayout serverLayout;
  private int maxPortsPlainText;
  private int maxPortsSsl;
  private int checkoutTimeoutMs;

  /**
   * Construct a MockNetworkClientFactory using the given parameters
   * @param vProps the VerifiableProperties
   * @param state the reference that will be used by the callers to set the state of the MockSelector.
   * @param maxPortsPlainText max number of ports for plain text connections to a node.
   * @param maxPortsSsl max number of connections for ssl connections to a node.
   * @param checkoutTimeoutMs timeout for connection checkouts.
   * @param serverLayout the {@link MockServerLayout} used to get the {@link MockServer} given a host and port.
   * @param time the Time instance to use.
   */
  MockNetworkClientFactory(VerifiableProperties vProps, AtomicReference<MockSelectorState> state, int maxPortsPlainText,
      int maxPortsSsl, int checkoutTimeoutMs, MockServerLayout serverLayout, Time time) {
    super(new NetworkMetrics(new MetricRegistry()), new NetworkConfig(vProps), null, maxPortsPlainText, maxPortsSsl,
        checkoutTimeoutMs, time);
    this.state = state;
    this.time = time;
    this.serverLayout = serverLayout;
    this.maxPortsPlainText = maxPortsPlainText;
    this.maxPortsSsl = maxPortsSsl;
    this.checkoutTimeoutMs = checkoutTimeoutMs;
  }

  /**
   * Return a {@link SocketNetworkClient} instantiated with a {@link MockSelector}
   * @return the constructed {@link SocketNetworkClient}
   * @throws IOException if the selector could not be constructed.
   */
  @Override
  public SocketNetworkClient getNetworkClient() throws IOException {
    MockSelector selector = new MockSelector(serverLayout, state, time, networkConfig);
    return new SocketNetworkClient(selector, networkConfig, new NetworkMetrics(new MetricRegistry()), maxPortsPlainText,
        maxPortsSsl, checkoutTimeoutMs, time);
  }

  /**
   * Return a {@link MockNetworkClient} instantiated with a {@link MockSelector}
   * @return the constructed {@link MockNetworkClient}
   * @throws IOException if the selector could not be constructed.
   */
  public MockNetworkClient getMockNetworkClient() throws IOException {
    MockSelector selector = new MockSelector(serverLayout, state, time, networkConfig);
    return new MockNetworkClient(selector, networkConfig, new NetworkMetrics(new MetricRegistry()), maxPortsPlainText,
        maxPortsSsl, checkoutTimeoutMs, time);
  }
}


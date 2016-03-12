package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.ConnectionTrackerHelper;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;


class MockNetworkClientFactory extends NetworkClientFactory {
  private final Time time;
  private AtomicReference<MockSelectorState> state;
  private MockServerLayout serverLayout;
  private int maxPortsPlainText;
  private int maxPortsSsl;
  private int checkoutTimeoutMs;

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

  @Override
  public NetworkClient getNetworkClient()
      throws IOException {
    MockSelector selector = new MockSelector(time, serverLayout, state);
    ConnectionTrackerHelper connectionTracker = new ConnectionTrackerHelper(maxPortsPlainText, maxPortsSsl);
    return new NetworkClient(selector, connectionTracker, networkConfig, checkoutTimeoutMs, time);
  }
}


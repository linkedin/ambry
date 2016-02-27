package com.github.ambry.router;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.network.ConnectionManager;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NonBlockingConnectionManager;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.network.Selector;
import com.github.ambry.utils.Time;
import java.io.IOException;


/**
 * A factory class used to get new instances of a {@link Selector} and a {@link ConnectionManager}
 */
class RouterNetworkComponentsFactory {
  private final NetworkMetrics networkMetrics;
  private final NetworkConfig networkConfig;
  private final SSLFactory sslFactory;
  private final int maxConnectionsPerPortPlainText;
  private final int maxConnectionsPerPortSsl;
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
  RouterNetworkComponentsFactory(NetworkMetrics networkMetrics, NetworkConfig networkConfig, SSLFactory sslFactory,
      int maxConnectionsPerPortPlainText, int maxConnectionsPerPortSsl, Time time) {
    this.networkMetrics = networkMetrics;
    this.networkConfig = networkConfig;
    this.sslFactory = sslFactory;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
    this.time = time;
  }

  /**
   * Construct and return a new {@link RouterNetworkComponents} instance.
   * @return return a new {@link RouterNetworkComponents} instance.
   * @throws IOException if either the {@link Selector} or the {@link ConnectionManager} could not be constructed.
   */
  RouterNetworkComponents getRouterNetworkComponents()
      throws IOException {
    Selector selector = new Selector(networkMetrics, time, sslFactory);
    ConnectionManager connectionManager =
        new NonBlockingConnectionManager(selector, networkConfig, maxConnectionsPerPortPlainText,
            maxConnectionsPerPortSsl, time);
    return new RouterNetworkComponents(selector, connectionManager);
  }
}

/**
 * A composite class to store the network components required by the Router, together with getters and setters.
 */
class RouterNetworkComponents {
  private final Selector selector;
  private final ConnectionManager connectionManager;

  /**
   * Construct a RouterNetworkComponents with the given {@link Selector} and {@link ConnectionManager}
   * @param selector the Selector for this component.
   * @param connectionManager the ConnectionManager for this component.
   */
  RouterNetworkComponents(Selector selector, ConnectionManager connectionManager) {
    this.selector = selector;
    this.connectionManager = connectionManager;
  }

  /**
   * @return the Selector of this component.
   */
  Selector getSelector() {
    return selector;
  }

  /**
   * @return the ConnectionManager of this component.
   */
  ConnectionManager getConnectionManager() {
    return connectionManager;
  }
}

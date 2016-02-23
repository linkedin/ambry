package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Time;


/**
 * A factory class for the {@link NonBlockingConnectionManager}
 */
public class NonBlockingConnectionManagerFactory implements ConnectionManagerFactory {
  NetworkConfig networkConfig;
  int maxConnectionsPerPortPlainText;
  int maxConnectionsPerPortSsl;
  Time time;
  Selector selector;

  /**
   * Create a NonBlockingConnectionManagerFactory using the given parameters.
   * @param networkMetrics the NetworkMetrics used to instantiate the Selector for the NonBlockingConnectionManager.
   * @param networkConfig the NetworkConfig.
   * @param sslConfig the SSLConfig.
   * @param maxConnectionsPerPortPlainText the pool limit per port for plain text connections.
   * @param maxConnectionsPerPortSsl the pool limit per port for SSL connections.
   * @param time the Time instance to use.
   * @throws Exception if the Selector could not be instantiated.
   */
  public NonBlockingConnectionManagerFactory(NetworkMetrics networkMetrics, NetworkConfig networkConfig,
      SSLConfig sslConfig, Integer maxConnectionsPerPortPlainText, Integer maxConnectionsPerPortSsl, Time time)
      throws Exception {
    this.networkConfig = networkConfig;
    this.maxConnectionsPerPortPlainText = maxConnectionsPerPortPlainText;
    this.maxConnectionsPerPortSsl = maxConnectionsPerPortSsl;
    this.time = time;
    selector = new Selector(networkMetrics, time,
        sslConfig.sslEnabledDatacenters.length() > 0 ? new SSLFactory(sslConfig) : null);
  }

  /**
   * Return a {@link NonBlockingConnectionManager}
   * @return returns a {@link NonBlockingConnectionManager}
   */
  @Override
  public ConnectionManager getConnectionManager() {
    return new NonBlockingConnectionManager(selector, networkConfig, maxConnectionsPerPortPlainText,
        maxConnectionsPerPortSsl, time);
  }
}

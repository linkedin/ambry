package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.utils.Time;


public class NonBlockingConnectionManagerFactory implements ConnectionManagerFactory {
  NetworkConfig networkConfig;
  int maxConnectionsPerPortPlainText;
  int maxConnectionsPerPortSsl;
  Time time;
  Selector selector;

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

  @Override
  public ConnectionManager getConnectionManager() {
    return new NonBlockingConnectionManager(selector, networkConfig, maxConnectionsPerPortPlainText,
        maxConnectionsPerPortSsl, time);
  }
}

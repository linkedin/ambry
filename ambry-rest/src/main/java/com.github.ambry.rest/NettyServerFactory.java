package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import com.github.ambry.rest.RestRequestHandlerController;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link NettyServerFactory#getNioServer()}.
 */
public class NettyServerFactory implements NioServerFactory {

  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final RestRequestHandlerController restRequestHandlerController;

  public NettyServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestHandlerController restRequestHandlerController)
      throws InstantiationException {
    if (verifiableProperties != null && metricRegistry != null && restRequestHandlerController != null) {
      this.nettyConfig = new NettyConfig(verifiableProperties);
      this.nettyMetrics = new NettyMetrics(metricRegistry);
      this.restRequestHandlerController = restRequestHandlerController;
    } else {
      throw new InstantiationException("One of the received arguments is null");
    }
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return
   * @throws InstantiationException
   */
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, restRequestHandlerController);
  }
}

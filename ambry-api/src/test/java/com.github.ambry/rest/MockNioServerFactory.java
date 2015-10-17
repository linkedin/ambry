package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * Implementation of {@link NioServerFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockNioServer} and returns a new instance on
 * {@link MockNioServerFactory#getNioServer()}.
 */
public class MockNioServerFactory implements NioServerFactory {
  // key in properties file/in-memory representation.
  public static String IS_FAULTY_KEY = "mock.nio.server.isFaulty";

  private final boolean isFaulty;

  public MockNioServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      Object requestResponseHandlerController) {
    isFaulty = verifiableProperties.getBoolean(IS_FAULTY_KEY, false);
  }

  /**
   * Returns a new instance of {@link MockNioServer}.
   * @return a new instance of {@link MockNioServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new MockNioServer(isFaulty);
  }
}

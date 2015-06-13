package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * Implementation of NioServer that can be used in tests.
 */
public class MockNioServer implements NioServer {
  public static String IS_FAULTY_KEY = "isFaulty";
  /**
   * Sets this server to be faulty (mainly to check for behaviour under NioServer failures).
   */
  private final boolean isFaulty;

  public MockNioServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.isFaulty = verifiableProperties.getBoolean(IS_FAULTY_KEY, false);
  }

  public void start()
      throws InstantiationException {
    if (isFaulty) {
      throw new InstantiationException("Faulty rest server startup failed");
    }
  }

  public void shutdown()
      throws Exception {
    if (isFaulty) {
      throw new Exception("Faulty rest server shutdown failed");
    }
  }
}

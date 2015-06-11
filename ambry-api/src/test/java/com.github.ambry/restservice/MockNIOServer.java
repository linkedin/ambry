package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * TODO: write description
 */
public class MockNIOServer implements NIOServer {
  public static String IS_FAULTY_KEY = "isFaulty";
  private final boolean isFaulty;

  public MockNIOServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
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

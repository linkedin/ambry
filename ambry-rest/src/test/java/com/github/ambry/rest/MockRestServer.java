package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;


/**
 * TODO: write description
 */
public class MockRestServer implements RestServer {
  public static String IS_FAULTY_KEY = "isFaulty";
  private final boolean isFaulty;

  public MockRestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator restRequestDelegator) {
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

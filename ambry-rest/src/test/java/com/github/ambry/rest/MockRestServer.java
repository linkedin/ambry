package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class MockRestServer implements RestServer {
  public static String IS_FAULTY_KEY = "isFaulty";
  private final boolean isFaulty;

  private boolean up = false;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public MockRestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator restRequestDelegator) {
    this.isFaulty = verifiableProperties.getBoolean(IS_FAULTY_KEY);
  }

  public void start()
      throws InstantiationException {
    logger.info("Rest server starting");
    if (isFaulty) {
      throw new InstantiationException("Faulty rest server startup failed");
    }
    up = true;
    logger.info("Rest server started");
  }

  public void shutdown()
      throws Exception {
    logger.info("Rest server shutting down");
    if (isFaulty) {
      throw new Exception("Faulty rest server shutdown failed");
    }
    up = false;
  }

  public boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    if (isFaulty) {
      return false;
    }
    return true;
  }

  public boolean isUp() {
    return up;
  }

  public boolean isTerminated() {
    return !isUp();
  }
}

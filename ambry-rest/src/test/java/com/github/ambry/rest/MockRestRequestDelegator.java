package com.github.ambry.rest;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO: write description
 */
public class MockRestRequestDelegator implements RestRequestDelegator {
  private Logger logger = LoggerFactory.getLogger(getClass());

  public void start()
    throws InstantiationException {
    logger.info("Request delegator started");
  }

  public RestMessageHandler getMessageHandler()
    throws RestException {
    return null;
  }

  public void shutdown()
      throws Exception {
    logger.info("Request delegator shutdown");
  }
}

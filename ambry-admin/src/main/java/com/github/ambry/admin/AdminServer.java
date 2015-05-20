package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestServerFactory;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin server instance. Responsible for setting up the prerequisites and
 * starting the various components.
 */
public class AdminServer {
  private final VerifiableProperties verifiableProperties;
  private final MetricRegistry metricRegistry;

  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;

  private final AdminRequestDelegator requestDelegator;

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private Logger logger = LoggerFactory.getLogger(getClass());
  private RestServer restServer;
  private boolean up = false;

  public boolean isUp() {
    return up;
  }

  public AdminServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.verifiableProperties = verifiableProperties;
    this.metricRegistry = metricRegistry;

    adminConfig = new AdminConfig(verifiableProperties);
    adminMetrics = new AdminMetrics(metricRegistry);
    requestDelegator = new AdminRequestDelegator(adminConfig.getHandlerCount(), adminMetrics);
  }

  public void start()
      throws InstantiationException {
    if (isReadyToStart()) {
      try {
        logger.info("Starting server");

        requestDelegator.start();
        restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, requestDelegator);
        restServer.start();
        up = true;

        logger.info("Admin server started");
      } catch (Exception e) {
        logger.error("Error during start ", e);
        throw new InstantiationException("Error during start " + e);
      }
    } else {
      String msg = "Did not receive all required components for starting admin server";
      logger.error(msg);
      throw new InstantiationException(msg);
    }
  }

  public void shutdown() {
    logger.info("Shutting down admin server");
    try {
      restServer.shutdown();
      requestDelegator.shutdown();
      up = false;
      logger.info("Admin server shutdown complete");
    } catch (Exception e) {
      logger.error("Exception while shutting down AdminServer - " + e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }

  private boolean isReadyToStart() {
    return verifiableProperties != null && metricRegistry != null;
  }
}

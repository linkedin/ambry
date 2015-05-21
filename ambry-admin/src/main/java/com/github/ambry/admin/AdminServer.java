package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
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
  private final ClusterMap clusterMap;

  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final AdminRequestDelegator requestDelegator;
  private final RestServer restServer;

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws Exception {
    this.verifiableProperties = verifiableProperties;
    this.metricRegistry = metricRegistry;
    this.clusterMap = clusterMap;

    if(serverReadyForStart()) {
      adminConfig = new AdminConfig(verifiableProperties);
      adminMetrics = new AdminMetrics(metricRegistry);
      requestDelegator = new AdminRequestDelegator(adminConfig.getHandlerCount(), adminMetrics);
      restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, requestDelegator);
    } else {
      String msg = "Did not receive all required components for starting admin server";
      logger.error(msg);
      throw new Exception(msg);
    }
  }

  public void start()
      throws InstantiationException {
    try {
      logger.info("Starting server");

      requestDelegator.start();
      restServer.start();

      logger.info("Admin server started");
    } catch (Exception e) {
      logger.error("Error during start ", e);
      throw new InstantiationException("Error during start " + e);
    }
  }

  public void shutdown() {
    logger.info("Shutting down admin server");
    try {
      restServer.shutdown();
      requestDelegator.shutdown();
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

  private boolean serverReadyForStart() {
    return verifiableProperties != null
        && metricRegistry != null
        && clusterMap != null;
  }
}

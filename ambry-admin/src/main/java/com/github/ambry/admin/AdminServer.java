package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestServer;
import com.github.ambry.rest.RestServerFactory;
import java.util.concurrent.TimeUnit;
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
  private final AdminBlobStorageService adminBlobStorageService;
  private final AdminRequestDelegator requestDelegator;
  private final RestServer restServer;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public AdminServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws Exception {
    this.verifiableProperties = verifiableProperties;
    this.metricRegistry = metricRegistry;
    this.clusterMap = clusterMap;

    if (serverReadyForStart()) {
      adminConfig = new AdminConfig(verifiableProperties);
      adminMetrics = new AdminMetrics(metricRegistry);
      adminBlobStorageService = new AdminBlobStorageService(clusterMap);
      requestDelegator =
          new AdminRequestDelegator(adminConfig.getHandlerCount(), adminMetrics, adminBlobStorageService);
      restServer = RestServerFactory.getRestServer(verifiableProperties, metricRegistry, requestDelegator);
    } else {
      throw new Exception("Did not receive all required components for starting admin server");
    }
  }

  public void start()
      throws InstantiationException {
    try {
      logger.info("Starting server");

      adminBlobStorageService.start();
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
      adminBlobStorageService.shutdown();
      logger.info("Admin server shutdown complete");
    } catch (Exception e) {
      logger.error("Exception while shutting down AdminServer - " + e);
    }
  }

  public boolean awaitShutdown(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return restServer.awaitShutdown(timeout, timeUnit) && requestDelegator.awaitShutdown(timeout, timeUnit)
        && adminBlobStorageService.awaitShutdown(timeout, timeUnit);
  }

  public boolean isUp() {
    return adminBlobStorageService.isUp() && requestDelegator.isUp() && restServer.isUp();
  }

  private boolean serverReadyForStart() {
    return verifiableProperties != null && metricRegistry != null && clusterMap != null;
  }
}

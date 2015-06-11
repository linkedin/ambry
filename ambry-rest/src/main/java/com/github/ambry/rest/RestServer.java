package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.NIOServer;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rest server instance. Responsible for setting up the prerequisites and
 * starting the various components.
 */
public class RestServer {
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  private final VerifiableProperties verifiableProperties;
  private final MetricRegistry metricRegistry;
  private final ClusterMap clusterMap;

  private final RestServerConfig restServerConfig;
  private final RestServerMetrics restServerMetrics;
  private final BlobStorageService blobStorageService;
  private final RestRequestDelegator requestDelegator;
  private final NIOServer NIOServer;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public RestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws InstantiationException {
    this.verifiableProperties = verifiableProperties;
    this.metricRegistry = metricRegistry;
    this.clusterMap = clusterMap;

    if (serverReadyForStart()) {
      try {
        restServerConfig = new RestServerConfig(verifiableProperties);
        restServerMetrics = new RestServerMetrics(metricRegistry);
        blobStorageService =
            BlobStorageServiceFactory.getBlobStorageService(verifiableProperties, clusterMap, metricRegistry);
        requestDelegator =
            new RestRequestDelegator(restServerConfig.getMessageHandlerCount(), restServerMetrics, blobStorageService);
        NIOServer = NIOServerFactory.getNIOServer(verifiableProperties, metricRegistry, requestDelegator);
      } catch (Exception e) {
        throw new InstantiationException("Error while creating rest server components - " + e);
      }
    } else {
      throw new InstantiationException("Did not receive all required components for starting rest server");
    }
  }

  public void start()
      throws InstantiationException {
    try {
      logger.info("Starting server");

      blobStorageService.start();
      requestDelegator.start();
      NIOServer.start();

      logger.info("Rest server started");
    } catch (Exception e) {
      logger.error("Error during start ", e);
      throw new InstantiationException("Error during start " + e);
    }
  }

  public void shutdown()
      throws Exception {
    logger.info("Shutting down rest server");
    NIOServer.shutdown();
    requestDelegator.shutdown();
    blobStorageService.shutdown();
    logger.info("Rest server shutdown complete");
    shutdownLatch.countDown();
  }

  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }

  private boolean serverReadyForStart() {
    return verifiableProperties != null && metricRegistry != null && clusterMap != null;
  }
}

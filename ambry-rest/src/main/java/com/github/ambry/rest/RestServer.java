package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.NioServer;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rest server instance. Responsible for setting up the prerequisites and
 * starting the various components.
 */
public class RestServer {
  /**
   * Tracks completion of shutdown.
   */
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  /**
   * Place where the configs are stored.
   */
  private final VerifiableProperties verifiableProperties;
  /**
   * Place where metrics need to go.
   */
  private final MetricRegistry metricRegistry;
  /**
   * The cluster map provided as input.
   */
  private final ClusterMap clusterMap;

  private final RestServerConfig restServerConfig;
  private final RestServerMetrics restServerMetrics;

  /**
   * Component instances.
   */
  private final BlobStorageService blobStorageService;
  private final RestRequestDelegator requestDelegator;
  private final NioServer nioServer;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public RestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws InstantiationException {
    this.verifiableProperties = verifiableProperties;
    this.metricRegistry = metricRegistry;
    this.clusterMap = clusterMap;

    verifyInput();
    try {
      restServerConfig = new RestServerConfig(verifiableProperties);
      restServerMetrics = new RestServerMetrics(metricRegistry);

      // create instances of components.
      blobStorageService =
          BlobStorageServiceFactory.getBlobStorageService(verifiableProperties, clusterMap, metricRegistry);
      requestDelegator =
          new RestRequestDelegator(restServerConfig.getMessageHandlerCount(), restServerMetrics, blobStorageService);
      nioServer = NioServerFactory.getNIOServer(verifiableProperties, metricRegistry, requestDelegator);
    } catch (Exception e) {
      throw new InstantiationException("Error while creating rest server components - " + e);
    }
    verifyComponents();
  }

  /**
   * Starts up all the components. Returns when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException {
    try {
      logger.info("Starting server");

      blobStorageService.start();
      requestDelegator.start();
      nioServer.start();

      logger.info("Rest server started");
    } catch (Exception e) {
      logger.error("Error during start ", e);
      throw new InstantiationException("Error during start " + e);
    }
  }

  /**
   * Shuts down up all the components. Returns when shutdown is FULLY complete.
   * @throws Exception
   */
  public void shutdown()
      throws Exception {
    logger.info("Shutting down rest server");
    nioServer.shutdown();
    requestDelegator.shutdown();
    blobStorageService.shutdown();
    logger.info("Rest server shutdown complete");
    shutdownLatch.countDown();
  }

  /**
   * Wait for shutdown to be triggered and complete.
   * @throws InterruptedException
   */
  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }

  /**
   * Checks sanity of input arguments.
   * @throws InstantiationException
   */
  private void verifyInput()
      throws InstantiationException {
    if (verifiableProperties == null || metricRegistry == null || clusterMap == null) {
      throw new InstantiationException("Received some null arguments while instantiating RestServer");
    }
  }

  /**
   * Checks that all components have been instantiated correctly.
   * @throws InstantiationException
   */
  private void verifyComponents()
      throws InstantiationException {
    if (blobStorageService == null || requestDelegator == null || nioServer == null) {
      throw new InstantiationException("Failed to instantiate one of the components of RestServer");
    }
  }
}

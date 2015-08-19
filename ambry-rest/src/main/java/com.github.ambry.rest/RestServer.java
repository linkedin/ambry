package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The RestServer represents any RESTful service (frontend, admin etc.) whose main concern is to receive requests from
 * clients through a REST protocol (HTTP), handle them appropriately by contacting Ambry backend storage if required and
 * return responses via the same REST protocol.
 * <p/>
 * The RestServer is responsible for starting up (and shutting down) multiple services required to handle requests from
 * clients. Currently it starts/shuts down the following: -
 * 1. A {@link BlobStorageService} - A service that understands the operations supported by Ambry (including those
 * through the storage backend) and can handle requests from clients for such operations.
 * 2. A {@link NioServer} - To receive requests and return responses via a REST protocol (HTTP).
 * 3. A {@link RestRequestHandlerController} - To start the scaling units (instances of
 * {@link RestRequestHandler}) that are responsible for interfacing between the
 * {@link NioServer} and the {@link BlobStorageService}.
 * <p/>
 * Depending upon what is specified in the configuration file, the RestServer can start different implementations of
 * {@link NioServer} and {@link BlobStorageService} and behave accordingly.
 * <p/>
 * With RestServer, the goals are threefold:-
 * 1. To support ANY RESTful frontend service as long as it can provide an implementation of {@link BlobStorageService}.
 * 2. Make it easy to plug in any implementation of {@link NioServer} as long as it can provide implementations that
 * abstract framework specific objects and actions (like write/read from channel) into generic APIs through
 * {@link RestRequestMetadata}, {@link RestRequestContent},
 * {@link RestResponseChannel} etc.
 * 3. Provide scaling capabilities independent of any other component through implementations of
 * {@link RestRequestHandlerController} and {@link RestRequestHandler}.
 */
public class RestServer {
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerConfig restServerConfig;
  private final RestServerMetrics restServerMetrics;
  private final BlobStorageService blobStorageService;
  private final RestRequestHandlerController requestHandlerController;
  private final NioServer nioServer;

  public RestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws InstantiationException {
    if (verifiableProperties == null || metricRegistry == null || clusterMap == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received during instantiation of RestServer -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (metricRegistry == null) {
        errorMessage.append(" [MetricRegistry] ");
      }
      if (clusterMap == null) {
        errorMessage.append(" [ClusterMap] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }

    restServerConfig = new RestServerConfig(verifiableProperties);
    restServerMetrics = new RestServerMetrics(metricRegistry);
    try {
      BlobStorageServiceFactory blobStorageServiceFactory = Utils
          .getObj(restServerConfig.restBlobStorageServiceFactory, verifiableProperties, metricRegistry, clusterMap);
      blobStorageService = blobStorageServiceFactory.getBlobStorageService();
      requestHandlerController =
          new RequestHandlerController(restServerConfig.restRequestHandlerCount, restServerMetrics, blobStorageService);
      NioServerFactory nioServerFactory = Utils
          .getObj(restServerConfig.restNioServerFactory, verifiableProperties, metricRegistry,
              requestHandlerController);
      nioServer = nioServerFactory.getNioServer();
    } catch (Exception e) {
      logger.error("Exception during instantiation of RestServer", e);
      restServerMetrics.restServerInstantiationError.inc();
      throw new InstantiationException("Exception while creating RestServer components - " + e.getLocalizedMessage());
    }

    if (blobStorageService == null || nioServer == null) {
      StringBuilder errorMessage = new StringBuilder("Failed to instantiate some components of RestServer -");
      if (blobStorageService == null) {
        errorMessage.append(" [BlobStorageService] ");
      }
      if (nioServer == null) {
        errorMessage.append(" [NioServer] ");
      }
      restServerMetrics.restServerInstantiationError.inc();
      throw new InstantiationException(errorMessage.toString());
    }
    logger.trace("Instantiated RestServer");
  }

  /**
   * Starts up all the components required. Returns when startup is FULLY complete.
   * @throws InstantiationException if the RestServer is unable to start.
   */
  public void start()
      throws InstantiationException {
    logger.info("Starting RestServer");
    long startupBeginTime = System.currentTimeMillis();
    try {
      // ordering is important.
      blobStorageService.start();
      requestHandlerController.start();
      nioServer.start();
    } finally {
      long startupTime = System.currentTimeMillis() - startupBeginTime;
      logger.info("RestServer start took {} ms", startupTime);
      restServerMetrics.restServerStartTimeInMs.update(startupTime);
    }
  }

  /**
   * Shuts down all the components. Returns when shutdown is FULLY complete.
   */
  public void shutdown() {
    logger.info("Shutting down RestServer");
    long shutdownBeginTime = System.currentTimeMillis();
    try {
      //ordering is important.
      nioServer.shutdown();
      requestHandlerController.shutdown();
      blobStorageService.shutdown();
    } finally {
      long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
      logger.info("RestServer shutdown took {} ms", shutdownTime);
      restServerMetrics.restServerShutdownTimeInMs.update(shutdownTime);
      shutdownLatch.countDown();
    }
  }

  /**
   * Wait for shutdown to be triggered and for it to complete.
   * @throws InterruptedException if the wait for shutdown is interrupted.
   */
  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }
}

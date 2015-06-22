package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.BlobStorageServiceFactory;
import com.github.ambry.restservice.NioServer;
import com.github.ambry.restservice.NioServerFactory;
import com.github.ambry.restservice.RestRequestHandlerController;
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
 * {@link com.github.ambry.restservice.RestRequestHandler}) that are responsible for interfacing between the
 * {@link NioServer} and the {@link BlobStorageService}.
 * <p/>
 * Depending upon what is specified in the configuration file, the RestServer can start different implementations of
 * {@link NioServer} and {@link BlobStorageService} and behave accordingly.
 * <p/>
 * With RestServer, the goals are threefold:-
 * 1. To support ANY RESTful frontend service as long as it can provide an implementation of {@link BlobStorageService}.
 * 2. Make it easy to plug in any implementation of {@link NioServer} as long as it can provide implementations that
 * abstract framework specific objects and actions (like write/read from channel) into generic APIs through
 * {@link com.github.ambry.restservice.RestRequestMetadata}, {@link com.github.ambry.restservice.RestRequestContent},
 * {@link com.github.ambry.restservice.RestResponseHandler} etc.
 * 3. Provide scaling capabilities independent of any other component through implementations of
 * {@link RestRequestHandlerController} and {@link com.github.ambry.restservice.RestRequestHandler}.
 */
public class RestServer {
  // Tracks completion of shutdown.
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerConfig restServerConfig;
  private final RestServerMetrics restServerMetrics;

  // component instances.
  private final BlobStorageService blobStorageService;
  private final RestRequestHandlerController requestHandlerController;
  private final NioServer nioServer;

  public RestServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry, ClusterMap clusterMap)
      throws InstantiationException {
    if (verifiableProperties == null || metricRegistry == null || clusterMap == null) {
      throw new InstantiationException("Received some null arguments while instantiating RestServer");
    }
    try {
      restServerConfig = new RestServerConfig(verifiableProperties);
      restServerMetrics = new RestServerMetrics(metricRegistry);

      // create instances of components.
      BlobStorageServiceFactory blobStorageServiceFactory = Utils
          .getObj(restServerConfig.getBlobStorageServiceFactory(), verifiableProperties, metricRegistry, clusterMap);
      blobStorageService = blobStorageServiceFactory.getBlobStorageService();
      requestHandlerController =
          new RequestHandlerController(restServerConfig.getRequestHandlerCount(), restServerMetrics,
              blobStorageService);
      NioServerFactory nioServerFactory = Utils
          .getObj(restServerConfig.getNioServerFactory(), verifiableProperties, metricRegistry,
              requestHandlerController);
      nioServer = nioServerFactory.getNioServer();
    } catch (Exception e) {
      throw new InstantiationException("Error while creating rest server components - " + e);
    }
    if (blobStorageService == null || requestHandlerController == null || nioServer == null) {
      throw new InstantiationException("Failed to instantiate one of the components of RestServer");
    }
  }

  /**
   * Starts up all the components required. Returns when startup is FULLY complete.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException {
    try {
      logger.info("Starting RestServer..");

      // ordering is important.
      blobStorageService.start();
      requestHandlerController.start();
      nioServer.start();

      logger.info("RestServer has started");
    } catch (Exception e) {
      logger.error("Error during start ", e);
      throw new InstantiationException("Error during start " + e);
    }
  }

  /**
   * Shuts down up all the components. Returns when shutdown is FULLY complete.
   */
  public void shutdown() {
    logger.info("Shutting down RestServer..");

    //ordering is important.
    nioServer.shutdown();
    requestHandlerController.shutdown();
    blobStorageService.shutdown();
    shutdownLatch.countDown();

    logger.info("RestServer shutdown complete");
  }

  /**
   * Wait for shutdown to be triggered and for it to complete.
   * @throws InterruptedException
   */
  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }
}

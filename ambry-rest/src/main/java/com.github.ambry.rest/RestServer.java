package com.github.ambry.rest;

import com.codahale.metrics.JmxReporter;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterFactory;
import com.github.ambry.utils.Utils;
import java.io.IOException;
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
 * 1. A {@link Router} - A service that is used to contact the Ambry storage backend.
 * 2. A {@link BlobStorageService} - A service that understands the operations supported by Ambry (including those
 * through the storage backend) and can handle requests from clients for such operations.
 * 3. A {@link NioServer} - To receive requests and return responses via a REST protocol (HTTP).
 * 4. A {@link RequestResponseHandlerController} - To start the scaling units (instances of
 * {@link AsyncRequestResponseHandler}) that are responsible for interfacing between the {@link NioServer} and the
 * {@link BlobStorageService}.
 * <p/>
 * Depending upon what is specified in the configuration file, the RestServer can start different implementations of
 * {@link NioServer} and {@link BlobStorageService} and behave accordingly.
 * <p/>
 * With RestServer, the goals are threefold:-
 * 1. To support ANY RESTful frontend service as long as it can provide an implementation of {@link BlobStorageService}.
 * 2. Make it easy to plug in any implementation of {@link NioServer} as long as it can provide implementations that
 * abstract framework specific objects and actions (like write/read from channel) into generic APIs through
 * {@link RestRequest}, {@link RestResponseChannel} etc.
 * 3. Provide scaling capabilities independent of any other component through {@link RequestResponseHandlerController}
 * and {@link AsyncRequestResponseHandler}.
 */
public class RestServer {
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerMetrics restServerMetrics;
  private final JmxReporter reporter;
  private final Router router;
  private final BlobStorageService blobStorageService;
  private final RequestResponseHandlerController requestResponseHandlerController;
  private final NioServer nioServer;

  public RestServer(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem)
      throws InstantiationException {
    if (verifiableProperties == null || clusterMap == null || notificationSystem == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received during instantiation of RestServer -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (clusterMap == null) {
        errorMessage.append(" [ClusterMap] ");
      }
      if (notificationSystem == null) {
        errorMessage.append(" [NotificationSystem] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    RestServerConfig restServerConfig = new RestServerConfig(verifiableProperties);
    reporter = JmxReporter.forRegistry(clusterMap.getMetricRegistry()).build();
    RestRequestMetrics.setDefaults(clusterMap.getMetricRegistry());
    restServerMetrics = new RestServerMetrics(clusterMap.getMetricRegistry());
    try {
      requestResponseHandlerController =
          new RequestResponseHandlerController(restServerConfig.restScalingUnitCount, restServerMetrics);
      RouterFactory routerFactory =
          Utils.getObj(restServerConfig.restRouterFactory, verifiableProperties, clusterMap, notificationSystem);
      router = routerFactory.getRouter();
      BlobStorageServiceFactory blobStorageServiceFactory = Utils
          .getObj(restServerConfig.restBlobStorageServiceFactory, verifiableProperties, clusterMap,
              requestResponseHandlerController, router);
      blobStorageService = blobStorageServiceFactory.getBlobStorageService();
      requestResponseHandlerController.setBlobStorageService(blobStorageService);
      NioServerFactory nioServerFactory = Utils
          .getObj(restServerConfig.restNioServerFactory, verifiableProperties, clusterMap.getMetricRegistry(),
              requestResponseHandlerController);
      nioServer = nioServerFactory.getNioServer();
    } catch (Exception e) {
      logger.error("Exception during instantiation of RestServer", e);
      restServerMetrics.restServerInstantiationError.inc();
      throw new InstantiationException("Exception while creating RestServer components - " + e.getLocalizedMessage());
    }

    if (router == null || blobStorageService == null || nioServer == null) {
      StringBuilder errorMessage = new StringBuilder("Failed to instantiate some components of RestServer -");
      if (router == null) {
        errorMessage.append(" [Router] ");
      }
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
      reporter.start();
      blobStorageService.start();
      requestResponseHandlerController.start();
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
      requestResponseHandlerController.shutdown();
      blobStorageService.shutdown();
      router.close();
      reporter.stop();
    } catch (IOException e) {
      logger.error("Exception during shutdown", e);
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

/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.account.HelixAccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.NettyInternalMetrics;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.RestServerConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
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
 * clients through a REST protocol (HTTP), handle them appropriately by contacting the backend service if required and
 * return responses via the same REST protocol.
 * <p/>
 * The RestServer is responsible for starting up (and shutting down) multiple services required to handle requests from
 * clients. Currently it starts/shuts down the following: -
 * 1. A {@link Router} - A service that is used to contact the backend service.
 * 2. A {@link RestRequestService} - A service that understands the operations supported by the backend service and can
 * handle requests from clients for such operations.
 * 3. A {@link NioServer} - To receive requests and return responses via a REST protocol (HTTP).
 * 4. A {@link RestRequestHandler} and a {@link RestResponseHandler} - Scaling units that are responsible for
 * interfacing between the {@link NioServer} and the {@link RestRequestService}.
 * 5. A {@link PublicAccessLogger} - To assist in public access logging
 * 6. A {@link RestServerState} - To maintain the health of the server
 * <p/>
 * Depending upon what is specified in the configuration file, the RestServer can start different implementations of
 * {@link NioServer} and {@link RestRequestService} and behave accordingly.
 * <p/>
 * With RestServer, the goals are threefold:-
 * 1. To support ANY RESTful frontend service as long as it can provide an implementation of {@link RestRequestService}.
 * 2. Make it easy to plug in any implementation of {@link NioServer} as long as it can provide implementations that
 * abstract framework specific objects and actions (like write/read from channel) into generic APIs through
 * {@link RestRequest}, {@link RestResponseChannel} etc.
 * 3. Provide scaling capabilities independent of any other component through {@link RestRequestHandler} and
 * {@link RestResponseHandler}.
 */
public class RestServer {
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final RestServerMetrics restServerMetrics;
  private final JmxReporter reporter;
  private final AccountService accountService;
  private final Router router;
  private final RestRequestService restRequestService;
  private final RestRequestHandler restRequestHandler;
  private final RestResponseHandler restResponseHandler;
  private final NioServer nioServer;
  private final PublicAccessLogger publicAccessLogger;
  private final RestServerState restServerState;
  private final NettyInternalMetrics nettyInternalMetrics;

  /**
   * {@link RestServer} specific metrics tracking.
   */
  private class RestServerMetrics {
    // Errors
    public final Counter restServerInstantiationError;

    // Others
    public final Histogram restRequestServiceShutdownTimeInMs;
    public final Histogram restRequestServiceStartTimeInMs;
    public final Histogram nioServerShutdownTimeInMs;
    public final Histogram nioServerStartTimeInMs;
    public final Histogram jmxReporterShutdownTimeInMs;
    public final Histogram jmxReporterStartTimeInMs;
    public final Histogram restRequestHandlerShutdownTimeInMs;
    public final Histogram restRequestHandlerStartTimeInMs;
    public final Histogram restResponseHandlerShutdownTimeInMs;
    public final Histogram restResponseHandlerStartTimeInMs;
    public final Histogram restServerShutdownTimeInMs;
    public final Histogram restServerStartTimeInMs;
    public final Histogram routerCloseTimeInMs;
    public final Histogram accountServiceCloseTimeInMs;

    /**
     * Creates an instance of RestServerMetrics using the given {@code metricRegistry}.
     * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
     * @param restServerState the {@link RestServerState} object used to track the state of the {@link RestServer}.
     */
    public RestServerMetrics(MetricRegistry metricRegistry, final RestServerState restServerState) {
      // Errors
      restServerInstantiationError =
          metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));

      // Others
      restRequestServiceShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestServiceShutdownTimeInMs"));
      restRequestServiceStartTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestServiceStartTimeInMs"));
      jmxReporterShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "JmxShutdownTimeInMs"));
      jmxReporterStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "JmxStartTimeInMs"));
      nioServerShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "NioServerShutdownTimeInMs"));
      nioServerStartTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "NioServerStartTimeInMs"));
      restRequestHandlerShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestHandlerShutdownTimeInMs"));
      restRequestHandlerStartTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestHandlerStartTimeInMs"));
      restResponseHandlerShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestResponseHandlerShutdownTimeInMs"));
      restResponseHandlerStartTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestResponseHandlerStartTimeInMs"));
      restServerShutdownTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerShutdownTimeInMs"));
      restServerStartTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerStartTimeInMs"));
      routerCloseTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RouterCloseTimeInMs"));
      accountServiceCloseTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(RestServer.class, "AccountServiceCloseTimeInMs"));

      Gauge<Integer> restServerStatus = () -> restServerState.isServiceUp() ? 1 : 0;
      metricRegistry.register(MetricRegistry.name(RestServer.class, "RestServerState"), restServerStatus);
    }
  }

  /**
   * Creates an instance of RestServer.
   * @param verifiableProperties the properties that define the behavior of the RestServer and its components.
   * @param clusterMap the {@link ClusterMap} instance that needs to be used.
   * @param notificationSystem the {@link NotificationSystem} instance that needs to be used.
   * @param sslFactory the {@link SSLFactory} to be used. This can be {@code null} if no components require SSL support.
   * @throws InstantiationException if there is any error instantiating an instance of RestServer.
   */
  public RestServer(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, SSLFactory sslFactory) throws Exception {
    if (verifiableProperties == null || clusterMap == null || notificationSystem == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of RestServer");
    }
    MetricRegistry metricRegistry = clusterMap.getMetricRegistry();
    RestServerConfig restServerConfig = new RestServerConfig(verifiableProperties);
    reporter = JmxReporter.forRegistry(metricRegistry).build();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    restServerState = new RestServerState(restServerConfig.restServerHealthCheckUri);
    restServerMetrics = new RestServerMetrics(metricRegistry, restServerState);

    AccountServiceFactory accountServiceFactory =
        Utils.getObj(restServerConfig.restServerAccountServiceFactory, verifiableProperties,
            clusterMap.getMetricRegistry());
    accountService = accountServiceFactory.getAccountService();

    SSLFactory routerSslFactory;
    if (new RouterConfig(verifiableProperties).routerEnableHttp2NetworkClient) {
      routerSslFactory = new NettySslHttp2Factory(new SSLConfig(verifiableProperties));
    } else {
      routerSslFactory = sslFactory;
    }

    RouterFactory routerFactory =
        Utils.getObj(restServerConfig.restServerRouterFactory, verifiableProperties, clusterMap, notificationSystem,
            routerSslFactory, accountService);
    router = routerFactory.getRouter();

    // setup the router for the account service
    if (accountService instanceof HelixAccountService) {
      ((HelixAccountService) accountService).setupRouter(router);
    }

    // setup restRequestService
    RestRequestServiceFactory restRequestServiceFactory =
        Utils.getObj(restServerConfig.restServerRestRequestServiceFactory, verifiableProperties, clusterMap, router,
            accountService);
    restRequestService = restRequestServiceFactory.getRestRequestService();
    if (restRequestService == null) {
      throw new InstantiationException("RestRequestService is null");
    }

    RestRequestResponseHandlerFactory restHandlerFactory =
        Utils.getObj(restServerConfig.restServerRequestResponseHandlerFactory,
            restServerConfig.restServerRequestHandlerScalingUnitCount, metricRegistry, restRequestService);
    restRequestHandler = restHandlerFactory.getRestRequestHandler();
    restResponseHandler = restHandlerFactory.getRestResponseHandler();

    publicAccessLogger = new PublicAccessLogger(restServerConfig.restServerPublicAccessLogRequestHeaders.split(","),
        restServerConfig.restServerPublicAccessLogResponseHeaders.split(","));

    NioServerFactory nioServerFactory =
        Utils.getObj(restServerConfig.restServerNioServerFactory, verifiableProperties, metricRegistry,
            restRequestHandler, publicAccessLogger, restServerState, sslFactory);
    nioServer = nioServerFactory.getNioServer();

    if (accountService == null || router == null || restResponseHandler == null || restRequestHandler == null
        || nioServer == null) {
      throw new InstantiationException("Some of the server components were null");
    }
    nettyInternalMetrics = new NettyInternalMetrics(metricRegistry, new NettyConfig(verifiableProperties));
    logger.trace("Instantiated RestServer");
  }

  /**
   * Starts up all the components required. Returns when startup is FULLY complete.
   * @throws InstantiationException if the RestServer is unable to start.
   */
  public void start() throws InstantiationException {
    logger.info("Starting RestServer");
    long startupBeginTime = System.currentTimeMillis();
    try {
      // ordering is important.
      reporter.start();
      long reporterStartTime = System.currentTimeMillis();
      long elapsedTime = reporterStartTime - startupBeginTime;
      logger.info("JMX reporter start took {} ms", elapsedTime);
      restServerMetrics.jmxReporterStartTimeInMs.update(elapsedTime);

      restResponseHandler.start();
      long restResponseHandlerStartTime = System.currentTimeMillis();
      elapsedTime = restResponseHandlerStartTime - reporterStartTime;
      logger.info("Response handler and Request Handler start took {} ms", elapsedTime);
      restServerMetrics.restResponseHandlerStartTimeInMs.update(elapsedTime);

      restRequestService.start();
      long restRequestServiceStartTime = System.currentTimeMillis();
      elapsedTime = restRequestServiceStartTime - restResponseHandlerStartTime;
      logger.info("Rest request service start took {} ms", elapsedTime);
      restServerMetrics.restRequestServiceStartTimeInMs.update(elapsedTime);

      nioServer.start();
      elapsedTime = System.currentTimeMillis() - restRequestServiceStartTime;
      logger.info("NIO server start took {} ms", elapsedTime);
      restServerMetrics.nioServerStartTimeInMs.update(elapsedTime);

      if (nettyInternalMetrics != null) {
        nettyInternalMetrics.start();
        logger.info("NettyInternalMetric starts");
      }

      restServerState.markServiceUp();
      logger.info("Service marked as up");
    } finally {
      long startupTime = System.currentTimeMillis() - startupBeginTime;
      logger.info("RestServer start took {} ms", startupTime);
      restServerMetrics.restServerStartTimeInMs.update(startupTime);
    }
  }

  /**
   * Shuts down all the components. Returns when shutdown is FULLY complete.
   * This method is expected to be called in the exit path as long as the RestServer instance construction was
   * successful. This is expected to be called even if {@link #start()} did not succeed.
   */
  public void shutdown() {
    logger.info("Shutting down RestServer");
    long shutdownBeginTime = System.currentTimeMillis();
    try {
      //ordering is important.
      restServerState.markServiceDown();
      logger.info("Service marked as down ");

      if (nettyInternalMetrics != null) {
        nettyInternalMetrics.stop();
        logger.info("NettyInternalMetrics stops");
      }

      nioServer.shutdown();
      long nioServerShutdownTime = System.currentTimeMillis();
      long elapsedTime = nioServerShutdownTime - shutdownBeginTime;
      logger.info("NIO server shutdown took {} ms", elapsedTime);
      restServerMetrics.nioServerShutdownTimeInMs.update(elapsedTime);

      restRequestHandler.shutdown();
      long requestHandlerShutdownTime = System.currentTimeMillis();
      elapsedTime = requestHandlerShutdownTime - nioServerShutdownTime;
      logger.info("Request handler shutdown took {} ms", elapsedTime);
      restServerMetrics.restRequestHandlerShutdownTimeInMs.update(elapsedTime);

      restRequestService.shutdown();
      long restRequestServiceShutdownTime = System.currentTimeMillis();
      elapsedTime = restRequestServiceShutdownTime - requestHandlerShutdownTime;
      logger.info("Rest request service shutdown took {} ms", elapsedTime);
      restServerMetrics.restRequestServiceShutdownTimeInMs.update(elapsedTime);

      restResponseHandler.shutdown();
      long responseHandlerShutdownTime = System.currentTimeMillis();
      elapsedTime = responseHandlerShutdownTime - restRequestServiceShutdownTime;
      logger.info("Response handler shutdown took {} ms", elapsedTime);
      restServerMetrics.restResponseHandlerShutdownTimeInMs.update(elapsedTime);

      router.close();
      long routerCloseTime = System.currentTimeMillis();
      elapsedTime = routerCloseTime - responseHandlerShutdownTime;
      logger.info("Router close took {} ms", elapsedTime);
      restServerMetrics.routerCloseTimeInMs.update(elapsedTime);

      accountService.close();
      long accountServiceCloseTime = System.currentTimeMillis();
      elapsedTime = accountServiceCloseTime - routerCloseTime;
      logger.info("Account service close took {} ms", elapsedTime);
      restServerMetrics.accountServiceCloseTimeInMs.update(elapsedTime);

      reporter.stop();
      elapsedTime = System.currentTimeMillis() - accountServiceCloseTime;
      logger.info("JMX reporter shutdown took {} ms", elapsedTime);
      restServerMetrics.jmxReporterShutdownTimeInMs.update(elapsedTime);
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
  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}

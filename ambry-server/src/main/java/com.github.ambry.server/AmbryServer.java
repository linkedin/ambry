package com.github.ambry.server;

import com.github.ambry.config.MetricsConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.metrics.JmxServer;
import com.github.ambry.metrics.JvmMetrics;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.MetricsReporterFactory;
import com.github.ambry.metrics.MetricsReporter;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.SocketServer;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.BlobStore;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Ambry server
 */
public class AmbryServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private Scheduler scheduler = new Scheduler(4, false);
  private Store store = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final JmxServer jmxServer;
  private final JvmMetrics jvmMetrics;
  private MetricsRegistryMap registryMap;
  private final Map<String, MetricsReporter> reporters;

  public AmbryServer(VerifiableProperties properties) throws IOException {
    this.properties = properties;
    // start jmx server
    jmxServer = new JmxServer();
    reporters = new HashMap<String, MetricsReporter>();
    registryMap = new MetricsRegistryMap("Data Node");
    jvmMetrics = new JvmMetrics(registryMap);
  }

  public void startup() throws IOException, InterruptedException, StoreException {
    logger.info("starting");
    logger.info("Setting up JVM metrics.");

    jvmMetrics.start();

    // create the configs
    NetworkConfig networkConfig = new NetworkConfig(properties);
    StoreConfig storeConfig = new StoreConfig(properties);
    MetricsConfig metricsConfig = new MetricsConfig(properties);
    // verify the configs
    properties.verify();

    logger.info("Setting up metrics reporters.");

    List<String> reporterFactoryNames = metricsConfig.getMetricsReporterFactoryClassNames();

    try {

      for (String factoryClass : reporterFactoryNames) {
        MetricsReporterFactory factory = Utils.getObj(factoryClass);
        MetricsReporter reporter = factory.getMetricsReporter(factoryClass, "Data Node", metricsConfig);
        reporters.put(factoryClass, reporter);
      }
      logger.info("Got metrics reporters: {}", reporters.keySet());
    }
    catch (Exception e) {
      logger.error("Error while creating reporters. Logging and proceeding " + e);
    }

    scheduler.startup();
    networkServer = new SocketServer(networkConfig);
    networkServer.start();
    store = new BlobStore(storeConfig, scheduler, registryMap);
    store.start();
    requests = new AmbryRequests(store, networkServer.getRequestResponseChannel());
    requestHandlerPool = new RequestHandlerPool(7, networkServer.getRequestResponseChannel(), requests);

    logger.info("Starting all the metrics reporters");
    for (Map.Entry<String, MetricsReporter> reporterEntry : reporters.entrySet()) {
      reporterEntry.getValue().register("data node", registryMap);
      reporterEntry.getValue().start();
    }
    logger.info("started");
  }

  public void shutdown() {
    try {
      logger.info("shutdown started");
      if (jmxServer != null)
        jmxServer.stop();
      if (jvmMetrics != null)
        jvmMetrics.stop();
      if (reporters != null)
        for (Map.Entry<String, MetricsReporter> reporterEntry : reporters.entrySet())
          reporterEntry.getValue().stop();
      if(networkServer != null)
        networkServer.shutdown();
      if(requestHandlerPool != null)
        requestHandlerPool.shutdown();
      if (scheduler != null) {
        scheduler.shutdown();
      }
      if (store != null) {
        store.shutdown();
      }
      shutdownLatch.countDown();
      logger.info("shutdown completed");
    }
    catch (Exception e) {
      logger.error("Error while shutting down server {}", e);
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}

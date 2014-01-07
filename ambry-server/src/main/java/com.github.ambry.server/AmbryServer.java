package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.MetricsConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobRecovery;
import com.github.ambry.metrics.JmxServer;
import com.github.ambry.metrics.JvmMetrics;
import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.MetricsReporterFactory;
import com.github.ambry.metrics.MetricsReporter;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.SocketServer;
import com.github.ambry.store.*;
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
  private StoreManager storeManager = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final JmxServer jmxServer;
  private final JvmMetrics jvmMetrics;
  private final ClusterMap clusterMap;
  private MetricsRegistryMap registryMap;
  private final Map<String, MetricsReporter> reporters;

  public AmbryServer(VerifiableProperties properties, ClusterMap clusterMap) throws IOException {
    this.properties = properties;
    // start jmx server
    jmxServer = new JmxServer();
    reporters = new HashMap<String, MetricsReporter>();
    registryMap = new MetricsRegistryMap("Blob Node");
    jvmMetrics = new JvmMetrics(registryMap);
    this.clusterMap = clusterMap;
  }

  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      logger.info("Setting up JVM metrics.");

      jvmMetrics.start();

      logger.info("creating configs");
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
          MetricsReporter reporter = factory.getMetricsReporter(factoryClass, "Blob Node", metricsConfig);
          reporters.put(factoryClass, reporter);
        }
        logger.info("Got metrics reporters: {}", reporters.keySet());
      }
      catch (Exception e) {
        logger.error("Error while creating reporters. Logging and proceeding " + e);
      }
      scheduler.startup();
      logger.info("check if node exist in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null)
        throw new IllegalArgumentException("The node is not present in the clustermap. Failing to start the datanode");

      StoreKeyFactory factory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      networkServer = new SocketServer(networkConfig);
      networkServer.start();
      storeManager = new StoreManager(storeConfig, scheduler, registryMap, clusterMap.getReplicaIds(nodeId), factory, new BlobRecovery());
      storeManager.start();
      requests = new AmbryRequests(storeManager, networkServer.getRequestResponseChannel(), clusterMap);
      requestHandlerPool = new RequestHandlerPool(7, networkServer.getRequestResponseChannel(), requests);

      logger.info("Starting all the metrics reporters");
      for (Map.Entry<String, MetricsReporter> reporterEntry : reporters.entrySet()) {
        reporterEntry.getValue().register("data node", registryMap);
        reporterEntry.getValue().start();
      }
      logger.info("started");
    }
    catch (Exception e) {
      logger.error("Error during startup {}", e);
      throw new InstantiationException("failure during startup " + e);
    }
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
      if (storeManager != null) {
        storeManager.shutdown();
      }
      logger.info("shutdown completed");
    }
    catch (Exception e) {
      logger.error("Error while shutting down server {}", e);
    }
    finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}

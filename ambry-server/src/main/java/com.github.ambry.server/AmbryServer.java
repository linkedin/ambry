package com.github.ambry.server;

import com.codahale.metrics.JmxReporter;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.*;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.SocketServer;
import com.github.ambry.store.*;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.MetricRegistry;

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
  private final ClusterMap clusterMap;
  private MetricRegistry registry;
  private JmxReporter reporter;

  public AmbryServer(VerifiableProperties properties, ClusterMap clusterMap) throws IOException {
    this.properties = properties;
    registry = new MetricRegistry();
    this.clusterMap = clusterMap;
  }

  public void startup() throws InstantiationException {
    try {
      logger.info("starting");
      logger.info("Setting up JMX.");
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      MetricsConfig metricsConfig = new MetricsConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      // verify the configs
      properties.verify();

      scheduler.startup();
      logger.info("check if node exist in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null)
        throw new IllegalArgumentException("The node is not present in the clustermap. Failing to start the datanode");

      StoreKeyFactory factory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      networkServer = new SocketServer(networkConfig, registry);
      networkServer.start();
      storeManager = new StoreManager(storeConfig,
                                      scheduler,
                                      registry,
                                      clusterMap.getReplicaIds(nodeId),
                                      factory,
                                      new BlobStoreRecovery());
      storeManager.start();
      requests = new AmbryRequests(storeManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId, registry);
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
                                                  networkServer.getRequestResponseChannel(),
                                                  requests);
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
      if (reporter != null)
        reporter.stop();
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

package com.github.ambry.server;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.network.SocketServer;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.shared.BlockingChannelConnectionPool;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.LoggingNotificationSystem;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.store.StoreManager;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ambry server
 */
public class AmbryServer {

  private CountDownLatch shutdownLatch = new CountDownLatch(1);
  private NetworkServer networkServer = null;
  private AmbryRequests requests = null;
  private RequestHandlerPool requestHandlerPool = null;
  private Scheduler scheduler = null;
  private StoreManager storeManager = null;
  private ReplicationManager replicationManager = null;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final VerifiableProperties properties;
  private final ClusterMap clusterMap;
  private MetricRegistry registry = null;
  private JmxReporter reporter = null;
  private ConnectionPool connectionPool = null;
  private final NotificationSystem notificationSystem;

  public AmbryServer(VerifiableProperties properties, ClusterMap clusterMap)
      throws IOException {
    this(properties, clusterMap, new LoggingNotificationSystem());
  }

  public AmbryServer(VerifiableProperties properties, ClusterMap clusterMap, NotificationSystem notificationSystem)
      throws IOException {
    this.properties = properties;
    this.clusterMap = clusterMap;
    this.notificationSystem = notificationSystem;
  }

  public void startup()
      throws InstantiationException {
    try {
      logger.info("starting");
      logger.info("Setting up JMX.");
      registry = clusterMap.getMetricRegistry();
      reporter = JmxReporter.forRegistry(registry).build();
      reporter.start();

      logger.info("creating configs");
      NetworkConfig networkConfig = new NetworkConfig(properties);
      StoreConfig storeConfig = new StoreConfig(properties);
      ServerConfig serverConfig = new ServerConfig(properties);
      ReplicationConfig replicationConfig = new ReplicationConfig(properties);
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(properties);
      // verify the configs
      properties.verify();

      scheduler = new Scheduler(serverConfig.serverSchedulerNumOfthreads, false);
      scheduler.startup();
      logger.info("check if node exist in clustermap host {} port {}", networkConfig.hostName, networkConfig.port);
      DataNodeId nodeId = clusterMap.getDataNodeId(networkConfig.hostName, networkConfig.port);
      if (nodeId == null) {
        throw new IllegalArgumentException("The node " + networkConfig.hostName + ":" + networkConfig.port +
            "is not present in the clustermap. Failing to start the datanode");
      }

      StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, clusterMap);
      FindTokenFactory findTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
      storeManager =
          new StoreManager(storeConfig, scheduler, registry, clusterMap.getReplicaIds(nodeId), storeKeyFactory,
              new BlobStoreRecovery());
      storeManager.start();

      networkServer = new SocketServer(networkConfig, registry);

      connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig, registry);
      connectionPool.start();

      replicationManager =
          new ReplicationManager(replicationConfig, storeConfig, storeManager, storeKeyFactory, clusterMap, scheduler,
              nodeId, connectionPool, registry, notificationSystem);
      replicationManager.start();

      requests =
          new AmbryRequests(storeManager, networkServer.getRequestResponseChannel(), clusterMap, nodeId, registry,
              findTokenFactory, notificationSystem, replicationManager);
      requestHandlerPool = new RequestHandlerPool(serverConfig.serverRequestHandlerNumOfThreads,
          networkServer.getRequestResponseChannel(), requests);
      networkServer.start();

      logger.info("started");
    } catch (Exception e) {
      logger.error("Error during startup", e);
      throw new InstantiationException("failure during startup " + e);
    }
  }

  public void shutdown() {
    try {
      logger.info("shutdown started");

      if (scheduler != null) {
        scheduler.shutdown();
      }
      if (networkServer != null) {
        networkServer.shutdown();
      }
      if (requestHandlerPool != null) {
        requestHandlerPool.shutdown();
      }
      if (replicationManager != null) {
        replicationManager.shutdown();
      }
      if (storeManager != null) {
        storeManager.shutdown();
      }
      if (connectionPool != null) {
        connectionPool.shutdown();
      }
      if (reporter != null) {
        reporter.stop();
      }
      if (notificationSystem != null) {
        try {
          notificationSystem.close();
        } catch (IOException e) {
          logger.error("Error while closing notification system.", e);
        }
      }

      logger.info("shutdown completed");
    } catch (Exception e) {
      logger.error("Error while shutting down server", e);
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown()
      throws InterruptedException {
    shutdownLatch.await();
  }
}

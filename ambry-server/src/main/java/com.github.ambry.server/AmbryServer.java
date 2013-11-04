package com.github.ambry.server;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkServer;
import com.github.ambry.store.Store;
import com.github.ambry.network.SocketServer;
import com.github.ambry.store.BlobStore;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.store.IndexCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;

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

  public AmbryServer(VerifiableProperties properties) {
    this.properties = properties;
  }

  public void startup() throws InterruptedException, IOException, IndexCreationException {
    logger.info("starting");
    // create the configs
    NetworkConfig config = new NetworkConfig(properties);
    scheduler.startup();

    // verify the configs
    properties.verify();
    networkServer = new SocketServer(config);
    networkServer.start();
    store = new BlobStore("/Users/srsubram/testdir", scheduler);
    requests = new AmbryRequests(store, networkServer.getRequestResponseChannel());
    requestHandlerPool = new RequestHandlerPool(7, networkServer.getRequestResponseChannel(), requests);
    logger.info("started");
  }

  public void shutdown() {
    logger.info("shutdown started");
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

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}

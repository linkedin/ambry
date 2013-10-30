package com.github.ambry.server;

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
import java.util.Properties;

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
  private final Properties properties;

  public AmbryServer(Properties properties) {
    this.properties = properties;
  }

  public void startup() throws InterruptedException, IOException, IndexCreationException {
    logger.info("starting");
    networkServer = new SocketServer("localhost", 8990, 7, 16, 1000, 1000, 1000);
    networkServer.start();
    requests = new AmbryRequests(networkServer.getRequestResponseChannel());
    requestHandlerPool = new RequestHandlerPool(7, networkServer.getRequestResponseChannel(), requests);
    store = new BlobStore("/home/srsubram/testdir", scheduler);
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

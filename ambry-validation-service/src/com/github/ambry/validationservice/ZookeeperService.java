package com.github.ambry.validationservice;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to instantiate a standalone Zookeeper instance
 */
public class ZookeeperService {

  private final String zkAddress;
  private final String rootDirPath;
  private ZkServer server;
  private static Logger logger = LoggerFactory.getLogger(ZookeeperService.class);
  private CountDownLatch shutdownLatch = new CountDownLatch(1);

  public ZookeeperService(String zkAddress, String rootDirPath) {
    this.zkAddress = zkAddress;
    this.rootDirPath = rootDirPath;
  }

  public static void main(String[] args) {
    final ZookeeperService zookeeperService;
    int exitCode = 0;
    try {
      final ZookeeperService.InvocationOptions options = new ZookeeperService.InvocationOptions(args);
      logger.info("Bootstrapping ValidationServiceOld");
      zookeeperService = new ZookeeperService(options.zkAddress, options.rootDirPath);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down ValidationServiceOld");
          zookeeperService.shutdown();
        }
      });
      zookeeperService.startup();
      zookeeperService.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of ValidationServiceOld", e);
      exitCode = 1;
    }
    logger.info("Exiting ValidationService");
    System.exit(exitCode);
  }

  /**
   * Starts up a standalone zookeeper instance
   */
  public void startup() {
    logger.info("STARTING Zookeeper at " + zkAddress);
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient) {
      }
    };
    new File(rootDirPath).mkdirs();
    // start zookeeper
    int port = Integer.valueOf(zkAddress.substring(zkAddress.indexOf(':') + 1));
    server = new ZkServer(rootDirPath + "/dataDir", rootDirPath + "/logDir", defaultNameSpace, port);
    server.start();
  }

  public void shutdown() {
    try {
      server.shutdown();
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  static class InvocationOptions {
    String zkAddress;
    String rootDirPath;

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args the command line argument list.
     * @throws InstantiationException if all required arguments were not provided.
     * @throws IOException if help text could not be printed.
     */
    public InvocationOptions(String args[]) throws InstantiationException, IOException {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> zkAddressOpt = parser.accepts("zkAddress", "Zookeeper end point address")
          .withRequiredArg()
          .describedAs("zkAddress")
          .ofType(String.class)
          .defaultsTo("localhost:2199");

      ArgumentAcceptingOptionSpec<String> rootDirPathOpt =
          parser.accepts("rootDirPath", "Root Directory path or Zookeeper")
              .withRequiredArg()
              .describedAs("rootDirPath")
              .ofType(String.class)
              .defaultsTo("/tmp/ambryDevCluster");

      OptionSet options = parser.parse(args);
      this.zkAddress = options.valueOf(zkAddressOpt);
      logger.trace("ZK Address : {}", this.zkAddress);
      this.rootDirPath = options.valueOf(rootDirPathOpt);
      logger.trace("Root Directory Parth : {} ", this.rootDirPath);
    }
  }
}

package com.github.ambry.validationservice;

import java.util.concurrent.CountDownLatch;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Instantiates Helix Controller for Ambry Validation Cluster
 */
public class ValidationController {
  private static Logger logger = LoggerFactory.getLogger(ValidationController.class);
  private CountDownLatch shutdownLatch = new CountDownLatch(1);

  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> zkAddressOpt = parser.accepts("zkAddress", "Zookeeper end point address")
        .withRequiredArg()
        .describedAs("zkAddress")
        .ofType(String.class)
        .defaultsTo("localhost:2199");

    ArgumentAcceptingOptionSpec<String> clusterNameOpt =
        parser.accepts("clusterName", "Cluster name of the Ambry's validation service")
            .withRequiredArg()
            .describedAs("clusterName")
            .ofType(String.class)
            .defaultsTo("AmbryDev_Cluster");

    OptionSet options = parser.parse(args);
    String zkAddress = options.valueOf(zkAddressOpt);
    String clusterName = options.valueOf(clusterNameOpt);

    ValidationController validationController = new ValidationController();
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        logger.info("Received shutdown signal. Shutting down Validation Controller");
        validationController.shutdown();
      }
    });
    validationController.startupController(zkAddress, clusterName);
    validationController.awaitShutdown();
  }

  private void startupController(String zkAddress, String clusterName) throws Exception {
    // start controller
    logger.info("Starting Helix Controller");
    HelixManager manager = HelixControllerMain.startHelixController(zkAddress, clusterName, "localhost_9100",
        HelixControllerMain.STANDALONE);
    manager.connect();
  }

  public void shutdown() {
    try {
    } finally {
      shutdownLatch.countDown();
    }
  }

  public void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }
}

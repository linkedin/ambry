package com.github.ambry;

import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Start point for creating an instance of {@link ValidationService} and starting/shutting it down.
 */
public class ValidationServiceMain {
  private static Logger logger = LoggerFactory.getLogger(ValidationServiceMain.class);

  public static void main(String[] args) {
    final ValidationService validationService;
    int exitCode = 0;
    try {
      final InvocationOptions options = new InvocationOptions(args);
      final String zkAddress = options.zkAddress;
      // final VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      logger.info("Bootstrapping ValidationService");
      validationService = new ValidationService(zkAddress, options.clusterName, options.hostName, options.servicePort,
          options.stateModelName, SystemTime.getInstance());
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Shutting down ValidationService");
          validationService.shutdown();
        }
      });
      validationService.startup();
      validationService.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of ValidationService", e);
      exitCode = 1;
    }
    logger.info("Exiting ValidationServiceMain");
    System.exit(exitCode);
  }

  static class InvocationOptions {
    String zkAddress;
    String hostName;
    int servicePort;
    String clusterName;
    String stateModelName;

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
          .ofType(String.class).defaultsTo("localhost:2199");

      ArgumentAcceptingOptionSpec<String> hostNameOpt = parser.accepts("hostName", "HostName of this host")
          .withRequiredArg()
          .describedAs("hostName")
          .ofType(String.class).defaultsTo("localhost");

      ArgumentAcceptingOptionSpec<Integer> servicePortOpt =
          parser.accepts("servicePort", "Validation Service port number")
              .withRequiredArg()
              .describedAs("servicePort")
              .ofType(Integer.class)
              .defaultsTo(6999);

      ArgumentAcceptingOptionSpec<String> clusterNameOpt =
          parser.accepts("clusterName", "Cluster name of the Ambry's " + "validation service")
              .withRequiredArg()
              .describedAs("clusterName")
              .ofType(String.class).defaultsTo("Trail_Cluster");

      ArgumentAcceptingOptionSpec<String> stateModelNameOpt = parser.accepts("stateModelName",
          "SateModelName that" + "this node should register itself while adding as participant")
          .withRequiredArg()
          .describedAs("stateModelName")
          .ofType(String.class).defaultsTo("OnlineOfflineStateModel");

      OptionSet options = parser.parse(args);
        this.zkAddress = options.valueOf(zkAddressOpt);
        logger.trace("ZK Address : {}", this.zkAddress);
        this.hostName = options.valueOf(hostNameOpt);
        logger.trace("Host Name : {}", this.hostName);
        this.servicePort = options.valueOf(servicePortOpt);
        logger.trace("Service Port : {}", this.servicePort);
        this.clusterName = options.valueOf(clusterNameOpt);
        logger.trace("ClusterName : {}", this.clusterName);
        this.stateModelName = options.valueOf(stateModelNameOpt);
        logger.trace("SateModelName : {}", this.stateModelName);
    }
  }
}
package com.github.ambry.restservice;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Start point for creating an instance of {@link RestServer} and starting/shutting it down.
 */
public class RestServerMain {
  private static Logger logger = LoggerFactory.getLogger(RestServerMain.class);

  public static void main(String[] args) {
    final RestServer restServer;
    try {
      final InvocationOptions options = new InvocationOptions(args);
      PropertyConfigurator.configure(options.logPropsFile);
      final Properties properties = Utils.loadProps(options.propsFilePath);
      final VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      final MetricRegistry metricRegistry = new MetricRegistry();
      final ClusterMap clusterMap =
          new ClusterMapManager(options.hardwareLayoutFilePath, options.partitionLayoutFilePath,
              new ClusterMapConfig(verifiableProperties));

      logger.info("Bootstrapping RestServer..");
      restServer = new RestServer(verifiableProperties, metricRegistry, clusterMap);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          restServer.shutdown();
        }
      });
      restServer.start();
      restServer.awaitShutdown();
    } catch (Exception e) {
      logger.error("RestServerMain failed", e);
    }
    System.exit(0);
  }
}

/**
 * Abstraction class for all the parameters we expect to receive.
 */
class InvocationOptions {
  public final String hardwareLayoutFilePath;
  public final String logPropsFile;
  public final String partitionLayoutFilePath;
  public final String propsFilePath;

  /**
   * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
   * @param args - the command line argument list.
   * @throws InstantiationException
   * @throws IOException
   */
  public InvocationOptions(String args[])
      throws InstantiationException, IOException {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> hardwareLayoutFilePath =
        parser.accepts("hardwareLayoutFilePath", "Path to hardware layout file").withRequiredArg()
            .describedAs("hardwareLayoutFilePath").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> logPropsFilePath =
        parser.accepts("logPropsFilePath", "Path to log4j properties file").withRequiredArg()
            .describedAs("logPropsFilePath").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> partitionLayoutFilePath =
        parser.accepts("partitionLayoutFilePath", "Path to partition layout file").withRequiredArg()
            .describedAs("partitionLayoutFilePath").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> propsFilePath =
        parser.accepts("propsFilePath", "Path to properties file").withRequiredArg().describedAs("propsFilePath")
            .ofType(String.class);

    ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<OptionSpec<?>>();
    requiredArgs.add(hardwareLayoutFilePath);
    requiredArgs.add(logPropsFilePath);
    requiredArgs.add(partitionLayoutFilePath);
    requiredArgs.add(propsFilePath);

    OptionSet options = parser.parse(args);
    if (hasRequiredOptions(requiredArgs, options)) {
      this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
      this.logPropsFile = options.valueOf(logPropsFilePath);
      this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
      this.propsFilePath = options.valueOf(propsFilePath);
    } else {
      parser.printHelpOn(System.err);
      throw new InstantiationException("Did not receive all required arguments");
    }
  }

  /**
   * Checks if all required arguments are present. Prints the ones that are not.
   * @param requiredArgs - the list of required arguments.
   * @param options - the list of received options.
   * @return - whether required options are present.
   * @throws IOException
   */
  private boolean hasRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options)
      throws IOException {
    boolean haveAll = true;
    for (OptionSpec opt : requiredArgs) {
      if (!options.has(opt)) {
        System.err.println("Missing required argument \"" + opt + "\"");
        haveAll = false;
      }
    }
    return haveAll;
  }
}

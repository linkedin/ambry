package com.github.ambry.rest;

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
import org.json.JSONException;
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
      if (!options.hasError()) {
        PropertyConfigurator.configure(options.getLogPropsFile());
        final Properties properties = Utils.loadProps(options.getPropsFilePath());
        final VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
        final MetricRegistry metricRegistry = new MetricRegistry();
        final ClusterMap clusterMap =
            new ClusterMapManager(options.getHardwareLayoutFilePath(), options.getPartitionLayoutFilePath(),
                new ClusterMapConfig(verifiableProperties));

        logger.info("Bootstrapping rest server..");
        restServer = new RestServer(verifiableProperties, metricRegistry, clusterMap);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            restServer.shutdown();
          }
        });

        restServer.start();
        // block until RestServer shuts down.
        restServer.awaitShutdown();
      }
    } catch (JSONException e) {
      logger.error("Cluster map load failed.", e);
    } catch (InstantiationException e) {
      logger.error("InstantiationException while starting RestServer.", e);
    } catch (IOException e) {
      logger.error("Options parse failed or properties file was not loaded.", e);
    } catch (Exception e) {
      logger.error("RestServerMain failed", e);
    }
  }

  private static class InvocationOptions {
    private final boolean hasError;
    private final String hardwareLayoutFilePath;
    private final String logPropsFile;
    private final String partitionLayoutFilePath;
    private final String propsFilePath;

    public boolean hasError() {
      return hasError;
    }

    private String getHardwareLayoutFilePath() {
      return hardwareLayoutFilePath;
    }

    private String getLogPropsFile() {
      return logPropsFile;
    }

    private String getPartitionLayoutFilePath() {
      return partitionLayoutFilePath;
    }

    public String getPropsFilePath() {
      return propsFilePath;
    }

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
     * @param args - the command line argument list.
     * @throws IOException
     */
    public InvocationOptions(String args[])
        throws IOException {
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

      if (haveRequiredOptions(requiredArgs, options)) {
        this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
        this.logPropsFile = options.valueOf(logPropsFilePath);
        this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
        this.propsFilePath = options.valueOf(propsFilePath);
        this.hasError = false;
      } else {
        this.hardwareLayoutFilePath = null;
        this.logPropsFile = null;
        this.partitionLayoutFilePath = null;
        this.propsFilePath = null;
        this.hasError = true;
        parser.printHelpOn(System.err);
      }
    }

    /**
     * Checks if all required options are present. Prints required arguments that are not.
     * @param requiredArgs - the list of required arguments.
     * @param options - the list of received options.
     * @return
     * @throws IOException
     */
    private boolean haveRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options)
        throws IOException {
      for (OptionSpec opt : requiredArgs) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          return false;
        }
      }
      return true;
    }
  }
}

package com.github.ambry.admin;

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
 * Start point for creating an instance of AdminServer
 */
public class AdminMain {

  private static Logger logger = LoggerFactory.getLogger(AdminMain.class);

  public static void main(String[] args) {
    final AdminServer adminServer;
    try {
      // TODO: These two lines have to be replaced by something generic
      String log4jConfPath =
          "/Users/gholla/Documents/Work/Ambry/Code/IndividualCode/gholla_ambry/config/" + "log4j.properties";
      PropertyConfigurator.configure(log4jConfPath);

      InvocationOptions options = new InvocationOptions();
      options.parseOptions(args);

      if (!options.hasError()) {
        //TODO: all this loaded differently in LI
        Properties properties = Utils.loadProps(options.getPropsFilePath());
        VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
        MetricRegistry metricRegistry = new MetricRegistry();
        ClusterMap clusterMap =
            new ClusterMapManager(options.getHardwareLayoutFilePath(), options.getPartitionLayoutFilePath(),
                new ClusterMapConfig(verifiableProperties));

        logger.info("Bootstrapping admin..");
        adminServer = new AdminServer(verifiableProperties, metricRegistry, clusterMap);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            try {
              adminServer.shutdown();
            } catch (Exception e) {
              logger.error("Shutdown threw exception - " + e);
            }
          }
        });

        adminServer.start();
        adminServer.awaitShutdown();
      }
    } catch (JSONException e) {
      logger.error("Cluster map load failed - " + e);
    } catch (InstantiationException e) {
      logger.error("InstantiationException while starting admin - " + e);
      logger.error("Admin bootstrap failed");
    } catch (IOException e) {
      logger.error("Options parse failed or properties file was not loaded - " + e);
    } catch (Exception e) {
      logger.error("Exception - " + e);
    }
  }

  private static class InvocationOptions {
    private boolean hasError = false;
    private String propsFilePath = null;
    private String hardwareLayoutFilePath = null;
    private String partitionLayoutFilePath = null;

    public boolean hasError() {
      return hasError;
    }

    public String getPropsFilePath() {
      return propsFilePath;
    }

    private String getHardwareLayoutFilePath() {
      return hardwareLayoutFilePath;
    }

    private String getPartitionLayoutFilePath() {
      return partitionLayoutFilePath;
    }

    public void parseOptions(String args[])
        throws IOException {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> propsFilePath =
          parser.accepts("propsFilePath", "Path to properties file").withRequiredArg().describedAs("propsFilePath")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> hardwareLayoutFilePath =
          parser.accepts("hardwareLayoutFilePath", "Path to hardware layout file").withRequiredArg()
              .describedAs("hardwareLayoutFilePath").ofType(String.class);
      ArgumentAcceptingOptionSpec<String> partitionLayoutFilePath =
          parser.accepts("partitionLayoutFilePath", "Path to partition layout file").withRequiredArg()
              .describedAs("partitionLayoutFilePath").ofType(String.class);

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<OptionSpec<?>>();
      requiredArgs.add(propsFilePath);
      requiredArgs.add(hardwareLayoutFilePath);
      requiredArgs.add(partitionLayoutFilePath);

      OptionSet options = parser.parse(args);

      if (haveRequiredOptions(parser, requiredArgs, options)) {
        this.propsFilePath = options.valueOf(propsFilePath);
        this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
        this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
      } else {
        hasError = true;
      }
    }

    private boolean haveRequiredOptions(OptionParser parser, ArrayList<OptionSpec<?>> requiredArgs, OptionSet options)
        throws IOException {
      boolean success = true;
      for (OptionSpec opt : requiredArgs) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          success = false;
        }
      }
      if (!success) {
        parser.printHelpOn(System.err);
      }
      return success;
    }
  }
}

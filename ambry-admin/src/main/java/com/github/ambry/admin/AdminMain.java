package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
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
        Properties properties = Utils.loadProps(options.getPropertiesFileLocation());
        VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
        //TODO: Retreive metrics registry from somewhere?
        MetricRegistry metricRegistry = new MetricRegistry();

        logger.info("Bootstrapping admin..");
        adminServer = new AdminServer(verifiableProperties, metricRegistry);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            adminServer.shutdown();
          }
        });

        adminServer.start();
        adminServer.awaitShutdown();
      }
    } catch (InterruptedException e) {
      logger.error("Await shutdown interuptted - " + e);
    } catch (IOException e) {
      logger.error("Options parse failed or properties file was not loaded - " + e);
      logger.error("Admin bootstrap failed");
    } catch (InstantiationException e) {
      logger.error("InstantiationException while starting admin - " + e);
      logger.error("Admin bootstrap failed");
    }
  }

  private static class InvocationOptions {
    private boolean hasError = false;
    private String propertiesFileLocation = null;

    public boolean hasError() {
      return hasError;
    }

    public String getPropertiesFileLocation() {
      if (!hasError()) {
        return propertiesFileLocation;
      }
      return null;
    }

    public void parseOptions(String args[])
        throws IOException {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> propsFileLocation =
          parser.accepts("propsFileLocation", "Path to properties file").withRequiredArg()
              .describedAs("propsFileLocation").ofType(String.class);

      ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<OptionSpec<?>>();
      requiredArgs.add(propsFileLocation);

      OptionSet options = parser.parse(args);

      if (haveRequiredOptions(parser, requiredArgs, options)) {
        propertiesFileLocation = options.valueOf(propsFileLocation);
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
          parser.printHelpOn(System.err);
          success = false;
        }
      }
      return success;
    }
  }
}

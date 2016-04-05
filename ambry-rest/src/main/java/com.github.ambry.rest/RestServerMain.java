/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.commons.LoggingNotificationSystem;
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
      final Properties properties = Utils.loadProps(options.serverPropsFilePath);
      final VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      final ClusterMap clusterMap =
          new ClusterMapManager(options.hardwareLayoutFilePath, options.partitionLayoutFilePath,
              new ClusterMapConfig(verifiableProperties));
      logger.info("Bootstrapping RestServer");
      restServer = new RestServer(verifiableProperties, clusterMap, new LoggingNotificationSystem());
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Requesting RestServer shutdown");
          restServer.shutdown();
        }
      });
      restServer.start();
      restServer.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during bootstrap of RestServer", e);
    } finally {
      logger.info("Exiting RestServerMain");
      System.exit(0);
    }
  }
}

/**
 * Abstraction class for all the parameters we expect to receive.
 */
class InvocationOptions {
  public final String hardwareLayoutFilePath;
  public final String logPropsFile;
  public final String partitionLayoutFilePath;
  public final String serverPropsFilePath;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
   * @param args the command line argument list.
   * @throws InstantiationException if all required arguments were not provided.
   * @throws IOException if help text could not be printed.
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
    ArgumentAcceptingOptionSpec<String> serverPropsFilePath =
        parser.accepts("serverPropsFilePath", "Path to server properties file").withRequiredArg()
            .describedAs("serverPropsFilePath").ofType(String.class);

    ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<OptionSpec<?>>();
    requiredArgs.add(hardwareLayoutFilePath);
    requiredArgs.add(logPropsFilePath);
    requiredArgs.add(partitionLayoutFilePath);
    requiredArgs.add(serverPropsFilePath);

    OptionSet options = parser.parse(args);
    if (hasRequiredOptions(requiredArgs, options)) {
      this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
      logger.trace("Hardware layout file path: {}", this.hardwareLayoutFilePath);
      this.logPropsFile = options.valueOf(logPropsFilePath);
      logger.trace("Log4j properties file path: {}", this.logPropsFile);
      this.partitionLayoutFilePath = options.valueOf(partitionLayoutFilePath);
      logger.trace("Partition layout file path: {}", this.partitionLayoutFilePath);
      this.serverPropsFilePath = options.valueOf(serverPropsFilePath);
      logger.trace("Server properties file path: {}", this.serverPropsFilePath);
    } else {
      parser.printHelpOn(System.err);
      throw new InstantiationException("Did not receive all required arguments for starting RestServer");
    }
  }

  /**
   * Checks if all required arguments are present. Prints the ones that are not.
   * @param requiredArgs the list of required arguments.
   * @param options the list of received options.
   * @return whether required options are present.
   */
  private boolean hasRequiredOptions(ArrayList<OptionSpec<?>> requiredArgs, OptionSet options) {
    boolean haveAll = true;
    for (OptionSpec opt : requiredArgs) {
      if (!options.has(opt)) {
        System.err.println("Missing required argument " + opt);
        haveAll = false;
      }
    }
    return haveAll;
  }
}

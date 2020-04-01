/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import java.io.IOException;
import java.util.ArrayList;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstraction class for all the parameters we expect to receive to start Ambry frontend and data nodes.
 */
public class InvocationOptions {
  public final String hardwareLayoutFilePath;
  public final String partitionLayoutFilePath;
  public final String serverPropsFilePath;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Parses the arguments provided and extracts them into variables that can be retrieved through APIs.
   * @param args the command line argument list.
   * @throws InstantiationException if all required arguments were not provided.
   * @throws IOException if help text could not be printed.
   */
  public InvocationOptions(String args[]) throws InstantiationException, IOException {
    OptionParser parser = new OptionParser();
    ArgumentAcceptingOptionSpec<String> hardwareLayoutFilePath =
        parser.accepts("hardwareLayoutFilePath", "Path to hardware layout file")
            .withRequiredArg()
            .describedAs("hardwareLayoutFilePath")
            .ofType(String.class);
    ArgumentAcceptingOptionSpec<String> partitionLayoutFilePath =
        parser.accepts("partitionLayoutFilePath", "Path to partition layout file")
            .withRequiredArg()
            .describedAs("partitionLayoutFilePath")
            .ofType(String.class);
    ArgumentAcceptingOptionSpec<String> serverPropsFilePath =
        parser.accepts("serverPropsFilePath", "Path to server properties file")
            .withRequiredArg()
            .describedAs("serverPropsFilePath")
            .ofType(String.class);

    ArrayList<OptionSpec<?>> requiredArgs = new ArrayList<>();
    requiredArgs.add(hardwareLayoutFilePath);
    requiredArgs.add(partitionLayoutFilePath);
    requiredArgs.add(serverPropsFilePath);

    OptionSet options = parser.parse(args);
    if (hasRequiredOptions(requiredArgs, options)) {
      this.hardwareLayoutFilePath = options.valueOf(hardwareLayoutFilePath);
      logger.trace("Hardware layout file path: {}", this.hardwareLayoutFilePath);
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

/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static com.github.ambry.clustermap.HelixVcrUtil.*;


/**
 * This tool provides function to create vcr cluster and update vcr cluster by referencing src cluster.
 */
public class HelixVcrPopulateTool {

  public static void main(String[] args) throws IOException {
    OptionParser parser = new OptionParser();
    OptionSpec createClusterOpt = parser.accepts("createCluster",
        "Create cluster in dest zk(no resource creation). --createCluster --dest destZkEndpoint/destClusterName --config configFilePath");

    OptionSpec updateClusterOpt = parser.accepts("updateCluster",
        "Update resources in dest by copying from src to dest. --updateCluster [--src srcZkEndpoint/srcClusterName] --dest destZkEndpoint/destClusterName --config configFilePath");
    OptionSpec dryRunOpt = parser.accepts("dryRun", "Do dry run.");

    OptionSpec controlResourceOpt = parser.accepts("controlResource",
        "Enable/Disable a resource. --controlResource --dest destZkEndpoint/destClusterName --resource resource --enable true");
    ArgumentAcceptingOptionSpec<String> resourceOpt =
        parser.accepts("resource").withRequiredArg().describedAs("resource name").ofType(String.class);

    OptionSpec maintenanceOpt = parser.accepts("maintainCluster",
        "Enter/Exit helix maintenance mode. --maintainCluster --dest destZkEndpoint/destClusterName --enable true");

    // Some shared options.
    // VCR cluster argument is always required
    ArgumentAcceptingOptionSpec<String> destOpt =
        parser.accepts("dest").withRequiredArg().required().describedAs("vcr zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> srcOpt =
        parser.accepts("src").withRequiredArg().describedAs("src zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<Boolean> enableOpt =
        parser.accepts("enable").withRequiredArg().describedAs("enable/disable").ofType(Boolean.class);
    ArgumentAcceptingOptionSpec<String> configFileOpt =
        parser.accepts("config").withRequiredArg().describedAs("config file path").ofType(String.class);

    OptionSet options = parser.parse(args);

    String[] destZkAndCluster = options.valueOf(destOpt).split(HelixVcrUtil.SEPARATOR);
    if (destZkAndCluster.length != 2) {
      errorAndExit("dest argument must have form 'zkString/clusterName'");
    }
    String destZkString = destZkAndCluster[0];
    String destClusterName = destZkAndCluster[1];
    if (!destClusterName.contains("VCR")) {
      errorAndExit("dest should be a VCR cluster.(VCR string should be included)");
    }

    VcrHelixConfig config = null;
    if (options.has(createClusterOpt) || options.has(updateClusterOpt)) {
      try {
        config = new ObjectMapper().readValue(Utils.readStringFromFile(options.valueOf(configFileOpt)),
            VcrHelixConfig.class);
      } catch (IOException ioEx) {
        errorAndExit("Couldn't read the config file: " + options.valueOf(configFileOpt));
      }
    }

    if (options.has(createClusterOpt)) {
      System.out.println("Creating cluster: " + destClusterName);
      createCluster(destZkString, destClusterName, config);
    }

    if (options.has(updateClusterOpt)) {
      boolean dryRun = options.has(dryRunOpt);
      if (options.has(srcOpt)) {
        String[] srcZkAndCluster = options.valueOf(srcOpt).split(SEPARATOR);
        if (srcZkAndCluster.length != 2) {
          errorAndExit("src argument must have form 'zkString/clusterName'");
        }
        String srcZkString = srcZkAndCluster[0];
        String srcClusterName = srcZkAndCluster[1];
        System.out.println("Updating cluster: " + destClusterName + " by checking " + srcClusterName);
        updateResourceAndPartition(srcZkString, srcClusterName, destZkString, destClusterName, config, dryRun);
      } else {
        System.out.println("Updating cluster config for: " + destClusterName);
        // Update the cluster config and resources to the latest settings.
        setClusterConfig(getHelixZkClient(destZkString), destClusterName, config, dryRun);
        updateResourceIdealState(destZkString, destClusterName, config, dryRun);
        if (!dryRun) {
          System.out.println("Cluster " + destClusterName + " is updated successfully!");
        }
      }
    }

    if (options.has(controlResourceOpt)) {
      String resourceName = options.valueOf(resourceOpt);
      Boolean enable = options.valueOf(enableOpt);
      controlResource(destZkString, destClusterName, resourceName, enable);
      System.out.println("Resource " + resourceName + " status: " + enable);
    }

    if (options.has(maintenanceOpt)) {
      boolean maintenanceMode = options.valueOf(enableOpt);
      maintainCluster(destZkString, destClusterName, maintenanceMode);
      System.out.println("Cluster " + destClusterName + " maintenance mode: " + maintenanceMode);
    }
    System.out.println("Done.");
  }
}

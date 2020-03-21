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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;


/**
 * This tool provides function to create vcr cluster and update vcr cluster by referencing src cluster.
 */
public class HelixVcrPopulateTool {

  private static String SEPARATOR = "/";
  static List<String> ignoreResourceKeyWords = Arrays.asList("aggregation", "trigger", "stats");
  private static int replicaNumber = 3;

  public static void main(String[] args) {
    OptionParser parser = new OptionParser();
    OptionSpec createClusterOpt = parser.accepts("createCluster",
        "Create cluster in dest zk(no resource creation). --createCluster --dest destZkEndpoint/destClusterName");

    OptionSpec updateClusterOpt = parser.accepts("updateCluster",
        "Update resources in dest by copying from src to dest. --updateCluster"
            + " --src srcZkEndpoint/srcClusterName --dest destZkEndpoint/destClusterName");
    OptionSpec dryRunOpt = parser.accepts("dryRun", "Do dry run.");

    OptionSpec controlResourceOpt = parser.accepts("controlResource",
        "Enable/Disable a resource. --controlResource --dest destZkEndpoint/destClusterName --resource resource --enable true");
    ArgumentAcceptingOptionSpec<String> resourceOpt =
        parser.accepts("resource").withRequiredArg().describedAs("resource name").ofType(String.class);

    ArgumentAcceptingOptionSpec<Boolean> maintenanceOpt = parser.accepts("maintainCluster",
        "Enter/Exit helix maintenance mode. --maintainCluster --dest destZkEndpoint/destClusterName --enable true")
        .withRequiredArg()
        .ofType(Boolean.class);

    // Some shared options.
    ArgumentAcceptingOptionSpec<String> srcOpt =
        parser.accepts("src").withRequiredArg().describedAs("src zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> destOpt =
        parser.accepts("dest").withRequiredArg().describedAs("dest zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<Boolean> enableOpt =
        parser.accepts("enable").withRequiredArg().describedAs("enable/disable").ofType(Boolean.class);

    OptionSet options = parser.parse(args);

    String destZkString = options.valueOf(destOpt).split(SEPARATOR)[0];
    String destClusterName = options.valueOf(destOpt).split(SEPARATOR)[1];
    if (!destClusterName.contains("VCR")) {
      System.err.println("dest should be a VCR cluster.(VCR string should be included)");
      return;
    }

    if (options.has(createClusterOpt)) {
      System.out.println("Creating cluster: " + destClusterName);
      createCluster(destZkString, destClusterName);
    }

    if (options.has(updateClusterOpt)) {
      String srcZkString = options.valueOf(srcOpt).split(SEPARATOR)[0];
      String srcClusterName = options.valueOf(srcOpt).split(SEPARATOR)[1];
      System.out.println("Updating cluster: " + destClusterName + "by checking " + srcClusterName);
      boolean dryRun = options.has(dryRunOpt);
      updateResourceAndPartition(srcZkString, srcClusterName, destZkString, destClusterName, dryRun);
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

  /**
   * Create a helix cluster with given information.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   */
  static void createCluster(String destZkString, String destClusterName) {
    HelixZkClient destZkClient =
        DedicatedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(destZkString));
    destZkClient.setZkSerializer(new ZNRecordSerializer());
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    if (destAdmin.getClusters().contains(destClusterName)) {
      System.err.println("Failed to create cluster becuase " + destClusterName + " already exist.");
      return;
    }
    ClusterSetup clusterSetup = new ClusterSetup(destZkString);
    clusterSetup.addCluster(destClusterName, true);

    // set ALLOW_PARTICIPANT_AUTO_JOIN
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(destClusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    destAdmin.setConfig(configScope, helixClusterProperties);
    // set PersistBestPossibleAssignment
    ConfigAccessor configAccessor = new ConfigAccessor(destZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(destClusterName);
    // if offline instances >= 4, helix enters maintenance mode.
    clusterConfig.setPersistBestPossibleAssignment(true);
    clusterConfig.setMaxOfflineInstancesAllowed(4);
    // if offline instances <= 2, helix exit maintenance mode.
    clusterConfig.setNumOfflineInstancesForAutoExit(2);
    configAccessor.setClusterConfig(destClusterName, clusterConfig);
    System.out.println("Cluster " + destClusterName + " is created successfully!");
    return;
  }

  /**
   * Update dest cluster information based on src cluster.
   * Dest cluster resource will be recreated if it mismatches that in src cluster.
   * @param srcZkString the src cluster's zk string
   * @param srcClusterName the src cluster's name
   * @param destZkString the dest cluster's zk string
   * @param destClusterName the dest cluster's name
   * @param dryRun run the update process but without actual change.
   */
  static void updateResourceAndPartition(String srcZkString, String srcClusterName, String destZkString,
      String destClusterName, boolean dryRun) {

    HelixAdmin srcAdmin = new ZKHelixAdmin(srcZkString);
    Set<String> srcResources = new HashSet<>(srcAdmin.getResourcesInCluster(srcClusterName));
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));

    for (String resource : srcResources) {
      if (ignoreResourceKeyWords.stream().anyMatch(resource::contains)) {
        System.out.println("Resource " + resource + " from src cluster is ignored");
        continue;
      }
      boolean createNewResource = false;
      boolean dropResource = false;
      if (destResources.contains(resource)) {
        // check if every partition exist.
        Set<String> srcPartitions = srcAdmin.getResourceIdealState(srcClusterName, resource).getPartitionSet();
        Set<String> destPartitions = destAdmin.getResourceIdealState(destClusterName, resource).getPartitionSet();
        if (srcPartitions.size() != destPartitions.size()) {
          dropResource = true;
          createNewResource = true;
        } else {
          for (String partition : srcPartitions) {
            if (!destPartitions.contains(partition)) {
              dropResource = true;
              createNewResource = true;
              break;
            }
          }
        }
        if (dropResource && dryRun) {
          System.out.println("DryRun: Drop Resource " + resource);
        } else if (dropResource) {
          // This resource need to be recreate.
          destAdmin.dropResource(destClusterName, resource);
          System.out.println("Dropped Resource " + resource);
        }
      } else {
        createNewResource = true;
      }
      if (createNewResource) {
        // add new resource
        Set<String> srcPartitions = srcAdmin.getResourceIdealState(srcClusterName, resource).getPartitionSet();
        FullAutoModeISBuilder builder = new FullAutoModeISBuilder(resource);
        builder.setStateModel(LeaderStandbySMD.name);
        for (String partition : srcPartitions) {
          builder.add(partition);
        }
        builder.setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
        IdealState idealState = builder.build();
        if (dryRun) {
          System.out.println("DryRun: Add Resource " + resource + " with partition " + srcPartitions);
        } else {
          destAdmin.addResource(destClusterName, resource, idealState);
          destAdmin.rebalance(destClusterName, resource, replicaNumber, "", "");
          System.out.println("Added Resource " + resource + " with partition " + srcPartitions);
        }
      }
    }
    System.out.println("Cluster " + destClusterName + " is updated successfully!");
  }

  /**
   * Enable or disable a resource in dest cluster.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   * @param resourceName the resource to enable/disable
   * @param enable enable the resource if true
   */
  static void controlResource(String destZkString, String destClusterName, String resourceName, boolean enable) {
    HelixZkClient destZkClient =
        DedicatedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(destZkString));
    destZkClient.setZkSerializer(new ZNRecordSerializer());
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    destAdmin.enableResource(destClusterName, resourceName, enable);
    return;
  }

  /**
   * Enable or disable maintenance mode for a cluster.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   * @param enable enter maintenance mode if true
   */
  static void maintainCluster(String destZkString, String destClusterName, boolean enable) {
    HelixZkClient destZkClient =
        DedicatedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(destZkString));
    destZkClient.setZkSerializer(new ZNRecordSerializer());
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    destAdmin.enableMaintenanceMode(destClusterName, enable);
    return;
  }
}


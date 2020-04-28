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
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
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

  private static final String SEPARATOR = "/";
  static List<String> ignoreResourceKeyWords = Arrays.asList("aggregation", "trigger", "stats");
  private static final int REPLICA_NUMBER = 3;
  private static final ZNRecordSerializer ZN_RECORD_SERIALIZER = new ZNRecordSerializer();
  // TODO: get from properties file
  private static final int MAX_OFFLINE_INSTANCES_ALLOWED = 4;
  private static final int NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT = 2;

  public static void main(String[] args) {
    OptionParser parser = new OptionParser();
    OptionSpec createClusterOpt = parser.accepts("createCluster",
        "Create cluster in dest zk(no resource creation). --createCluster --dest destZkEndpoint/destClusterName");

    OptionSpec updateClusterOpt = parser.accepts("updateCluster",
        "Update resources in dest by copying from src to dest. --updateCluster"
            + " [--src srcZkEndpoint/srcClusterName] --dest destZkEndpoint/destClusterName");
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
    // VCR cluster argument is always required
    ArgumentAcceptingOptionSpec<String> destOpt =
        parser.accepts("dest").withRequiredArg().required().describedAs("vcr zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<String> srcOpt =
        parser.accepts("src").withRequiredArg().describedAs("src zk and cluster name").ofType(String.class);
    ArgumentAcceptingOptionSpec<Boolean> enableOpt =
        parser.accepts("enable").withRequiredArg().describedAs("enable/disable").ofType(Boolean.class);

    OptionSet options = parser.parse(args);

    String[] destZkAndCluster = options.valueOf(destOpt).split(SEPARATOR);
    if (destZkAndCluster.length != 2) {
      errorAndExit("dest argument must have form 'zkString/clusterName'");
    }
    String destZkString = destZkAndCluster[0];
    String destClusterName = destZkAndCluster[1];
    if (!destClusterName.contains("VCR")) {
      errorAndExit("dest should be a VCR cluster.(VCR string should be included)");
    }

    if (options.has(createClusterOpt)) {
      System.out.println("Creating cluster: " + destClusterName);
      createCluster(destZkString, destClusterName);
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
        System.out.println("Updating cluster: " + destClusterName + "by checking " + srcClusterName);
        updateResourceAndPartition(srcZkString, srcClusterName, destZkString, destClusterName, dryRun);
      } else {
        System.out.println("Updating cluster config for: " + destClusterName);
        // Update the cluster config and resources to the latest settings.
        setClusterConfig(getHelixZkClient(destZkString), destClusterName, dryRun);
        updateResourceIdealState(destZkString, destClusterName, dryRun);
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

  /**
   * Create a helix cluster with given information.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   */
  static void createCluster(String destZkString, String destClusterName) {
    HelixZkClient destZkClient = getHelixZkClient(destZkString);
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    if (destAdmin.getClusters().contains(destClusterName)) {
      errorAndExit("Failed to create cluster becuase " + destClusterName + " already exist.");
    }
    ClusterSetup clusterSetup = new ClusterSetup(destZkString);
    clusterSetup.addCluster(destClusterName, true);

    // set ALLOW_PARTICIPANT_AUTO_JOIN
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(destClusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    destAdmin.setConfig(configScope, helixClusterProperties);

    setClusterConfig(destZkClient, destClusterName, false);
    System.out.println("Cluster " + destClusterName + " is created successfully!");
  }

  /**
   * Set the cluster config in the destination cluster using the latest settings.
   * @param destZkClient the {@link HelixZkClient} for the cluster.
   * @param destClusterName the cluster name.
   * @param dryRun run without actual change.
   */
  static void setClusterConfig(HelixZkClient destZkClient, String destClusterName, boolean dryRun) {
    ConfigAccessor configAccessor = new ConfigAccessor(destZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(destClusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    // if offline instances >= 4, helix enters maintenance mode.
    clusterConfig.setMaxOfflineInstancesAllowed(MAX_OFFLINE_INSTANCES_ALLOWED);
    // if offline instances <= 2, helix exit maintenance mode.
    clusterConfig.setNumOfflineInstancesForAutoExit(NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT);
    if (dryRun) {
      System.out.println("Will update cluster config to: " + clusterConfig.toString());
    }
    configAccessor.setClusterConfig(destClusterName, clusterConfig);
  }

  /**
   * Update the resources in the destination cluster with the new IdealState settings.
   * @param destZkString the destination Zookeeper server string.
   * @param destClusterName the destination cluster name.
   * @param dryRun run without actual change.
   */
  static void updateResourceIdealState(String destZkString, String destClusterName, boolean dryRun) {
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));

    for (String resource : destResources) {
      IdealState currentIdealState = destAdmin.getResourceIdealState(destClusterName, resource);
      IdealState newIdealState = buildIdealState(resource, currentIdealState.getPartitionSet());
      if (dryRun) {
        System.out.println("Will update " + resource + " to new ideal state " + newIdealState.toString());
      } else {
        destAdmin.setResourceIdealState(destClusterName, resource, newIdealState);
      }
    }
  }

  /**
   * Build the IdealState for the specified resource.
   * @param resource the Helix resource name.
   * @param partitionSet the set of partitions managed by the resource.
   * @return the {@link IdealState}.
   */
  static IdealState buildIdealState(String resource, Set<String> partitionSet) {
    FullAutoModeISBuilder builder = new FullAutoModeISBuilder(resource);
    builder.setStateModel(LeaderStandbySMD.name);
    for (String partition : partitionSet) {
      builder.add(partition);
    }
    builder.setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
    builder.setRebalancerClass(DelayedAutoRebalancer.class.getName());
    return builder.build();
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
        IdealState idealState = buildIdealState(resource, srcPartitions);
        if (dryRun) {
          System.out.println("DryRun: Add Resource " + resource + " with partition " + srcPartitions);
        } else {
          destAdmin.addResource(destClusterName, resource, idealState);
          destAdmin.rebalance(destClusterName, resource, REPLICA_NUMBER, "", "");
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
    HelixZkClient destZkClient = getHelixZkClient(destZkString);
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    destAdmin.enableResource(destClusterName, resourceName, enable);
  }

  /**
   * Enable or disable maintenance mode for a cluster.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   * @param enable enter maintenance mode if true
   */
  static void maintainCluster(String destZkString, String destClusterName, boolean enable) {
    HelixZkClient destZkClient = getHelixZkClient(destZkString);
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    destAdmin.enableMaintenanceMode(destClusterName, enable);
  }

  /**
   * @return the {@link HelixZkClient} from the zookeeper connection string.
   * @param zkString the zookeeper connection string.
   */
  static HelixZkClient getHelixZkClient(String zkString) {
    HelixZkClient zkClient =
        DedicatedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(zkString));
    zkClient.setZkSerializer(ZN_RECORD_SERIALIZER);
    return zkClient;
  }

  static void errorAndExit(String error) {
    System.err.println(error);
    System.exit(1);
  }
}


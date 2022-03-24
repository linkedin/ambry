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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This tool provides function to create vcr cluster and update vcr cluster by referencing src cluster.
 */
public class HelixVcrUtil {
  private static final Logger logger = LoggerFactory.getLogger(HelixVcrUtil.class);
  static final String SEPARATOR = "/";
  private static final ZNRecordSerializer ZN_RECORD_SERIALIZER = new ZNRecordSerializer();
  private static final String DELAYED_REBALANCER_CLASS_NAME = "DelayedAutoRebalancer";
  private static final Set<String> smallIntegers = new HashSet<>();

  static {
    for (int i = 0; i < 512; i++) {
      smallIntegers.add(String.valueOf(i));
    }
  }

  /**
   * Return {@code true} if the given resource name is a resource that contains partition information. Our current
   * partition resources are all named as integer values.
   * @param resourceName The resource name.
   * @return {@code true} when the given resource contains partition information
   */
  public static boolean isPartitionResourceName(String resourceName) {
    // all the partitions are placed under resource named as integer values.
    if (smallIntegers.contains(resourceName)) {
      return true;
    }
    try {
      Integer.parseUnsignedInt(resourceName);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Create a helix cluster with given information.
   * @param destZkString the cluster's zk string
   * @param destClusterName the cluster's name
   */
  public static void createCluster(String destZkString, String destClusterName, VcrHelixConfig config) {
    HelixZkClient destZkClient = getHelixZkClient(destZkString);
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkClient);
    if (ZKUtil.isClusterSetup(destClusterName, destZkClient)) {
      errorAndExit("Failed to create cluster because " + destClusterName + " already exist.");
    }
    ClusterSetup clusterSetup = new ClusterSetup.Builder().setZkAddress(destZkString).build();
    clusterSetup.addCluster(destClusterName, true);

    // set ALLOW_PARTICIPANT_AUTO_JOIN
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).
        forCluster(destClusterName).build();
    Map<String, String> helixClusterProperties = new HashMap<>();
    helixClusterProperties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN,
        String.valueOf(config.getClusterConfigFields().isAllowAutoJoin()));
    destAdmin.setConfig(configScope, helixClusterProperties);

    setClusterConfig(destZkClient, destClusterName, config, false);
    logger.info("Cluster {} is created successfully!", destClusterName);
  }

  /**
   * Set the cluster config in the destination cluster using the latest settings.
   * @param destZkClient the {@link HelixZkClient} for the cluster.
   * @param destClusterName the cluster name.
   * @param dryRun run without actual change.
   */
  static void setClusterConfig(HelixZkClient destZkClient, String destClusterName, VcrHelixConfig config,
      boolean dryRun) {
    ConfigAccessor configAccessor = new ConfigAccessor(destZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(destClusterName);
    clusterConfig.setPersistBestPossibleAssignment(true);
    ClusterConfigFields clusterConfigFields = config.getClusterConfigFields();
    // if offline instances >= max offline instances allowed config, helix enters maintenance mode.
    clusterConfig.setMaxOfflineInstancesAllowed(clusterConfigFields.getMaxOfflineInstancesAllowed());
    // if offline instances <= number of offline instances config, helix exit maintenance mode.
    clusterConfig.setNumOfflineInstancesForAutoExit(clusterConfigFields.getNumOfflineInstancesForAutoExit());
    if (dryRun) {
      logger.info("Will update cluster config to: {}", clusterConfig.toString());
    } else {
      configAccessor.setClusterConfig(destClusterName, clusterConfig);
    }
  }

  /**
   * Update the resources in the destination cluster with the new IdealState settings.
   * @param destZkString the destination Zookeeper server string.
   * @param destClusterName the destination cluster name.
   * @param dryRun run without actual change.
   */
  static void updateResourceIdealState(String destZkString, String destClusterName, VcrHelixConfig config,
      boolean dryRun) {
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));

    for (String resource : destResources) {
      IdealState currentIdealState = destAdmin.getResourceIdealState(destClusterName, resource);
      IdealState newIdealState =
          buildIdealState(resource, currentIdealState.getPartitionSet(), config.getIdealStateConfigFields());
      if (dryRun) {
        logger.info("Will update {} to new ideal state {}", resource, newIdealState.toString());
      } else {
        destAdmin.setResourceIdealState(destClusterName, resource, newIdealState);
        logger.info("Updated the ideal state for resource {} ", resource);
        destAdmin.rebalance(destClusterName, resource, config.getIdealStateConfigFields().getNumReplicas(), "", "");
        logger.info("Rebalanced resource with REPLICA_NUM: {}", resource,
            config.getIdealStateConfigFields().getNumReplicas());
      }
    }
  }

  /**
   * Build the IdealState for the specified resource.
   * @param resource the Helix resource name.
   * @param partitionSet the set of partitions managed by the resource.
   * @param idealStateConfigFields {@link IdealStateConfigFields} object containing the ideal state configs.
   * @return the {@link IdealState}.
   */
  static IdealState buildIdealState(String resource, Set<String> partitionSet,
      IdealStateConfigFields idealStateConfigFields) {
    FullAutoModeISBuilder builder = new FullAutoModeISBuilder(resource);
    builder.setStateModel(idealStateConfigFields.getStateModelDefRef());
    for (String partition : partitionSet) {
      builder.add(partition);
    }
    if (isDelayedRebalanceEnabled(idealStateConfigFields.getRebalancerClassName())) {
      builder.setMinActiveReplica(idealStateConfigFields.getMinActiveReplicas());
      builder.setRebalanceDelay((int) TimeUnit.MINUTES.toMillis(idealStateConfigFields.getRebalanceDelayInMins()));
      builder.setRebalancerClass(idealStateConfigFields.getRebalancerClassName());
    }
    builder.setRebalanceStrategy(idealStateConfigFields.getRebalanceStrategy());
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
  public static void updateResourceAndPartition(String srcZkString, String srcClusterName, String destZkString,
      String destClusterName, VcrHelixConfig config, boolean dryRun) {

    HelixAdmin srcAdmin = new ZKHelixAdmin(srcZkString);
    Set<String> srcResources = new HashSet<>(srcAdmin.getResourcesInCluster(srcClusterName));
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));
    logger.info("========VCR Helix Update Starts========");
    // Remove stale resources
    for (String resource : destResources) {
      if (!srcResources.contains(resource)) {
        if (dryRun) {
          logger.info("DryRun: Drop Resource {}", resource);
        } else {
          // This resource need to be dropped.
          logger.info("Dropped Resource {}, Partitions: {}", resource,
              destAdmin.getResourceIdealState(destClusterName, resource).getPartitionSet());
          destAdmin.dropResource(destClusterName, resource);
        }
      }
    }
    for (String resource : srcResources) {
      if (!isPartitionResourceName(resource)) {
        logger.info("Resource {} from src cluster is ignored", resource);
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
          logger.info("DryRun: Drop Resource {}", resource);
        } else if (dropResource) {
          // This resource need to be recreate.
          logger.info("Dropped Resource {}, Partitions: {}", resource,
              destAdmin.getResourceIdealState(destClusterName, resource).getPartitionSet());
          destAdmin.dropResource(destClusterName, resource);
        }
      } else {
        createNewResource = true;
      }
      if (createNewResource) {
        // add new resource
        Set<String> srcPartitions = srcAdmin.getResourceIdealState(srcClusterName, resource).getPartitionSet();
        IdealState idealState = buildIdealState(resource, srcPartitions, config.getIdealStateConfigFields());
        if (dryRun) {
          logger.info("DryRun: Add Resource {} with partition {}", resource, srcPartitions);
        } else {
          destAdmin.addResource(destClusterName, resource, idealState);
          destAdmin.rebalance(destClusterName, resource, config.getIdealStateConfigFields().getNumReplicas(), "", "");
          logger.info("Added Resource {}  with partition {}", resource, srcPartitions);
        }
      } else {
        logger.info("Resource {} is up to date. No action needed.", resource);
      }
    }
    logger.info("Cluster {} is updated successfully!", destClusterName);
    logger.info("========VCR Helix Update Ends========");
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
  public static void maintainCluster(String destZkString, String destClusterName, boolean enable) {
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

  /**
   * Is the rebalancer class {@link org.apache.helix.controller.rebalancer.DelayedAutoRebalancer}
   * @param rebalancerClassName name of the rebalancer class.
   * @return true if rebalancer is {@link org.apache.helix.controller.rebalancer.DelayedAutoRebalancer}, false otherwise.
   */
  private static boolean isDelayedRebalanceEnabled(String rebalancerClassName) {
    return (rebalancerClassName != null) && rebalancerClassName.endsWith(DELAYED_REBALANCER_CLASS_NAME);
  }

  /**
   * A method to verify resources and partitions in src cluster and dest cluster are same.
   */
  public static boolean isSrcDestSync(String srcZkString, String srcClusterName, String destZkString,
      String destClusterName) {

    HelixAdmin srcAdmin = new ZKHelixAdmin(srcZkString);
    Set<String> srcResources = new HashSet<>(srcAdmin.getResourcesInCluster(srcClusterName));
    HelixAdmin destAdmin = new ZKHelixAdmin(destZkString);
    Set<String> destResources = new HashSet<>(destAdmin.getResourcesInCluster(destClusterName));

    for (String resource : srcResources) {
      if (!isPartitionResourceName(resource)) {
        System.out.println("Resource " + resource + " from src cluster is ignored");
        continue;
      }
      if (destResources.contains(resource)) {
        // check if every partition exist.
        Set<String> srcPartitions = srcAdmin.getResourceIdealState(srcClusterName, resource).getPartitionSet();
        Set<String> destPartitions = destAdmin.getResourceIdealState(destClusterName, resource).getPartitionSet();
        for (String partition : srcPartitions) {
          if (!destPartitions.contains(partition)) {
            return false;
          }
        }
      } else {
        return false;
      }
    }
    return true;
  }

  /**
   * Class for ideal state configs.
   */
  static class IdealStateConfigFields {
    private int numReplicas;
    private String stateModelDefRef;
    private String rebalanceStrategy;
    private int minActiveReplicas;
    private String rebalancerClassName;
    private long rebalanceDelayInMins;

    /**
     * @return {@code numReplicas}
     */
    public int getNumReplicas() {
      return numReplicas;
    }

    /**
     * @return {@code stateModelDefRef}
     */
    public String getStateModelDefRef() {
      return stateModelDefRef;
    }

    /**
     * @return {@code rebalanceStrategy}
     */
    public String getRebalanceStrategy() {
      return rebalanceStrategy;
    }

    /**
     * @return {@code minActiveReplicas}
     */
    public int getMinActiveReplicas() {
      return minActiveReplicas;
    }

    /**
     * @return {@code rebalancerClassName}
     */
    public String getRebalancerClassName() {
      return rebalancerClassName;
    }

    /**
     * @return {@code rebalanceDelayInMins}
     */
    public long getRebalanceDelayInMins() {
      return rebalanceDelayInMins;
    }
  }

  /**
   * Class for cluster config fields.
   */
  static class ClusterConfigFields {
    private int maxOfflineInstancesAllowed;
    private int numOfflineInstancesForAutoExit;
    private boolean allowAutoJoin;

    /**
     * @return {@code maxOfflineInstancesAllowed}
     */
    public int getMaxOfflineInstancesAllowed() {
      return maxOfflineInstancesAllowed;
    }

    /**
     * @return {@code numOfflineInstancesForAutoExit}
     */
    public int getNumOfflineInstancesForAutoExit() {
      return numOfflineInstancesForAutoExit;
    }

    /**
     * @return {@code allowAutoJoin}
     */
    public boolean isAllowAutoJoin() {
      return allowAutoJoin;
    }
  }

  /**
   * Class for vcr helix configs
   */
  public static class VcrHelixConfig {
    private IdealStateConfigFields idealStateConfigFields;
    private ClusterConfigFields clusterConfigFields;

    /**
     * @return {@code idealStateConfigFields}
     */
    public IdealStateConfigFields getIdealStateConfigFields() {
      return idealStateConfigFields;
    }

    /**
     * @return {@code clusterConfigFields}
     */
    public ClusterConfigFields getClusterConfigFields() {
      return clusterConfigFields;
    }
  }
}

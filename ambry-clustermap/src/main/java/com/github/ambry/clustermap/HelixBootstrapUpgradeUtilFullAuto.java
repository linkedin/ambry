/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONException;


public class HelixBootstrapUpgradeUtilFullAuto extends HelixBootstrapUpgradeUtil {

  // Waged auto rebalancer default configs. TODO: Provide them as configurable inputs to tool.
  static final String DISK_KEY = "DISK";
  static final String RACK_KEY = "rack";
  static final String HOST_KEY = "host";
  static final String TOPOLOGY = "/" + RACK_KEY + "/" + HOST_KEY;
  static final String FAULT_ZONE_TYPE = "rack";
  static final int MAX_PARTITIONS_IN_TRANSITION = 50;
  static final int LESS_MOVEMENT = 2;
  static final int EVENNESS = 3;
  // the default of partition. each partition can store up to 24 * 16 = 384 GB data, and leave 2 GB here for margin
  static final int DEFAULT_PARTITION_WEIGHT = 100;
  // Allow a host to be filled upto 95%
  static final int INSTANCE_MAX_CAPACITY_PERCENTAGE = 95;

  /**
   * Instantiates this class with the given information.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of datacenters that needs to be upgraded/bootstrapped.
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @param forceRemove if true, removes any hosts from Helix not present in the json files.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @param hostname the host (if not null) on which the admin operation should be performed.
   * @param portNum the port number (if not null) associated with host.
   * @param partitionName the partition (if not null) on which the admin operation should be performed.
   * @param helixAdminOperation the {@link HelixAdminOperation} to perform.
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} associated with this cluster.
   * @param overrideReplicaStatus whether to override sealed/stopped/disabled replica status lists.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  public HelixBootstrapUpgradeUtilFullAuto(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef, String hostname, Integer portNum, String partitionName,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus) throws Exception {
    super(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs, maxPartitionsInOneResource,
        dryRun, forceRemove, helixAdminFactory, stateModelDef, hostname, portNum, partitionName, helixAdminOperation,
        dataNodeConfigSourceType, overrideReplicaStatus);
  }

  /**
   * Convert resources from semi-auto to full-auto.
   * @param commaSeparatedResources the comma-separated list of resources that needs to be migrated
   */
  public void migrateToFullAuto(String commaSeparatedResources) throws InterruptedException {
    CountDownLatch migrationComplete = new CountDownLatch(adminForDc.size());
    // different DCs can be migrated in parallel
    adminForDc.forEach((dcName, helixAdmin) -> Utils.newThread(() -> {
      try {
        logger.info("Starting resource migration to full-auto in {}", dcName);

        // Get property store
        ClusterMapConfig config = getClusterMapConfig(clusterName, dcName, null);
        String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
        try (PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter = new PropertyStoreToDataNodeConfigAdapter(
            zkConnectStr, config)) {

          // Get resources to migrate from user input.
          Set<String> helixResources = helixAdmin.getResourcesInCluster(clusterName)
              .stream()
              .filter(s -> s.matches("\\d+"))
              .collect(Collectors.toSet());
          Set<String> resources;
          if (commaSeparatedResources.equalsIgnoreCase(ALL)) {
            resources = helixResources;
          } else {
            resources = Arrays.stream(commaSeparatedResources.replaceAll("\\p{Space}", "").split(","))
                .collect(Collectors.toSet());
            resources.removeIf(resource -> {
              if (!helixResources.contains(resource)) {
                logger.info("Resource {} is not present in data center {}", resource, dcName);
                return true;
              }
              return false;
            });
          }

          Map<String, IdealState> resourceToIdealState = new HashMap<>();
          for (String resource : resources) {
            resourceToIdealState.put(resource, helixAdmin.getResourceIdealState(clusterName, resource));
          }

          ConfigAccessor configAccessor =
              new ConfigAccessor(dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0));

          // 1. Update cluster config
          setupCluster(configAccessor);

          // 2. Update instance config for all instances present in this resource
          Set<String> allInstances = new HashSet<>();
          for (String resource : resources) {
            // a. Get list of instances sharing partitions in this resource
            IdealState idealState = resourceToIdealState.get(resource);
            Set<String> partitions = idealState.getPartitionSet();
            Set<String> instances = new HashSet<>();
            for (String partition : partitions) {
              instances.addAll(idealState.getInstanceSet(partition));
            }
            // b. Update instance config for each instance
            for (String instance : instances) {
              setupInstanceConfig(dcName, instance, resource, configAccessor, propertyStoreAdapter);
            }
            allInstances.addAll(instances);
          }

          // 3. No resource config to update for now

          // 4. Validate configs
          Map<String, Boolean> instanceValidationResultMap =
              helixAdmin.validateInstancesForWagedRebalance(clusterName, new ArrayList<>(allInstances));
          if (instanceValidationResultMap.containsValue(Boolean.FALSE)) {
            throw new IllegalStateException("Instances are not configured correctly for waged rebalancer");
          }
          Map<String, Boolean> resourceValidationResultMap =
              helixAdmin.validateResourcesForWagedRebalance(clusterName, new ArrayList<>(resources));
          if (resourceValidationResultMap.containsValue(Boolean.FALSE)) {
            throw new IllegalStateException("Resources are not configured correctly for waged rebalancer");
          }

          if (!dryRun) {
            // Before
            Map<String, Set<String>> replicaPlacementBefore = getReplicaPlacement(helixAdmin, resources);

            // 5. Enter maintenance mode
            helixAdmin.manuallyEnableMaintenanceMode(clusterName, true, "Migrating to Full auto",
                Collections.emptyMap());

            // 6. Update ideal state and enable waged rebalancer
            for (String resource : resources) {
              IdealState idealState = resourceToIdealState.get(resource);
              idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
              idealState.setInstanceGroupTag(getResourceTag(resource));
              helixAdmin.updateIdealState(clusterName, resource, idealState);
            }
            helixAdmin.enableWagedRebalance(clusterName, new ArrayList<>(resources));

            // 7. Exit maintenance mode
            helixAdmin.manuallyEnableMaintenanceMode(clusterName, false, "Complete migrating to Full auto",
                Collections.emptyMap());

            // After
            Map<String, Set<String>> replicaPlacementAfter = getReplicaPlacement(helixAdmin, resources);

            // Log stats
            logMigrationStats(dcName, propertyStoreAdapter, replicaPlacementBefore, replicaPlacementAfter);
          }
          logger.info("Successfully migrated resources to full-auto in {}", dcName);
        }
      } catch (Throwable t) {
        logger.error("Error while migrating resources to full-auto in {}", dcName, t);
      } finally {
        migrationComplete.countDown();
      }
    }, false).start());

    migrationComplete.await();
  }

  /**
   * Return tag for resources and their instances
   * @param resource resource name
   * @return tag name
   */
  private String getResourceTag(String resource) {
    return "TAG_" + resource;
  }

  /**
   * Get replica placement.
   * @param helixAdmin {@link HelixAdmin} to talk to helix
   * @param resources list of resources for which we need the partition-replica placement.
   * @return
   */
  private Map<String, Set<String>> getReplicaPlacement(HelixAdmin helixAdmin, Set<String> resources) {
    Map<String, Set<String>> partitionToInstances = new HashMap<>();
    for (String resource : resources) {
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resource);
      for (String partitionName : idealState.getPartitionSet()) {
        partitionToInstances.put(partitionName, idealState.getInstanceSet(partitionName));
      }
    }
    return partitionToInstances;
  }

  /**
   * Sets up cluster to use waged rebalancer
   * @param configAccessor the {@link ConfigAccessor} to access configuration in Helix.
   */
  private void setupCluster(ConfigAccessor configAccessor) {
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    // 1. Set topology awareness
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(FAULT_ZONE_TYPE);
    StateTransitionThrottleConfig stateTransitionThrottleConfig =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, MAX_PARTITIONS_IN_TRANSITION);
    clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(stateTransitionThrottleConfig));
    // 2. Set default weight for each partition
    Map<String, Integer> defaultPartitionWeightMap = new HashMap<>();
    defaultPartitionWeightMap.put(DISK_KEY, DEFAULT_PARTITION_WEIGHT);
    clusterConfig.setDefaultPartitionWeightMap(defaultPartitionWeightMap);
    // 3. Set instance capacity keys
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK_KEY));
    // 4. Set evenness and less movement for cluster
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreferenceKeyIntegerMap = new HashMap<>();
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, EVENNESS);
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, LESS_MOVEMENT);
    clusterConfig.setGlobalRebalancePreference(globalRebalancePreferenceKeyIntegerMap);
    // Update cluster config in Helix/ZK
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  /**
   * Sets up instance to use waged rebalancer
   * @param dcName data center name
   * @param instanceName instance name
   * @param resource resource name
   * @param configAccessor the {@link ConfigAccessor} to access configuration in Helix.
   * @param propertyStoreToDataNodeConfigAdapter {@link PropertyStoreToDataNodeConfigAdapter} to access property store data.
   */
  private void setupInstanceConfig(String dcName, String instanceName, String resource, ConfigAccessor configAccessor,
      PropertyStoreToDataNodeConfigAdapter propertyStoreToDataNodeConfigAdapter) {
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    DataNodeConfig nodeConfigFromHelix =
        getDataNodeConfigFromHelix(dcName, instanceName, propertyStoreToDataNodeConfigAdapter, null);
    // 1. Add tag
    instanceConfig.addTag(getResourceTag(resource));
    // 2. Setup domain information
    Map<String, String> domainMap = new HashMap<>();
    domainMap.put(RACK_KEY, nodeConfigFromHelix.getRackId());
    domainMap.put(HOST_KEY, instanceName);
    instanceConfig.setDomain(domainMap);
    // 3. Set instance capacity
    Map<String, Integer> capacityMap = new HashMap<>();
    long capacity = nodeConfigFromHelix.getDiskConfigs()
        .values()
        .stream()
        .mapToLong(DataNodeConfig.DiskConfig::getDiskCapacityInBytes)
        .sum();
    long capacityInGB = capacity / 1024 / 1024 / 1024;
    capacityMap.put(DISK_KEY, (int) ((double) INSTANCE_MAX_CAPACITY_PERCENTAGE / 100 * capacityInGB));
    instanceConfig.setInstanceCapacityMap(capacityMap);
    // Update instance config in Helix/ZK
    configAccessor.updateInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  /**
   * @param dcName data center name
   * @param propertyStoreToDataNodeConfigAdapter {@link PropertyStoreToDataNodeConfigAdapter} to get node configs.
   * @param replicaPlacementBefore replica placement before the migration
   * @param replicaPlacementAfter replica placement after the migration
   */
  private void logMigrationStats(String dcName,
      PropertyStoreToDataNodeConfigAdapter propertyStoreToDataNodeConfigAdapter,
      Map<String, Set<String>> replicaPlacementBefore, Map<String, Set<String>> replicaPlacementAfter) {

    // 1. See host usages in new assignment
    DoubleSummaryStatistics doubleSummaryStatistics = new DoubleSummaryStatistics();
    Map<String, Integer> replicasPerInstance = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : replicaPlacementAfter.entrySet()) {
      Set<String> instances = entry.getValue();
      for (String instance : instances) {
        replicasPerInstance.put(instance, replicasPerInstance.getOrDefault(instance, 0) + 1);
      }
    }
    for (String instance : replicasPerInstance.keySet()) {
      // Get instance capacity
      DataNodeConfig nodeConfigFromHelix =
          getDataNodeConfigFromHelix(dcName, instance, propertyStoreToDataNodeConfigAdapter, null);
      long capacity = nodeConfigFromHelix.getDiskConfigs()
          .values()
          .stream()
          .mapToLong(DataNodeConfig.DiskConfig::getDiskCapacityInBytes)
          .sum();
      // Usage = (replicasInTheInstance / instance capacity) * 100;
      double usage = (double) (replicasPerInstance.get(instance) * DEFAULT_PARTITION_WEIGHT) / capacity * 100;
      doubleSummaryStatistics.accept(usage);
    }
    logger.info("Host usage stats");
    logger.info("Min usage {}", (float) doubleSummaryStatistics.getMin());
    logger.info("Max usage {}", (float) doubleSummaryStatistics.getMax());
    logger.info("Average usage {}", (float) doubleSummaryStatistics.getAverage());

    // 2. See replicas for partition before and after
    int numReplicasMoved = 0;
    for (String partition : replicaPlacementBefore.keySet()) {
      Set<String> replicasBefore = new HashSet<>(replicaPlacementBefore.get(partition));
      Set<String> replicasAfter = new HashSet<>(replicaPlacementAfter.get(partition));
      replicasBefore.removeAll(replicaPlacementAfter.get(partition));
      replicasAfter.removeAll(replicaPlacementBefore.get(partition));
      logger.info("Replicas for partition {} moved from {} to {}", partition, replicasBefore, replicasAfter);
      numReplicasMoved += replicasBefore.size();
    }
    logger.info("Percentage of replicas moved is {}",
        (float) numReplicasMoved / (replicaPlacementAfter.size() * 3) * 100.0);
  }

  @Override
  protected void updateClusterMapInHelix(boolean startValidatingClusterManager) throws Exception {

  }

  @Override
  protected void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {

  }

  @Override
  protected void addUpdateResources(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {

  }

  @Override
  protected void validateAndClose() {

  }

  @Override
  protected void logSummary() {

  }
}

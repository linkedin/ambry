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

  // Waged auto rebalancer configs
  static final String FULL_AUTO_FAULT_ZONE_TYPE = "rack";
  static final int FULL_AUTO_MAX_PARTITIONS_IN_TRANSITION = 50;
  static final int LESS_MOVEMENT = 2;
  static final int EVENNESS = 3;
  // the default of partition. each partition can store up to 24 * 16 = 384 GB data, and leave 2 GB here for margin
  static final int PARTITION_WEIGHT = 386;
  static final String CAPACITY_DISK_KEY = "DISK";
  static final String RACK_KEY = "rack";
  static final String HOST_KEY = "host";
  static final String FULL_AUTO_TOPOLOGY = "/" + RACK_KEY + "/" + HOST_KEY;

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
          Set<String> helixResources = new HashSet<>(helixAdmin.getResourcesInCluster(clusterName));
          Set<String> resources;
          if (commaSeparatedResources.equalsIgnoreCase(ALL)) {
            resources = helixResources;
          } else {
            resources = Arrays.stream(commaSeparatedResources.replaceAll("\\p{Space}", "").split(","))
                .collect(Collectors.toSet());
          }

          ConfigAccessor configAccessor =
              new ConfigAccessor(dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0));

          // 1. Update cluster config
          updateClusterConfig(configAccessor);

          // 2. Update instance config for all instances present in this resource
          Set<String> allInstances = new HashSet<>();
          for (String resource : resources) {
            // a. Get list of instances sharing partitions in this resource
            IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resource);
            Set<String> partitions = idealState.getPartitionSet();
            Set<String> instances = new HashSet<>();
            partitions.forEach(partitionName -> instances.addAll(idealState.getInstanceSet(partitionName)));
            // b. Update instance config for each instance
            for (String instance : instances) {
              DataNodeConfig nodeConfigFromHelix =
                  getDataNodeConfigFromHelix(dcName, instance, propertyStoreAdapter, null);
              long instanceCapacity = 0;
              for (DataNodeConfig.DiskConfig diskConfig : nodeConfigFromHelix.getDiskConfigs().values()) {
                instanceCapacity += diskConfig.getDiskCapacityInBytes();
              }
              updateInstanceConfig(configAccessor, instance, "TAG_" + resource, nodeConfigFromHelix.getRackId(),
                  instanceCapacity);
            }
            allInstances.addAll(instances);
          }

          // 3. Validate configs
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

          if(!dryRun){
            // TODO. For testing, we will use helix rest APIs
            // 4. Enter maintenance mode

            // 5. Update ideal state

            // 6. Exit maintainence mode

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
   * Update cluster config to use waged rebalancer
   * @param configAccessor the {@link ConfigAccessor} to access configuration in Helix.
   */
  private void updateClusterConfig(ConfigAccessor configAccessor) {
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    // 1. Set topology awareness
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology(FULL_AUTO_TOPOLOGY);
    clusterConfig.setFaultZoneType(FULL_AUTO_FAULT_ZONE_TYPE);
    StateTransitionThrottleConfig stateTransitionThrottleConfig =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, FULL_AUTO_MAX_PARTITIONS_IN_TRANSITION);
    clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(stateTransitionThrottleConfig));
    // 2. Set default weight for each partition
    Map<String, Integer> defaultPartitionWeightMap = new HashMap<>();
    defaultPartitionWeightMap.put(CAPACITY_DISK_KEY, PARTITION_WEIGHT);
    clusterConfig.setDefaultPartitionWeightMap(defaultPartitionWeightMap);
    // 3. Set instance capacity keys
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(CAPACITY_DISK_KEY));
    // 4. Set evenness and less movement for cluster
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreferenceKeyIntegerMap = new HashMap<>();
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, EVENNESS);
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, LESS_MOVEMENT);
    clusterConfig.setGlobalRebalancePreference(globalRebalancePreferenceKeyIntegerMap);
    // Send request to update modified cluster config in Helix/ZK
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  /**
   * @param configAccessor the {@link ConfigAccessor} to access configuration in Helix.
   * @param instanceName instance name
   * @param tag tag for the instance.
   * @param rackId rack in which the instance lives in.
   * @param capacity storage capacity of the instance.
   */
  private void updateInstanceConfig(ConfigAccessor configAccessor, String instanceName, String tag, String rackId,
      long capacity) {
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    // 1. Add tag
    instanceConfig.addTag(tag);
    // 2. Setup domain information
    Map<String, String> domainMap = new HashMap<>();
    domainMap.put(RACK_KEY, rackId);
    domainMap.put(HOST_KEY, instanceName);
    instanceConfig.setDomain(domainMap);
    // 3. Set instance capacity
    Map<String, Integer> capacityMap = new HashMap<>();
    capacityMap.put(CAPACITY_DISK_KEY, (int) capacity);
    instanceConfig.setInstanceCapacityMap(capacityMap);
    configAccessor.updateInstanceConfig(clusterName, instanceName, instanceConfig);
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

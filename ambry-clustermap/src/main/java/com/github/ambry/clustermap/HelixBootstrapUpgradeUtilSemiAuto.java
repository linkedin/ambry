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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.json.JSONException;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * A class to bootstrap static cluster map information into Helix.
 *
 * For each node that is added to Helix, its {@link InstanceConfig} will contain the node level information, which is
 * of the following format currently:
 *
 *InstanceConfig: {
 *  "id" : "localhost_17088",                              # id is the instanceName [host_port]
 *  "mapFields" : {
 *    "/tmp/c/0" : {                                       # disk is identified by the [mountpath]. DiskInfo consists of:
 *      "capacityInBytes" : "912680550400",                # [capacity]
 *      "diskState" : "AVAILABLE",                         # [state]
 *      "Replicas" : "10:107374182400:default,"            # comma-separated list of partition ids whose replicas are
 *    },                                                   # hosted on this disk in
 *                                                         # [replica:replicaCapacity:partitionClass] format.
 *    "/tmp/c/1" : {
 *      "capacityInBytes" : "912680550400",
 *      "diskState" : "AVAILABLE",
 *      "Replicas" : "40:107374182400:default,20:107374182400:special,"
 *    },
 *    "/tmp/c/2" : {
 *      "capacityInBytes" : "912680550400",
 *      "diskState" : "AVAILABLE",
 *      "Replicas" : "30:107374182400:default,"
 *    }
 *  },
 *  "listFields" : {
 *    "SEALED" : [ "20" ]                                  # comma-separated list of sealed replicas on this node.
 *  },
 *  "simpleFields" : {
 *    "HELIX_HOST" : "localhost",                          #  hostname (Helix field)
 *    "HELIX_PORT" : "17088",                              #  port     (Helix field)
 *    "datacenter" : "dc1",                                # [datacenterName]
 *    "rackId" : "1611",                                   # [rackId]
 *    "sslPort": "27088"                                   # [sslPort]
 *    "schemaVersion": "0"                                 # [schema version]
 *    "xid" : "0"                                          # [xid (last update to the data in this InstanceConfig)]
 *  }
 *}
 */
public class HelixBootstrapUpgradeUtilSemiAuto extends HelixBootstrapUpgradeUtil {

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
  public HelixBootstrapUpgradeUtilSemiAuto(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef, String hostname, Integer portNum, String partitionName,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus) throws Exception {
    super(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs, maxPartitionsInOneResource,
        dryRun, forceRemove, helixAdminFactory, stateModelDef, hostname, portNum, partitionName, helixAdminOperation,
        dataNodeConfigSourceType, overrideReplicaStatus);
  }

  /**
   * Map the information in the layout files to Helix. Calling this method multiple times has no effect if the
   * information in the static files do not change. This tool is therefore safe to use for upgrades.
   *
   * Instead of defining the entire cluster under a single resource, or defining a resource for every partition, the
   * tool groups together partitions under resources, with a limit to the number of partitions that will be grouped
   * under a single resource.
   *
   * The logic is as follows to optimize a single setInstanceConfig() all for any node:
   *
   * for each datacenter/admin:
   *   for each instance in datacenter present in both static clustermap and Helix:
   *     get replicas it hosts with their sealed state and capacity from static.
   *     newInstanceConfig = create one with the replicas and other information for the instance.
   *     existingInstanceConfig = get InstanceConfig from helix;
   *     if (newInstanceConfig.equals(existingInstanceConfig)):
   *         continue;
   *     else:
   *       setInstanceConfig(newInstanceConfig);
   *     endif
   *   endfor
   *
   *   for each instance in datacenter present in static clustermap but not in Helix:
   *     get replicas it hosts with their sealed state and capacity from static.
   *     newInstanceConfig = create one with the replicas and other information for the instance.
   *     addNewInstanceInHelix(newInstanceConfig);
   *   endfor
   *
   *   // Optional:
   *   for each instance in Helix not present in static:
   *     dropInstanceFromHelix()
   *   endfor
   * endfor
   *
   * @param startValidatingClusterManager whether validation should include staring up a {@link HelixClusterManager}
   */
  @Override
  protected void updateClusterMapInHelix(boolean startValidatingClusterManager) throws Exception {
    info("Initializing admins and possibly adding cluster in Helix (if non-existent)");
    maybeAddCluster();
    info("Validating cluster manager is {}", startValidatingClusterManager ? "ENABLED" : "DISABLED");
    if (startValidatingClusterManager) {
      startClusterManager();
    }
    info("Populating resources and partitions set");
    populateInstancesAndPartitionsMap();
    info("Populated resources and partitions set");
    final CountDownLatch bootstrapLatch = new CountDownLatch(adminForDc.size());
    for (Datacenter dc : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (adminForDc.containsKey(dc.getName())) {
        Utils.newThread(() -> {
          info("\n=======Starting datacenter: {}=========\n", dc.getName());
          Map<String, Set<String>> partitionsToInstancesInDc = new HashMap<>();
          addUpdateInstances(dc.getName(), partitionsToInstancesInDc);
          // Process those partitions that are already under resources. Just update their instance sets if that has changed.
          info(
              "[{}] Done adding all instances in {}, now scanning resources in Helix and ensuring instance set for partitions are the same.",
              dc.getName().toUpperCase(), dc.getName());
          addUpdateResources(dc.getName(), partitionsToInstancesInDc);
          bootstrapLatch.countDown();
        }, false).start();
      } else {
        info("\n========Skipping datacenter: {}==========\n", dc.getName());
      }
    }
    // make sure bootstrap has completed in all dcs (can extend timeout if amount of resources in each datacenter is really large)
    bootstrapLatch.await(15, TimeUnit.MINUTES);
  }

  @Override
  protected void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
    ClusterMapConfig config = getClusterMapConfig(clusterName, dcName, null);
    String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
    try (PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter = new PropertyStoreToDataNodeConfigAdapter(
        zkConnectStr, config)) {
      InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
          new InstanceConfigToDataNodeConfigAdapter.Converter(config);
      info("[{}] Getting list of instances in {}", dcName.toUpperCase(), dcName);
      Set<String> instancesInHelix = new HashSet<>(getInstanceNamesInHelix(dcName, propertyStoreAdapter));
      Set<String> instancesInStatic = dcToInstanceNameToDataNodeId.get(dcName) == null ? new HashSet<>()
          : new HashSet<>(dcToInstanceNameToDataNodeId.get(dcName).keySet());
      Set<String> instancesInBoth = new HashSet<>(instancesInHelix);
      // set instances in both correctly.
      instancesInBoth.retainAll(instancesInStatic);
      // set instances in Helix only correctly.
      instancesInHelix.removeAll(instancesInBoth);
      // set instances in Static only correctly.
      instancesInStatic.removeAll(instancesInBoth);
      int totalInstances = instancesInBoth.size() + instancesInHelix.size() + instancesInStatic.size();
      for (String instanceName : instancesInBoth) {
        DataNodeConfig nodeConfigFromHelix =
            getDataNodeConfigFromHelix(dcName, instanceName, propertyStoreAdapter, instanceConfigConverter);
        DataNodeConfig nodeConfigFromStatic =
            createDataNodeConfigFromStatic(dcName, instanceName, nodeConfigFromHelix, partitionsToInstancesInDc,
                instanceConfigConverter);
        if (!nodeConfigFromStatic.equals(nodeConfigFromHelix, !overrideReplicaStatus)) {
          if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
            if (!dryRun) {
              info(
                  "[{}] Instance {} already present in Helix {}, but config has changed, updating. Remaining instances: {}",
                  dcName.toUpperCase(), instanceName, dataNodeConfigSourceType.name(), --totalInstances);
              // Continuing on the note above, if there is indeed a change, we must make a call on whether RO/RW, replica
              // availability and so on should be updated at all (if not, nodeConfigFromStatic should be replaced with
              // the appropriate dataNodeConfig that is constructed with the correct values from both).
              // For now, only bootstrapping cluster is allowed to directly change DataNodeConfig
              setDataNodeConfigInHelix(dcName, instanceName, nodeConfigFromStatic, propertyStoreAdapter,
                  instanceConfigConverter);
            } else {
              info(
                  "[{}] Instance {} already present in Helix {}, but config has changed, no action as dry run. Remaining instances: {}",
                  dcName.toUpperCase(), instanceName, dataNodeConfigSourceType.name(), --totalInstances);
              logger.debug("[{}] Previous config: {} \n New config: {}", dcName.toUpperCase(), nodeConfigFromHelix,
                  nodeConfigFromStatic);
            }
            // for dryRun, we update counter but don't really change the DataNodeConfig in Helix
            instancesUpdated.getAndIncrement();
          }
        } else {
          if (!dryRun) {
            info("[{}] Instance {} already present in Helix {}, with same Data, skipping. Remaining instances: {}",
                dcName.toUpperCase(), instanceName, dataNodeConfigSourceType.name(), --totalInstances);
          }
        }
      }

      for (String instanceName : instancesInStatic) {
        DataNodeConfig nodeConfigFromStatic =
            createDataNodeConfigFromStatic(dcName, instanceName, null, partitionsToInstancesInDc,
                instanceConfigConverter);
        info("[{}] Instance {} is new, {}. Remaining instances: {}", dcName.toUpperCase(), instanceName,
            dryRun ? "no action as dry run" : "adding to Helix " + dataNodeConfigSourceType.name(), --totalInstances);
        // Note: if we want to move replica to new instance (not present in cluster yet), we can prepare a transient
        // clustermap in which we keep existing replicas and add new replicas/instances. We should be able to upgrade cluster
        // normally (update both datanode configs and IdealState). Helix controller will notify new instance to perform
        // replica addition.
        if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
          if (!dryRun) {
            addDataNodeConfigToHelix(dcName, nodeConfigFromStatic, propertyStoreAdapter, instanceConfigConverter);
          }
          instancesAdded.getAndIncrement();
        }
      }

      for (String instanceName : instancesInHelix) {
        if (forceRemove) {
          info("[{}] Instance {} is in Helix {}, but not in static. {}. Remaining instances: {}", dcName.toUpperCase(),
              instanceName, dataNodeConfigSourceType.name(), dryRun ? "No action as dry run" : "Forcefully removing",
              --totalInstances);
          if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
            if (!dryRun) {
              removeDataNodeConfigFromHelix(dcName, instanceName, propertyStoreAdapter);
            }
            instancesDropped.getAndIncrement();
          }
        } else {
          info(
              "[{}] Instance {} is in Helix {}, but not in static. Ignoring for now (use --forceRemove to forcefully remove). "
                  + "Remaining instances: {}", dcName.toUpperCase(), instanceName, dataNodeConfigSourceType.name(),
              --totalInstances);
          expectMoreInHelixDuringValidate = true;
          instancesNotForceRemovedByDc.computeIfAbsent(dcName, k -> ConcurrentHashMap.newKeySet()).add(instanceName);
        }
      }
    }
  }

  /**
   * Add and/or update resources in Helix based on the information in the static cluster map. This may involve adding
   * or removing partitions from under a resource, and adding or dropping resources altogether. This may also involve
   * changing the instance set for a partition under a resource, based on the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  @Override
  protected void addUpdateResources(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    List<String> resourcesInCluster = dcAdmin.getResourcesInCluster(clusterName);
    List<String> instancesWithDisabledPartition = new ArrayList<>();
    HelixPropertyStore<ZNRecord> helixPropertyStore =
        helixAdminOperation == HelixAdminOperation.DisablePartition ? createHelixPropertyStore(dcName) : null;
    // maxResource may vary from one dc to another (special partition class allows partitions to exist in one dc only)
    int maxResource = -1;
    for (String resourceName : resourcesInCluster) {
      boolean resourceModified = false;
      if (!resourceName.matches("\\d+")) {
        // there may be other resources created under the cluster (say, for stats) that are not part of the
        // cluster map. These will be ignored.
        continue;
      }
      maxResource = Math.max(maxResource, Integer.parseInt(resourceName));
      IdealState resourceIs = dcAdmin.getResourceIdealState(clusterName, resourceName);
      for (String partitionName : new HashSet<>(resourceIs.getPartitionSet())) {
        Set<String> instanceSetInHelix = resourceIs.getInstanceSet(partitionName);
        Set<String> instanceSetInStatic = partitionsToInstancesInDc.remove(partitionName);
        if (instanceSetInStatic == null || instanceSetInStatic.isEmpty()) {
          if (forceRemove) {
            info("[{}] *** Partition {} no longer present in the static clustermap, {} *** ", dcName.toUpperCase(),
                partitionName, dryRun ? "no action as dry run" : "removing from Resource");
            // this is a hacky way of removing a partition from the resource, as there isn't another way today.
            // Helix team is planning to provide an API for this.
            if (!dryRun) {
              resourceIs.getRecord().getListFields().remove(partitionName);
            }
            resourceModified = true;
          } else {
            info(
                "[{}] *** forceRemove option not provided, resources will not be removed (use --forceRemove to forcefully remove)",
                dcName.toUpperCase());
            expectMoreInHelixDuringValidate = true;
            partitionsNotForceRemovedByDc.computeIfAbsent(dcName, k -> ConcurrentHashMap.newKeySet())
                .add(partitionName);
          }
        } else if (!instanceSetInStatic.equals(instanceSetInHelix)) {
          // we change the IdealState only when the operation is meant to bootstrap cluster or indeed update IdealState
          if (EnumSet.of(HelixAdminOperation.UpdateIdealState, HelixAdminOperation.BootstrapCluster)
              .contains(helixAdminOperation)) {
            // @formatter:off
            info(
                "[{}] Different instance sets for partition {} under resource {}. {}. "
                    + "Previous instance set: [{}], new instance set: [{}]",
                dcName.toUpperCase(), partitionName, resourceName,
                dryRun ? "No action as dry run" : "Updating Helix using static",
                String.join(",", instanceSetInHelix), String.join(",", instanceSetInStatic));
            // @formatter:on
            if (!dryRun) {
              ArrayList<String> newInstances = new ArrayList<>(instanceSetInStatic);
              Collections.shuffle(newInstances);
              resourceIs.setPreferenceList(partitionName, newInstances);
              // Existing resources may not have ANY_LIVEINSTANCE set as the numReplicas (which allows for different
              // replication for different partitions under the same resource). So set it here (We use the name() method and
              // not the toString() method for the enum as that is what Helix uses).
              resourceIs.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
            }
            resourceModified = true;
          } else if (helixAdminOperation == HelixAdminOperation.DisablePartition) {
            // if this is DisablePartition operation, we don't modify IdealState and only make InstanceConfig to disable
            // certain partition on specific node.
            // 1. extract difference between Helix and Static instance sets. Determine which replica is removed
            instanceSetInHelix.removeAll(instanceSetInStatic);
            // 2. disable removed replica on certain node.
            for (String instanceInHelixOnly : instanceSetInHelix) {
              info("Partition {} under resource {} on node {} is no longer in static clustermap. {}.", partitionName,
                  resourceName, instanceInHelixOnly, dryRun ? "No action as dry run" : "Disabling it");
              if (!dryRun) {
                InstanceConfig instanceConfig = dcAdmin.getInstanceConfig(clusterName, instanceInHelixOnly);
                String instanceName = instanceConfig.getInstanceName();
                // create a Znode (if not present) in PropertyStore for this instance before disabling partition, which
                // will be deleted in the end. The reason to create a ZNode is to ensure replica decommission is blocked
                // on updating InstanceConfig until this tool has completed. TODO, remove this logic once migration to PropertyStore is done.
                if (!instancesWithDisabledPartition.contains(instanceName)) {
                  ZNRecord znRecord = new ZNRecord(instanceName);
                  String path = PARTITION_DISABLED_ZNODE_PATH + instanceName;
                  if (!helixPropertyStore.create(path, znRecord, AccessOption.PERSISTENT)) {
                    logger.error("Failed to create a ZNode for {} in datacenter {} before disabling partition.",
                        instanceName, dcName);
                    continue;
                  }
                }
                instanceConfig.setInstanceEnabledForPartition(resourceName, partitionName, false);
                dcAdmin.setInstanceConfig(clusterName, instanceInHelixOnly, instanceConfig);
                instancesWithDisabledPartition.add(instanceName);
              }
              partitionsDisabled.getAndIncrement();
            }
            // Disabling partition won't remove certain replica from IdealState. So replicas of this partition in Helix
            // will be more than that in static clustermap.
            expectMoreInHelixDuringValidate = true;
          }
        }
      }
      // update state model def if necessary
      if (!resourceIs.getStateModelDefRef().equals(stateModelDef)) {
        info("[{}] Resource {} has different state model {}. Updating it with {}", dcName.toUpperCase(), resourceName,
            resourceIs.getStateModelDefRef(), stateModelDef);
        resourceIs.setStateModelDefRef(stateModelDef);
        resourceModified = true;
      }
      resourceIs.setNumPartitions(resourceIs.getPartitionSet().size());
      if (resourceModified) {
        if (resourceIs.getPartitionSet().isEmpty()) {
          info("[{}] Resource {} has no partition, {}", dcName.toUpperCase(), resourceName,
              dryRun ? "no action as dry run" : "dropping");
          if (!dryRun) {
            dcAdmin.dropResource(clusterName, resourceName);
          }
          resourcesDropped.getAndIncrement();
        } else {
          if (!dryRun) {
            dcAdmin.setResourceIdealState(clusterName, resourceName, resourceIs);
            System.out.println("------------------add resource!");
            System.out.println(resourceName);
            System.out.println(resourceName);
          }
          resourcesUpdated.getAndIncrement();
        }
      }
    }
    // note that disabling partition also updates InstanceConfig of certain nodes which host the partitions.
    instancesUpdated.getAndAdd(instancesWithDisabledPartition.size());
    // if there are some partitions are disabled, mark whole disabling process complete in Helix PropertyStore to unblock
    // replica decommission thread on each datanode.
    if (helixPropertyStore != null) {
      maybeAwaitForLatch();
      for (String instanceName : instancesWithDisabledPartition) {
        String path = PARTITION_DISABLED_ZNODE_PATH + instanceName;
        if (!helixPropertyStore.remove(path, AccessOption.PERSISTENT)) {
          logger.error("Failed to remove a ZNode for {} in datacenter {} after disabling partition completed.",
              instanceName, dcName);
        }
      }
      helixPropertyStore.stop();
    }

    // Add what is not already in Helix under new resources.
    int fromIndex = 0;
    List<Map.Entry<String, Set<String>>> newPartitions = new ArrayList<>(partitionsToInstancesInDc.entrySet());
    while (fromIndex < newPartitions.size()) {
      String resourceName = Integer.toString(++maxResource);
      int toIndex = Math.min(fromIndex + maxPartitionsInOneResource, newPartitions.size());
      List<Map.Entry<String, Set<String>>> partitionsUnderNextResource = newPartitions.subList(fromIndex, toIndex);
      fromIndex = toIndex;
      IdealState idealState = new IdealState(resourceName);
      idealState.setStateModelDefRef(stateModelDef);
      info("[{}] Adding partitions for next resource {} in {}. {}.", dcName.toUpperCase(), resourceName, dcName,
          dryRun ? "Actual IdealState is not changed as dry run" : "IdealState is being updated");
      for (Map.Entry<String, Set<String>> entry : partitionsUnderNextResource) {
        String partitionName = entry.getKey();
        ArrayList<String> instances = new ArrayList<>(entry.getValue());
        Collections.shuffle(instances);
        idealState.setPreferenceList(partitionName, instances);
      }
      idealState.setNumPartitions(partitionsUnderNextResource.size());
      idealState.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
      if (!idealState.isValid()) {
        throw new IllegalStateException("IdealState could not be validated for new resource " + resourceName);
      }
      if (!dryRun) {
        dcAdmin.addResource(clusterName, resourceName, idealState);
        info("[{}] Added {} new partitions under resource {} in datacenter {}", dcName.toUpperCase(),
            partitionsUnderNextResource.size(), resourceName, dcName);
      } else {
        info("[{}] Under DryRun mode, {} new partitions are added to resource {} in datacenter {}",
            dcName.toUpperCase(), partitionsUnderNextResource.size(), resourceName, dcName);
      }
      resourcesAdded.getAndIncrement();
    }
  }

  @Override
  protected void validateAndClose() {
    try {
      info("Validating static and Helix cluster maps");
      verifyEquivalencyWithStaticClusterMap(staticClusterMap.hardwareLayout, staticClusterMap.partitionLayout);
      if (validatingHelixClusterManager != null) {
        ensureOrThrow(validatingHelixClusterManager.getErrorCount() == 0,
            "Helix cluster manager should not have encountered any errors");
      }
    } catch (Exception e) {
      logger.error("Exception occurred when verifying equivalency with state clustermap", e);
    } finally {
      if (validatingHelixClusterManager != null) {
        validatingHelixClusterManager.close();
      }
      for (HelixAdmin admin : adminForDc.values()) {
        admin.close();
      }
    }
  }

  @Override
  protected void logSummary() {
    if (instancesUpdated.get() + instancesAdded.get() + instancesDropped.get() + resourcesUpdated.get() + resourcesAdded
        .get() + resourcesDropped.get() + partitionsDisabled.get() + partitionsEnabled.get() + partitionsReset.get()
        > 0) {
      if (!dryRun) {
        info("========Cluster in Helix was updated, summary:========");
      } else {
        info("========Dry run: Actual run would update the cluster in the following way:========");
      }
      info("New instances added: {}", instancesAdded.get());
      info("Existing instances updated: {}", instancesUpdated.get());
      info("Existing instances dropped: {}", instancesDropped.get());
      info("New resources added: {}", resourcesAdded.get());
      info("Existing resources updated: {}", resourcesUpdated.get());
      info("Existing resources dropped: {}", resourcesDropped.get());
      info("Partitions disabled: {}", partitionsDisabled.get());
      info("Partitions enabled: {}", partitionsEnabled.get());
      info("Partitions reset: {}", partitionsReset.get());
    } else {
      info("========No updates were done to the cluster in Helix========");
    }
    if (validatingHelixClusterManager != null) {
      info("========Validating HelixClusterManager metrics========");
      info("Instance config change count: {}",
          validatingHelixClusterManager.helixClusterManagerMetrics.instanceConfigChangeTriggerCount.getCount());
      info("Instance config update ignored count: {}",
          validatingHelixClusterManager.helixClusterManagerMetrics.ignoredUpdatesCount.getCount());
    }
  }

  private void verifyEquivalencyWithStaticClusterMap(HardwareLayout hardwareLayout, PartitionLayout partitionLayout)
      throws Exception {
    String clusterNameInStaticClusterMap = hardwareLayout.getClusterName();
    info("Verifying equivalency of static cluster: " + clusterNameInStaticClusterMap + " with the "
        + "corresponding cluster in Helix: " + clusterName);
    CountDownLatch verificationLatch = new CountDownLatch(adminForDc.size());
    AtomicInteger errorCount = new AtomicInteger();
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      if (admin == null) {
        info("Skipping {}", dc.getName());
        continue;
      }
      ensureOrThrow(isClusterPresent(dc.getName()),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      Utils.newThread(() -> {
        try {
          verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout);
          verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, partitionLayout);
        } catch (Throwable t) {
          logger.error("[{}] error message: {}", dc.getName().toUpperCase(), t.getMessage());
          errorCount.getAndIncrement();
        } finally {
          verificationLatch.countDown();
        }
      }, false).start();
    }
    verificationLatch.await(10, TimeUnit.MINUTES);
    ensureOrThrow(errorCount.get() == 0, "Error occurred when verifying equivalency with static cluster map");
    info("Successfully verified equivalency of static cluster: " + clusterNameInStaticClusterMap
        + " with the corresponding cluster in Helix: " + clusterName);
  }

  /**
   * Verify that the hardware layout information is in sync - which includes the node and disk information. Also verify
   * that the replicas belonging to disks are in sync between the static cluster map and Helix.
   * @param dc the datacenter whose information is to be verified.
   * @param clusterName the cluster to be verified.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   */
  private void verifyDataNodeAndDiskEquivalencyInDc(Datacenter dc, String clusterName,
      PartitionLayout partitionLayout) {
    String dcName = dc.getName();
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    ClusterMapConfig clusterMapConfig = getClusterMapConfig(clusterName, dcName, null);
    StaticClusterManager staticClusterMap =
        (new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout)).getClusterMap();
    String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
    try (PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter = new PropertyStoreToDataNodeConfigAdapter(
        zkConnectStr, clusterMapConfig)) {
      InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
          new InstanceConfigToDataNodeConfigAdapter.Converter(clusterMapConfig);
      Set<String> allInstancesInHelix = new HashSet<>(getInstanceNamesInHelix(dcName, propertyStoreAdapter));
      for (DataNodeId dataNodeId : dc.getDataNodes()) {
        Map<String, Map<String, ReplicaId>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
        DataNode dataNode = (DataNode) dataNodeId;
        String instanceName = getInstanceName(dataNode);
        ensureOrThrow(allInstancesInHelix.remove(instanceName), "Instance not present in Helix " + instanceName);
        DataNodeConfig dataNodeConfig =
            getDataNodeConfigFromHelix(dcName, instanceName, propertyStoreAdapter, instanceConfigConverter);

        Map<String, DataNodeConfig.DiskConfig> diskInfos = new HashMap<>(dataNodeConfig.getDiskConfigs());
        for (Disk disk : dataNode.getDisks()) {
          DataNodeConfig.DiskConfig diskInfoInHelix = diskInfos.remove(disk.getMountPath());
          ensureOrThrow(diskInfoInHelix != null,
              "[" + dcName.toUpperCase() + "] Disk not present for instance " + instanceName + " disk "
                  + disk.getMountPath());
          ensureOrThrow(disk.getRawCapacityInBytes() == diskInfoInHelix.getDiskCapacityInBytes(),
              "[" + dcName.toUpperCase() + "] Capacity mismatch for instance " + instanceName + " disk "
                  + disk.getMountPath());

          // We check replicaInfo only when this is a Bootstrap or Validate operation. For other operations, replica infos
          // are expected to be different on certain nodes.
          if (EnumSet.of(HelixAdminOperation.BootstrapCluster, HelixAdminOperation.ValidateCluster)
              .contains(helixAdminOperation)) {
            Set<String> replicasInClusterMap = new HashSet<>();
            Map<String, ReplicaId> replicaList = mountPathToReplicas.get(disk.getMountPath());
            if (replicaList != null) {
              replicasInClusterMap.addAll(replicaList.keySet());
            }
            Set<String> replicasInHelix = new HashSet<>();
            Map<String, DataNodeConfig.ReplicaConfig> replicaConfigMap = diskInfoInHelix.getReplicaConfigs();
            for (Map.Entry<String, DataNodeConfig.ReplicaConfig> replicaConfigEntry : replicaConfigMap.entrySet()) {
              String replicaName = replicaConfigEntry.getKey();
              DataNodeConfig.ReplicaConfig replicaConfig = replicaConfigEntry.getValue();
              replicasInHelix.add(replicaName);
              ReplicaId replica = replicaList.get(replicaName);
              ensureOrThrow(replicaConfig.getReplicaCapacityInBytes() == replica.getCapacityInBytes(),
                  "[" + dcName.toUpperCase() + "] Replica capacity should be the same.");
              ensureOrThrow(replicaConfig.getPartitionClass().equals(replica.getPartitionId().getPartitionClass()),
                  "[" + dcName.toUpperCase() + "] Partition class should be the same.");
            }
            ensureOrThrow(replicasInClusterMap.equals(replicasInHelix),
                "[" + dcName.toUpperCase() + "] Replica information not consistent for instance " + instanceName
                    + " disk " + disk.getMountPath() + "\n in Helix: " + replicaList + "\n in static clustermap: "
                    + replicasInClusterMap);
          }
        }
        for (Map.Entry<String, DataNodeConfig.DiskConfig> entry : diskInfos.entrySet()) {
          String mountPath = entry.getKey();
          if (!mountPath.startsWith("/mnt")) {
            logger.warn("[{}] Instance {} has unidentifiable mount path in Helix: {}", dcName.toUpperCase(),
                instanceName, mountPath);
          } else {
            throw new AssertionError(
                "[" + dcName.toUpperCase() + "] Instance " + instanceName + " has extra disk in Helix: " + entry);
          }
        }
        ensureOrThrow(!dataNode.hasSSLPort() || (dataNode.getSSLPort() == dataNodeConfig.getSslPort()),
            "[" + dcName.toUpperCase() + "] SSL Port mismatch for instance " + instanceName);
        ensureOrThrow(!dataNode.hasHttp2Port() || (dataNode.getHttp2Port() == dataNodeConfig.getHttp2Port()),
            "[" + dcName.toUpperCase() + "] HTTP2 Port mismatch for instance " + instanceName);
        ensureOrThrow(dataNode.getDatacenterName().equals(dataNodeConfig.getDatacenterName()),
            "[" + dcName.toUpperCase() + "] Datacenter mismatch for instance " + instanceName);
        ensureOrThrow(Objects.equals(dataNode.getRackId(), dataNodeConfig.getRackId()),
            "[" + dcName.toUpperCase() + "] Rack Id mismatch for instance " + instanceName);
        // xid is not set in PropertyStore based DataNodeConfig and will be decommissioned eventually, hence we don't check xid equivalence
      }
      if (expectMoreInHelixDuringValidate) {
        ensureOrThrow(
            allInstancesInHelix.equals(instancesNotForceRemovedByDc.getOrDefault(dc.getName(), new HashSet<>())),
            "[" + dcName.toUpperCase() + "] Additional instances in Helix: " + allInstancesInHelix
                + " not what is expected " + instancesNotForceRemovedByDc.get(dc.getName()));
        info("[{}] *** Helix may have more instances than in the given clustermap as removals were not forced.",
            dcName.toUpperCase());
      } else {
        ensureOrThrow(allInstancesInHelix.isEmpty(),
            "[" + dcName.toUpperCase() + "] Following instances in Helix not found in the clustermap "
                + allInstancesInHelix);
      }
    }
    info("[{}] Successfully verified datanode and disk equivalency in dc {}", dcName.toUpperCase(), dc.getName());
  }

  /**
   * Verify that the partition layout information is in sync.
   * @param dc the datacenter whose information is to be verified.
   * @param clusterName the cluster to be verified.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   */
  private void verifyResourcesAndPartitionEquivalencyInDc(Datacenter dc, String clusterName,
      PartitionLayout partitionLayout) {
    String dcName = dc.getName();
    HelixAdmin admin = adminForDc.get(dc.getName());
    Map<String, Set<String>> allPartitionsToInstancesInHelix = new HashMap<>();
    for (String resourceName : admin.getResourcesInCluster(clusterName)) {
      if (!resourceName.matches("\\d+")) {
        info("[{}] Ignoring resource {} as it is not part of the cluster map", dcName.toUpperCase(), resourceName);
        continue;
      }
      IdealState resourceIS = admin.getResourceIdealState(clusterName, resourceName);
      ensureOrThrow(resourceIS.getStateModelDefRef().equals(stateModelDef),
          "[" + dcName.toUpperCase() + "] StateModel name mismatch for resource " + resourceName);
      Set<String> resourcePartitions = resourceIS.getPartitionSet();
      for (String resourcePartition : resourcePartitions) {
        Set<String> partitionInstanceSet = resourceIS.getInstanceSet(resourcePartition);
        ensureOrThrow(allPartitionsToInstancesInHelix.put(resourcePartition, partitionInstanceSet) == null,
            "[" + dcName.toUpperCase() + "] Partition " + resourcePartition
                + " already found under a different resource.");
      }
    }
    for (PartitionId partitionId : partitionLayout.getPartitions(null)) {
      Partition partition = (Partition) partitionId;
      String partitionName = Long.toString(partition.getId());
      Set<String> replicaHostsInHelix = allPartitionsToInstancesInHelix.remove(partitionName);
      Set<String> expectedInHelix = new HashSet<>();
      List<ReplicaId> replicasInStatic = partition.getReplicas()
          .stream()
          .filter(replica -> replica.getDataNodeId().getDatacenterName().equals(dcName))
          .collect(Collectors.toList());
      ensureOrThrow(replicasInStatic.size() == 0 || replicaHostsInHelix != null,
          "[" + dcName.toUpperCase() + "] No replicas found for partition " + partitionName + " in Helix");
      for (ReplicaId replica : replicasInStatic) {
        String instanceName = getInstanceName(replica.getDataNodeId());
        expectedInHelix.add(instanceName);
        ensureOrThrow(replicaHostsInHelix.remove(instanceName),
            "[" + dcName.toUpperCase() + "] Instance " + instanceName
                + " for the given replica in the clustermap not found in Helix");
      }
      if (!expectMoreInHelixDuringValidate) {
        ensureOrThrow(replicaHostsInHelix == null || replicaHostsInHelix.isEmpty(),
            "[" + dcName.toUpperCase() + "] More instances in Helix than in clustermap for partition: " + partitionName
                + ", expected: " + expectedInHelix + ", found additional instances: " + replicaHostsInHelix);
      }
    }
    if (expectMoreInHelixDuringValidate) {
      ensureOrThrow(allPartitionsToInstancesInHelix.keySet()
              .equals(partitionsNotForceRemovedByDc.getOrDefault(dcName, new HashSet<>())),
          "[" + dcName.toUpperCase() + "] Additional partitions in Helix: " + allPartitionsToInstancesInHelix.keySet()
              + " not what is expected " + partitionsNotForceRemovedByDc.get(dcName));
      info(
          "[{}] *** Helix may have more partitions or replicas than in the given clustermap as removals were not forced.",
          dcName.toUpperCase());
    } else {
      ensureOrThrow(allPartitionsToInstancesInHelix.isEmpty(),
          "[" + dcName.toUpperCase() + "] More partitions in Helix than in clustermap, additional partitions: "
              + allPartitionsToInstancesInHelix.keySet());
    }
    info("[{}] Successfully verified resources and partitions equivalency in dc {}", dcName.toUpperCase(), dcName);
  }

  /**
   * Exposed for testing
   */
  private void maybeAwaitForLatch() {
    if (disablePartitionLatch != null) {
      disablePartitionLatch.countDown();
    }
    if (blockRemovingNodeLatch != null) {
      try {
        blockRemovingNodeLatch.await();
      } catch (Exception e) {
        logger.error("Interrupted when waiting for latch ", e);
      }
    }
  }
}


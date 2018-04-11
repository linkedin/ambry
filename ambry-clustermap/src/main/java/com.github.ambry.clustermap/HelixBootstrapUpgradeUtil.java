/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.SemiAutoModeISBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to bootstrap static cluster map information into Helix.
 *
 * For each node that is added to Helix, its {@link InstanceConfig} will contain the node level information, which is
 * of the following format currently:
 *
 *InstanceConfig: {
 *  "id" : "localhost_17088",                              # id is the instanceName [host_port]
 *  "mapFields" : {
 *    "/tmp/c/0" : {                                       # disk is identified by the [mountpath]. DiskInfo conists of:
 *      "capacityInBytes" : "912680550400",                # [capacity]
 *      "diskState" : "AVAILABLE",                         # [state]
 *      "Replicas" : "10:107374182400,"                    # comma-separated list of partition ids whose replicas are
 *    },                                                   # hosted on this disk in [replica:replicaCapacity] format.
 *    "/tmp/c/1" : {
 *      "capacityInBytes" : "912680550400",
 *      "diskState" : "AVAILABLE",
 *      "Replicas" : "40:107374182400,20:107374182400,"
 *    },
 *    "/tmp/c/2" : {
 *      "capacityInBytes" : "912680550400",
 *      "diskState" : "AVAILABLE",
 *      "Replicas" : "30:107374182400,"
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
class HelixBootstrapUpgradeUtil {
  private final StaticClusterManager staticClusterMap;
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  private final HashMap<String, HashMap<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap = new HashMap<>();
  private final HashMap<String, HashMap<String, DataNodeId>> dcToInstanceNameToDataNodeId = new HashMap<>();
  final String clusterName;
  private final int maxPartitionsInOneResource;
  private final boolean dryRun;
  private Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;
  private static final Logger logger = LoggerFactory.getLogger("Helix bootstrap tool");

  /**
   * Takes in the path to the files that make up the static cluster map and adds or updates the cluster map information
   * in Helix to make the two consistent.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param maxPartitionsInOneResource the maximum number of Ambry partitions to group under a single Helix resource.
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, int maxPartitionsInOneResource, boolean dryRun, HelixAdminFactory helixAdminFactory)
      throws Exception {
    if (dryRun) {
      info("==== This is a dry run ====");
      info("No changes will be made to the information in Helix (except adding the cluster if it does not exist.");
    }
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            maxPartitionsInOneResource, dryRun);
    if (dryRun) {
      info("To drop the cluster, run this tool again with the '--dropCluster {}' argument.",
          clusterMapToHelixMapper.clusterName);
    }
    clusterMapToHelixMapper.updateClusterMapInHelixAlt(helixAdminFactory);
    if (!dryRun) {
      info("Validating static and Helix clustermaps");
      clusterMapToHelixMapper.validateAndClose();
    }
  }

  /**
   * Drop a cluster from Helix.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterName the name of the cluster in Helix.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @throws Exception if there is an error reading a file or in parsing json.
   */
  static void dropCluster(String zkLayoutPath, String clusterName, HelixAdminFactory helixAdminFactory)
      throws Exception {
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress =
        ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));
    info("Dropping cluster {} from Helix", clusterName);
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(entry.getValue().getZkConnectStr());
      admin.dropCluster(clusterName);
      info("Dropped cluster from {}", entry.getKey());
    }
  }

  /**
   * Instantiates this class with the given information.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, int maxPartitionsInOneResource, boolean dryRun) throws Exception {
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    this.dryRun = dryRun;
    this.dataCenterToZkAddress = ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));

    Properties props = new Properties();
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "");
    props.setProperty("clustermap.datacenter.name", "");
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    if (new File(partitionLayoutPath).exists()) {
      staticClusterMap =
          (new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
    } else {
      staticClusterMap = (new StaticClusterAgentsFactory(clusterMapConfig, new PartitionLayout(
          new HardwareLayout(new JSONObject(Utils.readStringFromFile(hardwareLayoutPath)),
              clusterMapConfig)))).getClusterMap();
    }
    String clusterNameInStaticClusterMap = staticClusterMap.partitionLayout.getClusterName();
    this.clusterName = clusterNamePrefix + clusterNameInStaticClusterMap;
    info("Associating static Ambry cluster \"" + clusterNameInStaticClusterMap + "\" with cluster\"" + clusterName
        + "\" in Helix");
    for (Datacenter datacenter : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (!dataCenterToZkAddress.keySet().contains(datacenter.getName())) {
        throw new IllegalArgumentException(
            "There is no ZK host for datacenter " + datacenter.getName() + " in the static clustermap");
      }
    }
  }

  /**
   * Read from the static cluster map and populate a map of {@link DataNodeId} -> list of its {@link ReplicaId}.
   */
  private void populateInstancesAndPartitionsMap() {
    for (DataNodeId dataNodeId : staticClusterMap.getDataNodeIds()) {
      dcToInstanceNameToDataNodeId.computeIfAbsent(dataNodeId.getDatacenterName(), k -> new HashMap<>())
          .put(getInstanceName(dataNodeId), dataNodeId);
      for (DiskId disk : ((DataNode) dataNodeId).getDisks()) {
        instanceToDiskReplicasMap.computeIfAbsent(getInstanceName(dataNodeId), k -> new HashMap<>())
            .put(disk, new TreeSet<>(new ReplicaComparator()));
      }
    }
    for (PartitionId partitionId : staticClusterMap.getAllPartitionIds()) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        instanceToDiskReplicasMap.get(getInstanceName(replicaId.getDataNodeId()))
            .get(replicaId.getDiskId())
            .add((Replica) replicaId);
      }
    }
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
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   */

  private void updateClusterMapInHelixAlt(HelixAdminFactory helixAdminFactory) {
    info("Initializing admins and possibly adding cluster in Helix (if non-existent)");
    initializeAdminsAndAddCluster(helixAdminFactory);
    info("Initialized");
    info("Populating resources and partitions set");
    info("Populated resources and partitions set");
    populateInstancesAndPartitionsMap();
    for (Datacenter dc : staticClusterMap.hardwareLayout.getDatacenters()) {
      info("\n=======Starting datacenter: {}=========\n", dc.getName());
      HelixAdmin dcAdmin = adminForDc.get(dc.getName());
      Map<String, Set<String>> partitionsToInstancesInDc = new HashMap<>();
      info("Getting list of instances in {}", dc.getName());
      Set<String> instancesInHelix = new HashSet<>(dcAdmin.getInstancesInCluster(clusterName));
      Set<String> instancesInStatic = new HashSet<>(dcToInstanceNameToDataNodeId.get(dc.getName()).keySet());
      Set<String> instancesInBoth = new HashSet<>(instancesInHelix);
      // set instances in both correctly.
      instancesInBoth.retainAll(instancesInStatic);
      // set instances in Helix only correctly.
      instancesInHelix.removeAll(instancesInBoth);
      // set instances in Static only correctly.
      instancesInStatic.removeAll(instancesInBoth);
      int totalInstances = instancesInBoth.size() + instancesInHelix.size() + instancesInStatic.size();
      for (String instanceName : instancesInBoth) {
        InstanceConfig instanceConfigInHelix = dcAdmin.getInstanceConfig(clusterName, instanceName);
        InstanceConfig instanceConfigFromStatic =
            createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dc.getName()).get(instanceName),
                partitionsToInstancesInDc);
        // Note about the following check: This compares ZNode data for exact equality, which suffices for now.
        // However, in the future, this needs to discount comparisons for certain things that are dynamically set by
        // instances (and hence will have outdated information in the static map). Such as
        // 1. RO/RW status of replicas.
        // 2. Replica availability (for which support has not yet been added).
        // 3. xid (for which support has not yet been added).
        if (!instanceConfigFromStatic.getRecord().equals(instanceConfigInHelix.getRecord())) {
          info(
              "Instance {} already present in Helix, but InstanceConfig has changed, updating. Remaining instances: {}",
              instanceName, --totalInstances);
          // Continuing on the note above, if there is indeed a change, we must make a call on whether RO/RW, replica
          // availability and so on should be updated at all (if not, instanceConfigFromStatic should be replaced with
          // the appropriate instanceConfig that is constructed with the correct values from both.
          if (!dryRun) {
            dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfigFromStatic);
          }
        } else {
          info("Instance {} already present in Helix, with same InstanceConfig, skipping. Remaining instances: {}",
              instanceName, --totalInstances);
        }
      }

      for (String instanceName : instancesInStatic) {
        InstanceConfig instanceConfigFromStatic =
            createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dc.getName()).get(instanceName),
                partitionsToInstancesInDc);
        info("Instance {} is new, adding to Helix. Remaining instances: {}", instanceName, --totalInstances);
        if (!dryRun) {
          dcAdmin.addInstance(clusterName, instanceConfigFromStatic);
        }
      }

      for (String instanceName : instancesInHelix) {
        info("Instance {} is in Helix, but not in static. Ignoring for now. Remaining instances: {}", instanceName,
            --totalInstances);
        // At this time we are not allowing for dropping instances, to be safe.
        //if (!dryRun) {
        //  dcAdmin.dropInstance(clusterName, new InstanceConfig(instanceName));
        //}
      }

      // Process those partitions that are already under resources. Just update their instance sets if that has changed.
      info(
          "Done adding all instances in {}, now scanning resources in Helix and ensuring instance set for partitions are the same.",
          dc.getName());
      long maxResource = -1;
      for (String resourceName : dcAdmin.getResourcesInCluster(clusterName)) {
        if (!resourceName.matches("\\d+")) {
          // there may be other resources created under the cluster (say, for stats) that are not part of the
          // cluster map. These will be ignored.
          info("Ignoring resource {} as it is not part of the cluster map", resourceName);
          continue;
        }
        maxResource = Math.max(maxResource, Long.valueOf(resourceName));
        IdealState resourceIs = dcAdmin.getResourceIdealState(clusterName, resourceName);
        for (String partitionName : resourceIs.getPartitionSet()) {
          Set<String> instanceSetInHelix = resourceIs.getInstanceSet(partitionName);
          Set<String> instanceSetInStatic = partitionsToInstancesInDc.remove(partitionName);
          if (instanceSetInHelix.equals(instanceSetInStatic)) {
            info("Instance set is the same for partition {} which is under resource {}", partitionName, resourceName);
          } else {
            info("Different instance sets for partition {} which is under resource {}. Updating Helix using static.",
                partitionName, resourceName);
            ArrayList<String> instances = new ArrayList<>(instanceSetInStatic);
            Collections.shuffle(instances);
            resourceIs.setPreferenceList(partitionName, instances);
            if (!dryRun) {
              dcAdmin.setResourceIdealState(clusterName, resourceName, resourceIs);
            }
          }
        }
      }

      info("Done updating all existing partitions, now adding new partitions under new resources, if any.");
      long nextResource = maxResource + 1;
      TreeMap<String, Set<String>> partitionsUnderNextResource = new TreeMap<>();
      for (Map.Entry<String, Set<String>> entry : partitionsToInstancesInDc.entrySet()) {
        partitionsUnderNextResource.put(entry.getKey(), entry.getValue());
        if (partitionsUnderNextResource.size() == maxPartitionsInOneResource) {
          info("maxPartitions reached for resource {}, adding them under it", nextResource);
          addPartitionsUnderResource(dcAdmin, dc.getName(), partitionsUnderNextResource, Long.toString(nextResource));
          info("Added partitions from {} to {} under resource {}", partitionsUnderNextResource.firstKey(),
              partitionsUnderNextResource.lastKey(), nextResource);
          partitionsUnderNextResource.clear();
          nextResource++;
        }
      }
      if (!partitionsUnderNextResource.isEmpty()) {
        info("last resource " + nextResource + ", actually adding all newly added partitions");
        addPartitionsUnderResource(dcAdmin, dc.getName(), partitionsUnderNextResource, Long.toString(nextResource));
        info("Added all remaining partitions {} to {} under resource {}", partitionsUnderNextResource.firstKey(),
            partitionsUnderNextResource.lastKey(), nextResource);
      }
      info("Done with {}\n", dc.getName());
    }
  }

  /**
   * A comparator for replicas that compares based on the partition ids.
   */
  private class ReplicaComparator implements Comparator<ReplicaId> {
    @Override
    public int compare(ReplicaId a, ReplicaId b) {
      return a.getPartitionId().compareTo(b.getPartitionId());
    }
  }

  /**
   * Create an {@link InstanceConfig} for the given node from the static cluster information.
   * @param node the {@link DataNodeId}
   * @param partitionToInstances the map of partitions to instances that will be populated for this instance.
   * @return the constructed {@link InstanceConfig}
   */
  private InstanceConfig createInstanceConfigFromStaticInfo(DataNodeId node,
      Map<String, Set<String>> partitionToInstances) {
    String instanceName = getInstanceName(node);
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(node.getHostname());
    instanceConfig.setPort(Integer.toString(node.getPort()));
    if (node.hasSSLPort()) {
      instanceConfig.getRecord().setSimpleField(ClusterMapUtils.SSLPORT_STR, Integer.toString(node.getSSLPort()));
    }
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.DATACENTER_STR, node.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.RACKID_STR, node.getRackId());
    instanceConfig.getRecord()
        .setSimpleField(ClusterMapUtils.SCHEMA_VERSION_STR, Integer.toString(ClusterMapUtils.CURRENT_SCHEMA_VERSION));
    // xid support has not yet been added.

    List<String> sealedPartitionsList = new ArrayList<>();

    Map<String, Map<String, String>> diskInfos = new HashMap<>();
    for (HashMap.Entry<DiskId, SortedSet<Replica>> diskToReplicas : instanceToDiskReplicasMap.get(instanceName)
        .entrySet()) {
      DiskId disk = diskToReplicas.getKey();
      SortedSet<Replica> replicasInDisk = diskToReplicas.getValue();
      // Note: An instance config has to contain the information for each disk about the replicas it hosts.
      // This information will be initialized to the empty string - but will be updated whenever the partition
      // is added to the cluster.
      StringBuilder replicasStrBuilder = new StringBuilder();
      for (ReplicaId replicaId : replicasInDisk) {
        Replica replica = (Replica) replicaId;
        replicasStrBuilder.append(replica.getPartition().getId())
            .append(ClusterMapUtils.REPLICAS_STR_SEPARATOR)
            .append(replica.getCapacityInBytes())
            .append(ClusterMapUtils.REPLICAS_DELIM_STR);
        if (replica.isSealed()) {
          sealedPartitionsList.add(Long.toString(replica.getPartition().getId()));
        }
        partitionToInstances.computeIfAbsent(Long.toString(replica.getPartition().getId()), k -> new HashSet<>())
            .add(instanceName);
      }
      Map<String, String> diskInfo = new HashMap<>();
      diskInfo.put(ClusterMapUtils.DISK_CAPACITY_STR, Long.toString(disk.getRawCapacityInBytes()));
      diskInfo.put(ClusterMapUtils.DISK_STATE, ClusterMapUtils.AVAILABLE_STR);
      diskInfo.put(ClusterMapUtils.REPLICAS_STR, replicasStrBuilder.toString());
      diskInfos.put(disk.getMountPath(), diskInfo);
    }
    instanceConfig.getRecord().setMapFields(diskInfos);
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedPartitionsList);
    return instanceConfig;
  }

  /**
   * Add partitions under a given resource.
   * @param dcAdmin the {@link HelixAdmin} to use.
   * @param dcName the name of the datacenter in which this resource is being added.
   * @param partitionsToInstances the map reflecting partitions to instances hosting its replicas. All these partitions
   *                              will be added under the given resource
   * @param resourceName the name of the resource under which the given partitions are to be added.
   */
  private void addPartitionsUnderResource(HelixAdmin dcAdmin, String dcName,
      Map<String, Set<String>> partitionsToInstances, String resourceName) {
    // Multiple resources are created and partitions are grouped under these resources upto a maximum threshold.
    if (partitionsToInstances.isEmpty()) {
      throw new IllegalArgumentException("Cannot add resource with zero partitions");
    }
    SemiAutoModeISBuilder resourceISBuilder = new SemiAutoModeISBuilder(resourceName);
    int numReplicas = 0;
    resourceISBuilder.setStateModel(LeaderStandbySMD.name);
    info("Adding partitions for instances in " + dcName);
    for (Map.Entry<String, Set<String>> entry : partitionsToInstances.entrySet()) {
      String partitionName = entry.getKey();
      numReplicas = entry.getValue().size();
      String[] instances = entry.getValue().toArray(new String[0]);
      Collections.shuffle(Arrays.asList(instances));
      resourceISBuilder.assignPreferenceList(partitionName, instances);
    }
    resourceISBuilder.setNumPartitions(partitionsToInstances.size());
    resourceISBuilder.setNumReplica(numReplicas);
    IdealState idealState = resourceISBuilder.build();
    if (!dryRun) {
      dcAdmin.addResource(clusterName, resourceName, idealState);
    }
    info("Added " + partitionsToInstances.size() + " new partitions under resource " + resourceName + " in datacenter "
        + dcName);
  }

  /**
   * Initialize a map of dataCenter to HelixAdmin based on the given zk Connect Strings.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   */
  private void initializeAdminsAndAddCluster(HelixAdminFactory helixAdminFactory) {
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(entry.getValue().getZkConnectStr());
      adminForDc.put(entry.getKey(), admin);
      // Add a cluster entry in every DC
      if (!admin.getClusters().contains(clusterName)) {
        info("Adding cluster {} in {}", clusterName, entry.getKey());
        admin.addCluster(clusterName);
        admin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
      }
    }
  }

  /**
   * Get the instance name string associated with this data node in Helix.
   * @param dataNode the {@link DataNodeId} of the data node.
   * @return the instance name string.
   */
  private static String getInstanceName(DataNodeId dataNode) {
    return ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
  }

  /**
   * Validate that the information in Helix is consistent with the information in the static clustermap; and close
   * all the admin connections to ZK hosts.
   */
  private void validateAndClose() throws Exception {
    try {
      verifyEquivalencyWithStaticClusterMap(staticClusterMap.hardwareLayout, staticClusterMap.partitionLayout);
    } finally {
      for (HelixAdmin admin : adminForDc.values()) {
        admin.close();
      }
    }
  }

  /**
   * Verify that the information in Helix and the information in the static clustermap are equivalent.
   * @param hardwareLayout the {@link HardwareLayout} of the static clustermap.
   * @param partitionLayout the {@link PartitionLayout} of the static clustermap.
   */
  private void verifyEquivalencyWithStaticClusterMap(HardwareLayout hardwareLayout, PartitionLayout partitionLayout)
      throws Exception {
    String clusterNameInStaticClusterMap = hardwareLayout.getClusterName();
    info("Verifying equivalency of static cluster: " + clusterNameInStaticClusterMap + " with the "
        + "corresponding cluster in Helix: " + clusterName);
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      ensureOrThrow(admin != null, "No ZkInfo for datacenter " + dc.getName());
      ensureOrThrow(admin.getClusters().contains(clusterName),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout);
      verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, partitionLayout);
    }
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
  private void verifyDataNodeAndDiskEquivalencyInDc(Datacenter dc, String clusterName, PartitionLayout partitionLayout)
      throws Exception {
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dc.getName());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    StaticClusterManager staticClusterMap =
        (new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout)).getClusterMap();
    HelixAdmin admin = adminForDc.get(dc.getName());
    List<String> allInstancesInHelix = admin.getInstancesInCluster(clusterName);
    for (DataNodeId dataNodeId : dc.getDataNodes()) {
      Map<String, Map<String, String>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
      DataNode dataNode = (DataNode) dataNodeId;
      String instanceName = getInstanceName(dataNode);
      ensureOrThrow(allInstancesInHelix.remove(instanceName), "Instance not present in Helix " + instanceName);
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);

      Map<String, Map<String, String>> diskInfos = new HashMap<>(instanceConfig.getRecord().getMapFields());
      for (Disk disk : dataNode.getDisks()) {
        Map<String, String> diskInfoInHelix = diskInfos.remove(disk.getMountPath());
        ensureOrThrow(diskInfoInHelix != null,
            "Disk not present for instance " + instanceName + " disk " + disk.getMountPath());
        ensureOrThrow(
            disk.getRawCapacityInBytes() == Long.valueOf(diskInfoInHelix.get(ClusterMapUtils.DISK_CAPACITY_STR)),
            "Capacity mismatch for instance " + instanceName + " disk " + disk.getMountPath());

        Set<String> replicasInClusterMap;
        Map<String, String> replicaList = mountPathToReplicas.get(disk.getMountPath());
        replicasInClusterMap = new HashSet<>();
        if (replicaList != null) {
          replicasInClusterMap.addAll(replicaList.keySet());
        }

        Set<String> replicasInHelix;
        String replicasStr = diskInfoInHelix.get(ClusterMapUtils.REPLICAS_STR);
        if (replicasStr.isEmpty()) {
          replicasInHelix = new HashSet<>();
        } else {
          replicasInHelix = new HashSet<>();
          List<String> replicaInfoList = Arrays.asList(replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR));
          for (String replicaInfo : replicaInfoList) {
            String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
            // This is schema specific, but the assumption is that partition id and capacity fields should always be
            // there. At this time these are the only things checked.
            ensureOrThrow(info.length >= 2,
                "Replica info field should have at least two fields - partition id and capacity");
            replicasInHelix.add(info[0]);
            ensureOrThrow(info[1].equals(replicaList.get(info[0])), "Replica capacity should be the same.");
          }
        }

        ensureOrThrow(replicasInClusterMap.equals(replicasInHelix),
            "Replica information not consistent for instance " + instanceName + " disk " + disk.getMountPath()
                + "\n in Helix: " + replicaList + "\n in static clustermap: " + replicasInClusterMap);
      }
      ensureOrThrow(diskInfos.isEmpty(), "Instance " + instanceName + " has extra disks in Helix: " + diskInfos);

      ensureOrThrow(!dataNode.hasSSLPort() || (dataNode.getSSLPort() == Integer.valueOf(
          instanceConfig.getRecord().getSimpleField(ClusterMapUtils.SSLPORT_STR))),
          "SSL Port mismatch for instance " + instanceName);
      ensureOrThrow(dataNode.getDatacenterName()
              .equals(instanceConfig.getRecord().getSimpleField(ClusterMapUtils.DATACENTER_STR)),
          "Datacenter mismatch for instance " + instanceName);
      ensureOrThrow(
          Objects.equals(dataNode.getRackId(), instanceConfig.getRecord().getSimpleField(ClusterMapUtils.RACKID_STR)),
          "Rack Id mismatch for instance " + instanceName);
      Set<String> sealedReplicasInHelix =
          new HashSet<>(instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR));
      Set<String> sealedReplicasInClusterMap = new HashSet<>();
      for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
        if (replica.getPartition().partitionState.equals(PartitionState.READ_ONLY)) {
          sealedReplicasInClusterMap.add(Long.toString(replica.getPartition().getId()));
        }
      }
      ensureOrThrow(sealedReplicasInClusterMap.equals(sealedReplicasInHelix),
          "Sealed replicas info mismatch for " + "instance " + instanceName);
    }
    ensureOrThrow(allInstancesInHelix.isEmpty(),
        "Following instances in Helix not found in the clustermap " + allInstancesInHelix);
    info("Successfully verified datanode and disk equivalency in dc {}", dc.getName());
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
        info("Ignoring resource {} as it is not part of the cluster map", resourceName);
        continue;
      }
      IdealState resourceIS = admin.getResourceIdealState(clusterName, resourceName);
      ensureOrThrow(resourceIS.getStateModelDefRef().equals(LeaderStandbySMD.name),
          "StateModel name mismatch for resource " + resourceName);
      int numReplicasAtResourceLevel = Integer.valueOf(resourceIS.getReplicas());
      Set<String> resourcePartitions = resourceIS.getPartitionSet();
      for (String resourcePartition : resourcePartitions) {
        Set<String> partitionInstanceSet = resourceIS.getInstanceSet(resourcePartition);
        ensureOrThrow(numReplicasAtResourceLevel == partitionInstanceSet.size(),
            "NumReplicas at resource level " + numReplicasAtResourceLevel
                + " different from number of replicas for partition " + partitionInstanceSet);
        ensureOrThrow(allPartitionsToInstancesInHelix.put(resourcePartition, partitionInstanceSet) == null,
            "Partition " + resourcePartition + " already found under a different resource.");
      }
    }
    for (PartitionId partitionId : partitionLayout.getPartitions()) {
      Partition partition = (Partition) partitionId;
      String partitionName = Long.toString(partition.getId());
      Set<String> replicaHostsInHelix = allPartitionsToInstancesInHelix.remove(partitionName);
      Set<String> expectedInHelix = new HashSet<>();
      ensureOrThrow(replicaHostsInHelix != null, "No replicas found for partition " + partitionName + " in Helix");
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDataNodeId().getDatacenterName().equals(dcName)) {
          String instanceName = getInstanceName(replica.getDataNodeId());
          expectedInHelix.add(instanceName);
          ensureOrThrow(replicaHostsInHelix.remove(instanceName),
              "Instance " + instanceName + " for the given " + "replica in the clustermap not found in Helix");
        }
      }
      ensureOrThrow(replicaHostsInHelix.isEmpty(),
          "More instances in Helix than in clustermap for partition: " + partitionName + ", expected: "
              + expectedInHelix + ", found additional instances: " + replicaHostsInHelix);
    }
    ensureOrThrow(allPartitionsToInstancesInHelix.isEmpty(),
        "More partitions in Helix than in clustermap, additional partitions: "
            + allPartitionsToInstancesInHelix.keySet());
    info("Successfully verified resources and partitions equivalency in dc {}", dcName);
  }

  /**
   * A helper method that returns a map of mountPaths to a map of replicas -> replicaCapacity for a given
   * {@link DataNodeId}
   * @param staticClusterMap the static {@link StaticClusterManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  private static Map<String, Map<String, String>> getMountPathToReplicas(StaticClusterManager staticClusterMap,
      DataNodeId dataNodeId) {
    Map<String, Map<String, String>> mountPathToReplicas = new HashMap<>();
    for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
      Map<String, String> replicaStrs = mountPathToReplicas.get(replica.getMountPath());
      if (replicaStrs != null) {
        replicaStrs.put(Long.toString(replica.getPartition().getId()), Long.toString(replica.getCapacityInBytes()));
      } else {
        replicaStrs = new HashMap<>();
        replicaStrs.put(Long.toString(replica.getPartition().getId()), Long.toString(replica.getCapacityInBytes()));
        mountPathToReplicas.put(replica.getMountPath(), replicaStrs);
      }
    }
    return mountPathToReplicas;
  }

  /**
   * Throw {@link AssertionError} if the given condition is false.
   * @param condition the boolean condition to check.
   * @param errStr the error message to associate with the assertion error.
   */
  private void ensureOrThrow(boolean condition, String errStr) {
    if (!condition) {
      throw new AssertionError(errStr);
    }
  }

  /**
   * Log in INFO mode
   * @param format format
   * @param arguments arguments
   */
  private static void info(String format, Object... arguments) {
    logger.info(format, arguments);
  }
}


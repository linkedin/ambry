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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.AutoModeISBuilder;
import org.json.JSONException;
import org.json.JSONObject;


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
 *  }
 *}
 */
class HelixBootstrapUpgradeUtil {
  private final StaticClusterManager staticClusterMap;
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  // The set of partitions already present in Helix when this tool is run.
  private final TreeSet<Long> existingPartitions = new TreeSet<>();
  // The set of resources already present in Helix when this tool is run.
  private final TreeSet<Long> existingResources = new TreeSet<>();
  private final String localDc;
  private final String clusterName;
  private final int maxPartitionsInOneResource;
  private Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;

  /**
   * Takes in the path to the files that make up the static cluster map and adds or updates the cluster map information
   * in Helix to make the two consistent.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param localDc the name of the local datacenter. This can be null.
   * @param maxPartitionsInOneResource the maximum number of Ambry partitions to group under a single Helix resource.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String localDc, int maxPartitionsInOneResource, HelixAdminFactory helixAdminFactory)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, localDc,
            maxPartitionsInOneResource);
    clusterMapToHelixMapper.updateClusterMapInHelix(helixAdminFactory);
    clusterMapToHelixMapper.validateAndClose();
  }

  /**
   * Instantiates this class with the given information.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param localDc the name of the local datacenter. This can be null.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String localDc, int maxPartitionsInOneResource) throws Exception {
    this.localDc = localDc;
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    this.dataCenterToZkAddress = ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));

    Properties props = new Properties();
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", "");
    props.setProperty("clustermap.datacenter.name", localDc == null ? "" : localDc);
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
    System.out.println(
        "Associating static Ambry cluster \"" + clusterNameInStaticClusterMap + "\" with cluster\"" + clusterName
            + "\" in Helix");
    for (Datacenter datacenter : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (!dataCenterToZkAddress.keySet().contains(datacenter.getName())) {
        throw new IllegalArgumentException(
            "There is no ZK host for datacenter " + datacenter.getName() + " in the static clustermap");
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
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   */
  private void updateClusterMapInHelix(HelixAdminFactory helixAdminFactory) {
    initializeAdminsAndAddCluster(helixAdminFactory);
    HelixAdmin refAdmin = localDc != null ? adminForDc.get(localDc) : adminForDc.values().iterator().next();
    populateResourcesAndPartitionsSet(refAdmin);
    addNewDataNodes();
    long nextResource = existingResources.isEmpty() ? 1 : existingResources.last() + 1;
    List<Partition> partitionsUnderNextResource = new ArrayList<>();
    for (PartitionId partitionId : staticClusterMap.partitionLayout.getPartitions()) {
      Partition partition = (Partition) partitionId;
      if (existingPartitions.contains(partition.getId())) {
        updatePartitionInfoIfChanged(partition);
      } else {
        partitionsUnderNextResource.add(partition);
        if (partitionsUnderNextResource.size() == maxPartitionsInOneResource) {
          addNewAmbryPartitions(partitionsUnderNextResource, Long.toString(nextResource));
          partitionsUnderNextResource.clear();
          nextResource++;
        }
      }
    }
    if (!partitionsUnderNextResource.isEmpty()) {
      addNewAmbryPartitions(partitionsUnderNextResource, Long.toString(nextResource));
    }
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
        admin.addCluster(clusterName);
        admin.addStateModelDef(clusterName, LeaderStandbySMD.name, LeaderStandbySMD.build());
      }
    }
  }

  /**
   * Populate the set of existing resources and existing partitions in the cluster. This assumes that all partitions
   * and resources exist in all datacenters (This assumption helps simplify the logic. This can be gotten rid of in
   * the future if need be).
   * @param dcAdmin the reference admin (preferably the admin to the zookeeper server in the local datacenter).
   */
  private void populateResourcesAndPartitionsSet(HelixAdmin dcAdmin) {
    for (String resource : dcAdmin.getResourcesInCluster(clusterName)) {
      existingResources.add(Long.valueOf(resource));
      for (String partition : dcAdmin.getResourceIdealState(clusterName, resource).getPartitionSet()) {
        existingPartitions.add(Long.valueOf(partition));
      }
    }
  }

  /**
   * Add nodes in the static cluster map that is not already present in Helix.
   * Ignores those that are already present. This is to make upgrades smooth.
   *
   * Replica/Partition information is not updated by this method. That is updated when
   * replicas and partitions are added.
   *
   * At this time, node removals are not dealt with.
   */
  private void addNewDataNodes() {
    for (Datacenter dc : staticClusterMap.hardwareLayout.getDatacenters()) {
      HelixAdmin dcAdmin = adminForDc.get(dc.getName());
      for (DataNode node : dc.getDataNodes()) {
        String instanceName = getInstanceName(node);
        if (!dcAdmin.getInstancesInCluster(clusterName).contains(instanceName)) {
          InstanceConfig instanceConfig = new InstanceConfig(instanceName);
          instanceConfig.setHostName(node.getHostname());
          instanceConfig.setPort(Integer.toString(node.getPort()));

          // populate mountPath -> Disk information.
          Map<String, Map<String, String>> diskInfos = new HashMap<>();
          for (Disk disk : node.getDisks()) {
            Map<String, String> diskInfo = new HashMap<>();
            diskInfo.put(ClusterMapUtils.DISK_CAPACITY_STR, Long.toString(disk.getRawCapacityInBytes()));
            diskInfo.put(ClusterMapUtils.DISK_STATE, ClusterMapUtils.AVAILABLE_STR);
            // Note: An instance config has to contain the information for each disk about the replicas it hosts.
            // This information will be initialized to the empty string - but will be updated whenever the partition
            // is added to the cluster.
            diskInfo.put(ClusterMapUtils.REPLICAS_STR, "");
            diskInfos.put(disk.getMountPath(), diskInfo);
          }

          // Add all instance configuration.
          instanceConfig.getRecord().setMapFields(diskInfos);
          if (node.hasSSLPort()) {
            instanceConfig.getRecord().setSimpleField(ClusterMapUtils.SSLPORT_STR, Integer.toString(node.getSSLPort()));
          }
          instanceConfig.getRecord().setSimpleField(ClusterMapUtils.DATACENTER_STR, node.getDatacenterName());
          instanceConfig.getRecord().setSimpleField(ClusterMapUtils.RACKID_STR, Long.toString(node.getRackId()));
          instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, new ArrayList<String>());

          // Finally, add this node to the DC.
          dcAdmin.addInstance(clusterName, instanceConfig);
        }
      }
      System.out.println("Added all new nodes in datacenter " + dc.getName());
    }
  }

  /**
   * Goes through each existing partition and updates the {@link PartitionState} and capacity information for the
   * replicas in each of the instances that hosts a replica for this partition, if it has changed and is different from
   * the current information in the static cluster map.
   * @param partition the partition whose {@link PartitionState} and/or capacity may have to be updated.
   */
  private void updatePartitionInfoIfChanged(Partition partition) {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      String dcName = entry.getKey();
      HelixAdmin dcAdmin = entry.getValue();
      String partitionName = Long.toString(partition.getId());
      long replicaCapacityInStatic = partition.getReplicaCapacityInBytes();
      boolean isSealed = partition.getPartitionState().equals(PartitionState.READ_ONLY);
      List<ReplicaId> replicaList = getReplicasInDc(partition, dcName);
      for (ReplicaId replicaId : replicaList) {
        DataNodeId node = replicaId.getDataNodeId();
        String instanceName = getInstanceName(node);
        InstanceConfig instanceConfig = dcAdmin.getInstanceConfig(clusterName, instanceName);
        boolean shouldSetInstanceConfig = false;
        if (updateSealedStateIfRequired(partitionName, instanceConfig, isSealed)) {
          System.out.println(
              "Sealed state change of partition " + partitionName + " will be updated for instance " + instanceName);
          shouldSetInstanceConfig = true;
        }
        if (updateReplicaCapacityIfRequired(partitionName, instanceConfig, replicaId.getMountPath(),
            replicaCapacityInStatic)) {
          System.out.println(
              "Replica capacity change of partition " + partitionName + " will be updated for instance" + instanceName);
          shouldSetInstanceConfig = true;
        }
        if (shouldSetInstanceConfig) {
          dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
          System.out.println("Successfully updated InstanceConfig for instance " + instanceName);
        }
      }
    }
  }

  /**
   * Update the sealed state of the given partition on the node corresponding to the given {@link InstanceConfig}, if
   * there has been a change.
   * @param partitionName the partition
   * @param instanceConfig the {@link InstanceConfig} of the node.
   * @param isSealed whether the partition is in SEALED state in the static cluster map.
   * @return true, if the {@link InstanceConfig} was updated by this method; false otherwise.
   */
  private boolean updateSealedStateIfRequired(String partitionName, InstanceConfig instanceConfig, boolean isSealed) {
    boolean instanceConfigUpdated = false;
    List<String> currentSealedPartitions = instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR);
    List<String> newSealedPartitionsList = new ArrayList<>(currentSealedPartitions);
    if (isSealed && !currentSealedPartitions.contains(partitionName)) {
      newSealedPartitionsList.add(partitionName);
    } else if (!isSealed && currentSealedPartitions.contains(partitionName)) {
      newSealedPartitionsList.remove(partitionName);
    }
    if (!currentSealedPartitions.equals(newSealedPartitionsList)) {
      instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, newSealedPartitionsList);
      instanceConfigUpdated = true;
    }
    return instanceConfigUpdated;
  }

  /**
   * Update replica capacity for the given partition on the node corresponding to the given
   * {@link InstanceConfig}, if there has been a change.
   * @param partitionName the partition
   * @param instanceConfig the {@link InstanceConfig} of the node.
   * @param mountPath the mount path of the replica.
   * @return true, if the {@link InstanceConfig} was updated by this method; false otherwise.
   */
  private boolean updateReplicaCapacityIfRequired(String partitionName, InstanceConfig instanceConfig, String mountPath,
      long actualReplicaCapacity) {
    boolean instanceConfigUpdated = false;
    Map<String, String> diskInfo = instanceConfig.getRecord().getMapField(mountPath);
    String currentReplicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);
    StringBuilder newReplicaStrBuilder = new StringBuilder();
    List<String> replicaInfoList = Arrays.asList(currentReplicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR));
    for (String replicaInfo : replicaInfoList) {
      String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
      if (info[0].equals(partitionName)) {
        long capacityInHelix = Long.valueOf(info[1]);
        if (capacityInHelix != actualReplicaCapacity) {
          info[1] = Long.toString(actualReplicaCapacity);
        }
      }
      newReplicaStrBuilder.append(info[0])
          .append(ClusterMapUtils.REPLICAS_STR_SEPARATOR)
          .append(info[1])
          .append(ClusterMapUtils.REPLICAS_DELIM_STR);
    }
    String newReplicaStr = newReplicaStrBuilder.toString();
    if (!currentReplicasStr.equals(newReplicaStr)) {
      diskInfo.put(ClusterMapUtils.REPLICAS_STR, newReplicaStr);
      instanceConfig.getRecord().setMapField(mountPath, diskInfo);
      instanceConfigUpdated = true;
    }
    return instanceConfigUpdated;
  }

  /**
   * Adds all partitions to every datacenter with replicas in nodes as specified in the static clustermap (unless it
   * was already added).
   *
   * The assumption is that in the static layout, every partition is contained in every colo. We make this assumption
   * to ensure that partitions are grouped under the same resource in all colos (since the resource id is not
   * something that is present today in the static cluster map). This is not a strict requirement though, but helps
   * ease the logic.
   *
   * Note: 1. We ensure that the partition names are unique in the Ambry cluster even across resources.
   *       2. New Ambry partitions will not be added to Helix resources that are already present before the call to this
   *          method.
   */
  private void addNewAmbryPartitions(List<Partition> partitions, String resourceName) {
    // In the future, a resource may be used to group together partitions of a container. For now, multiple
    // resources are created and partitions are grouped under these resources upto a maximum threshold.
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException("Cannot add resource with zero partitions");
    }
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      String dcName = entry.getKey();
      HelixAdmin dcAdmin = entry.getValue();
      AutoModeISBuilder resourceISBuilder = new AutoModeISBuilder(resourceName);
      int numReplicas = 0;
      resourceISBuilder.setStateModel(LeaderStandbySMD.name);
      for (Partition partition : partitions) {
        String partitionName = Long.toString(partition.getId());
        boolean sealed = partition.getPartitionState().equals(PartitionState.READ_ONLY);
        List<ReplicaId> replicaList = getReplicasInDc(partition, dcName);
        numReplicas = replicaList.size();
        String[] instances = updateInstancesAndGetInstanceNames(dcAdmin, partitionName, replicaList, sealed);
        Collections.shuffle(Arrays.asList(instances));
        resourceISBuilder.assignPreferenceList(partitionName, instances);
      }
      resourceISBuilder.setNumPartitions(partitions.size());
      resourceISBuilder.setNumReplica(numReplicas);
      IdealState idealState = resourceISBuilder.build();
      dcAdmin.addResource(clusterName, resourceName, idealState);
      System.out.println(
          "Added " + partitions.size() + " new partitions under resource " + resourceName + " in datacenter " + dcName);
    }
  }

  /**
   * Updates instances that hosts replicas of this partition with the replica information (including the mount points
   * on which these replicas should reside, which will be purely an instance level information).
   * @param dcAdmin the admin to the Zk server on which this operation is to be done.
   * @param partitionName the partition name.
   * @param replicaList the list of replicas of this partition.
   * @param sealed whether the given partition state is sealed.
   * @return an array of Strings containing the names of the instances on which the replicas of this partition reside.
   */
  private String[] updateInstancesAndGetInstanceNames(HelixAdmin dcAdmin, String partitionName,
      List<ReplicaId> replicaList, boolean sealed) {
    String[] instances = new String[replicaList.size()];
    for (int i = 0; i < replicaList.size(); i++) {
      Replica replica = (Replica) replicaList.get(i);
      DataNodeId node = replica.getDataNodeId();
      String instanceName = getInstanceName(node);
      instances[i] = instanceName;
      InstanceConfig instanceConfig = dcAdmin.getInstanceConfig(clusterName, instanceName);
      Map<String, String> diskInfo = instanceConfig.getRecord().getMapField(replica.getMountPath());
      String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);
      replicasStr +=
          replica.getPartition().getId() + ClusterMapUtils.REPLICAS_STR_SEPARATOR + replica.getCapacityInBytes()
              + ClusterMapUtils.REPLICAS_DELIM_STR;
      diskInfo.put(ClusterMapUtils.REPLICAS_STR, replicasStr);
      instanceConfig.getRecord().setMapField(replica.getMountPath(), diskInfo);
      if (sealed) {
        List<String> currentSealedPartitions = instanceConfig.getRecord().getListField(ClusterMapUtils.SEALED_STR);
        List<String> newSealedPartitionsList = new ArrayList<>(currentSealedPartitions);
        newSealedPartitionsList.add(partitionName);
        instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, newSealedPartitionsList);
      }
      dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    }
    return instances;
  }

  /**
   * Helper method to get the list of {@link ReplicaId} of all replicas of a partition in the given datacenter.
   * @param partition the partition of interest.
   * @param dcName the datacenter to which the returned replicas should belong.
   * @return a list of {@link ReplicaId} of all replicas of the given partition in the given datacenter.
   */
  private List<ReplicaId> getReplicasInDc(Partition partition, String dcName) {
    // returns a copy unlike getReplicas()
    List<ReplicaId> replicaList = partition.getReplicaIds();
    ListIterator<ReplicaId> iter = replicaList.listIterator();
    while (iter.hasNext()) {
      if (!iter.next().getDataNodeId().getDatacenterName().equals(dcName)) {
        iter.remove();
      }
    }
    return replicaList;
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
    System.out.println("Verifying equivalency of static cluster: " + clusterNameInStaticClusterMap + " with the "
        + "corresponding cluster in Helix: " + clusterName);
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      ensureOrThrow(admin != null, "No ZkInfo for datacenter " + dc.getName());
      ensureOrThrow(admin.getClusters().contains(clusterName),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout);
      verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, partitionLayout);
    }
    System.out.println("Successfully verified equivalency of static cluster: " + clusterNameInStaticClusterMap
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
          dataNode.getRackId() == Long.valueOf(instanceConfig.getRecord().getSimpleField(ClusterMapUtils.RACKID_STR)),
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
      ensureOrThrow(replicaHostsInHelix != null, "No replicas found for partition " + partitionName + " in Helix");
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDataNodeId().getDatacenterName().equals(dcName)) {
          String instanceName = getInstanceName(replica.getDataNodeId());
          ensureOrThrow(replicaHostsInHelix.remove(instanceName),
              "Instance " + instanceName + " for the given " + "replica in the clustermap not found in Helix");
        }
      }
      ensureOrThrow(replicaHostsInHelix.isEmpty(),
          "More instances in Helix than in clustermap for partition: " + partitionName + " additional instances: "
              + replicaHostsInHelix);
    }
    ensureOrThrow(allPartitionsToInstancesInHelix.isEmpty(),
        "More partitions in Helix than in clustermap, additional partitions: "
            + allPartitionsToInstancesInHelix.keySet());
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
}


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
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import com.google.common.collect.Sets;
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
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.builder.AutoModeISBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * This tool takes the hardware layout, partition layout and the Zk hosts information json files as input,
 * and updates the ZK hosts with the contents of the layout files. It adds all partitions and hosts that have not
 * previously been added (so, initially this will bootstrap the cluster information and on an ongoing basis, this can
 * add new nodes and partitions).
 *
 * The existing hardware and partition layout json files will be read in as is.
 *
 * The ZkLayoutPath argument containing the Zk hosts information in each datacenter should be a json of the
 * following example form:
 *
 * {
 *   "zkInfo" : [
 *     {
 *       "datacenter":"dc1",
 *       "zkConnectStr":"abc.example.com:2199",
 *     },
 *     {
 *       "datacenter":"dc2",
 *       "zkConnectStr":"def.example.com:2300",
 *     }
 *   ]
 * }
 *
 * This tool should be run from an admin node that has access to the nodes in the hardware layout. The access is
 * required because the static {@link ClusterMapManager} that we use to parse the static layout files validates
 * these nodes.
 *
 * The tool does the following:
 * 1. Bootstraps a static cluster map by adding nodes and partitions to Helix.
 * 2. Upgrades the information with changes in the static clustermap that involve new nodes and new partitions.
 *    To avoid over-complicating things, it assumes that the existing partition assignment does not change during an
 *    upgrade. Newly added partitions can be distributed in any way (new partitions can have replicas even in
 *    previously added nodes).
 * 3. Upgrades will also update the partition states if required (READ_WRITE to SEALED or vice versa) for existing
 *    partitions.
 *
 */
public class HelixBootstrapUpgradeTool {
  private final ClusterMapManager staticClusterMap;
  private final Map<String, String> dataCenterToZkAddress = new HashMap<>();
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  // The set of partitions already present in Helix when this tool is run.
  private final TreeSet<Long> existingPartitions = new TreeSet<>();
  // The set of resources already present in Helix when this tool is run.
  private final TreeSet<Long> existingResources = new TreeSet<>();
  private final String localDc;
  private final String clusterName;
  private final int maxPartitionsInOneResource;

  private static final String CAPACITY_STR = "capacityInBytes";
  private static final String REPLICAS_STR = "Replicas";
  private static final String REPLICAS_DELIM_STR = ",";
  private static final String SSLPORT_STR = "sslPort";
  private static final String DATACENTER_STR = "datacenter";
  private static final String RACKID_STR = "rackId";
  private static final String SEALED_STR = "SEALED";
  private static final String ZKINFO_STR = "zkInfo";
  private static final String ZKCONNECTSTR_STR = "zkConnectStr";

  static final int DEFAULT_MAX_PARTITIONS_PER_RESOURCE = 100;

  /**
   * @param args takes in three mandatory arguments: the hardware layout path, the partition layout path and the zk
   *             layout path.
   *             The Zk layout has to be of the following form:
   *             {
   *               "zkInfo" : [
   *                 {
   *                   "datacenter":"dc1",
   *                   "zkConnectStr":"abc.example.com:2199",
   *                 },
   *                 {
   *                   "datacenter":"dc2",
   *                   "zkConnectStr":"def.example.com:2300",
   *                 }
   *               ]
   *             }
   *
   *             Also takes in an optional argument that specifies the local datacenter name, so that can be used as
   *             the "reference" datacenter. If none provided, the tool simply chooses one of the datacenters in the
   *             layout as the reference datacenter.
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> hardwareLayoutPathOpt =
        parser.accepts("hardwareLayoutPath", "The path to the hardware layout json file")
            .withRequiredArg()
            .describedAs("hardware_layout_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> partitionLayoutPathOpt =
        parser.accepts("partitionLayoutPath", "The path to the partition layout json file")
            .withRequiredArg()
            .describedAs("partition_layout_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> zkLayoutPathOpt = parser.accepts("zkLayoutPath",
        "The path to the json file containing zookeeper connect info. This should be of the following form: \n{\n"
            + "  \"zkInfo\" : [\n" + "     {\n" + "       \"datacenter\":\"dc1\",\n"
            + "       \"zkConnectStr\":\"abc.example.com:2199\",\n" + "     },\n" + "     {\n"
            + "       \"datacenter\":\"dc2\",\n" + "       \"zkConnectStr\":\"def.example.com:2300\",\n" + "     },\n"
            + "     {\n" + "       \"datacenter\":\"dc3\",\n" + "       \"zkConnectStr\":\"ghi.example.com:2400\",\n"
            + "     }\n" + "  ]\n" + "}").
        withRequiredArg().
        describedAs("zk_connect_info_path").
        ofType(String.class);

    ArgumentAcceptingOptionSpec<String> localDcOpt =
        parser.accepts("localDc", "(Optional argument) The local datacenter name")
            .withRequiredArg()
            .describedAs("local_dc")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> maxPartitionsInOneResourceOpt = parser.accepts("maxPartitionsInOneResource",
        "(Optional argument) The maximum number of partitions that should be grouped under a Helix resource")
        .withRequiredArg()
        .describedAs("max_partitions_in_one_resource")
        .ofType(String.class);

    OptionSet options = parser.parse(args);
    String hardwareLayoutPath = options.valueOf(hardwareLayoutPathOpt);
    String partitionLayoutPath = options.valueOf(partitionLayoutPathOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    ArrayList<OptionSpec> listOpt = new ArrayList<>();
    listOpt.add(hardwareLayoutPathOpt);
    listOpt.add(partitionLayoutPathOpt);
    listOpt.add(zkLayoutPathOpt);
    ToolUtils.ensureOrExit(listOpt, options, parser);
    bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, options.valueOf(localDcOpt),
        options.valueOf(maxPartitionsInOneResourceOpt) == null ? DEFAULT_MAX_PARTITIONS_PER_RESOURCE
            : Integer.valueOf(options.valueOf(maxPartitionsInOneResourceOpt)));
  }

  /**
   * Takes in the path to the files that make up the static cluster map and adds or updates the cluster map information
   * in Helix to make the two consistent.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param localDc the name of the local datacenter. This can be null.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String localDc, int maxPartitionsInOneResource) throws IOException, JSONException {
    HelixBootstrapUpgradeTool clusterMapToHelixMapper =
        new HelixBootstrapUpgradeTool(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, localDc,
            maxPartitionsInOneResource);
    clusterMapToHelixMapper.updateClusterMapInHelix();
    clusterMapToHelixMapper.validateAndClose();
  }

  /**
   * Instantiates this class with the given information.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param localDc the name of the local datacenter. This can be null.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeTool(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String localDc, int maxPartitionsInOneResource) throws IOException, JSONException {
    this.localDc = localDc;
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    parseZkJsonAndPopulateZkInfo(zkLayoutPath);

    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));
    if (new File(partitionLayoutPath).exists()) {
      staticClusterMap = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath, clusterMapConfig);
    } else {
      staticClusterMap = new ClusterMapManager(new PartitionLayout(
          new HardwareLayout(new JSONObject(Utils.readStringFromFile(hardwareLayoutPath)), clusterMapConfig)));
    }
    clusterName = staticClusterMap.partitionLayout.getClusterName();
    for (Datacenter datacenter : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (!dataCenterToZkAddress.keySet().contains(datacenter.getName())) {
        throw new IllegalArgumentException(
            "There is no ZK host for datacenter " + datacenter.getName() + " in the static clustermap");
      }
    }
  }

  /**
   * Parses the zk layout JSON file and populates an internal map of datacenter name to Zk connect strings.
   * @param zkLayoutPath the path to the Zookeeper layout file.
   * @throws IOException if there is an error reading the file.
   * @throws JSONException if there is an error parsing the JSON.
   */
  private void parseZkJsonAndPopulateZkInfo(String zkLayoutPath) throws IOException, JSONException {
    JSONObject root = new JSONObject(Utils.readStringFromFile(zkLayoutPath));
    JSONArray all = (root).getJSONArray(ZKINFO_STR);
    for (int i = 0; i < all.length(); i++) {
      JSONObject entry = all.getJSONObject(i);
      dataCenterToZkAddress.put(entry.getString(DATACENTER_STR), entry.getString(ZKCONNECTSTR_STR));
    }
  }

  /**
   * Map the information in the layout files to Helix. Calling this method multiple times has no effect if the
   * information in the static files do not change. This tool is therefore safe to use for upgrades.
   *
   * Instead of defining the entire cluster under a single resource, or defining a resource for every partition, the
   * tool groups together partitions under resources, with a limit to the number of partitions that will be grouped
   * under a single resource.
   */
  private void updateClusterMapInHelix() {
    initializeAdminsAndAddCluster();
    HelixAdmin refAdmin = localDc != null ? adminForDc.get(localDc) : adminForDc.values().iterator().next();
    populateResourcesAndPartitionsSet(refAdmin);
    addNewDataNodes();
    long nextResource = existingResources.isEmpty() ? 1 : existingResources.last() + 1;
    List<Partition> partitionsUnderNextResource = new ArrayList<>();
    for (PartitionId partitionId : staticClusterMap.partitionLayout.getPartitions()) {
      Partition partition = (Partition) partitionId;
      if (existingPartitions.contains(partition.getId())) {
        updatePartitionStateIfChanged(partition);
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
   */
  private void initializeAdminsAndAddCluster() {
    for (Map.Entry<String, String> entry : dataCenterToZkAddress.entrySet()) {
      ZKHelixAdmin admin = new ZKHelixAdmin(entry.getValue());
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
            diskInfo.put(CAPACITY_STR, Long.toString(disk.getRawCapacityInBytes()));
            // Note: An instance config has to contain the information for each disk about the replicas it hosts.
            // This information will be initialized to the empty string - but will be updated whenever the partition
            // is added to the cluster.
            diskInfo.put(REPLICAS_STR, "");
            diskInfos.put(disk.getMountPath(), diskInfo);
          }

          // Add all instance configuration.
          instanceConfig.getRecord().setMapFields(diskInfos);
          instanceConfig.getRecord().setSimpleField(SSLPORT_STR, Integer.toString(node.getSSLPort()));
          instanceConfig.getRecord().setSimpleField(DATACENTER_STR, node.getDatacenterName());
          instanceConfig.getRecord().setSimpleField(RACKID_STR, Long.toString(node.getRackId()));
          instanceConfig.getRecord().setListField(SEALED_STR, new ArrayList<String>());

          // Finally, add this node to the DC.
          dcAdmin.addInstance(clusterName, instanceConfig);
        }
      }
    }
  }

  /**
   * Goes through each existing partition and changes the {@link PartitionState} for the replicas in each of the
   * instances that hosts a replica for this partition, if it has changed and is different from the current state
   * in Helix.
   * @param partition the partition whose {@link PartitionState} may have to be updated.
   */
  private void updatePartitionStateIfChanged(Partition partition) {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      String dcName = entry.getKey();
      HelixAdmin dcAdmin = entry.getValue();
      String partitionName = Long.toString(partition.getId());
      boolean isSealed = partition.getPartitionState().equals(PartitionState.READ_ONLY);
      List<ReplicaId> replicaList = getReplicasInDc(partition, dcName);
      for (ReplicaId replicaId : replicaList) {
        DataNodeId node = replicaId.getDataNodeId();
        String instanceName = getInstanceName(node);
        InstanceConfig instanceConfig = dcAdmin.getInstanceConfig(clusterName, instanceName);
        List<String> currentSealedPartitions = instanceConfig.getRecord().getListField(SEALED_STR);
        List<String> newSealedPartitionsList = new ArrayList<>(currentSealedPartitions);
        if (isSealed && !currentSealedPartitions.contains(partitionName)) {
          newSealedPartitionsList.add(partitionName);
        } else if (!isSealed && currentSealedPartitions.contains(partitionName)) {
          newSealedPartitionsList.remove(partitionName);
        }
        instanceConfig.getRecord().setListField(SEALED_STR, newSealedPartitionsList);
        // @todo: uncomment when we move to Helix 0.6.7.
        // dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
      }
    }
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
      resourceISBuilder.setNumReplica(numReplicas);
      IdealState idealState = resourceISBuilder.build();
      dcAdmin.addResource(clusterName, resourceName, idealState);
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
      String replicasStr = diskInfo.get(REPLICAS_STR);
      replicasStr += replica.getPartition().getId() + REPLICAS_DELIM_STR;
      diskInfo.put(REPLICAS_STR, replicasStr);
      instanceConfig.getRecord().setMapField(replica.getMountPath(), diskInfo);
      if (sealed) {
        List<String> currentSealedPartitions = instanceConfig.getRecord().getListField(SEALED_STR);
        List<String> newSealedPartitionsList = new ArrayList<>(currentSealedPartitions);
        newSealedPartitionsList.add(partitionName);
        instanceConfig.getRecord().setListField(SEALED_STR, newSealedPartitionsList);
      }
      // @todo: uncomment when we move to Helix 0.6.7.
      // dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
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
    return dataNode.getHostname() + "_" + dataNode.getPort();
  }

  /**
   * Validate that the information in Helix is consistent with the information in the static clustermap; and close
   * all the admin connections to ZK hosts.
   */
  private void validateAndClose() {
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
  private void verifyEquivalencyWithStaticClusterMap(HardwareLayout hardwareLayout, PartitionLayout partitionLayout) {
    String clusterName = hardwareLayout.getClusterName();
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      ensureOrThrow(admin != null, "No ZkInfo for datacenter " + dc.getName());
      ensureOrThrow(admin.getClusters().contains(clusterName),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout);
      verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, partitionLayout);
    }
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
    ClusterMapManager staticClusterMap = new ClusterMapManager(partitionLayout);
    HelixAdmin admin = adminForDc.get(dc.getName());
    List<String> allInstancesInHelix = admin.getInstancesInCluster(clusterName);
    for (DataNodeId dataNodeId : dc.getDataNodes()) {
      Map<String, List<String>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
      DataNode dataNode = (DataNode) dataNodeId;
      String instanceName = getInstanceName(dataNode);
      ensureOrThrow(allInstancesInHelix.remove(instanceName), "Instance not present in Helix " + instanceName);
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);

      Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
      for (Disk disk : dataNode.getDisks()) {
        Map<String, String> diskInfoInHelix = diskInfos.remove(disk.getMountPath());
        ensureOrThrow(diskInfoInHelix != null,
            "Disk not present for instance " + instanceName + " disk " + disk.getMountPath());
        ensureOrThrow(disk.getRawCapacityInBytes() == Long.valueOf(diskInfoInHelix.get(CAPACITY_STR)),
            "Capacity mismatch for instance " + instanceName + " disk " + disk.getMountPath());
        Set<String> replicasInHelix;
        String replicasStr = diskInfoInHelix.get(REPLICAS_STR);
        if (replicasStr.isEmpty()) {
          replicasInHelix = new HashSet<>();
        } else {
          replicasInHelix = Sets.newHashSet(replicasStr.split(REPLICAS_DELIM_STR));
        }
        Set<String> replicasInClusterMap;
        List<String> replicaList = mountPathToReplicas.get(disk.getMountPath());
        if (replicaList == null) {
          replicasInClusterMap = new HashSet<>();
        } else {
          replicasInClusterMap = Sets.newHashSet(replicaList);
        }
        ensureOrThrow(replicasInClusterMap.equals(replicasInHelix),
            "Replica information not consistent for instance " + instanceName + " disk " + disk.getMountPath()
                + "\n in Helix: " + replicaList + "\n in static clustermap: " + replicasInClusterMap);
      }
      ensureOrThrow(diskInfos.isEmpty(), "Instance " + instanceName + " has extra disks in Helix: " + diskInfos);

      ensureOrThrow(dataNode.getSSLPort() == Integer.valueOf(instanceConfig.getRecord().getSimpleField(SSLPORT_STR)),
          "SSL Port mismatch for instance " + instanceName);
      ensureOrThrow(dataNode.getDatacenterName().equals(instanceConfig.getRecord().getSimpleField(DATACENTER_STR)),
          "Datacenter mismatch for instance " + instanceName);
      ensureOrThrow(dataNode.getRackId() == Long.valueOf(instanceConfig.getRecord().getSimpleField(RACKID_STR)),
          "Rack Id mismatch for instance " + instanceName);
      Set<String> sealedReplicasInHelix = Sets.newHashSet(instanceConfig.getRecord().getListField(SEALED_STR));
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
   * A helper method that returns a map of mountPaths to a list of replicas for a given {@link DataNodeId}
   * @param staticClusterMap the static {@link ClusterMapManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  private static Map<String, List<String>> getMountPathToReplicas(ClusterMapManager staticClusterMap,
      DataNodeId dataNodeId) {
    Map<String, List<String>> mountPathToReplicas = new HashMap<>();
    for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
      List<String> replicaStrs = mountPathToReplicas.get(replica.getMountPath());
      if (replicaStrs != null) {
        replicaStrs.add(Long.toString(replica.getPartition().getId()));
      } else {
        replicaStrs = new ArrayList<>();
        replicaStrs.add(Long.toString(replica.getPartition().getId()));
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


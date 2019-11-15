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

import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
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
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.store.HelixPropertyStore;
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
class HelixBootstrapUpgradeUtil {
  private final StaticClusterManager staticClusterMap;
  private final String zkLayoutPath;
  private final String stateModelDef;
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  private final Map<String, Map<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap = new HashMap<>();
  private final Map<String, Map<String, DataNodeId>> dcToInstanceNameToDataNodeId = new HashMap<>();
  private final int maxPartitionsInOneResource;
  private final boolean dryRun;
  private final boolean forceRemove;
  private int maxResource = -1;
  private final String clusterName;
  private boolean expectMoreInHelixDuringValidate = false;
  private Map<String, Set<String>> instancesNotForceRemovedByDc = new HashMap<>();
  private Map<String, Set<String>> partitionsNotForceRemovedByDc = new HashMap<>();
  private int instancesAdded = 0;
  private int instancesUpdated = 0;
  private int instancesDropped = 0;
  private int resourcesAdded = 0;
  private int resourcesUpdated = 0;
  private int resourcesDropped = 0;
  private Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;
  private HelixClusterManager validatingHelixClusterManager;
  private static final Logger logger = LoggerFactory.getLogger("Helix bootstrap tool");
  private static final String ALL = "all";

  /**
   * Parse the dc string argument and return a map of dc -> DcZkInfo for every datacenter that is enabled.
   * @param dcs the string argument for dcs.
   * @param zkLayoutPath the path to the zkLayout file.
   * @return a map of dc -> {@link com.github.ambry.clustermap.ClusterMapUtils.DcZkInfo} for every enabled dc.
   * @throws IOException if there is an IO error reading the zkLayout file.
   */
  static Map<String, ClusterMapUtils.DcZkInfo> parseAndUpdateDcInfoFromArg(String dcs, String zkLayoutPath)
      throws IOException {
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress =
        ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));
    Set<String> parsedDcSet;
    if (Utils.isNullOrEmpty(dcs)) {
      throw new IllegalArgumentException("dcs string cannot be null or empty.");
    }
    if (dcs.equalsIgnoreCase(ALL)) {
      parsedDcSet = new HashSet<>(dataCenterToZkAddress.keySet());
    } else {
      parsedDcSet = Arrays.stream(dcs.replaceAll("\\p{Space}", "").split(",")).collect(Collectors.toSet());
      HashSet<String> diff = new HashSet<>(parsedDcSet);
      diff.removeAll(dataCenterToZkAddress.keySet());
      if (!diff.isEmpty()) {
        throw new IllegalArgumentException("Unknown datacenter(s) supplied" + diff);
      }
    }
    dataCenterToZkAddress.entrySet().removeIf(e -> !parsedDcSet.contains(e.getKey()));
    return dataCenterToZkAddress;
  }

  /**
   * Takes in the path to the files that make up the static cluster map and adds or updates the cluster map information
   * in Helix to make the two consistent.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of datacenters that should be updated in this run.
   * @param maxPartitionsInOneResource the maximum number of Ambry partitions to group under a single Helix resource.
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @param forceRemove if true, removes any hosts from Helix not present in the json files.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param startValidatingClusterManager whether validation should include starting up a {@link HelixClusterManager}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, boolean startValidatingClusterManager, String stateModelDef)
      throws Exception {
    if (dryRun) {
      info("==== This is a dry run ====");
      info("No changes will be made to the information in Helix (except adding the cluster if it does not exist.");
    }
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            maxPartitionsInOneResource, dryRun, forceRemove, helixAdminFactory, stateModelDef);
    if (dryRun) {
      info("To drop the cluster, run this tool again with the '--dropCluster {}' argument.",
          clusterMapToHelixMapper.clusterName);
    }
    clusterMapToHelixMapper.updateClusterMapInHelix(startValidatingClusterManager);
    if (!dryRun) {
      clusterMapToHelixMapper.validateAndClose();
    }
    clusterMapToHelixMapper.logSummary();
  }

  /**
   * Takes in the path to the files that make up the static cluster map and uploads the cluster admin configs (such as
   * partition override, replica addition) to HelixPropertyStore in zookeeper.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param maxPartitionsInOneResource the maximum number of Ambry partitions to group under a single Helix resource.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param adminTypes types of admin operation that requires to generate config and upload it to Helix PropertyStore.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void uploadClusterAdminConfigs(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, HelixAdminFactory helixAdminFactory,
      String[] adminTypes) throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            maxPartitionsInOneResource, false, false, helixAdminFactory, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF);
    for (String adminType : adminTypes) {
      generateAdminInfosAndUpload(clusterMapToHelixMapper, adminType);
    }
  }

  /**
   * Add given state model def to ambry cluster in enabled datacenter(s)
   */
  static void addStateModelDef(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, HelixAdminFactory helixAdminFactory,
      String stateModelDef) throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            maxPartitionsInOneResource, false, false, helixAdminFactory, stateModelDef);
    clusterMapToHelixMapper.addStateModelDef();
    info("State model def is successfully added");
  }

  /**
   * Takes in the path to the files that make up the static cluster map and validates that the information is consistent
   * with the information in Helix.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of datacenters that needs to be upgraded/bootstrapped.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void validate(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, HelixAdminFactory helixAdminFactory, String stateModelDef)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs, 0,
            false, false, helixAdminFactory, stateModelDef);
    clusterMapToHelixMapper.validateAndClose();
    clusterMapToHelixMapper.logSummary();
  }

  /**
   * Drop a cluster from Helix.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterName the name of the cluster in Helix.
   * @param dcs the comma-separated list of datacenters that needs to be upgraded/bootstrapped.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @throws Exception if there is an error reading a file or in parsing json.
   */
  static void dropCluster(String zkLayoutPath, String clusterName, String dcs, HelixAdminFactory helixAdminFactory)
      throws Exception {
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress = parseAndUpdateDcInfoFromArg(dcs, zkLayoutPath);
    info("Dropping cluster {} from Helix", clusterName);
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(entry.getValue().getZkConnectStr());
      admin.dropCluster(clusterName);
      info("Dropped cluster from {}", entry.getKey());
    }
  }

  /**
   * Generate cluster admin configs based on admin type and upload them to Helix PropertyStore
   * @param clusterMapToHelixMapper {@link HelixBootstrapUpgradeUtil} to use.
   * @param adminType the type of admin operation.
   */
  private static void generateAdminInfosAndUpload(HelixBootstrapUpgradeUtil clusterMapToHelixMapper, String adminType) {
    switch (adminType) {
      case ClusterMapUtils.PARTITION_OVERRIDE_STR:
        Map<String, Map<String, Map<String, String>>> partitionOverrideInfosByDc =
            clusterMapToHelixMapper.generatePartitionOverrideFromAllDCs();
        clusterMapToHelixMapper.uploadClusterAdminInfos(partitionOverrideInfosByDc,
            ClusterMapUtils.PARTITION_OVERRIDE_STR, ClusterMapUtils.PARTITION_OVERRIDE_ZNODE_PATH);
        break;
      case ClusterMapUtils.REPLICA_ADDITION_STR:
        Map<String, Map<String, Map<String, String>>> replicaAdditionInfosByDc =
            clusterMapToHelixMapper.generateReplicaAdditionMap();
        clusterMapToHelixMapper.uploadClusterAdminInfos(replicaAdditionInfosByDc, ClusterMapUtils.REPLICA_ADDITION_STR,
            ClusterMapUtils.REPLICA_ADDITION_ZNODE_PATH);
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized admin config type: " + adminType + ". Current supported types are: "
                + ClusterMapUtils.PARTITION_OVERRIDE_STR + ", " + ClusterMapUtils.REPLICA_ADDITION_STR);
    }
    info("Upload cluster configs completed.");
  }

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
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef) throws Exception {
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    this.dryRun = dryRun;
    this.forceRemove = forceRemove;
    this.zkLayoutPath = zkLayoutPath;
    this.stateModelDef = stateModelDef;
    dataCenterToZkAddress = parseAndUpdateDcInfoFromArg(dcs, zkLayoutPath);
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
          new HardwareLayout(new JSONObject(Utils.readStringFromFile(hardwareLayoutPath)), clusterMapConfig),
          null))).getClusterMap();
    }
    String clusterNameInStaticClusterMap = staticClusterMap.partitionLayout.getClusterName();
    clusterName = clusterNamePrefix + clusterNameInStaticClusterMap;
    info("Associating static Ambry cluster \"" + clusterNameInStaticClusterMap + "\" with cluster\"" + clusterName
        + "\" in Helix");
    for (Datacenter datacenter : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (dcs.equalsIgnoreCase(ALL) && !dataCenterToZkAddress.keySet().contains(datacenter.getName())) {
        throw new IllegalArgumentException(
            "There is no ZK host for datacenter " + datacenter.getName() + " in the static clustermap");
      }
    }
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(entry.getValue().getZkConnectStr());
      adminForDc.put(entry.getKey(), admin);
    }
  }

  /**
   * Generate the partition override map containing partition state from all datacenters.
   * @return the constructed partitionOverrideInfos by dc. The format is as follows.
   *
   * "mapFields": {
   *    "0": {
   *      "partitionClass": "max-replicas-all-datacenters", (TODO)
   *      "state": "RW"
   *    },
   *    "1": {
   *      "partitionClass": "max-replicas-all-datacenters", (TODO)
   *      "state": "RO"
   *    }
   * }
   */
  private Map<String, Map<String, Map<String, String>>> generatePartitionOverrideFromAllDCs() {
    Map<String, Map<String, String>> partitionOverrideInfos = new HashMap<>();
    for (PartitionId partitionId : staticClusterMap.getAllPartitionIds(null)) {
      String partitionName = partitionId.toPathString();
      Map<String, String> partitionProperties = new HashMap<>();
      partitionProperties.put(ClusterMapUtils.PARTITION_STATE,
          partitionId.getPartitionState() == PartitionState.READ_WRITE ? ClusterMapUtils.READ_WRITE_STR
              : ClusterMapUtils.READ_ONLY_STR);
      partitionOverrideInfos.put(partitionName, partitionProperties);
    }
    Map<String, Map<String, Map<String, String>>> partitionOverrideByDc = new HashMap<>();
    for (String dc : dataCenterToZkAddress.keySet()) {
      partitionOverrideByDc.put(dc, partitionOverrideInfos);
    }
    return partitionOverrideByDc;
  }

  /**
   * Generate replica addition infos map grouped by each dc. This map contains detailed replica info (size, mount path, etc)
   * that will be used by certain server to instantiate new added Ambry replica. The format is as follows.
   * "mapFields": {
   *     "1": {
   *         "replicaCapacityInBytes": 107374182400,
   *         "partitionClass": "max-replicas-all-datacenters",
   *         "localhost1_17088": "/tmp/c/1",
   *         "localhost2_17088": "/tmp/d/1"
   *     },
   *     "2": {
   *         "replicaCapacityInBytes": 107374182400,
   *         "partitionClass": "max-replicas-all-datacenters",
   *         "localhost3_17088": "/tmp/e/1"
   *     }
   * }
   * In above example, two new replicas of partition[1] will be added to localhost1 and localhost2 respectively.
   * The host name is followed by mount path on which the new replica should be placed.
   * @return a map that contains detailed replica info.
   */
  private Map<String, Map<String, Map<String, String>>> generateReplicaAdditionMap() {
    //populate dcToInstanceNameToDataNodeId and instanceToDiskReplicasMap
    populateInstancesAndPartitionsMap();
    Map<String, Map<String, Map<String, String>>> newAddedReplicasByDc = new HashMap<>();
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      HelixAdmin dcAdmin = entry.getValue();
      String dcName = entry.getKey();
      info("Generating replica addition map for datacenter {}", dcName);
      Map<String, Map<String, Replica>> partitionToInstancesAndReplicas = new HashMap<>();
      Map<String, Map<String, String>> newAddedReplicasInDc = new HashMap<>();
      for (String instanceName : dcToInstanceNameToDataNodeId.get(dcName).keySet()) {
        Map<DiskId, SortedSet<Replica>> diskToReplica = instanceToDiskReplicasMap.get(instanceName);
        for (SortedSet<Replica> replicas : diskToReplica.values()) {
          for (Replica replica : replicas) {
            partitionToInstancesAndReplicas.computeIfAbsent(replica.getPartitionId().toPathString(),
                key -> new HashMap<>()).put(instanceName, replica);
          }
        }
      }
      List<String> resourcesInCluster = dcAdmin.getResourcesInCluster(clusterName);
      for (String resourceName : resourcesInCluster) {
        if (!resourceName.matches("\\d+")) {
          continue;
        }
        IdealState idealState = dcAdmin.getResourceIdealState(clusterName, resourceName);
        for (String partitionStr : new HashSet<>(idealState.getPartitionSet())) {
          Set<String> instanceSetInHelix = idealState.getInstanceSet(partitionStr);
          Map<String, Replica> instanceAndReplicaInStatic = partitionToInstancesAndReplicas.get(partitionStr);
          if (instanceAndReplicaInStatic == null || instanceAndReplicaInStatic.isEmpty()) {
            info(
                "*** Partition {} no longer present in the static clustermap. Uploading cluster admin infos operation won't remove it *** ",
                partitionStr);
          } else if (!instanceAndReplicaInStatic.keySet().equals(instanceSetInHelix)) {
            info(
                "Different instance sets for partition {} under resource {}. Extracting new replicas from static clustermap.",
                partitionStr, resourceName);
            // instances in static only
            Set<String> instanceSetInStatic = instanceAndReplicaInStatic.keySet();
            instanceSetInStatic.removeAll(instanceSetInHelix);
            for (String instance : instanceSetInStatic) {
              Replica replica = instanceAndReplicaInStatic.get(instance);
              info("New replica of partition[{}] will be added to instance {} on {}", partitionStr, instance,
                  replica.getMountPath());
              newAddedReplicasInDc.computeIfAbsent(partitionStr, key -> {
                Map<String, String> partitionMap = new HashMap<>();
                partitionMap.put(ClusterMapUtils.PARTITION_CLASS_STR, replica.getPartitionId().getPartitionClass());
                partitionMap.put(ClusterMapUtils.REPLICAS_CAPACITY_STR, String.valueOf(replica.getCapacityInBytes()));
                return partitionMap;
              }).put(instance, replica.getMountPath());
            }
          }
        }
      }
      newAddedReplicasByDc.put(dcName, newAddedReplicasInDc);
    }
    return newAddedReplicasByDc;
  }

  /**
   * Uploads cluster config infos onto Helix PropertyStore.
   * @param adminInfosByDc the cluster admin information (overridden partitions, added replicas) grouped by DC that would
   *                       be applied to cluster.
   * @param clusterAdminType the type of cluster admin that would be uploaded (i.e. PartitionOverride, ReplicaAddition)
   * @param adminConfigZNodePath ZNode path of admin config associated with clusterAdminType.
   */
  private void uploadClusterAdminInfos(Map<String, Map<String, Map<String, String>>> adminInfosByDc,
      String clusterAdminType, String adminConfigZNodePath) {
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path",
        "/" + clusterName + "/" + ClusterMapUtils.PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      info("Uploading {} infos for datacenter {}.", clusterAdminType, entry.getKey());
      HelixPropertyStore<ZNRecord> helixPropertyStore =
          CommonUtils.createHelixPropertyStore(entry.getValue().getZkConnectStr(), propertyStoreConfig, null);
      ZNRecord znRecord = new ZNRecord(clusterAdminType);
      znRecord.setMapFields(adminInfosByDc.get(entry.getKey()));
      if (!helixPropertyStore.set(adminConfigZNodePath, znRecord, AccessOption.PERSISTENT)) {
        info("Failed to upload {} infos for datacenter {}", clusterAdminType, entry.getKey());
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
    for (PartitionId partitionId : staticClusterMap.getAllPartitionIds(null)) {
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
   * @param startValidatingClusterManager whether validation should include staring up a {@link HelixClusterManager}
   */
  private void updateClusterMapInHelix(boolean startValidatingClusterManager) throws IOException {
    info("Initializing admins and possibly adding cluster in Helix (if non-existent)");
    maybeAddCluster();
    info("Validating cluster manager is {}", startValidatingClusterManager ? "ENABLED" : "DISABLED");
    if (startValidatingClusterManager) {
      startClusterManager();
    }
    info("Populating resources and partitions set");
    populateInstancesAndPartitionsMap();
    info("Populated resources and partitions set");
    for (Datacenter dc : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (adminForDc.containsKey(dc.getName())) {
        info("\n=======Starting datacenter: {}=========\n", dc.getName());
        Map<String, Set<String>> partitionsToInstancesInDc = new HashMap<>();
        addUpdateInstances(dc.getName(), partitionsToInstancesInDc);
        // Process those partitions that are already under resources. Just update their instance sets if that has changed.
        info(
            "Done adding all instances in {}, now scanning resources in Helix and ensuring instance set for partitions are the same.",
            dc.getName());
        addUpdateResources(dc.getName(), partitionsToInstancesInDc);
      } else {
        info("\n========Skipping datacenter: {}==========\n", dc.getName());
      }
    }
  }

  /**
   * Add and/or update instances in Helix based on the information in the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  private void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    info("Getting list of instances in {}", dcName);
    Set<String> instancesInHelix = new HashSet<>(dcAdmin.getInstancesInCluster(clusterName));
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
      InstanceConfig instanceConfigInHelix = dcAdmin.getInstanceConfig(clusterName, instanceName);
      InstanceConfig instanceConfigFromStatic =
          createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dcName).get(instanceName),
              partitionsToInstancesInDc, instanceToDiskReplicasMap, instanceConfigInHelix);
      if (!instanceConfigFromStatic.getRecord().equals(instanceConfigInHelix.getRecord())) {
        info("Instance {} already present in Helix, but InstanceConfig has changed, updating. Remaining instances: {}",
            instanceName, --totalInstances);
        // Continuing on the note above, if there is indeed a change, we must make a call on whether RO/RW, replica
        // availability and so on should be updated at all (if not, instanceConfigFromStatic should be replaced with
        // the appropriate instanceConfig that is constructed with the correct values from both).
        if (!dryRun) {
          dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfigFromStatic);
        }
        instancesUpdated++;
      } else {
        info("Instance {} already present in Helix, with same InstanceConfig, skipping. Remaining instances: {}",
            instanceName, --totalInstances);
      }
    }

    for (String instanceName : instancesInStatic) {
      InstanceConfig instanceConfigFromStatic =
          createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dcName).get(instanceName),
              partitionsToInstancesInDc, instanceToDiskReplicasMap, null);
      info("Instance {} is new, adding to Helix. Remaining instances: {}", instanceName, --totalInstances);
      if (!dryRun) {
        dcAdmin.addInstance(clusterName, instanceConfigFromStatic);
      }
      instancesAdded++;
    }

    for (String instanceName : instancesInHelix) {
      if (forceRemove) {
        info("Instance {} is in Helix, but not in static. Forcefully removing. Remaining instances: {}", instanceName,
            --totalInstances);
        if (!dryRun) {
          dcAdmin.dropInstance(clusterName, new InstanceConfig(instanceName));
        }
        instancesDropped++;
      } else {
        info("Instance {} is in Helix, but not in static. Ignoring for now (use --forceRemove to forcefully remove). "
            + "Remaining instances: {}", instanceName, --totalInstances);
        expectMoreInHelixDuringValidate = true;
        instancesNotForceRemovedByDc.computeIfAbsent(dcName, k -> new HashSet<>()).add(instanceName);
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
  private void addUpdateResources(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    List<String> resourcesInCluster = dcAdmin.getResourcesInCluster(clusterName);
    for (String resourceName : resourcesInCluster) {
      boolean resourceModified = false;
      if (!resourceName.matches("\\d+")) {
        // there may be other resources created under the cluster (say, for stats) that are not part of the
        // cluster map. These will be ignored.
        continue;
      }
      maxResource = Math.max(maxResource, Integer.valueOf(resourceName));
      IdealState resourceIs = dcAdmin.getResourceIdealState(clusterName, resourceName);
      for (String partitionName : new HashSet<>(resourceIs.getPartitionSet())) {
        Set<String> instanceSetInHelix = resourceIs.getInstanceSet(partitionName);
        Set<String> instanceSetInStatic = partitionsToInstancesInDc.remove(partitionName);
        if (instanceSetInStatic == null || instanceSetInStatic.isEmpty()) {
          if (forceRemove) {
            info("*** Partition {} no longer present in the static clustermap, removing from Resource *** ",
                partitionName);
            // this is a hacky way of removing a partition from the resource, as there isn't another way today.
            // Helix team is planning to provide an API for this.
            resourceIs.getRecord().getListFields().remove(partitionName);
            resourceModified = true;
          } else {
            info(
                "*** forceRemove option not provided, resources will not be removed (use --forceRemove to forcefully remove)");
            expectMoreInHelixDuringValidate = true;
            partitionsNotForceRemovedByDc.computeIfAbsent(dcName, k -> new HashSet<>()).add(partitionName);
          }
        } else if (!instanceSetInStatic.equals(instanceSetInHelix)) {
          info("Different instance sets for partition {} under resource {}. Updating Helix using static.",
              partitionName, resourceName);
          ArrayList<String> newInstances = new ArrayList<>(instanceSetInStatic);
          Collections.shuffle(newInstances);
          resourceIs.setPreferenceList(partitionName, newInstances);
          // Existing resources may not have ANY_LIVEINSTANCE set as the numReplicas (which allows for different
          // replication for different partitions under the same resource). So set it here (We use the name() method and
          // not the toString() method for the enum as that is what Helix uses).
          resourceIs.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
          resourceModified = true;
          // TODO when move replica feature is ready, the infos of new added replicas should be uploaded to PropertyStore
        }
      }
      // update state model def if necessary
      if (!resourceIs.getStateModelDefRef().equals(stateModelDef)) {
        info("Resource {} has different state model {}. Updating it with {}", resourceName,
            resourceIs.getStateModelDefRef(), stateModelDef);
        resourceIs.setStateModelDefRef(stateModelDef);
        resourceModified = true;
      }
      resourceIs.setNumPartitions(resourceIs.getPartitionSet().size());
      if (resourceModified) {
        if (resourceIs.getPartitionSet().isEmpty()) {
          if (!dryRun) {
            dcAdmin.dropResource(clusterName, resourceName);
          }
          resourcesDropped++;
        } else {
          if (!dryRun) {
            dcAdmin.setResourceIdealState(clusterName, resourceName, resourceIs);
          }
          resourcesUpdated++;
        }
      }
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
      info("Adding partitions for next resource in {}", dcName);
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
      }
      resourcesAdded++;
      info("Added {} new partitions under resource {} in datacenter {}", partitionsUnderNextResource.size(),
          resourceName, dcName);
    }
  }

  /**
   * Add new state model def to ambry cluster in enabled datacenter(s).
   */
  private void addStateModelDef() {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      // Add a cluster entry in every enabled DC
      String dcName = entry.getKey();
      HelixAdmin admin = entry.getValue();
      if (!admin.getClusters().contains(clusterName)) {
        throw new IllegalStateException("Cluster " + clusterName + " in " + dcName + " doesn't exist!");
      }
      if (!admin.getStateModelDefs(clusterName).contains(stateModelDef)) {
        info("Adding state model def {} in {} for cluster {}", stateModelDef, dcName, clusterName);
        admin.addStateModelDef(clusterName, stateModelDef, getStateModelDefinition(stateModelDef));
      } else {
        info("{} in {} already has state model def {}, skip adding operation", clusterName, dcName, stateModelDef);
      }
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
   * @param instanceToDiskReplicasMap the map of instances to the map of disk to set of replicas.
   * @param referenceInstanceConfig the InstanceConfig used to set the fields that are not derived from the json files.
   *                                These are the SEALED state and STOPPED_REPLICAS configurations. If this field is null,
   *                                then these fields are derived from the json files. This can happen if this is a newly
   *                                added node.
   * @return the constructed {@link InstanceConfig}
   */
  static InstanceConfig createInstanceConfigFromStaticInfo(DataNodeId node,
      Map<String, Set<String>> partitionToInstances,
      Map<String, Map<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap, InstanceConfig referenceInstanceConfig) {
    String instanceName = getInstanceName(node);
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(node.getHostname());
    instanceConfig.setPort(Integer.toString(node.getPort()));
    if (node.hasSSLPort()) {
      instanceConfig.getRecord().setSimpleField(ClusterMapUtils.SSLPORT_STR, Integer.toString(node.getSSLPort()));
    }
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.DATACENTER_STR, node.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(ClusterMapUtils.RACKID_STR, node.getRackId());
    long xid = node.getXid();
    if (xid != ClusterMapUtils.DEFAULT_XID) {
      // Set the XID only if it is not the default, in order to avoid unnecessary updates.
      instanceConfig.getRecord().setSimpleField(ClusterMapUtils.XID_STR, Long.toString(node.getXid()));
    }
    instanceConfig.getRecord()
        .setSimpleField(ClusterMapUtils.SCHEMA_VERSION_STR, Integer.toString(ClusterMapUtils.CURRENT_SCHEMA_VERSION));

    List<String> sealedPartitionsList = new ArrayList<>();
    List<String> stoppedReplicasList = new ArrayList<>();
    if (instanceToDiskReplicasMap.containsKey(instanceName)) {
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
              .append(ClusterMapUtils.REPLICAS_STR_SEPARATOR)
              .append(replica.getPartition().getPartitionClass())
              .append(ClusterMapUtils.REPLICAS_DELIM_STR);
          if (referenceInstanceConfig == null && replica.isSealed()) {
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
    }

    // Set the fields that need to be preserved from the referenceInstanceConfig.
    if (referenceInstanceConfig != null) {
      sealedPartitionsList = ClusterMapUtils.getSealedReplicas(referenceInstanceConfig);
      stoppedReplicasList = ClusterMapUtils.getStoppedReplicas(referenceInstanceConfig);
    }
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedPartitionsList);
    instanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, stoppedReplicasList);
    return instanceConfig;
  }

  /**
   * Initialize a map of dataCenter to HelixAdmin based on the given zk Connect Strings.
   */
  private void maybeAddCluster() {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      // Add a cluster entry in every DC
      String dcName = entry.getKey();
      HelixAdmin admin = entry.getValue();
      if (!admin.getClusters().contains(clusterName)) {
        info("Adding cluster {} in {}", clusterName, dcName);
        admin.addCluster(clusterName);
        admin.addStateModelDef(clusterName, stateModelDef, getStateModelDefinition(stateModelDef));
      }
    }
  }

  /**
   * Get state model definition based on given name.
   * @param stateModelDefName the name of state model definition that would be employed by Ambry cluster.
   * @return {@link StateModelDefinition}
   */
  private StateModelDefinition getStateModelDefinition(String stateModelDefName) {
    StateModelDefinition stateModelDefinition = null;
    switch (stateModelDefName) {
      case ClusterMapConfig.OLD_STATE_MODEL_DEF:
        stateModelDefinition = LeaderStandbySMD.build();
        break;
      case ClusterMapConfig.AMBRY_STATE_MODEL_DEF:
        stateModelDefinition = AmbryStateModelDefinition.getDefinition();
        break;
      default:
        throw new IllegalArgumentException("Unsupported state model def: " + stateModelDefName);
    }
    return stateModelDefinition;
  }

  /**
   * Start the Helix Cluster Manager to be used for validation.
   * @throws IOException if there was an error instantiating the cluster manager.
   */
  private void startClusterManager() throws IOException {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", adminForDc.keySet().iterator().next());
    props.setProperty("clustermap.dcs.zk.connect.strings", Utils.readStringFromFile(zkLayoutPath));
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    HelixClusterAgentsFactory helixClusterAgentsFactory = new HelixClusterAgentsFactory(clusterMapConfig, null, null);
    validatingHelixClusterManager = helixClusterAgentsFactory.getClusterMap();
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
  private void validateAndClose() {
    try {
      info("Validating static and Helix cluster maps");
      verifyEquivalencyWithStaticClusterMap(staticClusterMap.hardwareLayout, staticClusterMap.partitionLayout);
      if (validatingHelixClusterManager != null) {
        ensureOrThrow(validatingHelixClusterManager.getErrorCount() == 0,
            "Helix cluster manager should not have encountered any errors");
      }
    } finally {
      if (validatingHelixClusterManager != null) {
        validatingHelixClusterManager.close();
      }
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
    String clusterNameInStaticClusterMap = hardwareLayout.getClusterName();
    info("Verifying equivalency of static cluster: " + clusterNameInStaticClusterMap + " with the "
        + "corresponding cluster in Helix: " + clusterName);
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      if (admin == null) {
        info("Skipping {}", dc.getName());
        continue;
      }
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
  private void verifyDataNodeAndDiskEquivalencyInDc(Datacenter dc, String clusterName,
      PartitionLayout partitionLayout) {
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dc.getName());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    StaticClusterManager staticClusterMap =
        (new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout)).getClusterMap();
    HelixAdmin admin = adminForDc.get(dc.getName());
    Set<String> allInstancesInHelix = new HashSet<>(admin.getInstancesInCluster(clusterName));
    for (DataNodeId dataNodeId : dc.getDataNodes()) {
      Map<String, Map<String, Replica>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
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
        Map<String, Replica> replicaList = mountPathToReplicas.get(disk.getMountPath());
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
            Replica replica = replicaList.get(info[0]);
            ensureOrThrow(info[1].equals(Long.toString(replica.getCapacityInBytes())),
                "Replica capacity should be the same.");
            if (info.length > 2) {
              ensureOrThrow(info[2].equals(replica.getPartition().getPartitionClass()),
                  "Partition class should be the same.");
            }
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

      String xidInHelix = instanceConfig.getRecord().getSimpleField(ClusterMapUtils.XID_STR);
      if (xidInHelix == null) {
        xidInHelix = Long.toString(ClusterMapUtils.DEFAULT_XID);
      }
      ensureOrThrow(Objects.equals(Long.toString(dataNode.getXid()), xidInHelix),
          "Xid mismatch for instance " + instanceName);
    }
    if (expectMoreInHelixDuringValidate) {
      ensureOrThrow(allInstancesInHelix.equals(instancesNotForceRemovedByDc.get(dc.getName())),
          "Additional instances in Helix: " + allInstancesInHelix + " not what is expected "
              + instancesNotForceRemovedByDc.get(dc.getName()));
      info("*** Helix may have more instances than in the given clustermap as removals were not forced.");
    } else {
      ensureOrThrow(allInstancesInHelix.isEmpty(),
          "Following instances in Helix not found in the clustermap " + allInstancesInHelix);
    }
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
      ensureOrThrow(resourceIS.getStateModelDefRef().equals(stateModelDef),
          "StateModel name mismatch for resource " + resourceName);
      Set<String> resourcePartitions = resourceIS.getPartitionSet();
      for (String resourcePartition : resourcePartitions) {
        Set<String> partitionInstanceSet = resourceIS.getInstanceSet(resourcePartition);
        ensureOrThrow(allPartitionsToInstancesInHelix.put(resourcePartition, partitionInstanceSet) == null,
            "Partition " + resourcePartition + " already found under a different resource.");
      }
    }
    for (PartitionId partitionId : partitionLayout.getPartitions(null)) {
      Partition partition = (Partition) partitionId;
      String partitionName = Long.toString(partition.getId());
      Set<String> replicaHostsInHelix = allPartitionsToInstancesInHelix.remove(partitionName);
      Set<String> expectedInHelix = new HashSet<>();
      List<Replica> replicasInStatic = partition.getReplicas()
          .stream()
          .filter(replica -> replica.getDataNodeId().getDatacenterName().equals(dcName))
          .collect(Collectors.toList());
      ensureOrThrow(replicasInStatic.size() == 0 || replicaHostsInHelix != null,
          "No replicas found for partition " + partitionName + " in Helix");
      for (Replica replica : replicasInStatic) {
        String instanceName = getInstanceName(replica.getDataNodeId());
        expectedInHelix.add(instanceName);
        ensureOrThrow(replicaHostsInHelix.remove(instanceName),
            "Instance " + instanceName + " for the given replica in the clustermap not found in Helix");
      }
      if (!expectMoreInHelixDuringValidate) {
        ensureOrThrow(replicaHostsInHelix == null || replicaHostsInHelix.isEmpty(),
            "More instances in Helix than in clustermap for partition: " + partitionName + ", expected: "
                + expectedInHelix + ", found additional instances: " + replicaHostsInHelix);
      }
    }
    if (expectMoreInHelixDuringValidate) {
      ensureOrThrow(allPartitionsToInstancesInHelix.keySet()
              .equals(partitionsNotForceRemovedByDc.getOrDefault(dcName, new HashSet<>())),
          "Additional partitions in Helix: " + allPartitionsToInstancesInHelix.keySet() + " not what is expected "
              + partitionsNotForceRemovedByDc.get(dcName));
      info("*** Helix may have more partitions or replicas than in the given clustermap as removals were not forced.");
    } else {
      ensureOrThrow(allPartitionsToInstancesInHelix.isEmpty(),
          "More partitions in Helix than in clustermap, additional partitions: "
              + allPartitionsToInstancesInHelix.keySet());
    }
    info("Successfully verified resources and partitions equivalency in dc {}", dcName);
  }

  /**
   * A helper method that returns a map of mountPaths to a map of replicas -> replicaCapacity for a given
   * {@link DataNodeId}
   * @param staticClusterMap the static {@link StaticClusterManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  private static Map<String, Map<String, Replica>> getMountPathToReplicas(StaticClusterManager staticClusterMap,
      DataNodeId dataNodeId) {
    Map<String, Map<String, Replica>> mountPathToReplicas = new HashMap<>();
    for (Replica replica : staticClusterMap.getReplicas(dataNodeId)) {
      Map<String, Replica> replicaStrs = mountPathToReplicas.get(replica.getMountPath());
      if (replicaStrs != null) {
        replicaStrs.put(Long.toString(replica.getPartition().getId()), replica);
      } else {
        replicaStrs = new HashMap<>();
        replicaStrs.put(Long.toString(replica.getPartition().getId()), replica);
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

  /**
   * Log the summary of this run.
   */
  private void logSummary() {
    if (instancesUpdated + instancesAdded + instancesDropped + resourcesUpdated + resourcesAdded + resourcesDropped
        > 0) {
      if (!dryRun) {
        info("========Cluster in Helix was updated, summary:========");
      } else {
        info("========Dry run: Actual run would update the cluster in the following way:========");
      }
      info("New instances added: {}", instancesAdded);
      info("Existing instances updated: {}", instancesUpdated);
      info("Existing instances dropped: {}", instancesDropped);
      info("New resources added: {}", resourcesAdded);
      info("Existing resources updated: {}", resourcesUpdated);
      info("Existing resources dropped: {}", resourcesDropped);
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
}


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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
public class HelixBootstrapUpgradeUtil {
  static final int DEFAULT_MAX_PARTITIONS_PER_RESOURCE = 100;
  static final String HELIX_DISABLED_PARTITION_STR =
      InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name();
  private final StaticClusterManager staticClusterMap;
  private final String zkLayoutPath;
  private final String stateModelDef;
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  // These two maps should be concurrent map because they are accessed in multi-threaded context (see addUpdateInstances
  // method).
  // For now, the inner map doesn't need to be concurrent map because it is within a certain dc which means there should
  // be only one thread that updates it.
  private final ConcurrentHashMap<String, Map<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Map<String, DataNodeId>> dcToInstanceNameToDataNodeId =
      new ConcurrentHashMap<>();
  private final int maxPartitionsInOneResource;
  private final boolean dryRun;
  private final boolean forceRemove;
  private final String clusterName;
  private final String hostName;
  private final Integer portNum;
  private final String partitionName;
  private final HelixAdminOperation helixAdminOperation;
  private boolean expectMoreInHelixDuringValidate = false;
  private ConcurrentHashMap<String, Set<String>> instancesNotForceRemovedByDc = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Set<String>> partitionsNotForceRemovedByDc = new ConcurrentHashMap<>();
  private final AtomicInteger instancesAdded = new AtomicInteger();
  private final AtomicInteger instancesUpdated = new AtomicInteger();
  private final AtomicInteger instancesDropped = new AtomicInteger();
  private final AtomicInteger resourcesAdded = new AtomicInteger();
  private final AtomicInteger resourcesUpdated = new AtomicInteger();
  private final AtomicInteger resourcesDropped = new AtomicInteger();
  private final AtomicInteger partitionsDisabled = new AtomicInteger();
  private final AtomicInteger partitionsEnabled = new AtomicInteger();
  private final AtomicInteger partitionsReset = new AtomicInteger();
  private Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;
  private HelixClusterManager validatingHelixClusterManager;
  private static final Logger logger = LoggerFactory.getLogger("Helix bootstrap tool");
  private static final String ALL = "all";

  enum HelixAdminOperation {
    BootstrapCluster, ValidateCluster, UpdateIdealState, DisablePartition, EnablePartition, ResetPartition
  }

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
    // nothing to do for cloud datacenters in the tool.
    dataCenterToZkAddress.values().removeIf(dcZkInfo -> dcZkInfo.getReplicaType() == ReplicaType.CLOUD_BACKED);
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
   * @param dcs the comma-separated list of data centers that should be updated in this run.
   * @param maxPartitionsInOneResource the maximum number of Ambry partitions to group under a single Helix resource.
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @param forceRemove if true, removes any hosts from Helix not present in the json files.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param startValidatingClusterManager whether validation should include starting up a {@link HelixClusterManager}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @param helixAdminOperation the {@link HelixAdminOperation} to perform on resources (partitions). This is used in
   *                           the context of move replica. So it only updates IdealState without touching anything else.
   *                           InstanceConfig will be updated by nodes themselves.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, boolean startValidatingClusterManager, String stateModelDef,
      HelixAdminOperation helixAdminOperation) throws Exception {
    if (dryRun) {
      info("==== This is a dry run ====");
      info("No changes will be made to the information in Helix (except adding the cluster if it does not exist.");
    }
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            maxPartitionsInOneResource, dryRun, forceRemove, helixAdminFactory, stateModelDef, null, null, null,
            helixAdminOperation);
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
   * Upload or delete cluster admin configs.
   * For upload, it takes in the path to the files that make up the static cluster map and uploads the cluster admin
   * configs (such as partition override, replica addition) to HelixPropertyStore in zookeeper.
   * For delete, it searches admin config names in given dc and delete them if corresponding znodes exist.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param forceRemove whether to remove admin configs from cluster.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param adminTypes types of admin operation that requires to generate config and upload it to Helix PropertyStore.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void uploadOrDeleteAdminConfigs(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, boolean forceRemove, HelixAdminFactory helixAdminFactory,
      String[] adminTypes) throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, forceRemove, helixAdminFactory,
            ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, null, null, null, null);
    if (forceRemove) {
      for (String adminType : adminTypes) {
        removeAdminInfosFromCluster(clusterMapToHelixMapper, adminType);
      }
    } else {
      for (String adminType : adminTypes) {
        generateAdminInfosAndUpload(clusterMapToHelixMapper, adminType);
      }
    }
  }

  /**
   * Add given state model def to ambry cluster in enabled datacenter(s)
   */
  static void addStateModelDef(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, HelixAdminFactory helixAdminFactory, String stateModelDef)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, helixAdminFactory, stateModelDef, null, null, null,
            null);
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
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void validate(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, HelixAdminFactory helixAdminFactory, String stateModelDef)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, helixAdminFactory, stateModelDef, null, null, null,
            HelixAdminOperation.ValidateCluster);
    clusterMapToHelixMapper.validateAndClose();
    clusterMapToHelixMapper.logSummary();
  }

  /**
   * Control the state of partition on given node. Currently this method supports Disable/Enable/Reset partition.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @param hostname the host on which admin operation should be performed
   * @param portNum the port number associated with host
   * @param operation the {@link HelixAdminOperation} to perform
   * @param partitionName the partition on which admin operation should be performed
   * @throws Exception
   */
  static void controlPartitionState(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, HelixAdminFactory helixAdminFactory, String hostname, Integer portNum,
      HelixAdminOperation operation, String partitionName) throws Exception {

    HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, helixAdminFactory, null,
            Objects.requireNonNull(hostname), portNum, Objects.requireNonNull(partitionName), operation);
    if (operation == HelixAdminOperation.ResetPartition) {
      helixBootstrapUpgradeUtil.resetPartition();
    } else {
      helixBootstrapUpgradeUtil.controlPartitionState();
    }
    helixBootstrapUpgradeUtil.logSummary();
  }

  /**
   * Drop a cluster from Helix.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterName the name of the cluster in Helix.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}
   * @throws Exception if there is an error reading a file or in parsing json.
   */
  static void dropCluster(String zkLayoutPath, String clusterName, String dcs, HelixAdminFactory helixAdminFactory)
      throws Exception {
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress = parseAndUpdateDcInfoFromArg(dcs, zkLayoutPath);
    info("Dropping cluster {} from Helix", clusterName);
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      List<String> zkConnectStrs = entry.getValue().getZkConnectStrs();
      if (zkConnectStrs.size() != 1) {
        throw new IllegalArgumentException(
            entry.getKey() + " has invalid number of ZK endpoints: " + zkConnectStrs.size());
      }
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(zkConnectStrs.get(0));
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
      case PARTITION_OVERRIDE_STR:
        Map<String, Map<String, Map<String, String>>> partitionOverrideInfosByDc =
            clusterMapToHelixMapper.generatePartitionOverrideFromAllDCs();
        clusterMapToHelixMapper.uploadClusterAdminInfos(partitionOverrideInfosByDc, PARTITION_OVERRIDE_STR,
            PARTITION_OVERRIDE_ZNODE_PATH);
        break;
      case REPLICA_ADDITION_STR:
        Map<String, Map<String, Map<String, String>>> replicaAdditionInfosByDc =
            clusterMapToHelixMapper.generateReplicaAdditionMap();
        clusterMapToHelixMapper.uploadClusterAdminInfos(replicaAdditionInfosByDc, REPLICA_ADDITION_STR,
            REPLICA_ADDITION_ZNODE_PATH);
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized admin config type: " + adminType + ". Current supported types are: " + PARTITION_OVERRIDE_STR
                + ", " + REPLICA_ADDITION_STR);
    }
    info("Upload cluster configs completed.");
  }

  /**
   * Remove admin config from cluster.
   * @param helixBootstrapUpgradeUtil the {@link HelixBootstrapUpgradeUtil} to use
   * @param adminType the name of admin config
   */
  private static void removeAdminInfosFromCluster(HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil,
      String adminType) {
    switch (adminType) {
      case PARTITION_OVERRIDE_STR:
        helixBootstrapUpgradeUtil.deleteClusterAdminInfos(adminType, PARTITION_OVERRIDE_ZNODE_PATH);
        break;
      case REPLICA_ADDITION_STR:
        helixBootstrapUpgradeUtil.deleteClusterAdminInfos(adminType, REPLICA_ADDITION_ZNODE_PATH);
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized admin config type: " + adminType + ". Current supported types are: " + PARTITION_OVERRIDE_STR
                + ", " + REPLICA_ADDITION_STR);
    }
    info("Delete cluster configs completed.");
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
   * @param hostname the host (if not null) on which the admin operation should be performed.
   * @param portNum the port number (if not null) associated with host.
   * @param partitionName the partition (if not null) on which the admin operation should be performed.
   * @param helixAdminOperation the {@link HelixAdminOperation} to perform.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef, String hostname, Integer portNum, String partitionName,
      HelixAdminOperation helixAdminOperation) throws Exception {
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    this.dryRun = dryRun;
    this.forceRemove = forceRemove;
    this.zkLayoutPath = zkLayoutPath;
    this.stateModelDef = stateModelDef;
    this.hostName = hostname;
    this.portNum = portNum;
    this.partitionName = partitionName;
    this.helixAdminOperation = helixAdminOperation;
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
          clusterMapConfig))).getClusterMap();
    }
    String clusterNameInStaticClusterMap = staticClusterMap.partitionLayout.getClusterName();
    clusterName = clusterNamePrefix + clusterNameInStaticClusterMap;
    info("Associating static Ambry cluster \"" + clusterNameInStaticClusterMap + "\" with cluster\"" + clusterName
        + "\" in Helix");
    for (Datacenter datacenter : staticClusterMap.hardwareLayout.getDatacenters()) {
      if (dcs.equalsIgnoreCase(ALL) && !dataCenterToZkAddress.containsKey(datacenter.getName())) {
        throw new IllegalArgumentException(
            "There is no ZK host for datacenter " + datacenter.getName() + " in the static clustermap");
      }
    }
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      List<String> zkConnectStrs = entry.getValue().getZkConnectStrs();
      if (zkConnectStrs.size() != 1) {
        throw new IllegalArgumentException(
            entry.getKey() + " has invalid number of ZK endpoints: " + zkConnectStrs.size());
      }
      HelixAdmin admin = helixAdminFactory.getHelixAdmin(zkConnectStrs.get(0));
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
      partitionProperties.put(PARTITION_STATE,
          partitionId.getPartitionState() == PartitionState.READ_WRITE ? READ_WRITE_STR : READ_ONLY_STR);
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
                partitionMap.put(PARTITION_CLASS_STR, replica.getPartitionId().getPartitionClass());
                partitionMap.put(REPLICAS_CAPACITY_STR, String.valueOf(replica.getCapacityInBytes()));
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
    storeProps.setProperty("helix.property.store.root.path", "/" + clusterName + "/" + PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      info("Uploading {} infos for datacenter {}.", clusterAdminType, entry.getKey());
      List<String> zkConnectStrs = entry.getValue().getZkConnectStrs();
      // The number of zk endpoints has been validated in the ctor of HelixBootstrapUpgradeUtil, no need to check it again
      HelixPropertyStore<ZNRecord> helixPropertyStore =
          CommonUtils.createHelixPropertyStore(zkConnectStrs.get(0), propertyStoreConfig, null);
      ZNRecord znRecord = new ZNRecord(clusterAdminType);
      znRecord.setMapFields(adminInfosByDc.get(entry.getKey()));
      if (!helixPropertyStore.set(adminConfigZNodePath, znRecord, AccessOption.PERSISTENT)) {
        logger.error("Failed to upload {} infos for datacenter {}", clusterAdminType, entry.getKey());
      }
    }
  }

  /**
   * Delete specified admin config at given znode path.
   * @param clusterAdminType the name of admin config.
   * @param adminConfigZNodePath the znode path of admin config.
   */
  private void deleteClusterAdminInfos(String clusterAdminType, String adminConfigZNodePath) {
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path", "/" + clusterName + "/" + PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      info("Deleting {} infos for datacenter {}.", clusterAdminType, entry.getKey());
      HelixPropertyStore<ZNRecord> helixPropertyStore =
          CommonUtils.createHelixPropertyStore(entry.getValue().getZkConnectStrs().get(0), propertyStoreConfig, null);
      if (!helixPropertyStore.remove(adminConfigZNodePath, AccessOption.PERSISTENT)) {
        logger.error("Failed to remove {} infos from datacenter {}", clusterAdminType, entry.getKey());
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
   * Control state of partition on certain node. (i.e. DisablePartition, EnablePartition)
   */
  private void controlPartitionState() {
    // for now, we only support controlling state of single partition on certain node. Hence, there should be only 1 dc
    // in adminForDc map.
    if (adminForDc.size() != 1) {
      throw new IllegalStateException("The dc count is not 1 for partition state control operation");
    }
    HelixAdmin helixAdmin = adminForDc.values().iterator().next();
    String instanceName;
    if (portNum == null) {
      Optional<DataNodeId> optionalDataNode =
          staticClusterMap.getDataNodeIds().stream().filter(node -> node.getHostname().equals(hostName)).findFirst();
      if (!optionalDataNode.isPresent()) {
        throw new IllegalStateException("Host " + hostName + " is not found in static clustermap");
      }
      DataNode dataNode = (DataNode) optionalDataNode.get();
      instanceName = getInstanceName(dataNode);
    } else {
      instanceName = ClusterMapUtils.getInstanceName(hostName, portNum);
    }
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    String resourceNameForPartition = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
    info("{} partition {} under resource {} on node {}",
        helixAdminOperation == HelixAdminOperation.EnablePartition ? "Enabling" : "Disabling", partitionName,
        resourceNameForPartition, instanceName);
    instanceConfig.setInstanceEnabledForPartition(resourceNameForPartition, partitionName,
        helixAdminOperation == HelixAdminOperation.EnablePartition);
    // clean up the disabled partition entry if it exists and is empty.
    Map<String, String> disabledPartitions =
        instanceConfig.getRecord().getMapFields().get(HELIX_DISABLED_PARTITION_STR);
    if (disabledPartitions != null && disabledPartitions.isEmpty()) {
      instanceConfig.getRecord().getMapFields().remove(HELIX_DISABLED_PARTITION_STR);
    }
    helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    instancesUpdated.getAndIncrement();
    if (helixAdminOperation == HelixAdminOperation.EnablePartition) {
      partitionsEnabled.getAndIncrement();
    } else {
      partitionsDisabled.getAndIncrement();
    }
  }

  /**
   * Reset the partition on specific node.
   */
  private void resetPartition() {
    if (adminForDc.size() != 1) {
      throw new IllegalStateException("The dc count is not 1 for resetting partition operation");
    }
    HelixAdmin helixAdmin = adminForDc.values().iterator().next();
    String instanceName;
    if (portNum == null) {
      Optional<DataNodeId> optionalDataNode =
          staticClusterMap.getDataNodeIds().stream().filter(node -> node.getHostname().equals(hostName)).findFirst();
      if (!optionalDataNode.isPresent()) {
        throw new IllegalStateException("Host " + hostName + " is not found in static clustermap");
      }
      DataNodeId dataNodeId = optionalDataNode.get();
      instanceName = getInstanceName(dataNodeId);
    } else {
      instanceName = ClusterMapUtils.getInstanceName(hostName, portNum);
    }
    String resourceName = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
    info("Resetting partition {} under resource {} on node {}", partitionName, resourceName, hostName);
    helixAdmin.resetPartition(clusterName, instanceName, resourceName, Collections.singletonList(partitionName));
    partitionsReset.getAndIncrement();
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
  private void updateClusterMapInHelix(boolean startValidatingClusterManager) throws Exception {
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

  /**
   * Add and/or update instances in Helix based on the information in the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  private void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    info("[{}] Getting list of instances in {}", dcName.toUpperCase(), dcName);
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
        if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
          if (!dryRun) {
            info(
                "[{}] Instance {} already present in Helix, but InstanceConfig has changed, updating. Remaining instances: {}",
                dcName.toUpperCase(), instanceName, --totalInstances);
            // Continuing on the note above, if there is indeed a change, we must make a call on whether RO/RW, replica
            // availability and so on should be updated at all (if not, instanceConfigFromStatic should be replaced with
            // the appropriate instanceConfig that is constructed with the correct values from both).
            // For now, only bootstrapping cluster is allowed to directly change InstanceConfig
            dcAdmin.setInstanceConfig(clusterName, instanceName, instanceConfigFromStatic);
          }
          // for dryRun, we update counter but don't really change the InstanceConfig in Helix
          instancesUpdated.getAndIncrement();
        }
      } else {
        info("[{}] Instance {} already present in Helix, with same InstanceConfig, skipping. Remaining instances: {}",
            dcName.toUpperCase(), instanceName, --totalInstances);
      }
    }

    for (String instanceName : instancesInStatic) {
      InstanceConfig instanceConfigFromStatic =
          createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dcName).get(instanceName),
              partitionsToInstancesInDc, instanceToDiskReplicasMap, null);
      info("[{}] Instance {} is new, adding to Helix. Remaining instances: {}", dcName.toUpperCase(), instanceName,
          --totalInstances);
      // Note: for now, if we want to move replica to new instance (not present in cluster yet), we should take two steps:
      // step1: add new instance (empty) to cluster, which is a regular bootstrap; step2: add replica to IdealState
      // Helix controller will notify new instance to perform replica addition.
      if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
        if (!dryRun) {
          dcAdmin.addInstance(clusterName, instanceConfigFromStatic);
        }
        instancesAdded.getAndIncrement();
      }
    }

    for (String instanceName : instancesInHelix) {
      if (forceRemove) {
        info("[{}] Instance {} is in Helix, but not in static. Forcefully removing. Remaining instances: {}",
            dcName.toUpperCase(), instanceName, --totalInstances);
        if (helixAdminOperation == HelixAdminOperation.BootstrapCluster) {
          if (!dryRun) {
            dcAdmin.dropInstance(clusterName, new InstanceConfig(instanceName));
          }
          instancesDropped.getAndIncrement();
        }
      } else {
        info(
            "[{}] Instance {} is in Helix, but not in static. Ignoring for now (use --forceRemove to forcefully remove). "
                + "Remaining instances: {}", dcName.toUpperCase(), instanceName, --totalInstances);
        expectMoreInHelixDuringValidate = true;
        instancesNotForceRemovedByDc.computeIfAbsent(dcName, k -> ConcurrentHashMap.newKeySet()).add(instanceName);
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
            info("[{}] *** Partition {} no longer present in the static clustermap, removing from Resource *** ",
                dcName.toUpperCase(), partitionName);
            // this is a hacky way of removing a partition from the resource, as there isn't another way today.
            // Helix team is planning to provide an API for this.
            resourceIs.getRecord().getListFields().remove(partitionName);
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
                "[{}] Different instance sets for partition {} under resource {}. "
                    + "Updating Helix using static. "
                    + "Previous instance set: [{}], new instance set: [{}]",
                dcName.toUpperCase(), partitionName, resourceName,
                String.join(",", instanceSetInHelix), String.join(",", instanceSetInStatic));
            // @formatter:on
            ArrayList<String> newInstances = new ArrayList<>(instanceSetInStatic);
            Collections.shuffle(newInstances);
            resourceIs.setPreferenceList(partitionName, newInstances);
            // Existing resources may not have ANY_LIVEINSTANCE set as the numReplicas (which allows for different
            // replication for different partitions under the same resource). So set it here (We use the name() method and
            // not the toString() method for the enum as that is what Helix uses).
            resourceIs.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
            resourceModified = true;
          } else if (helixAdminOperation == HelixAdminOperation.DisablePartition) {
            // if this is DisablePartition operation, we don't modify IdealState and only make InstanceConfig to disable
            // certain partition on specific node.
            // 1. extract difference between Helix and Static instance sets. Determine which replica is removed
            instanceSetInHelix.removeAll(instanceSetInStatic);
            // 2. disable removed replica on certain node.
            for (String instanceInHelixOnly : instanceSetInHelix) {
              info("Disabling partition {} under resource {} on node {}", partitionName, resourceName,
                  instanceInHelixOnly);
              InstanceConfig instanceConfig = dcAdmin.getInstanceConfig(clusterName, instanceInHelixOnly);
              instanceConfig.setInstanceEnabledForPartition(resourceName, partitionName, false);
              dcAdmin.setInstanceConfig(clusterName, instanceInHelixOnly, instanceConfig);
              // note that disabling partition also updates InstanceConfig of certain node which hosts the partition.
              instancesUpdated.getAndIncrement();
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
          if (!dryRun) {
            dcAdmin.dropResource(clusterName, resourceName);
          }
          resourcesDropped.getAndIncrement();
        } else {
          if (!dryRun) {
            dcAdmin.setResourceIdealState(clusterName, resourceName, resourceIs);
          }
          resourcesUpdated.getAndIncrement();
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
      info("[{}] Adding partitions for next resource in {}", dcName.toUpperCase(), dcName);
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
      resourcesAdded.getAndIncrement();
      info("[{}] Added {} new partitions under resource {} in datacenter {}", dcName.toUpperCase(),
          partitionsUnderNextResource.size(), resourceName, dcName);
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
      ConcurrentHashMap<String, Map<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap,
      InstanceConfig referenceInstanceConfig) {
    String instanceName = getInstanceName(node);
    InstanceConfig instanceConfig = new InstanceConfig(instanceName);
    instanceConfig.setHostName(node.getHostname());
    instanceConfig.setPort(Integer.toString(node.getPort()));
    if (node.hasSSLPort()) {
      instanceConfig.getRecord().setSimpleField(SSL_PORT_STR, Integer.toString(node.getSSLPort()));
    }
    if (node.hasHttp2Port()) {
      instanceConfig.getRecord().setSimpleField(HTTP2_PORT_STR, Integer.toString(node.getHttp2Port()));
    }
    instanceConfig.getRecord().setSimpleField(DATACENTER_STR, node.getDatacenterName());
    instanceConfig.getRecord().setSimpleField(RACKID_STR, node.getRackId());
    long xid = node.getXid();
    if (xid != DEFAULT_XID) {
      // Set the XID only if it is not the default, in order to avoid unnecessary updates.
      instanceConfig.getRecord().setSimpleField(XID_STR, Long.toString(node.getXid()));
    }
    instanceConfig.getRecord().setSimpleField(SCHEMA_VERSION_STR, Integer.toString(CURRENT_SCHEMA_VERSION));

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
              .append(REPLICAS_STR_SEPARATOR)
              .append(replica.getCapacityInBytes())
              .append(REPLICAS_STR_SEPARATOR)
              .append(replica.getPartition().getPartitionClass())
              .append(REPLICAS_DELIM_STR);
          if (referenceInstanceConfig == null && replica.isSealed()) {
            sealedPartitionsList.add(Long.toString(replica.getPartition().getId()));
          }
          partitionToInstances.computeIfAbsent(Long.toString(replica.getPartition().getId()), k -> new HashSet<>())
              .add(instanceName);
        }
        Map<String, String> diskInfo = new HashMap<>();
        diskInfo.put(DISK_CAPACITY_STR, Long.toString(disk.getRawCapacityInBytes()));
        diskInfo.put(DISK_STATE, AVAILABLE_STR);
        diskInfo.put(REPLICAS_STR, replicasStrBuilder.toString());
        diskInfos.put(disk.getMountPath(), diskInfo);
      }
      instanceConfig.getRecord().setMapFields(diskInfos);
    }

    // Set the fields that need to be preserved from the referenceInstanceConfig.
    if (referenceInstanceConfig != null) {
      sealedPartitionsList = ClusterMapUtils.getSealedReplicas(referenceInstanceConfig);
      stoppedReplicasList = ClusterMapUtils.getStoppedReplicas(referenceInstanceConfig);
    }
    instanceConfig.getRecord().setListField(SEALED_STR, sealedPartitionsList);
    instanceConfig.getRecord().setListField(STOPPED_REPLICAS_STR, stoppedReplicasList);
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
    StateModelDefinition stateModelDefinition;
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
  static String getInstanceName(DataNodeId dataNode) {
    return ClusterMapUtils.getInstanceName(dataNode.getHostname(), dataNode.getPort());
  }

  /**
   * Get resource name associated with given partition.
   * @param helixAdmin the {@link HelixAdmin} to access resources in cluster
   * @param clusterName the name of cluster in which the partition resides
   * @param partitionName name of partition
   * @return resource name associated with given partition. {@code null} if not found.
   */
  static String getResourceNameOfPartition(HelixAdmin helixAdmin, String clusterName, String partitionName) {
    String result = null;
    for (String resourceName : helixAdmin.getResourcesInCluster(clusterName)) {
      IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
      if (idealState.getPartitionSet().contains(partitionName)) {
        result = resourceName;
        break;
      }
    }
    return result;
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
    CountDownLatch verificationLatch = new CountDownLatch(adminForDc.size());
    AtomicInteger errorCount = new AtomicInteger();
    for (Datacenter dc : hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      if (admin == null) {
        info("Skipping {}", dc.getName());
        continue;
      }
      ensureOrThrow(admin.getClusters().contains(clusterName),
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
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dc.getName());
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(props));
    StaticClusterManager staticClusterMap =
        (new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout)).getClusterMap();
    String dcName = dc.getName();
    HelixAdmin admin = adminForDc.get(dcName);
    Set<String> allInstancesInHelix = new HashSet<>(admin.getInstancesInCluster(clusterName));
    for (DataNodeId dataNodeId : dc.getDataNodes()) {
      Map<String, Map<String, ReplicaId>> mountPathToReplicas = getMountPathToReplicas(staticClusterMap, dataNodeId);
      DataNode dataNode = (DataNode) dataNodeId;
      String instanceName = getInstanceName(dataNode);
      ensureOrThrow(allInstancesInHelix.remove(instanceName), "Instance not present in Helix " + instanceName);
      InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);

      Map<String, Map<String, String>> diskInfos = new HashMap<>(instanceConfig.getRecord().getMapFields());
      for (Disk disk : dataNode.getDisks()) {
        Map<String, String> diskInfoInHelix = diskInfos.remove(disk.getMountPath());
        ensureOrThrow(diskInfoInHelix != null,
            "[" + dcName.toUpperCase() + "] Disk not present for instance " + instanceName + " disk "
                + disk.getMountPath());
        ensureOrThrow(disk.getRawCapacityInBytes() == Long.parseLong(diskInfoInHelix.get(DISK_CAPACITY_STR)),
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
          String replicasStr = diskInfoInHelix.get(REPLICAS_STR);
          if (!replicasStr.isEmpty()) {
            String[] replicaInfoList = replicasStr.split(REPLICAS_DELIM_STR);
            for (String replicaInfo : replicaInfoList) {
              String[] info = replicaInfo.split(REPLICAS_STR_SEPARATOR);
              // This is schema specific, but the assumption is that partition id and capacity fields should always be
              // there. At this time these are the only things checked.
              ensureOrThrow(info.length >= 2, "[" + dcName.toUpperCase()
                  + "] Replica info field should have at least two fields - partition id and capacity");
              replicasInHelix.add(info[0]);
              ReplicaId replica = replicaList.get(info[0]);
              ensureOrThrow(info[1].equals(Long.toString(replica.getCapacityInBytes())),
                  "[" + dcName.toUpperCase() + "] Replica capacity should be the same.");
              if (info.length > 2) {
                ensureOrThrow(info[2].equals(replica.getPartitionId().getPartitionClass()),
                    "[" + dcName.toUpperCase() + "] Partition class should be the same.");
              }
            }
          }
          ensureOrThrow(replicasInClusterMap.equals(replicasInHelix),
              "[" + dcName.toUpperCase() + "] Replica information not consistent for instance " + instanceName
                  + " disk " + disk.getMountPath() + "\n in Helix: " + replicaList + "\n in static clustermap: "
                  + replicasInClusterMap);
        }
      }
      for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
        String mountPath = entry.getKey();
        if (!mountPath.startsWith("/mnt")) {
          logger.warn("[{}] Instance {} has unidentifiable mount path in Helix: {}", dcName.toUpperCase(), instanceName,
              mountPath);
        } else {
          throw new AssertionError(
              "[" + dcName.toUpperCase() + "] Instance " + instanceName + " has extra disk in Helix: " + entry);
        }
      }
      ensureOrThrow(!dataNode.hasSSLPort() || (dataNode.getSSLPort() == Integer.parseInt(
          instanceConfig.getRecord().getSimpleField(SSL_PORT_STR))),
          "[" + dcName.toUpperCase() + "] SSL Port mismatch for instance " + instanceName);
      ensureOrThrow(!dataNode.hasHttp2Port() || (dataNode.getHttp2Port() == Integer.parseInt(
          instanceConfig.getRecord().getSimpleField(HTTP2_PORT_STR))),
          "[" + dcName.toUpperCase() + "] HTTP2 Port mismatch for instance " + instanceName);
      ensureOrThrow(dataNode.getDatacenterName().equals(instanceConfig.getRecord().getSimpleField(DATACENTER_STR)),
          "[" + dcName.toUpperCase() + "] Datacenter mismatch for instance " + instanceName);
      ensureOrThrow(Objects.equals(dataNode.getRackId(), instanceConfig.getRecord().getSimpleField(RACKID_STR)),
          "[" + dcName.toUpperCase() + "] Rack Id mismatch for instance " + instanceName);

      String xidInHelix = instanceConfig.getRecord().getSimpleField(XID_STR);
      if (xidInHelix == null) {
        xidInHelix = Long.toString(DEFAULT_XID);
      }
      ensureOrThrow(Objects.equals(Long.toString(dataNode.getXid()), xidInHelix),
          "[" + dcName.toUpperCase() + "] Xid mismatch for instance " + instanceName);
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
   * A helper method that returns a map of mountPaths to a map of replicas -> replicaCapacity for a given
   * {@link DataNodeId}
   * @param staticClusterMap the static {@link StaticClusterManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  private static Map<String, Map<String, ReplicaId>> getMountPathToReplicas(StaticClusterManager staticClusterMap,
      DataNodeId dataNodeId) {
    Map<String, Map<String, ReplicaId>> mountPathToReplicas = new HashMap<>();
    for (ReplicaId replica : staticClusterMap.getReplicas(dataNodeId)) {
      Map<String, ReplicaId> replicaStrs = mountPathToReplicas.get(replica.getMountPath());
      if (replicaStrs != null) {
        replicaStrs.put(replica.getPartitionId().toPathString(), replica);
      } else {
        replicaStrs = new HashMap<>();
        replicaStrs.put(replica.getPartitionId().toPathString(), replica);
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
}


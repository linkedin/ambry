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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.DataNodeConfigSourceType.*;
import static com.github.ambry.utils.Utils.*;


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
public abstract class HelixBootstrapUpgradeUtil {
  static final int DEFAULT_MAX_PARTITIONS_PER_RESOURCE = 100;
  static final String HELIX_DISABLED_PARTITION_STR =
      InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name();
  // blockRemovingNodeLatch is for testing purpose
  static CountDownLatch blockRemovingNodeLatch = null;
  static CountDownLatch disablePartitionLatch = null;
  protected final StaticClusterManager staticClusterMap;
  protected final String zkLayoutPath;
  protected final String stateModelDef;
  protected final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  protected final Map<String, RealmAwareZkClient> zkClientForDc = new HashMap<>();
  // These two maps should be concurrent map because they are accessed in multi-threaded context (see addUpdateInstances
  // method).
  // For now, the inner map doesn't need to be concurrent map because it is within a certain dc which means there should
  // be only one thread that updates it.
  protected final ConcurrentHashMap<String, Map<DiskId, SortedSet<Replica>>> instanceToDiskReplicasMap =
      new ConcurrentHashMap<>();
  protected final ConcurrentHashMap<String, Map<String, DataNodeId>> dcToInstanceNameToDataNodeId =
      new ConcurrentHashMap<>();
  protected final int maxPartitionsInOneResource;
  protected final boolean dryRun;
  protected final boolean forceRemove;
  protected final String clusterName;
  protected final String hostName;
  protected final Integer portNum;
  protected final String partitionName;
  protected final HelixAdminOperation helixAdminOperation;
  protected final DataNodeConfigSourceType dataNodeConfigSourceType;
  protected final boolean overrideReplicaStatus;
  protected boolean expectMoreInHelixDuringValidate = false;
  protected ConcurrentHashMap<String, Set<String>> instancesNotForceRemovedByDc = new ConcurrentHashMap<>();
  protected ConcurrentHashMap<String, Set<String>> partitionsNotForceRemovedByDc = new ConcurrentHashMap<>();
  protected final AtomicInteger instancesAdded = new AtomicInteger();
  protected final AtomicInteger instancesUpdated = new AtomicInteger();
  protected final AtomicInteger instancesDropped = new AtomicInteger();
  protected final AtomicInteger resourcesAdded = new AtomicInteger();
  protected final AtomicInteger resourcesUpdated = new AtomicInteger();
  protected final AtomicInteger resourcesDropped = new AtomicInteger();
  protected final AtomicInteger partitionsDisabled = new AtomicInteger();
  protected final AtomicInteger partitionsEnabled = new AtomicInteger();
  protected final AtomicInteger partitionsReset = new AtomicInteger();
  protected Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress;
  protected HelixClusterManager validatingHelixClusterManager;
  protected static final Logger logger = LoggerFactory.getLogger("Helix bootstrap tool");
  protected static final String ALL = "all";

  public enum HelixAdminOperation {
    BootstrapCluster,
    ValidateCluster,
    UpdateIdealState,
    DisablePartition,
    EnablePartition,
    ResetPartition,
    ListSealedPartition,
    MigrateToPropertyStore,
    MigrateToFullAuto
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
   * @param helixAdminFactory the {@link HelixAdminFactory} to use to instantiate {@link HelixAdmin}, can be null.
   * @param startValidatingClusterManager whether validation should include starting up a {@link HelixClusterManager}
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @param helixAdminOperation the {@link HelixAdminOperation} to perform on resources (partitions). This is used in
   *                           the context of move replica. So it only updates IdealState without touching anything else.
   *                           InstanceConfig will be updated by nodes themselves.
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} to use when bootstrapping cluster.
   * @param overrideReplicaStatus whether to override sealed/stopped/disabled replica status lists.
   * @param rebalanceMode the {@link RebalanceMode} used in Helix.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  public static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, boolean startValidatingClusterManager, String stateModelDef,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus, RebalanceMode rebalanceMode) throws Exception {
    if (dryRun) {
      info("==== This is a dry run ====");
      info("No changes will be made to the information in Helix (except adding the cluster if it does not exist.");
    }
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper;
    if (rebalanceMode == RebalanceMode.SEMI_AUTO) {
      clusterMapToHelixMapper =
          new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
              clusterNamePrefix, dcs, maxPartitionsInOneResource, dryRun, forceRemove, helixAdminFactory, stateModelDef,
              null, null, null, helixAdminOperation, dataNodeConfigSourceType, overrideReplicaStatus);
    } else {
      throw new IllegalArgumentException("Unsupported rebalance mode " + rebalanceMode);
    }
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
   * @param adminTypes types of admin operation that requires to generate config and upload it to Helix PropertyStore.
   * @param adminConfigFilePath if not null, use this file to generate admin config infos. If null, use the standard
   *                               HardwareLayout and PartitionLayout files to generate admin configs to upload.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void uploadOrDeleteAdminConfigs(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, boolean forceRemove, String[] adminTypes, String adminConfigFilePath)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, forceRemove, null,
            ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, null, null, null, null, null, false);
    if (forceRemove) {
      for (String adminType : adminTypes) {
        removeAdminInfosFromCluster(clusterMapToHelixMapper, adminType);
      }
    } else {
      for (String adminType : adminTypes) {
        generateAdminInfosAndUpload(clusterMapToHelixMapper, adminType, adminConfigFilePath);
      }
    }
  }

  /**
   * Add given state model def to ambry cluster in enabled datacenter(s)
   */
  static void addStateModelDef(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, String stateModelDef) throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, stateModelDef, null, null, null, null, null,
            false);
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
   * @param stateModelDef the state model definition to use in Ambry cluster.
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} of this cluster.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  static void validate(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, String stateModelDef, DataNodeConfigSourceType dataNodeConfigSourceType)
      throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, stateModelDef, null, null, null,
            HelixAdminOperation.ValidateCluster, dataNodeConfigSourceType, false);
    clusterMapToHelixMapper.validateAndClose();
    clusterMapToHelixMapper.logSummary();
  }

  /**
   * List all sealed partitions in Helix cluster.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} associated with this cluster.
   * @return a set of sealed partitions in cluster
   * @throws Exception
   */
  static Set<String> listSealedPartition(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, DataNodeConfigSourceType dataNodeConfigSourceType) throws Exception {
    HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF,
            null, null, null, HelixAdminOperation.ListSealedPartition, dataNodeConfigSourceType, false);
    return helixBootstrapUpgradeUtil.getSealedPartitionsInHelixCluster();
  }

  /**
   * Control the state of partition on given node. Currently this method supports Disable/Enable/Reset partition.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be upgraded/bootstrapped.
   * @param hostname the host on which admin operation should be performed
   * @param portNum the port number associated with host
   * @param operation the {@link HelixAdminOperation} to perform
   * @param partitionName the partition on which admin operation should be performed
   * @throws Exception
   */
  static void controlPartitionState(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, String hostname, Integer portNum, HelixAdminOperation operation,
      String partitionName) throws Exception {

    HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, null, Objects.requireNonNull(hostname),
            portNum, Objects.requireNonNull(partitionName), operation, null, false);
    if (operation == HelixAdminOperation.ResetPartition) {
      helixBootstrapUpgradeUtil.resetPartition();
    } else {
      helixBootstrapUpgradeUtil.controlPartitionState();
    }
    helixBootstrapUpgradeUtil.logSummary();
  }

  /**
   * Copy the legacy custom InstanceConfig properties to the DataNodeConfigs tree in the property store.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be migrated
   * @throws Exception
   */
  static void migrateToPropertyStore(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs) throws Exception {

    HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil =
        new HelixBootstrapUpgradeUtilSemiAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, null, null, null, null,
            HelixAdminOperation.MigrateToPropertyStore, PROPERTY_STORE, false);
    helixBootstrapUpgradeUtil.migrateToPropertyStore();
  }

  /**
   * Migrate resources from semi-auto to full-auto.
   * @param hardwareLayoutPath the path to the hardware layout file.
   * @param partitionLayoutPath the path to the partition layout file.
   * @param zkLayoutPath the path to the zookeeper layout file.
   * @param clusterNamePrefix the prefix that when combined with the cluster name in the static cluster map files
   *                          will give the cluster name in Helix to bootstrap or upgrade.
   * @param dcs the comma-separated list of data centers that needs to be migrated
   * @param resources the comma-separated list of resources that needs to be migrated
   * @param dryRun
   * @throws Exception
   */
  static void migrateToFullAuto(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, String resources, boolean dryRun) throws Exception {
    HelixBootstrapUpgradeUtilFullAuto helixBootstrapUpgradeUtilFullAuto =
        new HelixBootstrapUpgradeUtilFullAuto(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix,
            dcs, DEFAULT_MAX_PARTITIONS_PER_RESOURCE, dryRun, false, null, null, null, null, null,
            HelixAdminOperation.MigrateToFullAuto, PROPERTY_STORE, false);
    helixBootstrapUpgradeUtilFullAuto.migrateToFullAuto(resources);
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
   * @param adminConfigFilePath if not null, use this file to generate admin config infos to upload to Helix.
   */
  protected static void generateAdminInfosAndUpload(HelixBootstrapUpgradeUtil clusterMapToHelixMapper, String adminType,
      String adminConfigFilePath) throws IOException {
    switch (adminType) {
      case PARTITION_OVERRIDE_STR:
        Map<String, Map<String, Map<String, String>>> partitionOverrideInfosByDc =
            adminConfigFilePath == null ? clusterMapToHelixMapper.generatePartitionOverrideFromClusterMap()
                : clusterMapToHelixMapper.generatePartitionOverrideFromConfigFile(adminConfigFilePath);
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
  protected static void removeAdminInfosFromCluster(HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil,
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
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} associated with this cluster.
   * @param overrideReplicaStatus whether to override sealed/stopped/disabled replica status lists.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  public HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef, String hostname, Integer portNum, String partitionName,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus) throws Exception {
    this.maxPartitionsInOneResource = maxPartitionsInOneResource;
    this.dryRun = dryRun;
    this.forceRemove = forceRemove;
    this.zkLayoutPath = zkLayoutPath;
    this.stateModelDef = stateModelDef;
    this.hostName = hostname;
    this.portNum = portNum;
    this.partitionName = partitionName;
    this.helixAdminOperation = helixAdminOperation;
    this.dataNodeConfigSourceType = dataNodeConfigSourceType == null ? PROPERTY_STORE : dataNodeConfigSourceType;
    this.overrideReplicaStatus = overrideReplicaStatus;
    dataCenterToZkAddress = parseAndUpdateDcInfoFromArg(dcs, zkLayoutPath);
    // The following properties are immaterial for the tool, but the ClusterMapConfig mandates their presence.
    ClusterMapConfig clusterMapConfig = getClusterMapConfig("", "", null);
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
      HelixAdmin admin;
      RealmAwareZkClient zkClient = null;
      if (helixAdminFactory == null) {
        // TODO update zk client creation to remove deprecated Helix APIs.
        RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
            new RealmAwareZkClient.RealmAwareZkClientConfig().setConnectInitTimeout(30 * 1000L)
                .setZkSerializer(new ZNRecordSerializer());
        zkClient = SharedZkClientFactory.getInstance()
            .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkConnectStrs.get(0)),
                clientConfig.createHelixZkClientConfig());
        admin = new ZKHelixAdmin(zkClient);
      } else {
        admin = helixAdminFactory.getHelixAdmin(zkConnectStrs.get(0));
      }
      adminForDc.put(entry.getKey(), admin);
      zkClientForDc.put(entry.getKey(), zkClient);
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
  protected Map<String, Map<String, Map<String, String>>> generatePartitionOverrideFromClusterMap() {
    Map<String, Map<String, String>> partitionOverrideInfos = new HashMap<>();
    for (PartitionId partitionId : staticClusterMap.getAllPartitionIds(null)) {
      String partitionName = partitionId.toPathString();
      Map<String, String> partitionProperties = new HashMap<>();
      partitionProperties.put(PARTITION_STATE, ClusterMapUtils.partitionStateToStr(partitionId.getPartitionState()));
      partitionOverrideInfos.put(partitionName, partitionProperties);
    }
    Map<String, Map<String, Map<String, String>>> partitionOverrideByDc = new HashMap<>();
    for (String dc : dataCenterToZkAddress.keySet()) {
      partitionOverrideByDc.put(dc, partitionOverrideInfos);
    }
    return partitionOverrideByDc;
  }

  /**
   * Generate partition override map a static config file. It requires file to be a list of partition ids (separated by
   * comma). These partitions will be overridden to READ_ONLY state.
   * @param adminConfigFilePath the path to config file
   * @return the constructed partitionOverrideInfos by dc. The format is as follows.
   * "mapFields": {
   *    "0": {
   *      "state": "RO"
   *    },
   *    "1": {
   *      "state": "RO"
   *    }
   * }
   * @throws IOException
   */
  protected Map<String, Map<String, Map<String, String>>> generatePartitionOverrideFromConfigFile(
      String adminConfigFilePath) throws IOException {
    Map<String, Map<String, String>> partitionOverrideInfos = new HashMap<>();
    long maxPartitionId = staticClusterMap.getAllPartitionIds(null)
        .stream()
        .map(p -> Long.parseLong(p.toPathString()))
        .max(Comparator.comparing(Long::valueOf))
        .get();
    String partitionStr = readStringFromFile(adminConfigFilePath);
    for (String partitionName : partitionStr.split(",")) {
      partitionName = partitionName.trim();
      if (partitionName.isEmpty()) {
        continue;
      }
      // if it is not numeric, it should throw exception when parsing long.
      long id = Long.parseLong(partitionName);
      if (id < 0 || id > maxPartitionId) {
        throw new IllegalArgumentException("Partition id is not in valid range: 0 - " + maxPartitionId);
      }
      Map<String, String> partitionProperties = new HashMap<>();
      partitionProperties.put(PARTITION_STATE, READ_ONLY_STR);
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
   *         "localhost1_17088": "/tmp/c/1,536870912000",
   *         "localhost2_17088": "/tmp/d/1,536870912000"
   *     },
   *     "2": {
   *         "replicaCapacityInBytes": 107374182400,
   *         "partitionClass": "max-replicas-all-datacenters",
   *         "localhost3_17088": "/tmp/e/1,536870912000"
   *     }
   * }
   * In above example, two new replicas of partition[1] will be added to localhost1 and localhost2 respectively.
   * The host name is followed by mount path and disk capacity (separated by comma) on which the new replica should be
   * placed.
   * @return a map that contains detailed replica info.
   */
  protected Map<String, Map<String, Map<String, String>>> generateReplicaAdditionMap() {
    //populate dcToInstanceNameToDataNodeId and instanceToDiskReplicasMap
    populateInstancesAndPartitionsMap();
    Map<String, Map<String, Map<String, String>>> newAddedReplicasByDc = new HashMap<>();
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      HelixAdmin dcAdmin = entry.getValue();
      String dcName = entry.getKey();
      info("[{}] Generating replica addition map for datacenter {}", dcName.toUpperCase(), dcName);
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
                "[{}] *** Partition {} no longer present in the static clustermap. Uploading cluster admin infos operation won't remove it *** ",
                dcName.toUpperCase(), partitionStr);
          } else if (!instanceAndReplicaInStatic.keySet().equals(instanceSetInHelix)) {
            info(
                "[{}] Different instance sets for partition {} under resource {}. Extracting new replicas from static clustermap.",
                dcName.toUpperCase(), partitionStr, resourceName);
            // instances in static only
            Set<String> instanceSetInStatic = instanceAndReplicaInStatic.keySet();
            instanceSetInStatic.removeAll(instanceSetInHelix);
            for (String instance : instanceSetInStatic) {
              Replica replica = instanceAndReplicaInStatic.get(instance);
              info("[{}] New replica of partition[{}] will be added to instance {} on {}", dcName.toUpperCase(),
                  partitionStr, instance, replica.getMountPath());
              newAddedReplicasInDc.computeIfAbsent(partitionStr, key -> {
                Map<String, String> partitionMap = new HashMap<>();
                partitionMap.put(PARTITION_CLASS_STR, replica.getPartitionId().getPartitionClass());
                partitionMap.put(REPLICAS_CAPACITY_STR, String.valueOf(replica.getCapacityInBytes()));
                return partitionMap;
              })
                  .put(instance,
                      replica.getMountPath() + DISK_CAPACITY_DELIM_STR + replica.getDiskId().getRawCapacityInBytes());
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
  protected void uploadClusterAdminInfos(Map<String, Map<String, Map<String, String>>> adminInfosByDc,
      String clusterAdminType, String adminConfigZNodePath) {
    for (String dcName : dataCenterToZkAddress.keySet()) {
      info("Uploading {} infos for datacenter {}.", clusterAdminType, dcName);
      HelixPropertyStore<ZNRecord> helixPropertyStore = createHelixPropertyStore(dcName);
      try {
        ZNRecord znRecord = new ZNRecord(clusterAdminType);
        znRecord.setMapFields(adminInfosByDc.get(dcName));
        if (!helixPropertyStore.set(adminConfigZNodePath, znRecord, AccessOption.PERSISTENT)) {
          logger.error("Failed to upload {} infos for datacenter {}", clusterAdminType, dcName);
        }
      } finally {
        helixPropertyStore.stop();
      }
    }
  }

  /**
   * Delete specified admin config at given znode path.
   * @param clusterAdminType the name of admin config.
   * @param adminConfigZNodePath the znode path of admin config.
   */
  protected void deleteClusterAdminInfos(String clusterAdminType, String adminConfigZNodePath) {
    for (Map.Entry<String, ClusterMapUtils.DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
      info("Deleting {} infos for datacenter {}.", clusterAdminType, entry.getKey());
      HelixPropertyStore<ZNRecord> helixPropertyStore = createHelixPropertyStore(entry.getKey());
      if (!helixPropertyStore.remove(adminConfigZNodePath, AccessOption.PERSISTENT)) {
        logger.error("Failed to remove {} infos from datacenter {}", clusterAdminType, entry.getKey());
      }
      helixPropertyStore.stop();
    }
  }

  /**
   * Convert instance configs to the new DataNodeConfig format and persist them in the property store.
   */
  protected void migrateToPropertyStore() throws InterruptedException {
    CountDownLatch migrationComplete = new CountDownLatch(adminForDc.size());
    // different DCs can be migrated in parallel
    adminForDc.forEach((dcName, helixAdmin) -> Utils.newThread(() -> {
      try {
        logger.info("Starting property store migration in {}", dcName);
        ClusterMapConfig config = getClusterMapConfig(clusterName, dcName, null);
        InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
            new InstanceConfigToDataNodeConfigAdapter.Converter(config);
        String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
        try (DataNodeConfigSource source = new PropertyStoreToDataNodeConfigAdapter(zkConnectStr, config)) {
          List<String> instanceNames = helixAdmin.getInstancesInCluster(clusterName);
          logger.info("Found {} instances in cluster", instanceNames.size());
          instanceNames.forEach(instanceName -> {
            logger.info("Copying config for node {}", instanceName);
            InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
            DataNodeConfig dataNodeConfig = instanceConfigConverter.convert(instanceConfig);
            logger.debug("Writing {} to property store in {}", dataNodeConfig, dcName);
            if (!source.set(dataNodeConfig)) {
              logger.error("Failed to persist config for node {} in the property store.",
                  dataNodeConfig.getInstanceName());
            }
          });
        }
        logger.info("Successfully migrated to property store in {}", dcName);
      } catch (Throwable t) {
        logger.error("Error while migrating to property store in {}", dcName, t);
      } finally {
        migrationComplete.countDown();
      }
    }, false).start());

    migrationComplete.await();
  }

  /**
   * Read from the static cluster map and populate a map of {@link DataNodeId} -> list of its {@link ReplicaId}.
   */
  protected void populateInstancesAndPartitionsMap() {
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
  protected void controlPartitionState() {
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
  protected void resetPartition() {
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
   * @param startValidatingClusterManager whether validation should include staring up a {@link HelixClusterManager}
   */
  protected abstract void updateClusterMapInHelix(boolean startValidatingClusterManager) throws Exception;

  /**
   * Add and/or update instances in Helix based on the information in the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  protected abstract void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc);

  protected List<String> getInstanceNamesInHelix(String dcName, PropertyStoreToDataNodeConfigAdapter adapter) {
    List<String> instanceNames;
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      instanceNames = adapter.getAllDataNodeNames();
    } else {
      instanceNames = adminForDc.get(dcName).getInstancesInCluster(clusterName);
    }
    return instanceNames;
  }

  protected DataNodeConfig createDataNodeConfigFromStatic(String dcName, String instanceName,
      DataNodeConfig referenceConfig, Map<String, Set<String>> partitionsToInstancesInDc,
      InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    InstanceConfig referenceInstanceConfig =
        overrideReplicaStatus || referenceConfig == null ? null : converter.convert(referenceConfig);
    return converter.convert(
        createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dcName).get(instanceName),
            partitionsToInstancesInDc, instanceToDiskReplicasMap, referenceInstanceConfig));
  }

  protected DataNodeConfig getDataNodeConfigFromHelix(String dcName, String instanceName,
      PropertyStoreToDataNodeConfigAdapter adapter, InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    DataNodeConfig dataNodeConfig;
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      dataNodeConfig = adapter.get(instanceName);
    } else {
      dataNodeConfig = converter.convert(adminForDc.get(dcName).getInstanceConfig(clusterName, instanceName));
    }
    return dataNodeConfig;
  }

  protected void setDataNodeConfigInHelix(String dcName, String instanceName, DataNodeConfig config,
      PropertyStoreToDataNodeConfigAdapter adapter, InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      if (!adapter.set(config)) {
        logger.error("[{}] Failed to persist config for node {} in the property store.", dcName.toUpperCase(),
            config.getInstanceName());
      }
    } else {
      adminForDc.get(dcName).setInstanceConfig(clusterName, instanceName, converter.convert(config));
    }
  }

  protected void addDataNodeConfigToHelix(String dcName, DataNodeConfig dataNodeConfig,
      PropertyStoreToDataNodeConfigAdapter adapter, InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    // if this is a new instance, we should add it to both InstanceConfig and PropertyStore
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      // when source type is PROPERTY_STORE, we only need to add an InstanceConfig with minimum required information (i.e. hostname, port etc)
      InstanceConfig instanceConfig = new InstanceConfig(dataNodeConfig.getInstanceName());
      instanceConfig.setHostName(dataNodeConfig.getHostName());
      instanceConfig.setPort(Integer.toString(dataNodeConfig.getPort()));
      adminForDc.get(dcName).addInstance(clusterName, instanceConfig);
    } else {
      adminForDc.get(dcName).addInstance(clusterName, converter.convert(dataNodeConfig));
    }
    if (!adapter.set(dataNodeConfig)) {
      logger.error("[{}] Failed to add config for new node {} in the property store.", dcName.toUpperCase(),
          dataNodeConfig.getInstanceName());
    }
  }

  protected void removeDataNodeConfigFromHelix(String dcName, String instanceName,
      PropertyStoreToDataNodeConfigAdapter adapter) {
    adminForDc.get(dcName).dropInstance(clusterName, new InstanceConfig(instanceName));
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      if (adapter.remove(instanceName)) {
        logger.error("[{}] Failed to remove config for node {} in the property store.", dcName.toUpperCase(),
            instanceName);
      }
    }
  }

  /**
   * Add and/or update resources in Helix based on the information in the static cluster map. This may involve adding
   * or removing partitions from under a resource, and adding or dropping resources altogether.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  protected abstract void addUpdateResources(String dcName, Map<String, Set<String>> partitionsToInstancesInDc);

  /**
   * Create a {@link HelixPropertyStore} for given datacenter.
   * @param dcName the name of datacenter
   * @return {@link HelixPropertyStore} associated with given dc.
   */
  protected HelixPropertyStore<ZNRecord> createHelixPropertyStore(String dcName) {
    Properties storeProps = new Properties();
    storeProps.setProperty("helix.property.store.root.path", "/" + clusterName + "/" + PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(storeProps));
    // The number of zk endpoints has been validated in the ctor of HelixBootstrapUpgradeUtil, no need to check it again
    String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
    return CommonUtils.createHelixPropertyStore(zkConnectStr, propertyStoreConfig, null);
  }

  /**
   * @param dcName the datacenter to check in.
   * @return true if the cluster is present and set up in this datacenter.
   */
  protected boolean isClusterPresent(String dcName) {
    if (zkClientForDc.get(dcName) == null) {
      return Objects.requireNonNull(adminForDc.get(dcName), () -> "no admin for " + dcName)
          .getClusters()
          .contains(clusterName);
    } else {
      return ZKUtil.isClusterSetup(clusterName, zkClientForDc.get(dcName));
    }
  }

  /**
   * Add new state model def to ambry cluster in enabled datacenter(s).
   */
  protected void addStateModelDef() {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      // Add a cluster entry in every enabled DC
      String dcName = entry.getKey();
      HelixAdmin admin = entry.getValue();
      if (!isClusterPresent(dcName)) {
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
  protected static class ReplicaComparator implements Comparator<ReplicaId> {
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
    List<String> partiallySealedPartitionsList = new ArrayList<>();
    List<String> stoppedReplicasList = new ArrayList<>();
    List<String> disabledReplicasList = new ArrayList<>();
    if (instanceToDiskReplicasMap.containsKey(instanceName)) {
      Map<String, Map<String, String>> diskInfos = new TreeMap<>();
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
          if (referenceInstanceConfig == null && replica.isPartiallySealed()) {
            partiallySealedPartitionsList.add(Long.toString(replica.getPartition().getId()));
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
      partiallySealedPartitionsList = ClusterMapUtils.getPartiallySealedReplicas(referenceInstanceConfig);
      stoppedReplicasList = ClusterMapUtils.getStoppedReplicas(referenceInstanceConfig);
      disabledReplicasList = ClusterMapUtils.getDisabledReplicas(referenceInstanceConfig);
    }
    instanceConfig.getRecord().setListField(SEALED_STR, sealedPartitionsList);
    instanceConfig.getRecord().setListField(PARTIALLY_SEALED_STR, partiallySealedPartitionsList);
    instanceConfig.getRecord().setListField(STOPPED_REPLICAS_STR, stoppedReplicasList);
    instanceConfig.getRecord().setListField(DISABLED_REPLICAS_STR, disabledReplicasList);
    return instanceConfig;
  }

  /**
   * Initialize a map of dataCenter to HelixAdmin based on the given zk Connect Strings.
   */
  protected void maybeAddCluster() {
    for (Map.Entry<String, HelixAdmin> entry : adminForDc.entrySet()) {
      // Add a cluster entry in every DC
      String dcName = entry.getKey();
      HelixAdmin admin = entry.getValue();
      if (!isClusterPresent(dcName)) {
        info("Adding cluster {} in {}", clusterName, dcName);
        admin.addCluster(clusterName);
        info("Adding state model {} to cluster {}", stateModelDef, clusterName);
        admin.addStateModelDef(clusterName, stateModelDef, getStateModelDefinition(stateModelDef));
      }
    }
  }

  /**
   * Get state model definition based on given name.
   * @param stateModelDefName the name of state model definition that would be employed by Ambry cluster.
   * @return {@link StateModelDefinition}
   */
  protected StateModelDefinition getStateModelDefinition(String stateModelDefName) {
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
  protected void startClusterManager() throws IOException {
    ClusterMapConfig clusterMapConfig =
        getClusterMapConfig(clusterName, adminForDc.keySet().iterator().next(), zkLayoutPath);
    HelixClusterAgentsFactory helixClusterAgentsFactory = new HelixClusterAgentsFactory(clusterMapConfig, null, null);
    validatingHelixClusterManager = helixClusterAgentsFactory.getClusterMap();
  }

  /**
   * Exposed for test use.
   * @param clusterName cluster name to use in the config
   * @param dcName datacenter name to use in the config
   * @param zkLayoutPath if non-null, read ZK connection configs from the file at this path.
   * @return the {@link ClusterMapConfig}
   */
  static ClusterMapConfig getClusterMapConfig(String clusterName, String dcName, String zkLayoutPath) {
    Properties props = new Properties();
    props.setProperty("clustermap.host.name", "localhost");
    props.setProperty("clustermap.cluster.name", clusterName);
    props.setProperty("clustermap.datacenter.name", dcName);
    if (zkLayoutPath != null) {
      try {
        props.setProperty("clustermap.dcs.zk.connect.strings", Utils.readStringFromFile(zkLayoutPath));
      } catch (IOException e) {
        throw new RuntimeException("Could not read zk layout file", e);
      }
    }
    return new ClusterMapConfig(new VerifiableProperties(props));
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
   * Validate that the information in Helix is consistent with the information in the static clustermap; and close
   * all the admin connections to ZK hosts.
   */
  protected abstract void validateAndClose();

  /**
   * Get sealed partitions from Helix cluster.
   * @return a set of sealed partitions across all DCs.
   */
  protected Set<String> getSealedPartitionsInHelixCluster() throws Exception {
    info("Aggregating sealed partitions from cluster {} in Helix", clusterName);
    CountDownLatch sealedPartitionLatch = new CountDownLatch(adminForDc.size());
    AtomicInteger errorCount = new AtomicInteger();
    Map<String, Set<String>> dcToSealedPartitions = new ConcurrentHashMap<>();
    Map<String, Set<String>> nodeToNonExistentReplicas = new ConcurrentHashMap<>();
    for (Datacenter dc : staticClusterMap.hardwareLayout.getDatacenters()) {
      HelixAdmin admin = adminForDc.get(dc.getName());
      if (admin == null) {
        info("Skipping {}", dc.getName());
        continue;
      }
      ensureOrThrow(isClusterPresent(dc.getName()),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      Utils.newThread(() -> {
        try {
          getSealedPartitionsInDc(dc, dcToSealedPartitions, nodeToNonExistentReplicas);
        } catch (Throwable t) {
          logger.error("[{}] error message: {}", dc.getName().toUpperCase(), t.getMessage());
          errorCount.getAndIncrement();
        } finally {
          sealedPartitionLatch.countDown();
        }
      }, false).start();
    }
    sealedPartitionLatch.await(10, TimeUnit.MINUTES);
    ensureOrThrow(errorCount.get() == 0, "Error occurred when aggregating sealed partitions in cluster " + clusterName);
    Set<String> sealedPartitionsInCluster = new HashSet<>();
    info("========================= Summary =========================");
    for (Map.Entry<String, Set<String>> entry : dcToSealedPartitions.entrySet()) {
      info("Dc {} has {} sealed partitions.", entry.getKey(), entry.getValue().size());
      sealedPartitionsInCluster.addAll(entry.getValue());
    }
    info("========================= Sealed Partitions across All DCs =========================");
    info("Total number of sealed partitions in cluster = {}", sealedPartitionsInCluster.size());
    info("Sealed partitions are {}", sealedPartitionsInCluster.toString());
    if (!nodeToNonExistentReplicas.isEmpty()) {
      info("Following {} nodes have sealed replica that are not actually present", nodeToNonExistentReplicas.size());
      for (Map.Entry<String, Set<String>> entry : nodeToNonExistentReplicas.entrySet()) {
        info("{} has non-existent replicas: {}", entry.getKey(), entry.getValue().toString());
      }
    }
    info("Successfully aggregate sealed from cluster {} in Helix", clusterName);
    return sealedPartitionsInCluster;
  }

  /**
   * Get sealed partitions from given datacenter.
   * @param dc the datacenter where sealed partitions come from.
   * @param dcToSealedPartitions a map to track sealed partitions in each dc. This entry associated with given dc will
   *                             be populated in this method.
   * @param nodeToNonExistentReplicas a map to track if any replica is in sealed list but not actually on local node.
   */
  protected void getSealedPartitionsInDc(Datacenter dc, Map<String, Set<String>> dcToSealedPartitions,
      Map<String, Set<String>> nodeToNonExistentReplicas) {
    String dcName = dc.getName();
    dcToSealedPartitions.put(dcName, new HashSet<>());
    ClusterMapConfig config = getClusterMapConfig(clusterName, dcName, null);
    String zkConnectStr = dataCenterToZkAddress.get(dcName).getZkConnectStrs().get(0);
    try (PropertyStoreToDataNodeConfigAdapter propertyStoreAdapter = new PropertyStoreToDataNodeConfigAdapter(
        zkConnectStr, config)) {
      InstanceConfigToDataNodeConfigAdapter.Converter instanceConfigConverter =
          new InstanceConfigToDataNodeConfigAdapter.Converter(config);
      Set<String> allInstancesInHelix = new HashSet<>(getInstanceNamesInHelix(dcName, propertyStoreAdapter));
      for (DataNodeId dataNodeId : dc.getDataNodes()) {
        DataNode dataNode = (DataNode) dataNodeId;
        Set<String> replicasOnNode = staticClusterMap.getReplicas(dataNode)
            .stream()
            .map(replicaId -> replicaId.getPartitionId().toPathString())
            .collect(Collectors.toSet());
        String instanceName = getInstanceName(dataNode);
        ensureOrThrow(allInstancesInHelix.contains(instanceName), "Instance not present in Helix " + instanceName);
        DataNodeConfig dataNodeConfig =
            getDataNodeConfigFromHelix(dcName, instanceName, propertyStoreAdapter, instanceConfigConverter);
        Set<String> sealedReplicas = dataNodeConfig.getSealedReplicas();
        if (sealedReplicas != null) {
          for (String sealedReplica : sealedReplicas) {
            info("Replica {} is sealed on {}", sealedReplica, instanceName);
            dcToSealedPartitions.get(dcName).add(sealedReplica);
            if (!replicasOnNode.contains(sealedReplica)) {
              logger.warn("Replica {} is in sealed list but not on node {}", sealedReplica, instanceName);
              nodeToNonExistentReplicas.computeIfAbsent(instanceName, key -> new HashSet<>()).add(sealedReplica);
            }
          }
        }
      }
    }
  }

  /**
   * A helper method that returns a map of mountPaths to a map of replicas -> replicaCapacity for a given
   * {@link DataNodeId}
   * @param staticClusterMap the static {@link StaticClusterManager}
   * @param dataNodeId the {@link DataNodeId} of interest.
   * @return the constructed map.
   */
  protected static Map<String, Map<String, ReplicaId>> getMountPathToReplicas(StaticClusterManager staticClusterMap,
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
  protected void ensureOrThrow(boolean condition, String errStr) {
    if (!condition) {
      throw new AssertionError(errStr);
    }
  }

  /**
   * Log in INFO mode
   * @param format format
   * @param arguments arguments
   */
  protected static void info(String format, Object... arguments) {
    logger.info(format, arguments);
  }

  /**
   * Log the summary of this run.
   */
  protected abstract void logSummary();
}


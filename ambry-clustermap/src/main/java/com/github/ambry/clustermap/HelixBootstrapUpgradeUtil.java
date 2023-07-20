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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.model.ResourceConfig;
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
public class HelixBootstrapUpgradeUtil {
  static final int FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER = 10000;
  static final int DEFAULT_MAX_PARTITIONS_PER_RESOURCE = 100;
  static final String HELIX_DISABLED_PARTITION_STR =
      InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name();
  // blockRemovingNodeLatch is for testing purpose
  static CountDownLatch blockRemovingNodeLatch = null;
  static CountDownLatch disablePartitionLatch = null;
  private final StaticClusterManager staticClusterMap;
  private final String zkLayoutPath;
  private final String stateModelDef;
  private final Map<String, HelixAdmin> adminForDc = new HashMap<>();
  private final Map<String, RealmAwareZkClient> zkClientForDc = new HashMap<>();
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
  private final DataNodeConfigSourceType dataNodeConfigSourceType;
  private final boolean overrideReplicaStatus;
  private boolean expectMoreInHelixDuringValidate = false;
  private final int maxInstancesInOneResourceForFullAuto;
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

  // Waged auto rebalancer default configs
  static final String DISK_KEY = "DISK";
  static final String RACK_KEY = "rack";
  static final String HOST_KEY = "host";
  static final String TOPOLOGY = "/" + RACK_KEY + "/" + HOST_KEY;
  static final String FAULT_ZONE_TYPE = "rack";
  // Allow a host to be filled upto 95%
  static final int INSTANCE_MAX_CAPACITY_PERCENTAGE = 95;

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
   * @param maxInstancesInOneResourceForFullAuto max number of instances to be assigned under one resource when the resources
   *                                             are in full auto compatible mode.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  public static void bootstrapOrUpgrade(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, boolean startValidatingClusterManager, String stateModelDef,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus, int maxInstancesInOneResourceForFullAuto) throws Exception {
    if (dryRun) {
      info("==== This is a dry run ====");
      info("No changes will be made to the information in Helix (except adding the cluster if it does not exist.");
    }
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            maxPartitionsInOneResource, dryRun, forceRemove, helixAdminFactory, stateModelDef, null, null, null,
            helixAdminOperation, dataNodeConfigSourceType, overrideReplicaStatus, maxInstancesInOneResourceForFullAuto);
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
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, forceRemove, null, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF,
            null, null, null, null, null, false, 0);
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
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, stateModelDef, null, null, null, null, null, false,
            0);
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
      String clusterNamePrefix, String dcs, String stateModelDef, DataNodeConfigSourceType dataNodeConfigSourceType,
      int maxInstancesInOneResourceForFullAuto) throws Exception {
    HelixBootstrapUpgradeUtil clusterMapToHelixMapper =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, stateModelDef, null, null, null,
            HelixAdminOperation.ValidateCluster, dataNodeConfigSourceType, false, maxInstancesInOneResourceForFullAuto);
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
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, ClusterMapConfig.DEFAULT_STATE_MODEL_DEF, null,
            null, null, HelixAdminOperation.ListSealedPartition, dataNodeConfigSourceType, false, 0);
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
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, null, Objects.requireNonNull(hostname), portNum,
            Objects.requireNonNull(partitionName), operation, null, false, 0);
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
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, false, false, null, null, null, null, null,
            HelixAdminOperation.MigrateToPropertyStore, PROPERTY_STORE, false, 0);
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
   * @param dryRun if true, perform a dry run; do not update anything in Helix.
   * @param wagedConfigFilePath disk capacity of each partition. This is used as a weight in Waged rebalancer.
   * @throws Exception
   */
  static void migrateToFullAuto(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, String resources, boolean dryRun, String wagedConfigFilePath)
      throws Exception {
    HelixBootstrapUpgradeUtil helixBootstrapUpgradeUtil =
        new HelixBootstrapUpgradeUtil(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath, clusterNamePrefix, dcs,
            DEFAULT_MAX_PARTITIONS_PER_RESOURCE, dryRun, false, null, null, null, null, null,
            HelixAdminOperation.MigrateToFullAuto, PROPERTY_STORE, false, 0);

    WagedHelixConfig wagedHelixConfig =
        new ObjectMapper().readValue(Utils.readStringFromFile(wagedConfigFilePath), WagedHelixConfig.class);

    helixBootstrapUpgradeUtil.migrateToFullAuto(resources, wagedHelixConfig);
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
  private static void generateAdminInfosAndUpload(HelixBootstrapUpgradeUtil clusterMapToHelixMapper, String adminType,
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
   * @param dataNodeConfigSourceType the {@link DataNodeConfigSourceType} associated with this cluster.
   * @param overrideReplicaStatus whether to override sealed/stopped/disabled replica status lists.
   * @throws IOException if there is an error reading a file.
   * @throws JSONException if there is an error parsing the JSON content in any of the files.
   */
  private HelixBootstrapUpgradeUtil(String hardwareLayoutPath, String partitionLayoutPath, String zkLayoutPath,
      String clusterNamePrefix, String dcs, int maxPartitionsInOneResource, boolean dryRun, boolean forceRemove,
      HelixAdminFactory helixAdminFactory, String stateModelDef, String hostname, Integer portNum, String partitionName,
      HelixAdminOperation helixAdminOperation, DataNodeConfigSourceType dataNodeConfigSourceType,
      boolean overrideReplicaStatus, int maxInstancesInOneResourceForFullAuto) throws Exception {
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
    this.maxInstancesInOneResourceForFullAuto = maxInstancesInOneResourceForFullAuto;
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
  private Map<String, Map<String, Map<String, String>>> generatePartitionOverrideFromClusterMap() {
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
  private Map<String, Map<String, Map<String, String>>> generatePartitionOverrideFromConfigFile(
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
  private Map<String, Map<String, Map<String, String>>> generateReplicaAdditionMap() {
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
  private void uploadClusterAdminInfos(Map<String, Map<String, Map<String, String>>> adminInfosByDc,
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
  private void deleteClusterAdminInfos(String clusterAdminType, String adminConfigZNodePath) {
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
  private void migrateToPropertyStore() throws InterruptedException {
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
   * Convert resources from semi-auto to full-auto.
   * @param commaSeparatedResources the comma-separated list of resources that needs to be migrated
   * @param wagedHelixConfig
   *
   */
  public void migrateToFullAuto(String commaSeparatedResources, WagedHelixConfig wagedHelixConfig)
      throws InterruptedException {
    CountDownLatch migrationComplete = new CountDownLatch(adminForDc.size());
    // different DCs can be migrated in parallel
    adminForDc.forEach((dcName, helixAdmin) -> Utils.newThread(() -> {
      try {

        // 0. Check if all resources in the cluster are full-auto compatible
        boolean resourcesAreFullAutoCompatible = maybeVerifyResourcesAreFullAutoCompatible(dcName, clusterName);
        if (!resourcesAreFullAutoCompatible) {
          throw new IllegalStateException("Resources are not full auto compatible");
        }
        verifyPartitionPlacementIsFullAutoCompatibleInStatic(dcName);
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
          setupCluster(configAccessor, wagedHelixConfig.getClusterConfigFields());

          // 2. Update instance config for all instances present in each resource
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
          // 4.1 Instance validation
          Map<String, Boolean> instanceValidationResultMap =
              helixAdmin.validateInstancesForWagedRebalance(clusterName, new ArrayList<>(allInstances));
          Set<String> invalidInstances = new HashSet<>();
          for (Map.Entry<String, Boolean> entry : instanceValidationResultMap.entrySet()) {
            if (entry.getValue() == Boolean.FALSE) {
              invalidInstances.add(entry.getKey());
            }
          }
          if (!invalidInstances.isEmpty()) {
            logger.error("Validation of waged rebalancer configuration failed for instances {}", invalidInstances);
            throw new IllegalStateException(
                "Instances" + invalidInstances + "are not configured correctly for waged rebalancer");
          }
          // 4.2 Resource validation
          Map<String, Boolean> resourceValidationResultMap =
              helixAdmin.validateResourcesForWagedRebalance(clusterName, new ArrayList<>(resources));
          Set<String> invalidResources = new HashSet<>();
          for (Map.Entry<String, Boolean> entry : resourceValidationResultMap.entrySet()) {
            if (entry.getValue() == Boolean.FALSE) {
              invalidResources.add(entry.getKey());
            }
          }
          if (!invalidResources.isEmpty()) {
            logger.error("Validation of waged rebalancer configuration failed for resources {}", invalidResources);
            throw new IllegalStateException(
                "Resources" + invalidResources + "are not configured correctly for waged rebalancer");
          }

          if (!dryRun) {
            info("[{}] Migrating resources {} in cluster {} from semi-auto to full-auto", dcName.toUpperCase(),
                resources, clusterName);

            // 5. Enter maintenance mode
            helixAdmin.manuallyEnableMaintenanceMode(clusterName, true, "Migrating to Full auto",
                Collections.emptyMap());

            // 6. Update ideal state and enable waged rebalancer
            for (String resource : resources) {
              IdealState idealState = resourceToIdealState.get(resource);
              idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
              // Set resource tag
              idealState.setInstanceGroupTag(getResourceTag(resource));
              // Set number of replicas
              idealState.setReplicas(String.valueOf(wagedHelixConfig.getIdealStateConfigFields().getNumReplicas()));
              idealState.setMinActiveReplicas(wagedHelixConfig.getIdealStateConfigFields().getMinActiveReplicas());
              helixAdmin.updateIdealState(clusterName, resource, idealState);
            }
            helixAdmin.enableWagedRebalance(clusterName, new ArrayList<>(resources));

            // 7. Update the list of resources migrating to full_auto in helix property store. This is used by servers
            // in case we want to fall back to semi_auto later.
             updateAdminConfigForFullAutoMigration(dcName, resources);

            // 8. Exit maintenance mode
            helixAdmin.manuallyEnableMaintenanceMode(clusterName, false, "Complete migrating to Full auto",
                Collections.emptyMap());

            info("[{}] Successfully migrated resources {} in cluster {} from semi-auto to full-auto",
                dcName.toUpperCase(), resources, clusterName);
          } else {
            info("[{}] DryRun. Migrating resources {} in cluster {} from semi-auto to full-auto", dcName.toUpperCase(),
                resources, clusterName);
          }
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
   * Add admin config in Helix to keep track of list of resources migrating to Full-Auto
   * @param dcName data center name
   * @param resources migrating to Full Auto
   */
  private void updateAdminConfigForFullAutoMigration(String dcName, Set<String> resources) {
    HelixPropertyStore<ZNRecord> helixPropertyStore = createHelixPropertyStore(dcName);
    try {
      // Get property store ZNode path
      ZNRecord zNRecord = helixPropertyStore.get(FULL_AUTO_MIGRATION_ZNODE_PATH, null, AccessOption.PERSISTENT);
      if (zNRecord == null) {
        info("Creating {} znode record for datacenter to keep list of resources migrating to full_auto {}.",
            FULL_AUTO_MIGRATION_ZNODE_PATH, dcName);
        zNRecord = new ZNRecord(FULL_AUTO_MIGRATION_STR);
      } else {
        info("Updating {} znode record for datacenter to keep list of resources migrating to full_auto {}.",
            FULL_AUTO_MIGRATION_ZNODE_PATH, dcName);
      }

      // Update list field in ZNode
      List<String> migratingResourcesList = zNRecord.getListField(RESOURCES_STR) == null ? new ArrayList<>()
          : zNRecord.getListField(RESOURCES_STR);
      Set<String> migratingResourcesSet = new HashSet<>(migratingResourcesList);
      migratingResourcesSet.addAll(resources);
      zNRecord.setListField(RESOURCES_STR, new ArrayList<>(migratingResourcesSet));

      // Set property store Znode path
      if (!helixPropertyStore.set(FULL_AUTO_MIGRATION_ZNODE_PATH, zNRecord, AccessOption.PERSISTENT)) {
        logger.error("Failed to create/update {} znode record for datacenter {}",
            FULL_AUTO_MIGRATION_ZNODE_PATH, dcName);
      }
    } finally {
      helixPropertyStore.stop();
    }
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
      // TODO: To check - In Helix full auto, ideal state may contain intermediate states.
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
   * @param wagedClusterConfigFields disk capacity of each partition.
   */
  private void setupCluster(ConfigAccessor configAccessor, WagedClusterConfigFields wagedClusterConfigFields) {
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);

    // 1. Set topology awareness. For example, if fault_zone_type is rack, helix avoids placing replicas on the hosts
    // in same rack
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(FAULT_ZONE_TYPE);

    // 2. Set max number of concurrent state transitions. This avoids too many replica movements in the cluster.
    StateTransitionThrottleConfig stateTransitionThrottleConfig =
        new StateTransitionThrottleConfig(StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
            StateTransitionThrottleConfig.ThrottleScope.INSTANCE, wagedClusterConfigFields.maxPartitionInTransition);
    clusterConfig.setStateTransitionThrottleConfigs(Collections.singletonList(stateTransitionThrottleConfig));

    // 3. Set delay re-balancing to true to avoid replica movements if host is temporarily down due to deployment,
    // maintenance, etc
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(wagedClusterConfigFields.delayRebalanceTimeInSecs);

    // 4. Set persistence of best possible assignment. This will allow us to see the partition assignment in IDEAL state.
    clusterConfig.setPersistBestPossibleAssignment(true);

    // 5. Set default weight for each partition. Helix will do partition assignment based on partition weight and host
    // capacity.
    Map<String, Integer> defaultPartitionWeightMap = new HashMap<>();
    defaultPartitionWeightMap.put(DISK_KEY, wagedClusterConfigFields.partitionDiskWeightInGB);
    clusterConfig.setDefaultPartitionWeightMap(defaultPartitionWeightMap);

    // 6. Set instance capacity keys
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(DISK_KEY));

    // 7. Set evenness and less movement for cluster. These help to balance evenness and replica movements in cluster.
    // Below are calculated values based on simulation.
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> globalRebalancePreferenceKeyIntegerMap = new HashMap<>();
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS,
        wagedClusterConfigFields.evenness);
    globalRebalancePreferenceKeyIntegerMap.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT,
        wagedClusterConfigFields.lessMovement);
    clusterConfig.setGlobalRebalancePreference(globalRebalancePreferenceKeyIntegerMap);

    // Update cluster config in Helix/ZK
    configAccessor.setClusterConfig(clusterName, clusterConfig);
    info("Updated cluster config fields to: topology {}, fault_zone_type {}, max_partition_in_transition {}, "
            + "delay_rebalance_time_in_secs {}, partition_disk_weight_in_gb {}, evenness {}, less_movement {} ", TOPOLOGY,
        FAULT_ZONE_TYPE, wagedClusterConfigFields.maxPartitionInTransition,
        wagedClusterConfigFields.delayRebalanceTimeInSecs, wagedClusterConfigFields.partitionDiskWeightInGB,
        wagedClusterConfigFields.evenness, wagedClusterConfigFields.lessMovement);
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
    // Remove previous tags
    List<String> tags = instanceConfig.getTags();
    for (String tag : tags) {
      instanceConfig.removeTag(tag);
    }
    // Add latest tag
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
    int actualCapacityInGB = (int) ((double) INSTANCE_MAX_CAPACITY_PERCENTAGE / 100 * capacityInGB);
    capacityMap.put(DISK_KEY, actualCapacityInGB);
    instanceConfig.setInstanceCapacityMap(capacityMap);
    // Update instance config in Helix/ZK
    configAccessor.updateInstanceConfig(clusterName, instanceName, instanceConfig);
    info("Updated instance config fields to: tags {}, rack Id {}, host key {}, instance capacity {}",
        instanceConfig.getTags(), nodeConfigFromHelix.getRackId(), instanceName, actualCapacityInGB);
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
        newThread(() -> {
          info("\n=======Starting datacenter: {}=========\n", dc.getName());
          boolean resourcesAreFullAutoCompatible = maybeVerifyResourcesAreFullAutoCompatible(dc.getName(), clusterName);
          if (resourcesAreFullAutoCompatible) {
            verifyPartitionPlacementIsFullAutoCompatibleInStatic(dc.getName());
          }
          Map<String, Set<String>> partitionsToInstancesInDc = new HashMap<>();
          addUpdateInstances(dc.getName(), partitionsToInstancesInDc);
          // Process those partitions that are already under resources. Just update their instance sets if that has changed.
          addUpdateResources(dc.getName(), partitionsToInstancesInDc, resourcesAreFullAutoCompatible);
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
   * Partitions has multiple replicas that are assigned to multiple instances. Therefore instances share partitions with
   * each other. This method would group all the instances that share any partition with each other and return those groups.
   *
   * For example, given such partition placement
   * {
   *   "partition1": {"host1", "host2", "host3"},
   *   "partition2": {"host4", "host5", "host6"},
   *   "partition3": {"host1", "host4", "host6"},
   *   "partition4": {"host10", "host11", "host12"},
   * }
   * we know that host1 to host6 shared some partitions and should be placed in the same group and host10 to host12 share
   * some partitions and should be a group. Instances in different groups never share partitions with each other.
   * @param partitionToInstances A map from partition id to a list of instances this partition is assigned to
   * @return A list of instance group. Each item of this list is a group of instances that share partitions with each other
   * but never share partitions outside this group.
   */
  List<Set<String>> groupInstancesBasedOnSharedPartition(Map<String, Set<String>> partitionToInstances) {
    int maxId = 0;
    TreeMap<Integer, Set<String>> instanceGroups = new TreeMap<>();
    Map<String, Integer> instanceNameToGroupId = new HashMap<>();
    TreeMap<Integer, Set<String>> orderedPartitionToInstances = new TreeMap<>();
    for (Map.Entry<String, Set<String>> ent : partitionToInstances.entrySet()) {
      orderedPartitionToInstances.put(Integer.parseInt(ent.getKey()), ent.getValue());
    }

    for (Map.Entry<Integer, Set<String>> ent : orderedPartitionToInstances.entrySet()) {
      Set<Integer> groupIds = new HashSet<>();
      for (String instanceName : ent.getValue()) {
        if (!instanceNameToGroupId.containsKey(instanceName)) {
          instanceNameToGroupId.put(instanceName, maxId++);
          instanceGroups.put(instanceNameToGroupId.get(instanceName),
              new HashSet<>(Collections.singleton(instanceName)));
        }
        groupIds.add(instanceNameToGroupId.get(instanceName));
      }
      if (groupIds.size() != 1) {
        // Merge groups
        List<Integer> idList = new ArrayList<>(groupIds);
        int mergeTo = idList.get(0);
        for (int i = 1; i < idList.size(); i++) {
          instanceGroups.get(mergeTo).addAll(instanceGroups.get(idList.get(i)));
          for (String instanceName : instanceGroups.get(idList.get(i))) {
            instanceNameToGroupId.put(instanceName, mergeTo);
          }
          instanceGroups.remove(idList.get(i));
        }
      }
    }
    return new ArrayList<>(instanceGroups.values());
  }

  /**
   * Given a map from partition to a set of instances, create reverse map from instances to a set of partitions.
   * @param partitionToInstances The partition placement map
   * @return A map from instances to a set of partitions.
   */
  private Map<String, Set<String>> generateInstanceToPartitionsMap(Map<String, Set<String>> partitionToInstances) {
    Map<String, Set<String>> result = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : partitionToInstances.entrySet()) {
      entry.getValue().forEach(in -> result.computeIfAbsent(in, k -> new HashSet<>()).add(entry.getKey()));
    }
    return result;
  }

  /**
   * Verify that partition placement in static layout is FULL_AUTO compatible.
   * In order to be FULL_AUTO compatible, the size of instance group should not exceed the given max number of instances
   * in one resource.
   * @param dcName The datacenter.
   */
  private void verifyPartitionPlacementIsFullAutoCompatibleInStatic(String dcName) {
    // First, for all partitions, find the instances
    Map<String, Set<String>> partitionToInstances = new HashMap<>();
    for (PartitionId partitionId : staticClusterMap.getAllPartitionIds(null)) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        if (replicaId.getDataNodeId().getDatacenterName().equals(dcName)) {
          partitionToInstances.computeIfAbsent(String.valueOf(partitionId.getId()), k -> new HashSet<>())
              .add(getInstanceName(replicaId.getDataNodeId()));
        }
      }
    }
    for (Set<String> instanceNames : groupInstancesBasedOnSharedPartition(partitionToInstances)) {
      ensureOrThrow(instanceNames.size() <= maxInstancesInOneResourceForFullAuto,
          "A group of instances who share partitions is bigger than " + maxInstancesInOneResourceForFullAuto + ": "
              + instanceNames);
    }
    info("[{}] Done verify partition placement in static file is full auto compatible", dcName.toUpperCase());
  }

  /**
   * Build and create a new IdealState for the given resource name and partition layout.
   * @param dcName The name of the datacenter
   * @param resourceName The name of the newly created resource
   * @param partitionsUnderResource The partition layout
   */
  private void buildAndCreateIdealState(String dcName, String resourceName,
      List<Map.Entry<String, Set<String>>> partitionsUnderResource) {
    IdealState idealState = buildIdealState(dcName, resourceName, partitionsUnderResource);
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    if (!dryRun) {
      dcAdmin.addResource(clusterName, resourceName, idealState);
      info("[{}] Added {} new partitions under resource {} in datacenter {}", dcName.toUpperCase(),
          partitionsUnderResource.size(), resourceName, dcName);
    } else {
      info("[{}] Under DryRun mode, {} new partitions are added to resource {} in datacenter {}", dcName.toUpperCase(),
          partitionsUnderResource.size(), resourceName, dcName);
    }
    resourcesAdded.getAndIncrement();
  }

  /**
   * Build a new IdealState for the given resource name and partition layout
   * @param dcName The name of the datacenter
   * @param resourceName The name of the newly created resource
   * @param partitionsUnderResource The partition layout
   * @return The new IdealState
   */
  private IdealState buildIdealState(String dcName, String resourceName,
      List<Map.Entry<String, Set<String>>> partitionsUnderResource) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef(stateModelDef);
    info("[{}] Adding partitions for next resource {} in {}. {}.", dcName.toUpperCase(), resourceName, dcName,
        dryRun ? "Actual IdealState is not changed as dry run" : "IdealState is being updated");
    updateIdealStatePartitions(idealState, partitionsUnderResource);
    if (!idealState.isValid()) {
      throw new IllegalStateException("IdealState could not be validated for new resource " + resourceName);
    }
    return idealState;
  }

  /**
   * Update the partition layout in the given ideal state.
   * @param idealState The ideal state to change partition layout
   * @param partitionsUnderResource The partition layout.
   */
  private void updateIdealStatePartitions(IdealState idealState,
      List<Map.Entry<String, Set<String>>> partitionsUnderResource) {
    for (Map.Entry<String, Set<String>> entry : partitionsUnderResource) {
      String partitionName = entry.getKey();
      ArrayList<String> instances = new ArrayList<>(entry.getValue());
      Collections.shuffle(instances);
      idealState.setPreferenceList(partitionName, instances);
    }
    idealState.setNumPartitions(idealState.getPartitionSet().size());
    idealState.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
  }

  /**
   * When resources and static partition layout are both FULL_AUTO compatible, this method would iterate through the new
   * partitions and find corresponding resources for them if possible.
   * Before calling this method, we should already verify that each instance has only one assigned resource. So if a
   * partition's instances are already assigned to the same resource, then this partition would be assigned to this resource
   * as well. If a partition's instances are assigned to different resources, then this method would fail.
   *
   * This method would also remove all the new partitions that are assigned to an existing resource from partitionToInstancesInDc.
   * @param dcName The name of the datacenter
   * @param partitionsToInstancesInDc The map of partition layout.
   * @param instanceNameToOneResource The map from instance name to its corresponding assigned resource id
   * @param resourceIdToInstances The map from resource id to a list of instance names that are assigned to this resource
   * @param resourceIdToIdealState The map from resource id to IdealState
   */
  private void updateIdealStateWithNewPartitionsForFullAuto(String dcName,
      Map<String, Set<String>> partitionsToInstancesInDc, Map<String, Integer> instanceNameToOneResource,
      Map<Integer, Set<String>> resourceIdToInstances, Map<Integer, IdealState> resourceIdToIdealState) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);

    // Find the partitions whose instances already exist in resources and assign those partitions to the corresponding resources.
    int round = 1;
    boolean allPartitionsDontHaveResources;
    while (round >= 0) {
      allPartitionsDontHaveResources = true;
      for (Map.Entry<String, Set<String>> ent : partitionsToInstancesInDc.entrySet()) {
        Set<String> instanceNames = ent.getValue();
        Set<Integer> resources = instanceNames.stream()
            .map(in -> instanceNameToOneResource.get(in))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        if (resources.isEmpty()) {
          // Partition doesn't belong to any host that are already in the helix
        } else if (resources.size() == 1) {
          allPartitionsDontHaveResources = false;
          int resourceId = resources.iterator().next();
          resourceIdToInstances.get(resourceId).addAll(instanceNames);
          // Some instance names might not have a resource name, now assign the resource name to it
          instanceNames.forEach(in -> instanceNameToOneResource.put(in, resourceId));
          Set<String> instances = resourceIdToInstances.get(resourceId);
          ensureOrThrow(instances.size() <= maxInstancesInOneResourceForFullAuto,
              "Resource has more than " + maxInstancesInOneResourceForFullAuto + " hosts: " + instances);
        } else {
          String errorMessage =
              String.format("Partition %s belong to different hosts of different resources, hosts: %s, resources %s",
                  ent.getKey(), instanceNames, resources);
          ensureOrThrow(false, errorMessage);
        }
      }
      // Run this logic for two rounds so that we can use the later partitions to exam prior partitions
      if (allPartitionsDontHaveResources) {
        info("[{}] All new partitions don't have hosts exist in old resources for cluster {}.", dcName.toUpperCase(),
            clusterName);
        break;
      }
      round--;
    }

    Map<Integer, IdealState> resourceToUpdate = new HashMap<>();
    Map<Integer, Integer> resourceToPartitionSize = new HashMap<>();
    Iterator<Map.Entry<String, Set<String>>> partitionIterator = partitionsToInstancesInDc.entrySet().iterator();
    int numPartitions = 0;
    while (partitionIterator.hasNext()) {
      Map.Entry<String, Set<String>> partitionToInstances = partitionIterator.next();
      // If we are here, we know that all partitions' instances would either belong to one resource or no resource at all.
      String anyInstance = partitionToInstances.getValue().iterator().next();
      if (instanceNameToOneResource.containsKey(anyInstance)) {
        Integer resourceId = instanceNameToOneResource.get(anyInstance);
        if (!resourceToUpdate.containsKey(resourceId)) {
          resourceToUpdate.put(resourceId, resourceIdToIdealState.get(resourceId));
          resourceToPartitionSize.put(resourceId, resourceIdToIdealState.get(resourceId).getPartitionSet().size());
        }
        IdealState idealState = resourceToUpdate.get(resourceId);
        ArrayList<String> newInstances = new ArrayList<>(partitionToInstances.getValue());
        Collections.shuffle(newInstances);
        idealState.setPreferenceList(partitionToInstances.getKey(), newInstances);
        partitionIterator.remove();
        numPartitions++;
      }
    }

    if (numPartitions > 0) {
      info(
          "[{}] We have {} partitions to add to existing resources, and we have {} existing resources to update for cluster {}",
          dcName.toUpperCase(), numPartitions, resourceToUpdate.size(), clusterName);
      for (Map.Entry<Integer, IdealState> entry : resourceToUpdate.entrySet()) {
        String resourceName = String.valueOf(entry.getKey());
        IdealState idealState = entry.getValue();
        int sizeBefore = resourceToPartitionSize.get(entry.getKey());
        if (!dryRun) {
          dcAdmin.setResourceIdealState(clusterName, resourceName, idealState);
          info("[{}] Update resource {} in datacenter {} from {} partitions to {} partitions", dcName.toUpperCase(),
              resourceName, dcName, sizeBefore, idealState.getPartitionSet().size());
        } else {
          info("[{}] Under DryRun mode, update resource {} in datacenter {} from {} partitions to {} partitions",
              dcName.toUpperCase(), resourceName, dcName, sizeBefore, idealState.getPartitionSet().size());
        }
        resourcesUpdated.incrementAndGet();
      }
      info("[{}] Successfully add partitions to existing resources for cluster {}", dcName.toUpperCase(), clusterName);
    } else {
      info("[{}] We don't have any partition to add to existing resources for cluster {}", dcName.toUpperCase(),
          clusterName);
    }
  }

  /**
   * Create or update ideal state for given partition layout when the resources and static layout are both FULL_AUTO
   * compatible. When calling this method, make sure that given partitions' instances are not assigned to any existing
   * resources, so they are all new partitions in new instances.
   * This method would first find all the instance group and partitions that belong to each group. It will then try to
   * merge those groups if the size of the group is less than the max number of instances in one resource. Also this
   * method would try to merge with last existing resource if the size allows.
   * @param dcName The name of the datacenter
   * @param partitionsToInstancesInDc The map of partition layout.
   * @param resourceIdToInstances The map from resource id to a list of instance names that are assigned to this resource
   * @param resourceIdToIdealState The map from resource id to IdealState
   */
  private void createOrUpdateIdealStateForFullAuto(String dcName, Map<String, Set<String>> partitionsToInstancesInDc,
      TreeMap<Integer, Set<String>> resourceIdToInstances, Map<Integer, IdealState> resourceIdToIdealState) {
    // The remaining partition would form a list of instance group. Each group would be added to the new partitions
    int remainingPartition = partitionsToInstancesInDc.size();
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    List<Set<String>> instanceGroups = groupInstancesBasedOnSharedPartition(partitionsToInstancesInDc);
    Map<String, Set<String>> instanceToPartitions = generateInstanceToPartitionsMap(partitionsToInstancesInDc);
    info("[{}] Found {} groups of instances for cluster {}", dcName.toUpperCase(), instanceGroups.size(), clusterName);
    Iterator<Set<String>> instanceGroupIterator = instanceGroups.iterator();
    int startResourceId = FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER;
    int maxResource = -1;
    Set<String> currentInstances = new HashSet<>();
    // The last resource might still be able to take some group
    if (!resourceIdToInstances.isEmpty()) {
      Map.Entry<Integer, Set<String>> lastEntry = resourceIdToInstances.lastEntry();
      startResourceId = lastEntry.getKey();
      maxResource = startResourceId;
      currentInstances.addAll(lastEntry.getValue());
      info("[{}] Already has resources, will try to use last resource {}, current size: {}, target size {}",
          dcName.toUpperCase(), startResourceId, currentInstances.size(), maxInstancesInOneResourceForFullAuto);
    }
    int curResourceId = startResourceId;
    while (instanceGroupIterator.hasNext()) {
      Set<String> group = instanceGroupIterator.next();
      if (currentInstances.size() + group.size() > maxInstancesInOneResourceForFullAuto) {
        resourceIdToInstances.put(curResourceId, new HashSet<>(currentInstances));
        curResourceId++;
        currentInstances.clear();
      }
      currentInstances.addAll(group);
    }
    if (currentInstances.size() != 0) {
      resourceIdToInstances.put(curResourceId, currentInstances);
    }

    for (; startResourceId <= curResourceId; startResourceId++) {
      String resourceName = Integer.toString(startResourceId);
      List<Map.Entry<String, Set<String>>> partitionsUnderResource = resourceIdToInstances.get(startResourceId)
          .stream()
          .map(in -> instanceToPartitions.get(in))
          .filter(Objects::nonNull) // instances in last resource id might not have partitions in this map
          .flatMap(ps -> ps.stream())
          .collect(Collectors.toSet())
          .stream()
          .map(p -> new AbstractMap.SimpleEntry<>(p, partitionsToInstancesInDc.get(p)))
          .collect(Collectors.toList());
      if (startResourceId == maxResource) {
        if (!partitionsUnderResource.isEmpty()) {
          // Update ideal state for this resource
          IdealState idealState = resourceIdToIdealState.get(startResourceId);
          int sizeBefore = idealState.getPartitionSet().size();
          updateIdealStatePartitions(idealState, partitionsUnderResource);
          if (!dryRun) {
            dcAdmin.setResourceIdealState(clusterName, resourceName, idealState);
            info("[{}] Update resource {} in datacenter {} from {} partitions to {} partitions", dcName.toUpperCase(),
                resourceName, dcName, sizeBefore, partitionsUnderResource.size());
          } else {
            info("[{}] Under DryRun mode, update resource {} in datacenter {} from {} partitions to {} partitions",
                dcName.toUpperCase(), resourceName, dcName, sizeBefore, partitionsUnderResource.size());
          }
          resourcesUpdated.incrementAndGet();
        } else {
          info("[{}] No changes made to resource {}, skip", dcName.toUpperCase(), resourceName);
        }
      } else {
        buildAndCreateIdealState(dcName, resourceName, partitionsUnderResource);
      }
    }
    info("[{}] Successfully added {} partitions to cluster {}", dcName.toUpperCase(), remainingPartition, clusterName);
  }

  /**
   * Add and/or update instances in Helix based on the information in the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  private void addUpdateInstances(String dcName, Map<String, Set<String>> partitionsToInstancesInDc) {
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
    info(
        "[{}] Done adding all instances in {}, now scanning resources in Helix and ensuring instance set for partitions are the same.",
        dcName.toUpperCase(), dcName);
  }

  private List<String> getInstanceNamesInHelix(String dcName, PropertyStoreToDataNodeConfigAdapter adapter) {
    List<String> instanceNames;
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      instanceNames = adapter.getAllDataNodeNames();
    } else {
      instanceNames = adminForDc.get(dcName).getInstancesInCluster(clusterName);
    }
    return instanceNames;
  }

  private DataNodeConfig createDataNodeConfigFromStatic(String dcName, String instanceName,
      DataNodeConfig referenceConfig, Map<String, Set<String>> partitionsToInstancesInDc,
      InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    InstanceConfig referenceInstanceConfig =
        overrideReplicaStatus || referenceConfig == null ? null : converter.convert(referenceConfig);
    return converter.convert(
        createInstanceConfigFromStaticInfo(dcToInstanceNameToDataNodeId.get(dcName).get(instanceName),
            partitionsToInstancesInDc, instanceToDiskReplicasMap, referenceInstanceConfig));
  }

  private DataNodeConfig getDataNodeConfigFromHelix(String dcName, String instanceName,
      PropertyStoreToDataNodeConfigAdapter adapter, InstanceConfigToDataNodeConfigAdapter.Converter converter) {
    DataNodeConfig dataNodeConfig;
    if (dataNodeConfigSourceType == PROPERTY_STORE) {
      dataNodeConfig = adapter.get(instanceName);
    } else {
      dataNodeConfig = converter.convert(adminForDc.get(dcName).getInstanceConfig(clusterName, instanceName));
    }
    return dataNodeConfig;
  }

  private void setDataNodeConfigInHelix(String dcName, String instanceName, DataNodeConfig config,
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

  private void addDataNodeConfigToHelix(String dcName, DataNodeConfig dataNodeConfig,
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

  private void removeDataNodeConfigFromHelix(String dcName, String instanceName,
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
   * or removing partitions from under a resource, and adding or dropping resources altogether. This may also involve
   * changing the instance set for a partition under a resource, based on the static cluster map.
   * @param dcName the name of the datacenter being processed.
   * @param partitionsToInstancesInDc a map to be filled with the mapping of partitions to their instance sets in the
   *                                  given datacenter.
   */
  private void addUpdateResources(String dcName, Map<String, Set<String>> partitionsToInstancesInDc,
      boolean resourcesAreFullAutoCompatible) {
    HelixAdmin dcAdmin = adminForDc.get(dcName);
    List<String> resourcesInCluster = dcAdmin.getResourcesInCluster(clusterName);
    List<String> instancesWithDisabledPartition = new ArrayList<>();
    HelixPropertyStore<ZNRecord> helixPropertyStore =
        helixAdminOperation == HelixAdminOperation.DisablePartition ? createHelixPropertyStore(dcName) : null;
    TreeMap<Integer, Set<String>> resourceIdToInstances = new TreeMap<>();
    Map<String, Set<Integer>> instanceNameToResources = new HashMap<>();
    Map<Integer, IdealState> resourceIdToIdealState = new HashMap<>();
    // maxResource may vary from one dc to another (special partition class allows partitions to exist in one dc only)
    int maxResource = -1;
    for (String resourceName : resourcesInCluster) {
      boolean resourceModified = false;
      if (!resourceName.matches("\\d+")) {
        // there may be other resources created under the cluster (say, for stats) that are not part of the
        // cluster map. These will be ignored.
        continue;
      }
      int resourceId = Integer.parseInt(resourceName);
      maxResource = Math.max(maxResource, resourceId);
      IdealState resourceIs = dcAdmin.getResourceIdealState(clusterName, resourceName);
      resourceIdToIdealState.put(resourceId, resourceIs);
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
          resourceIdToInstances.remove(resourceId);
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
            resourceIdToInstances.computeIfAbsent(resourceId, k -> new HashSet<>()).addAll(instanceSetInStatic);
            instanceSetInStatic.forEach(
                ins -> instanceNameToResources.computeIfAbsent(ins, k -> new HashSet<>()).add(resourceId));
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
        } else {
          resourceIdToInstances.computeIfAbsent(resourceId, k -> new HashSet<>()).addAll(instanceSetInStatic);
          instanceSetInStatic.forEach(
              ins -> instanceNameToResources.computeIfAbsent(ins, k -> new HashSet<>()).add(resourceId));
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
          resourceIdToIdealState.remove(resourceId);
        } else {
          if (!dryRun) {
            dcAdmin.setResourceIdealState(clusterName, resourceName, resourceIs);
            System.out.println("------------------add resource!");
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

    // When the cluster is empty and we have maxInstancesInOneResourceForFullAuto greater than 0, we would by default create
    // resources in FULL_AUTO compatible mode.
    if (resourcesAreFullAutoCompatible) {
      // If the resources is FULL_AUTO compatible, then one instance should have partitions under one resource.
      info(
          "[{}] Resources are full auto compatible, will create new partitions in resources that are also full auto compatible",
          dcName.toUpperCase());
      Map<String, Integer> instanceNameToOneResource = new HashMap<>();
      for (Map.Entry<String, Set<Integer>> ent : instanceNameToResources.entrySet()) {
        ensureOrThrow(ent.getValue().size() == 1,
            "Instance " + ent.getKey() + " has partitions from multiple resources in FULL_AUTO mode: "
                + ent.getValue());
        instanceNameToOneResource.put(ent.getKey(), ent.getValue().iterator().next());
      }
      // Partitions that not are in the helix yet, there are two different cases.
      // The instances this partition is assigned to already in the helix resources, then this partition should go to the same resource.
      // The instances this partition is assigned to don't yet exist in helix resources, then this partition could go to a new resource.
      updateIdealStateWithNewPartitionsForFullAuto(dcName, partitionsToInstancesInDc, instanceNameToOneResource,
          resourceIdToInstances, resourceIdToIdealState);
      if (!partitionsToInstancesInDc.isEmpty()) {
        createOrUpdateIdealStateForFullAuto(dcName, partitionsToInstancesInDc, resourceIdToInstances,
            resourceIdToIdealState);
      }
    } else {
      // Add what is not already in Helix under new resources.
      int fromIndex = 0;
      List<Map.Entry<String, Set<String>>> newPartitions = new ArrayList<>(partitionsToInstancesInDc.entrySet());
      while (fromIndex < newPartitions.size()) {
        String resourceName = Integer.toString(++maxResource);
        int toIndex = Math.min(fromIndex + maxPartitionsInOneResource, newPartitions.size());
        List<Map.Entry<String, Set<String>>> partitionsUnderNextResource = newPartitions.subList(fromIndex, toIndex);
        fromIndex = toIndex;
        buildAndCreateIdealState(dcName, resourceName, partitionsUnderNextResource);
      }
    }
  }

  /**
   * Create a {@link HelixPropertyStore} for given datacenter.
   * @param dcName the name of datacenter
   * @return {@link HelixPropertyStore} associated with given dc.
   */
  private HelixPropertyStore<ZNRecord> createHelixPropertyStore(String dcName) {
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
  private boolean isClusterPresent(String dcName) {
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
  private void addStateModelDef() {
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
  private static class ReplicaComparator implements Comparator<ReplicaId> {
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
        diskInfo.put(DISK_STATE, disk.getState().toString());
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
  private void maybeAddCluster() {
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
   * Class for ideal state configs.
   */
  static class WagedIdealStateConfigFields {
    private int numReplicas;
    private int minActiveReplicas;

    /**
     * @return {@code numReplicas}
     */
    public int getNumReplicas() {
      return numReplicas;
    }

    /**
     * @return {@code minActiveReplicas}
     */
    public int getMinActiveReplicas() {
      return minActiveReplicas;
    }
  }

  /**
   * Class for cluster config fields.
   */
  static class WagedClusterConfigFields {
    private int evenness;
    private int lessMovement;
    private int partitionDiskWeightInGB;
    private int maxPartitionInTransition;
    private int delayRebalanceTimeInSecs;

    /**
     * @return evenness of cluster
     */
    public int getEvenness() {
      return evenness;
    }

    /**
     * @return stickyness of cluster
     */
    public int getLessMovement() {
      return lessMovement;
    }

    /**
     * @return disk capacity in GB
     */
    public int getPartitionDiskWeightInGB() {
      return partitionDiskWeightInGB;
    }

    public int getMaxPartitionInTransition() {
      return maxPartitionInTransition;
    }

    public int getDelayRebalanceTimeInSecs() {
      return delayRebalanceTimeInSecs;
    }
  }

  /**
   * Class for Full-auto waged helix configs
   */
  public static class WagedHelixConfig {
    private WagedIdealStateConfigFields wagedIdealStateConfigFields;
    private WagedClusterConfigFields wagedClusterConfigFields;

    /**
     * @return {@code wagedIdealStateConfigFields}
     */
    public WagedIdealStateConfigFields getIdealStateConfigFields() {
      return wagedIdealStateConfigFields;
    }

    /**
     * @return {@code wagedClusterConfigFields}
     */
    public WagedClusterConfigFields getClusterConfigFields() {
      return wagedClusterConfigFields;
    }
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
   * Get sealed partitions from Helix cluster.
   * @return a set of sealed partitions across all DCs.
   */
  private Set<String> getSealedPartitionsInHelixCluster() throws Exception {
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
  private void getSealedPartitionsInDc(Datacenter dc, Map<String, Set<String>> dcToSealedPartitions,
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
      ensureOrThrow(isClusterPresent(dc.getName()),
          "Cluster not found in ZK " + dataCenterToZkAddress.get(dc.getName()));
      Utils.newThread(() -> {
        try {
          verifyResourcesAndPartitionEquivalencyInDc(dc, clusterName, partitionLayout);
          verifyDataNodeAndDiskEquivalencyInDc(dc, clusterName, partitionLayout);
          maybeVerifyResourcesAreFullAutoCompatible(dc.getName(), clusterName);
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
   * If the resources are reconstructed to be FULL_AUTO compatible, then make sure that each instance only has partitions
   * from one resource.
   * @param dcName the datacenter whose information is to be verified.
   * @param clusterName the cluster to be verified.
   */
  private boolean maybeVerifyResourcesAreFullAutoCompatible(String dcName, String clusterName) {
    HelixAdmin admin = adminForDc.get(dcName);
    List<String> resourceNames = admin.getResourcesInCluster(clusterName);
    if (resourceNames == null || resourceNames.isEmpty()) {
      info(
          "[{}] There is no resource found for this cluster {}, max instance in one resource is {}, resource should {} be full auto compatible",
          dcName.toUpperCase(), clusterName, maxInstancesInOneResourceForFullAuto,
          maxInstancesInOneResourceForFullAuto > 0 ? "" : "NOT");
      return maxInstancesInOneResourceForFullAuto > 0;
    }
    boolean allResourceFullAutoCompatible = resourceNames.stream()
        .filter(rn -> rn.matches("\\d+"))
        .mapToInt(Integer::parseInt)
        .allMatch(i -> i >= FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER);
    boolean allResourceNotFullAutoCompatible = resourceNames.stream()
        .filter(rn -> rn.matches("\\d+"))
        .mapToInt(Integer::parseInt)
        .allMatch(i -> i < FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER);
    ensureOrThrow(allResourceFullAutoCompatible || allResourceNotFullAutoCompatible,
        "Resource has to be all greater than " + FULL_AUTO_COMPATIBLE_RESOURCE_NAME_START_NUMBER
            + " or all less than it");
    if (!allResourceFullAutoCompatible) {
      info("[{}] Resources are not FULL_AUTO compatible in cluster {}", dcName.toUpperCase(), clusterName);
      return false;
    }
    if (maxInstancesInOneResourceForFullAuto <= 0) {
      warning("****************************************");
      warning(
          "[{}] Cluster {} is in FULL_AUTO compatible mode, but max instances in one resource [{}] is not a valid number for FULL_AUTO",
          dcName.toUpperCase(), clusterName, maxInstancesInOneResourceForFullAuto);
      warning("Will not verify resources");
      return false;
    }
    SortedMap<Integer, Set<String>> resourceNameToInstances = new TreeMap<>();
    Map<String, Set<Integer>> instanceNameToResources = new HashMap<>();
    for (String resourceName : admin.getResourcesInCluster(clusterName)) {
      if (!resourceName.matches("\\d+")) {
        info("[{}] Ignoring resource {} as it is not part of the cluster map", dcName.toUpperCase(), resourceName);
        continue;
      }
      Integer resourceId = Integer.parseInt(resourceName);
      resourceNameToInstances.put(resourceId, new HashSet<>());
      IdealState resourceIS = admin.getResourceIdealState(clusterName, resourceName);
      Set<String> resourcePartitions = resourceIS.getPartitionSet();
      for (String resourcePartition : resourcePartitions) {
        Set<String> partitionInstanceSet = resourceIS.getInstanceSet(resourcePartition);
        partitionInstanceSet.forEach(
            in -> instanceNameToResources.computeIfAbsent(in, k -> new HashSet<>()).add(resourceId));
        resourceNameToInstances.get(resourceId).addAll(partitionInstanceSet);
      }
    }
    // Now verify
    // 1. all instances should only have one resource
    // 2. all resources except the last one, should have same numbers of instances.
    for (Map.Entry<String, Set<Integer>> ent : instanceNameToResources.entrySet()) {
      ensureOrThrow(ent.getValue().size() == 1,
          "Instances " + ent.getKey() + " has more than one resources, it's not FULL_AUTO compatible:"
              + ent.getValue());
    }
    Integer lastResourceId = resourceNameToInstances.lastKey();
    for (Map.Entry<Integer, Set<String>> ent : resourceNameToInstances.entrySet()) {
      if (ent.getKey().equals(lastResourceId)) {
        ensureOrThrow(maxInstancesInOneResourceForFullAuto >= ent.getValue().size(),
            "Resource " + ent.getKey() + " has " + ent.getValue().size()
                + " instances, but we are expecting less than or equal to " + maxInstancesInOneResourceForFullAuto
                + ": " + ent.getValue());
      } else {
        ensureOrThrow(maxInstancesInOneResourceForFullAuto == ent.getValue().size(),
            "Resource " + ent.getKey() + " has " + ent.getValue().size() + " instances, but we are expecting "
                + maxInstancesInOneResourceForFullAuto + ": " + ent.getValue());
      }
    }
    info("[{}] Successfully verified resources are FULL_AUTO compatible for cluster {}", dcName.toUpperCase(),
        clusterName);
    return true;
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
      info("[{}] resource {} has {} partitions in cluster {}", dcName.toUpperCase(), resourceName,
          resourcePartitions.size(), clusterName);
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

  /**
   * Log in INFO mode
   * @param format format
   * @param arguments arguments
   */
  private static void info(String format, Object... arguments) {
    logger.info(format, arguments);
  }

  /**
   * Log in WARN mode
   * @param format format
   * @param arguments arguments
   */
  private static void warning(String format, Object... arguments) {
    logger.warn(format, arguments);
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


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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.util.Strings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An implementation of {@link ClusterMap} that makes use of Helix to dynamically manage the cluster information.
 *
 * @see <a href="http://helix.apache.org">http://helix.apache.org</a>
 */
public class HelixClusterManager implements ClusterMap {
  private static final Logger logger = LoggerFactory.getLogger(HelixClusterManager.class);
  private static final int DEFAULT_NUM_REPLICAS = 3;
  private final String clusterName;
  private final String selfInstanceName;
  private final MetricRegistry metricRegistry;
  private final ClusterMapConfig clusterMapConfig;
  private final Map<String, DcInfo> dcToDcInfo = new HashMap<>();
  private final Map<Byte, String> dcIdToDcName = new HashMap<>();
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<AmbryDataNode>> dcToNodes = new ConcurrentHashMap<>();
  // A map whose key is ambry datanode and value is a map of partitionId to corresponding replica associated with this datanode
  private final ConcurrentHashMap<AmbryDataNode, ConcurrentHashMap<String, AmbryReplica>> ambryDataNodeToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryDisk>> ambryDataNodeToAmbryDisks = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Set<String>>> resourceNameToPartitionByDc = new ConcurrentHashMap<>();
  private final Map<String, ConcurrentHashMap<String, Set<String>>> partitionToResourceNameByDc =
      new ConcurrentHashMap<>();
  private final Map<String, ConcurrentHashMap<String, String>> partitionToDuplicateResourceNameByDc =
      new ConcurrentHashMap<>();
  private final Map<String, AtomicReference<RoutingTableSnapshot>> dcToRoutingTableSnapshotRef =
      new ConcurrentHashMap<>();
  private final Map<String, Map<String, ExternalView>> dcToResourceToExternalView = new ConcurrentHashMap<>();
  // Routing table snapshot reference used in aggregated cluster view.
  private final AtomicReference<RoutingTableSnapshot> globalRoutingTableSnapshotRef = new AtomicReference<>();
  private final Map<String, ExternalView> globalResourceToExternalView = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AmbryDataNode> instanceNameToAmbryDataNode = new ConcurrentHashMap<>();
  private final AtomicLong errorCount = new AtomicLong(0);
  private final AtomicLong clusterWideRawCapacityBytes = new AtomicLong(0);
  private final AtomicLong clusterWideAllocatedRawCapacityBytes = new AtomicLong(0);
  private final AtomicLong clusterWideAllocatedUsableCapacityBytes = new AtomicLong(0);
  private final HelixClusterManagerQueryHelper helixClusterManagerQueryHelper;
  private final byte localDatacenterId;
  private final AtomicLong sealedStateChangeCounter = new AtomicLong(0);
  private final PartitionSelectionHelper partitionSelectionHelper;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap = new HashMap<>();
  private final Map<String, ReplicaId> bootstrapReplicas = new ConcurrentHashMap<>();
  private final Map<String, Set<DiskId>> disksAttemptedForBootstrap = new ConcurrentHashMap<>();
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStoreInLocalDc = null;
  private HelixAdmin localHelixAdmin = null;
  // The current xid currently does not change after instantiation. This can change in the future, allowing the cluster
  // manager to dynamically incorporate newer changes in the cluster. This variable is atomic so that the gauge metric
  // reflects the current value.
  private final AtomicLong currentXid;
  final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private HelixAggregatedViewClusterInfo helixAggregatedViewClusterInfo = null;

  // The map from resource name to resource config, This is only used in FULL AUTO. This map is not going to be updated
  // if the ResourceConfig is updated, but we are only using default replica capacity from the ResourceConfig. So if you
  // update the default replica capacity in ResourceConfig, you have to restart the process for this map to get updated.
  // And this map doesn't store any resources from other datacenter.
  //
  // We probably only need to fetch one resource config, which this data node belongs to (This data node has to be a server node).
  private final ConcurrentHashMap<String, ResourceConfig> resourceConfigs = new ConcurrentHashMap<>();
  // This is a map from partition name to disk weight, for those partitions that don't default disk weight.
  // We would only get the partitions from the resources the current host belongs to. This map would be empty in ambry-frontend.
  private final ConcurrentHashMap<String, Integer> diskWeightForPartitions = new ConcurrentHashMap<>();
  private final AtomicInteger partitionDefaultDiskWeight = new AtomicInteger(0);

  // instance name to tag mapping and the reverse mapping from tag to instance name, these two maps
  // only contains the instances from local datacenter
  private final ConcurrentHashMap<String, InstanceConfig> instanceNameToInstanceConfig = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<String>> tagToInstanceNames = new ConcurrentHashMap<>();
  private final Map<String, ConcurrentHashMap<String, ResourceProperty>> dcToTagToResourceProperty =
      new ConcurrentHashMap<>();
  private final Map<String, ConcurrentHashMap<String, String>> dcToResourceNameToTag = new ConcurrentHashMap<>();
  private final AtomicBoolean localDataNodeInFullAuto = new AtomicBoolean(false);

  /**
   * Instantiate a HelixClusterManager.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this manager.
   * @param instanceName the String representation of the instance associated with this manager.
   * @param helixFactory the factory class to construct and get a reference to a {@link HelixManager}.
   * @param metricRegistry the registry of metric instances associated with this manager.
   * @throws IOException if there is an error in parsing the clusterMapConfig or in connecting with the associated
   *                     remote Zookeeper services.
   */
  public HelixClusterManager(ClusterMapConfig clusterMapConfig, String instanceName, HelixFactory helixFactory,
      MetricRegistry metricRegistry) throws IOException {
    this.clusterMapConfig = clusterMapConfig;
    currentXid = new AtomicLong(clusterMapConfig.clustermapCurrentXid);
    this.metricRegistry = metricRegistry;
    clusterName = clusterMapConfig.clusterMapClusterName;
    selfInstanceName = instanceName;
    helixClusterManagerQueryHelper = new HelixClusterManagerQueryHelper();
    helixClusterManagerMetrics = new HelixClusterManagerMetrics(metricRegistry, helixClusterManagerQueryHelper);
    Map<String, DcZkInfo> dataCenterToZkAddress = null;
    HelixManager localManager = null;
    Map<String, Exception> initializationFailureMap = new HashMap<>();
    try {
      dataCenterToZkAddress = parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings);
      // Make sure the HelixManager of local datacenter gets connected first and partitionOverrideInfoMap use PropertyStore
      // in local DC for initialization.
      localManager = initializeHelixManagerAndPropertyStoreInLocalDC(dataCenterToZkAddress, instanceName, helixFactory);
    } catch (Exception e) {
      initializationFailureMap.putIfAbsent(clusterMapConfig.clusterMapDatacenterName, e);
    }
    if (initializationFailureMap.get(clusterMapConfig.clusterMapDatacenterName) == null) {
      DataNodeConfigSourceMetrics dataNodeConfigSourceMetrics = new DataNodeConfigSourceMetrics(metricRegistry);
      if (clusterMapConfig.clusterMapUseAggregatedView) {
        try {
          HelixAggregatedViewClusterInitializer helixAggregatedViewClusterInitializer =
              new HelixAggregatedViewClusterInitializer(clusterMapConfig, dataCenterToZkAddress, helixFactory,
                  selfInstanceName, dataNodeConfigSourceMetrics, this);
          helixAggregatedViewClusterInfo = helixAggregatedViewClusterInitializer.start();
          // Populate dcToDCInfo and dcIdToDcName maps for all DCs if cluster initialization succeeds. The only thing to
          // note is DcInfo would have empty clusterChangeHandler associated with it.
          for (DcZkInfo dcZkInfo : dataCenterToZkAddress.values()) {
            dcToDcInfo.put(dcZkInfo.getDcName(), new DcInfo(dcZkInfo.getDcName(), dcZkInfo, null));
            dcIdToDcName.put(dcZkInfo.getDcId(), dcZkInfo.getDcName());
          }
        } catch (Exception e) {
          initializationFailureMap.putIfAbsent(clusterMapConfig.clusterMapDatacenterName, e);
        }
      } else {
        List<HelixDatacenterInitializer> helixDatacenterInitializers = new ArrayList<>();
        for (DcZkInfo dcZkInfo : dataCenterToZkAddress.values()) {
          // Initialize from every remote datacenter in a separate thread to speed things up.
          HelixDatacenterInitializer helixDatacenterInitializer =
              new HelixDatacenterInitializer(clusterMapConfig, localManager, helixFactory, dcZkInfo, selfInstanceName,
                  dataNodeConfigSourceMetrics, this);
          helixDatacenterInitializer.start();
          helixDatacenterInitializers.add(helixDatacenterInitializer);
        }
        for (HelixDatacenterInitializer helixDatacenterInitializer : helixDatacenterInitializers) {
          try {
            DcInfo dcInfo = helixDatacenterInitializer.join();
            dcToDcInfo.put(dcInfo.dcName, dcInfo);
            dcIdToDcName.put(dcInfo.dcZkInfo.getDcId(), dcInfo.dcName);
          } catch (Exception e) {
            initializationFailureMap.putIfAbsent(helixDatacenterInitializer.getDcName(), e);
          }
        }
      }
    }
    Exception blockingException = initializationFailureMap.get(clusterMapConfig.clusterMapDatacenterName);
    if (blockingException != null) {
      helixClusterManagerMetrics.initializeInstantiationMetric(false,
          initializationFailureMap.values().stream().filter(Objects::nonNull).count());
      close();
      throw new IOException(
          "Encountered startup blocking exception while parsing json, connecting or initializing in the local DC",
          blockingException);
    } else {
      // resolve the status of all partitions before completing initialization
      for (AmbryPartition partition : partitionMap.values()) {
        partition.resolvePartitionState();
      }
      helixClusterManagerMetrics.initializeInstantiationMetric(true,
          initializationFailureMap.values().stream().filter(Objects::nonNull).count());
      helixClusterManagerMetrics.initializeXidMetric(currentXid);
      helixClusterManagerMetrics.initializeDatacenterMetrics();
      helixClusterManagerMetrics.initializeDataNodeMetrics();
      helixClusterManagerMetrics.initializeDiskMetrics();
      helixClusterManagerMetrics.initializePartitionMetrics();
      helixClusterManagerMetrics.initializeCapacityMetrics();
    }
    localDatacenterId = dcToDcInfo.get(clusterMapConfig.clusterMapDatacenterName).dcZkInfo.getDcId();
    partitionSelectionHelper =
        new PartitionSelectionHelper(helixClusterManagerQueryHelper, clusterMapConfig.clusterMapDatacenterName,
            clusterMapConfig.clustermapWritablePartitionMinReplicaCount,
            clusterMapConfig.clusterMapDefaultPartitionClass, helixClusterManagerMetrics);
    // register partition selection helper as a listener of cluster map changes.
    registerClusterMapListener(partitionSelectionHelper);
  }

  /**
   * Initialize HelixManager in local datacenter and complete subscription of HelixPropertyStore to listen for
   * PartitionOverride zNode. This needs to happen before other datacenters are initialized so that any partition
   * overrides can be properly honored.
   * @param dataCenterToZkAddress the map mapping each datacenter to its corresponding ZkAddress.
   * @param instanceName the String representation of the instance associated with this manager.
   * @param helixFactory the factory class to construct and get a reference to a {@link HelixManager}.
   * @return the HelixManager of local datacenter, or {@code null} if the local datacenter is
   *         {@link ReplicaType#CLOUD_BACKED}, as we currently do not support getting cluster state from Helix for cloud
   *         datacenters.
   * @throws Exception
   */
  private HelixManager initializeHelixManagerAndPropertyStoreInLocalDC(Map<String, DcZkInfo> dataCenterToZkAddress,
      String instanceName, HelixFactory helixFactory) throws Exception {
    DcZkInfo dcZkInfo = dataCenterToZkAddress.get(clusterMapConfig.clusterMapDatacenterName);
    if (dcZkInfo.getReplicaType() == ReplicaType.CLOUD_BACKED) {
      return null;
    }
    // For now, the first ZK endpoint (if there are more than one endpoints) will be adopted by default. Note that, Ambry
    // doesn't support multiple HelixClusterManagers(spectators) on same node.
    String zkConnectStr = dcZkInfo.getZkConnectStrs().get(0);
    HelixManager manager =
        helixFactory.getZkHelixManagerAndConnect(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
    helixPropertyStoreInLocalDc = manager.getHelixPropertyStore();
    logger.info("HelixPropertyStore from local datacenter {} is: {}", dcZkInfo.getDcName(),
        helixPropertyStoreInLocalDc);
    localHelixAdmin = manager.getClusterManagmentTool();
    logger.info("HelixAdmin from local datacenter {} is: {}", dcZkInfo.getDcName(), localHelixAdmin);
    IZkDataListener dataListener = new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) {
        logger.info("Received data change notification for: {}", dataPath);
      }

      @Override
      public void handleDataDeleted(String dataPath) {
        logger.info("Received data delete notification for: {}", dataPath);
      }
    };
    logger.info("Subscribing data listener to HelixPropertyStore.");
    helixPropertyStoreInLocalDc.subscribeDataChanges(PARTITION_OVERRIDE_ZNODE_PATH, dataListener);
    logger.info("Getting PartitionOverride ZNRecord from HelixPropertyStore");
    ZNRecord zNRecord = helixPropertyStoreInLocalDc.get(PARTITION_OVERRIDE_ZNODE_PATH, null, AccessOption.PERSISTENT);
    if (clusterMapConfig.clusterMapEnablePartitionOverride) {
      if (zNRecord != null) {
        partitionOverrideInfoMap.putAll(zNRecord.getMapFields());
        logger.info("partitionOverrideInfoMap is initialized!");
      } else {
        logger.warn("ZNRecord from HelixPropertyStore is NULL, the partitionOverrideInfoMap is empty.");
      }
    }
    return manager;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dcToDcInfo.containsKey(datacenterName);
  }

  @Override
  public byte getLocalDatacenterId() {
    return localDatacenterId;
  }

  @Override
  public String getDatacenterName(byte id) {
    return dcIdToDcName.get(id);
  }

  @Override
  public AmbryDataNode getDataNodeId(String hostname, int port) {
    return instanceNameToAmbryDataNode.get(getInstanceName(hostname, port));
  }

  @Override
  public List<AmbryReplica> getReplicaIds(DataNodeId dataNodeId) {
    if (!(dataNodeId instanceof AmbryDataNode)) {
      throw new IllegalArgumentException("Incompatible type passed in");
    }
    ConcurrentHashMap<String, AmbryReplica> partitionToReplicaMap = ambryDataNodeToAmbryReplicas.get(dataNodeId);
    return partitionToReplicaMap != null ? new ArrayList<>(partitionToReplicaMap.values()) : Collections.emptyList();
  }

  @Override
  public List<AmbryDataNode> getDataNodeIds() {
    return new ArrayList<>(instanceNameToAmbryDataNode.values());
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    if (replicaId instanceof AmbryReplica) {
      AmbryReplica replica = (AmbryReplica) replicaId;
      switch (event) {
        case Node_Response:
          replica.getDataNodeId().onNodeResponse();
          break;
        case Node_Timeout:
          replica.getDataNodeId().onNodeTimeout();
          break;
        case Disk_Error:
          if (replica.getReplicaType() == ReplicaType.DISK_BACKED) {
            replica.getDiskId().onDiskError();
          }
          break;
        case Disk_Ok:
          if (replica.getReplicaType() == ReplicaType.DISK_BACKED) {
            replica.getDiskId().onDiskOk();
          }
          break;
        case Partition_ReadOnly:
          replica.getPartitionId().onPartitionReadOnly();
          break;
        case Replica_Unavailable:
          replica.onReplicaUnavailable();
          break;
        case Replica_Available:
          replica.onReplicaResponse();
          break;
      }
    }
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(IMPLEMENTATION, HelixClusterManager.class.getName());
    snapshot.put(CLUSTER_NAME, clusterName);
    snapshot.put(TIMESTAMP_MS, SystemTime.getInstance().milliseconds());
    JSONArray datacentersJsonArray = new JSONArray();
    dcIdToDcName.forEach((dcId, dcName) -> {
      JSONObject data = new JSONObject();
      data.put(DATACENTER_NAME, dcName);
      data.put(DATACENTER_ID, dcId);
      JSONArray datanodesInDc = new JSONArray();
      dcToNodes.get(dcName).forEach(node -> datanodesInDc.put(node.getSnapshot()));
      data.put(DATACENTER_NODES, datanodesInDc);
      datacentersJsonArray.put(data);
    });
    snapshot.put(DATACENTERS, datacentersJsonArray);
    JSONArray partitionsJsonArray = new JSONArray();
    getAllPartitionIds(null).forEach(partitionId -> partitionsJsonArray.put(partitionId.getSnapshot()));
    snapshot.put(PARTITIONS, partitionsJsonArray);
    return snapshot;
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    byte[] partitionBytes = AmbryPartition.readPartitionBytesFromStream(stream);
    AmbryPartition partition = partitionMap.get(ByteBuffer.wrap(partitionBytes));
    if (partition == null) {
      throw new IOException("Partition id from stream is unknown (not present in current clustermap)");
    }
    return partition;
  }

  @Override
  public PartitionId getPartitionIdByName(String partitionIdStr) {
    return partitionNameToAmbryPartition.get(partitionIdStr);
  }

  @Override
  public List<PartitionId> getWritablePartitionIds(String partitionClass) {
    return partitionSelectionHelper.getWritablePartitions(partitionClass);
  }

  @Override
  public List<PartitionId> getFullyWritablePartitionIds(String partitionClass) {
    return partitionSelectionHelper.getFullyWritablePartitions(partitionClass);
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    return partitionSelectionHelper.getRandomWritablePartition(partitionClass, partitionsToExclude);
  }

  @Override
  public PartitionId getRandomFullyWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    return partitionSelectionHelper.getRandomFullyWritablePartition(partitionClass, partitionsToExclude);
  }

  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    return partitionSelectionHelper.getPartitions(partitionClass);
  }

  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    String instanceName = getInstanceName(dataNodeId.getHostname(), dataNodeId.getPort());
    try {
      ReplicaId bootstrapReplica =
          isDataNodeInFullAutoMode(dataNodeId, true) ? getBootstrapReplicaInFullAuto(partitionIdStr, dataNodeId)
              : getBootstrapReplicaInSemiAuto(partitionIdStr, dataNodeId);
      // For now this method is only called by server which new replica will be added to. So if datanode equals to current
      // node, we temporarily add this into a map (because we don't know whether store addition in storage manager
      // succeeds or not). After store addition succeeds, current node is supposed to update InstanceConfig and will
      // receive notification from Helix afterwards. At that time, dynamic cluster change handler will move replica from
      // this map to clustermap related data structures that can be queried by other components.
      if (bootstrapReplica != null && instanceName.equals(selfInstanceName)) {
        // Note that this method might be called by several state transition threads concurrently.
        bootstrapReplicas.put(partitionIdStr, bootstrapReplica);
        // Add the selected disk so that it is not picked
        disksAttemptedForBootstrap.computeIfAbsent(partitionIdStr, k -> new HashSet<>())
            .add(bootstrapReplica.getDiskId());
      }
      return bootstrapReplica;
    } catch (Exception e) {
      logger.error("Failed to create bootstrap replica for partition {} on {} due to exception: ", partitionIdStr,
          instanceName, e);
      return null;
    }
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    if (clusterMapConfig.clusterMapUseAggregatedView) {
      helixAggregatedViewClusterInfo.clusterChangeHandler.registerClusterMapListener(clusterMapChangeListener);
    } else {
      for (DcInfo helixDcInfo : dcToDcInfo.values()) {
        helixDcInfo.clusterChangeHandler.registerClusterMapListener(clusterMapChangeListener);
      }
    }
  }

  /**
   * {@inheritDoc}
   * Notice that this method only works for data nodes from the same datacenter.
   */
  @Override
  public boolean isDataNodeInFullAutoMode(DataNodeId dataNodeId) {
    return isDataNodeInFullAutoMode(dataNodeId, false);
  }

  /**
   * Returns true if all the helix resources that this host belongs to are in Full Auto mode. If the
   * {@code checkIfMigratingFromSemiAutoToFullAuto} is true, this mode would also check if the data node is going
   * through FULL_AUTO migration from property store.
   *
   * @param dataNodeId                             The {@link DataNodeId} to check.
   * @param checkIfMigratingFromSemiAutoToFullAuto True to check property store if this data node id is in full auto
   *                                               migration and return true if that's the case.
   * @return {@code True} if all the helix resources that this host belongs to are in Full Auto. Else, returns
   * {@code False}
   */
  boolean isDataNodeInFullAutoMode(DataNodeId dataNodeId, boolean checkIfMigratingFromSemiAutoToFullAuto) {
    String instanceName = getInstanceName(dataNodeId.getHostname(), dataNodeId.getPort());
    if (instanceNameToAmbryDataNode.get(instanceName) == null) {
      throw new IllegalArgumentException("Instance " + instanceName + " doesn't exist");
    }
    String dcName = dataNodeId.getDatacenterName();
    if (!dcName.equals(clusterMapConfig.clusterMapDatacenterName)) {
      throw new IllegalArgumentException("Instance " + instanceName + " is from different datacenter");
    }
    List<String> tags = instanceNameToInstanceConfig.get(instanceName).getTags();
    if (tags == null || tags.isEmpty()) {
      logger.trace("DataNode {} doesn't have any tags", instanceName);
      return false;
    }
    logger.trace("Instance {} has tags {}", instanceName, tags);

    // Get list of resources in middle of migration to Full Auto
    Set<String> resourcesMigratingToFullAuto = new HashSet<>();
    ZNRecord record = helixPropertyStoreInLocalDc.get(FULL_AUTO_MIGRATION_ZNODE_PATH, null, AccessOption.PERSISTENT);
    if (record != null && record.getListField(RESOURCES_STR) != null) {
      resourcesMigratingToFullAuto.addAll(record.getListField(RESOURCES_STR));
    }

    // Check if all the resources that this host belongs to are in FULL_AUTO mode
    for (String tag : tags) {
      ResourceProperty property = dcToTagToResourceProperty.get(dcName).get(tag);
      if (property == null) {
        logger.trace("Datanode {} is in SEMI_AUTO since its resource tag has no property found {}", instanceName, tag);
        return false;
      }

      if (property.rebalanceMode.equals(IdealState.RebalanceMode.FULL_AUTO)) {
        continue;
      }

      // Resource is not in FULL_AUTO. Check if we are in middle of migration to FULL_AUTO and are falling back to SEMI_AUTO
      if (checkIfMigratingFromSemiAutoToFullAuto) {
        if (!resourcesMigratingToFullAuto.contains(property.name)) {
          logger.trace("Datanode {} is in SEMI_AUTO since its resource with tag {} is in semi-auto", instanceName, tag);
          return false;
        }
      } else {
        // Resource is neither in FULL_AUTO nor is falling back from FULL_AUTO to SEMI_AUTO. Return false.
        logger.trace("Datanode {} is in SEMI_AUTO since its resource with tag {} is in semi-auto", instanceName, tag);
        return false;
      }
    }

    // All resources that this host belongs are either in FULL_AUTO or are falling back from FULL_AUTO to SEMI_AUTO.
    // Consider this node to be in FULL_AUTO
    logger.trace("Datanode {} is in FULL_AUTO", instanceName);
    return true;
  }

  @Override
  public boolean hasEnoughEligibleReplicasAvailableForPut(PartitionId partitionId, int requiredEligibleReplicaCount,
      boolean checkLocalDcOnly) {
    return partitionSelectionHelper.hasEnoughEligibleReplicasAvailableForPut(partitionId, requiredEligibleReplicaCount,
        checkLocalDcOnly);
  }

  /**
   * Disconnect from the HelixManagers associated with each and every datacenter.
   */
  @Override
  public void close() {
    for (DcInfo dcInfo : dcToDcInfo.values()) {
      dcInfo.close();
    }
    dcToDcInfo.clear();

    if (clusterMapConfig.clusterMapUseAggregatedView && helixAggregatedViewClusterInfo != null) {
      helixAggregatedViewClusterInfo.close();
    }
    if (helixPropertyStoreInLocalDc != null) {
      helixPropertyStoreInLocalDc.stop();
    }

    if (localHelixAdmin != null) {
      localHelixAdmin.close();
    }
  }

  /**
   * Return the unique {@link AmbryReplica} for a {@link AmbryPartition} on a {@link AmbryDataNode}.
   * @param dataNodeId the {@link DataNodeId} on which the ambry replica resides.
   * @param partitionString the partition id string associated with the {@link AmbryPartition}.
   * @return the {@link AmbryReplica} associated with the given parameters.
   */
  AmbryReplica getReplicaForPartitionOnNode(DataNodeId dataNodeId, String partitionString) {
    // Note: partitionString here is now from partitionId.toPathString()
    AmbryDataNode ambryDataNode = getDataNodeId(dataNodeId.getHostname(), dataNodeId.getPort());
    return ambryDataNodeToAmbryReplicas.getOrDefault(ambryDataNode, new ConcurrentHashMap<>()).get(partitionString);
  }

  /**
   * @return the count of errors encountered by the Cluster Manager.
   */
  long getErrorCount() {
    return errorCount.get();
  }

  /**
   * @return a map of datacenter names to {@link DcInfo}
   */
  Map<String, DcInfo> getDcInfosMap() {
    return Collections.unmodifiableMap(dcToDcInfo);
  }

  /**
   * Exposed for testing
   * @return a map of partition to its corresponding resource grouped by data center
   */
  Map<String, Map<String, Set<String>>> getPartitionToResourceMapByDC() {
    return Collections.unmodifiableMap(partitionToResourceNameByDc);
  }

  /**
   * Exposed for testing only.
   * @return
   */
  Map<String, Map<String, String>> getPartitionToDuplicateResourceMapByDC() {
    return Collections.unmodifiableMap(partitionToDuplicateResourceNameByDc);
  }

  /**
   * Exposed for testing
   * @return a map of data center to its data nodes
   */
  Map<String, Set<AmbryDataNode>> getDcToDataNodesMap() {
    return Collections.unmodifiableMap(dcToNodes);
  }

  /**
   * Exposed for testing
   * @return a map of data center to its {@link RoutingTableSnapshot}
   */
  Map<String, RoutingTableSnapshot> getRoutingTableSnapshots() {
    Map<String, RoutingTableSnapshot> dcToRoutingTableSnapshot = new ConcurrentHashMap<>();
    if (clusterMapConfig.clusterMapUseAggregatedView) {
      dcToRoutingTableSnapshot.put(clusterMapConfig.clusterMapDatacenterName, globalRoutingTableSnapshotRef.get());
    } else {
      for (DcInfo dcInfo : dcToDcInfo.values()) {
        dcToRoutingTableSnapshot.put(dcInfo.dcName, dcToRoutingTableSnapshotRef.get(dcInfo.dcName).get());
      }
    }
    return Collections.unmodifiableMap(dcToRoutingTableSnapshot);
  }

  /**
   * Exposed for testing
   * @return a snapshot of current bootstrap replica map
   */
  Map<String, ReplicaId> getBootstrapReplicaMap() {
    return Collections.unmodifiableMap(bootstrapReplicas);
  }

  /**
   * Exposed for testing
   */
  void clearBootstrapDiskSelectionMap() {
    disksAttemptedForBootstrap.clear();
  }

  /**
   * Exposed for testing
   * @param dcName data center name
   * @return {@link HelixClusterChangeHandler} that handles cluster changes for a given data center. If aggregated view
   * is enabled, same handler is used for all data centers.
   */
  HelixClusterChangeHandler getHelixClusterChangeHandler(String dcName) {
    if (clusterMapConfig.clusterMapUseAggregatedView) {
      return helixAggregatedViewClusterInfo.clusterChangeHandler;
    } else {
      return dcToDcInfo.get(dcName).clusterChangeHandler;
    }
  }

  /**
   * @return {@link HelixClusterManagerQueryHelper} associated with this cluster manager.
   */
  HelixClusterManagerQueryHelper getManagerQueryHelper() {
    return helixClusterManagerQueryHelper;
  }

  /**
   * @return localDC's zk connect string.
   */
  public String getLocalDcZkConnectString() {
    return dcToDcInfo.get(clusterMapConfig.clusterMapDatacenterName).dcZkInfo.getZkConnectStrs().get(0);
  }

  public Set<String> getResourceForPartitionInLocalDc(String partitionName) {
    return partitionToResourceNameByDc.get(clusterMapConfig.clusterMapDatacenterName).get(partitionName);
  }

  /**
   * Add partition if it's not present in cluster-wide partition map and also update cluster-wide allocated usable
   * capacity. If the partition already exists, skip addition and return current partition.
   * @param partition the {@link AmbryPartition} to add (if not present)
   * @param capacityBytes the capacity of partition in bytes
   * @return the current {@link AmbryPartition} present in the map.
   */
  private AmbryPartition addPartitionIfAbsent(AmbryPartition partition, long capacityBytes) {
    AmbryPartition currentPartition = partitionNameToAmbryPartition.putIfAbsent(partition.toPathString(), partition);
    if (currentPartition == null) {
      // this means the map previously didn't contain this partition and passed-in partition is successfully added
      // into the map
      currentPartition = partition;
      // it doesn't really need to synchronize this method. "partitionNameToAmbryPartition" guarantees each thread
      // will get same instance of ambry partition. The first one that succeeds adding partition into
      // "partitionNameToAmbryPartition" will update "partitionMap".
      partitionMap.put(ByteBuffer.wrap(currentPartition.getBytes()), currentPartition);
      // update cluster-wide capacity
      clusterWideAllocatedUsableCapacityBytes.getAndAdd(capacityBytes);
    }
    ambryPartitionToAmbryReplicas.putIfAbsent(currentPartition, ConcurrentHashMap.newKeySet());
    return currentPartition;
  }

  /**
   * Add a list of {@link AmbryReplica} to given {@link AmbryPartition} and update cluster-wide capacity stats
   * @param partition the {@link AmbryPartition} which replicas should be added to.
   * @param replicas list of {@link AmbryReplica} to be added.
   */
  private void addReplicasToPartition(AmbryPartition partition, List<AmbryReplica> replicas) {
    AmbryPartition currentPartition = addPartitionIfAbsent(partition, replicas.get(0).getCapacityInBytes());
    ambryPartitionToAmbryReplicas.compute(currentPartition, (k, v) -> {
      // calling addPartitionIfAbsent guarantees that v is not null
      v.addAll(replicas);
      return v;
    });
    clusterWideAllocatedRawCapacityBytes.getAndAdd(replicas.get(0).getCapacityInBytes() * replicas.size());
  }

  /**
   * Remove {@link AmbryReplica}(s) from given {@link AmbryPartition} and update cluster-wide capacity stats
   * @param partition the {@link AmbryPartition} which replicas should be removed from.
   * @param replicas list of {@link AmbryReplica} to be removed.
   */
  private void removeReplicasFromPartition(AmbryPartition partition, List<AmbryReplica> replicas) {
    ambryPartitionToAmbryReplicas.computeIfPresent(partition, (k, v) -> {
      v.removeAll(replicas);
      clusterWideAllocatedRawCapacityBytes.getAndAdd(-1 * replicas.get(0).getCapacityInBytes() * replicas.size());
      return v;
    });
  }

  /**
   * Add cluster-wide raw capacity. This is called when new disk is created and added to cluster. We update
   * cluster-wide raw capacity by adding the disk capacity.
   * @param diskRawCapacityBytes raw disk capacity to be added.
   */
  private void addClusterWideRawCapacity(long diskRawCapacityBytes) {
    clusterWideRawCapacityBytes.getAndAdd(diskRawCapacityBytes);
  }

  /**
   * Pop out bootstrap replica (if any) on current instance. A bootstrap replica is a replica dynamically added to
   * current node at runtime.
   * @param partitionName the partition name of bootstrap replica.
   * @return bootstrap replica or {@code null} if not found.
   */
  private AmbryReplica fetchBootstrapReplica(String partitionName) {
    // Clear disks tried for bootstrapping this replica
    disksAttemptedForBootstrap.remove(partitionName);
    return (AmbryReplica) bootstrapReplicas.remove(partitionName);
  }

  /**
   * Get bootstrap replica in Helix semi auto mode.
   * To create bootstrap replica, {@link HelixClusterManager} needs to fetch replica info (i.e. capacity, mount path)
   * from Helix PropertyStore. This method looks up the ZNode in local datacenter and does some validation. Right now,
   * {@link HelixClusterManager} supports getting bootstrap replica of new partition but it doesn't support getting replica
   * residing on hosts that are not present in clustermap.
   * The ZNRecord of REPLICA_ADDITION_ZNODE has following format in mapFields.
   * <pre>
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
   * </pre>
   * In above example, two bootstrap replicas of partition[1] will be added to localhost1 and localhost2 respectively.
   * The host name is followed by mount path on which the bootstrap replica should be placed.
   */
  private ReplicaId getBootstrapReplicaInSemiAuto(String partitionIdStr, DataNodeId dataNodeId) {
    ReplicaId bootstrapReplica = null;
    logger.info("Getting ReplicaAddition ZNRecord from HelixPropertyStore in local DC.");
    ZNRecord zNRecord = helixPropertyStoreInLocalDc.get(REPLICA_ADDITION_ZNODE_PATH, null, AccessOption.PERSISTENT);
    if (zNRecord == null) {
      logger.warn("ZNRecord from HelixPropertyStore is NULL, partition to replicaInfo map doesn't exist.");
      return null;
    }
    String instanceName = getInstanceName(dataNodeId.getHostname(), dataNodeId.getPort());
    Map<String, Map<String, String>> partitionToReplicas = zNRecord.getMapFields();
    Map<String, String> replicaInfos = partitionToReplicas.get(partitionIdStr);
    if (replicaInfos == null || !replicaInfos.containsKey(instanceName)) {
      logger.warn("Partition {} or replica on host {} is not found in replica info map", partitionIdStr, instanceName);
      return null;
    }
    long replicaCapacity = Long.parseLong(replicaInfos.get(REPLICAS_CAPACITY_STR));
    String partitionClass = replicaInfos.get(PARTITION_CLASS_STR);
    AmbryPartition mappedPartition =
        new AmbryPartition(Long.parseLong(partitionIdStr), partitionClass, helixClusterManagerQueryHelper);
    AmbryPartition currentPartition =
        partitionNameToAmbryPartition.putIfAbsent(mappedPartition.toPathString(), mappedPartition);
    if (currentPartition == null) {
      logger.info("Partition {} is currently not present in cluster map, a new partition is created", partitionIdStr);
      currentPartition = mappedPartition;
    }
    // Check if data node or disk is in current cluster map, if not, set bootstrapReplica to null.
    AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instanceName);
    String mountPathAndDiskCapacityFromHelix = replicaInfos.get(instanceName);
    String[] segments = mountPathAndDiskCapacityFromHelix.split(DISK_CAPACITY_DELIM_STR);
    String mountPath = segments[0];
    String diskCapacityStr = segments.length >= 2 ? segments[1] : null;
    Set<AmbryDisk> disks = dataNode != null ? ambryDataNodeToAmbryDisks.get(dataNode) : null;
    Optional<AmbryDisk> potentialDisk =
        disks != null ? disks.stream().filter(d -> d.getMountPath().equals(mountPath)).findAny() : Optional.empty();
    if (potentialDisk.isPresent()) {
      try {
        AmbryDisk targetDisk = potentialDisk.get();
        if (diskCapacityStr != null) {
          // update disk capacity if bootstrap replica info contains disk capacity in bytes.
          targetDisk.setDiskCapacityInBytes(Long.parseLong(diskCapacityStr));
        } else {
          logger.info("Replica addition infos map doesn't contain disk capacity. Disk {} capacity {} is not changed",
              targetDisk, targetDisk.getRawCapacityInBytes());
        }
        // Update disk usage. However, this is just for tracking. We only check the disk usage in Full-auto mode.
        targetDisk.decreaseAvailableSpaceInBytes(replicaCapacity);
        // A bootstrap replica is always ReplicaSealedStatus#NOT_SEALED.
        bootstrapReplica = new AmbryServerReplica(clusterMapConfig, currentPartition, targetDisk, true, replicaCapacity,
            ReplicaSealStatus.NOT_SEALED);
        logger.info("Created bootstrap replica {} for Partition {}", bootstrapReplica, partitionIdStr);

      } catch (Exception e) {
        logger.error("Failed to create bootstrap replica for partition {} on {} due to exception: ", partitionIdStr,
            instanceName, e);
        bootstrapReplica = null;
      }
    } else {
      logger.error(
          "Either datanode or disk that associated with bootstrap replica is not found in cluster map. Cannot create the replica.");
    }
    return bootstrapReplica;
  }

  /**
   * Gets bootstrap replica in Helix full auto mode. Since helix can add multiple replicas simultaneously, synchronizing
   * this method to avoid over-allocating a disk.
   * @param partitionIdStr the partition id string
   * @param dataNodeId the {@link DataNodeId} on which bootstrap replica is placed
   * @return {@link ReplicaId} if there is a new replica satisfying given partition and data node. {@code null} otherwise.
   */
  private synchronized ReplicaId getBootstrapReplicaInFullAuto(String partitionIdStr, DataNodeId dataNodeId) {
    long replicaCapacity;
    AmbryPartition ambryPartition;
    if (!partitionNameToAmbryPartition.containsKey(partitionIdStr)) {
      ambryPartition =
          new AmbryPartition(Long.parseLong(partitionIdStr), clusterMapConfig.clusterMapDefaultPartitionClass,
              helixClusterManagerQueryHelper);
      // If the partition is new, we get the default replica capacity from config
      replicaCapacity = clusterMapConfig.clustermapDefaultReplicaCapacityInBytes;
      logger.info("Partition {} is currently not present in cluster map, creating first replica with capacity {} ",
          partitionIdStr, replicaCapacity);
    } else {
      ambryPartition = partitionNameToAmbryPartition.get(partitionIdStr);
      // For existing partitions, we should already have other replicas in the map
      replicaCapacity = ambryPartition.getReplicaIds().get(0).getCapacityInBytes();
      logger.info("Partition {} is already present in cluster map, creating replica with capacity {} ", partitionIdStr,
          replicaCapacity);
    }

    try {
      AmbryDisk disk = getDiskForBootstrapReplica((AmbryDataNode) dataNodeId, replicaCapacity, partitionIdStr);
      if (disk == null) {
        logger.error("Failed to create bootstrap replica for partition {} due to insufficient disk space",
            partitionIdStr);
        return null;
      }
      partitionNameToAmbryPartition.putIfAbsent(ambryPartition.toPathString(), ambryPartition);
      AmbryServerReplica replica = new AmbryServerReplica(clusterMapConfig, ambryPartition, disk, true, replicaCapacity,
          ReplicaSealStatus.NOT_SEALED);
      logger.info("Created bootstrap replica {} for Partition {}", replica, partitionIdStr);
      Thread currentThread = Thread.currentThread();
      // TODO: Remove this code after figuring out why ReplicaThread is calling this method.
      if (currentThread.getName().contains("ReplicaThread")) {
        logger.error("Method invocation in wrong thread {}", currentThread.getName(), new Exception(""));
      }
      disk.decreaseAvailableSpaceInBytes(replicaCapacity);
      return replica;
    } catch (Exception e) {
      logger.error("Failed to create bootstrap replica for partition {} on {} due to exception: ", partitionIdStr,
          dataNodeId, e);
      return null;
    }
  }

  /**
   * Return resource config from the cache if it's present, otherwise, fetch it from Helix.
   * @param resourceName The resource name
   * @param dcName The datacenter name
   * @return The {@link ResourceConfig} object. If there is no config for the given resource name, null would be returned.
   */
  private ResourceConfig getResourceConfig(String resourceName, String dcName) {
    if (!resourceConfigs.contains(resourceName)) {
      ConfigAccessor configAccessor;
      if (clusterMapConfig.clusterMapUseAggregatedView) {
        configAccessor = helixAggregatedViewClusterInfo.helixManager.getConfigAccessor();
      } else {
        configAccessor = ((HelixDcInfo) dcToDcInfo.get(dcName)).helixManager.getConfigAccessor();
      }
      logger.info("Fetching resource config for {}", resourceName);
      ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, resourceName);
      if (resourceConfig != null) {
        try {
          Map<String, Map<String, Integer>> partitionCapacityMap = resourceConfig.getPartitionCapacityMap();
          if (partitionCapacityMap != null && !partitionCapacityMap.isEmpty()) {
            partitionCapacityMap.forEach((partition, capacityMap) -> {
              if (capacityMap.containsKey(DISK_KEY)) {
                diskWeightForPartitions.put(partition, capacityMap.get(DISK_KEY));
              }
            });
          }
        } catch (Exception e) {
          logger.error("Failed to get partition capacity map from resource config for resource {} in dc {}",
              resourceName, dcName, e);
        }
      } else {
        // There is no config created for this resource, just make an empty one
        logger.info("No resource config found for {} in dc {}", resourceName, dcName);
        resourceConfig = new ResourceConfig(resourceName);
      }
      resourceConfigs.putIfAbsent(resourceName, resourceConfig);
    }
    return resourceConfigs.get(resourceName);
  }

  /**
   * Return partition's default disk weight. It gets the weight from a local cache, if the weight is not present, it
   * will fetch it from cluster config.
   * @return
   */
  private int getPartitionDefaultDiskWeight() {
    if (partitionDefaultDiskWeight.get() == 0) {
      ConfigAccessor configAccessor;
      if (clusterMapConfig.clusterMapUseAggregatedView) {
        configAccessor = helixAggregatedViewClusterInfo.helixManager.getConfigAccessor();
      } else {
        configAccessor =
            ((HelixDcInfo) dcToDcInfo.get(clusterMapConfig.clusterMapDatacenterName)).helixManager.getConfigAccessor();
      }
      logger.info("Fetching cluster config for {}", clusterName);
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      int defaultPartitionDiskWeight = clusterConfig.getDefaultPartitionWeightMap().get(DISK_KEY);
      logger.info("Default partition disk weight is {} for cluster {}", defaultPartitionDiskWeight, clusterName);
      partitionDefaultDiskWeight.compareAndSet(0, defaultPartitionDiskWeight);
    }
    return partitionDefaultDiskWeight.get();
  }

  /**
   * Get a disk with maximum available space for bootstrapping replica in Full auto mode. This method is synchronized
   * since it can be queried concurrently when multiple replicas are bootstrapped.
   * @param dataNode        the {@link DataNodeId} on which disk is needed
   * @param replicaCapacity the capacity of the replica in bytes
   * @param partitionName partition name
   * @return {@link AmbryDisk} which has maximum available or free capacity. If none of the disks have free space,
   * returns null.
   */
  private AmbryDisk getDiskForBootstrapReplica(AmbryDataNode dataNode, long replicaCapacity, String partitionName) {
    Set<AmbryDisk> disks = ambryDataNodeToAmbryDisks.get(dataNode);
    List<AmbryDisk> potentialDisks = new ArrayList<>();
    long maxAvailableDiskSpace = 0;
    for (AmbryDisk disk : disks) {
      if (disk.getState() == HardwareState.UNAVAILABLE) {
        continue;
      }
      if (disk.getAvailableSpaceInBytes() < replicaCapacity) {
        logger.info(
            "Disk {} doesn't have space to host new replica. Disk space left {}, replica capacity {}. Checking another disk",
            disk, disk.getAvailableSpaceInBytes(), replicaCapacity);
        continue;
      }
      if (disksAttemptedForBootstrap.containsKey(partitionName) && disksAttemptedForBootstrap.get(partitionName)
          .contains(disk)) {
        logger.info("Disk {} has already been tried before for this partition {}. Checking another disk", disk,
            partitionName);
        continue;
      }
      if (disk.getAvailableSpaceInBytes() == maxAvailableDiskSpace) {
        potentialDisks.add(disk);
      } else if (disk.getAvailableSpaceInBytes() > maxAvailableDiskSpace) {
        potentialDisks.clear();
        potentialDisks.add(disk);
        maxAvailableDiskSpace = disk.getAvailableSpaceInBytes();
      }
    }
    if (potentialDisks.isEmpty()) {
      logger.error("No disk space available to host new replica with capacity {}", replicaCapacity);
      return null;
    }

    // Select first available disk with maximum available capacity.
    return potentialDisks.get(0);
  }

  /**
   * Callback method when this data node is becoming FULL AUTO mode.
   */
  void dataNodeOnBecomingFullAuto() {
    List<String> tags = instanceNameToInstanceConfig.get(selfInstanceName).getTags();
    DataNodeId dataNodeId = instanceNameToAmbryDataNode.get(selfInstanceName);
    List<String> resources = tags.stream()
        .map(tag -> dcToTagToResourceProperty.get(dataNodeId.getDatacenterName()).get(tag).name)
        .collect(Collectors.toList());
    helixClusterManagerMetrics.registerMetricsForFullAuto(resources, this);
  }

  /**
   * Callback method when this data node is becoming SEMI AUTO from FULL AUTO.
   */
  void dataNodeOnBecomingSemiAuto() {
    helixClusterManagerMetrics.deregisterMetricsForFullAuto();
  }

  Set<String> getAllInstancesForResource(String resource) {
    String dcName = clusterMapConfig.clusterMapDatacenterName;
    String tag = dcToResourceNameToTag.get(dcName).get(resource);
    return tagToInstanceNames.get(tag);
  }

  int getTotalInstanceCount(String resource) {
    return getAllInstancesForResource(resource).size();
  }

  int getLiveInstanceCount(String resource) {
    return (int) (getAllInstancesForResource(resource).stream()
        .map(instanceNameToAmbryDataNode::get)
        .filter(dn -> dn.getState() == HardwareState.AVAILABLE)
        .count());
  }

  int getUnavailableInstanceCount(String resource) {
    return (int) (getAllInstancesForResource(resource).stream()
        .map(instanceNameToAmbryDataNode::get)
        .filter(dn -> dn.getState() == HardwareState.UNAVAILABLE)
        .count());
  }

  long getResourceTotalRegisteredHostDiskCapacity(String resource) {
    return getAllInstancesForResource(resource).stream()
        .map(instanceNameToInstanceConfig::get)
        .mapToInt(instanceConfig -> instanceConfig.getInstanceCapacityMap().get(DISK_KEY))
        .sum();
  }

  long getResourceAvailableRegisteredHostDiskCapacity(String resource) {
    return getAllInstancesForResource(resource).stream()
        .map(instanceNameToAmbryDataNode::get)
        .filter(dn -> dn.getState() == HardwareState.AVAILABLE)
        .map(ClusterMapUtils::getInstanceName)
        .map(instanceNameToInstanceConfig::get)
        .mapToInt(instanceConfig -> instanceConfig.getInstanceCapacityMap().get(DISK_KEY))
        .sum();
  }

  int getNumberOfPartitionsInResource(String resource) {
    String dcName = clusterMapConfig.clusterMapDatacenterName;
    String tag = dcToResourceNameToTag.get(dcName).get(resource);
    return dcToTagToResourceProperty.get(dcName).get(tag).numPartitions;
  }

  int getReplicaCountForStateInResource(ReplicaState state, String resource) {
    ExternalView externalView;
    if (clusterMapConfig.clusterMapUseAggregatedView) {
      externalView = globalResourceToExternalView.get(resource);
    } else {
      externalView = dcToResourceToExternalView.get(clusterMapConfig.clusterMapDatacenterName).get(resource);
    }
    if (externalView == null) {
      return 0;
    }
    return getReplicaCountForStateInExternalView(state, externalView, clusterMapConfig.clusterMapUseAggregatedView);
  }

  int getReplicaCountForStateInExternalView(ReplicaState state, ExternalView externalView, boolean checkDatacenter) {
    int result = 0;
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> states = externalView.getStateMap(partition);
      if (!states.isEmpty()) {
        result += states.entrySet().stream().filter(ent -> {
          if (state != null && !ent.getValue().equals(state.name())) { // if state is null, then include all states
            return false;
          }
          if (checkDatacenter) {
            AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(ent.getKey());
            return dataNode != null && dataNode.getDatacenterName().equals(clusterMapConfig.clusterMapDatacenterName);
          }
          return true;
        }).count();
      }
    }
    return result;
  }

  int getPartitionDiskWeight(String partition) {
    return diskWeightForPartitions.getOrDefault(partition, getPartitionDefaultDiskWeight());
  }

  int getResourceExpectedTotalDiskCapacityUsage(String resource, int replicationFactor) {
    String dcName = clusterMapConfig.clusterMapDatacenterName;
    getResourceConfig(resource, dcName);
    int result = 0;
    for (String partition : resourceNameToPartitionByDc.get(dcName).get(resource)) {
      result += getPartitionDiskWeight(partition) * replicationFactor;
    }
    return result;
  }

  int getResourceTotalDiskCapacityUsage(String resource) {
    // call this method to fill up the partition capacity map from resource config
    String dcName = clusterMapConfig.clusterMapDatacenterName;
    getResourceConfig(resource, dcName);
    ExternalView externalView;
    if (clusterMapConfig.clusterMapUseAggregatedView) {
      externalView = globalResourceToExternalView.get(resource);
    } else {
      externalView = dcToResourceToExternalView.get(dcName).get(resource);
    }
    if (externalView == null) {
      return 0;
    }
    int result = 0;
    for (String partition : externalView.getPartitionSet()) {
      Map<String, String> states = externalView.getStateMap(partition);
      int weight = getPartitionDiskWeight(partition);
      if (!states.isEmpty()) {
        int numReplica;
        if (clusterMapConfig.clusterMapUseAggregatedView) {
          numReplica = (int) states.keySet().stream().filter(instance -> {
            AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instance);
            return dataNode != null && dataNode.getDatacenterName().equals(clusterMapConfig.clusterMapDatacenterName);
          }).count();
        } else {
          numReplica = states.size();
        }
        result += numReplica * weight;
      }
    }
    return result;
  }

  int getRegisteredHostDiskCapacity() {
    return instanceNameToInstanceConfig.get(selfInstanceName).getInstanceCapacityMap().get(DISK_KEY);
  }

  int getHostReplicaCount() {
    AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(selfInstanceName);
    return ambryDataNodeToAmbryReplicas.get(dataNode).size();
  }

  int getHostTotalDiskCapacityUsage() {
    AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(selfInstanceName);
    String dcName = dataNode.getDatacenterName();
    for (String tag : instanceNameToInstanceConfig.get(selfInstanceName).getTags()) {
      // Call this method to make sure the partition weight map is populated
      getResourceConfig(dcToTagToResourceProperty.get(dcName).get(tag).name, clusterMapConfig.clusterMapDatacenterName);
    }
    Map<String, AmbryReplica> replicas = ambryDataNodeToAmbryReplicas.get(dataNode);
    return replicas.keySet().stream().mapToInt(this::getPartitionDiskWeight).sum();
  }

  /**
   * A helper class used by components of cluster to query information from the {@link HelixClusterManager}. This helps
   * avoid circular dependency between classes like {@link HelixClusterManager} and {@link AmbryPartition}.
   */
  class HelixClusterManagerQueryHelper
      implements ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> {
    /**
     * Get all replica ids associated with the given {@link AmbryPartition}
     * @param partition the {@link AmbryPartition} for which to get the list of replicas.
     * @return the list of {@link AmbryReplica}s associated with the given partition.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      Set<AmbryReplica> replicas = ambryPartitionToAmbryReplicas.get(partition);

      if (replicas == null) {
        // Returns an empty list if the partition does not exist
        logger.error("Partition {} has no replicas", partition.getId());
        return Collections.emptyList();
      }

      return new ArrayList<>(replicas);
    }

    @Override
    public List<String> getResourceNamesForPartition(AmbryPartition partition) {
      // Go through all datacenters since:
      //  1. A partition can reside in one datacenter only or
      //  2. A partition can belong to different resources in different data centers.
      Set<String> resourceNames = new HashSet<>();
      for (String dcName : partitionToResourceNameByDc.keySet()) {
        Set<String> resourceNamesInDC = partitionToResourceNameByDc.get(dcName).get(partition.toPathString());
        if (resourceNamesInDC != null && !resourceNamesInDC.isEmpty()) {
          resourceNames.addAll(resourceNamesInDC);
        }
      }
      return new ArrayList<>(resourceNames);
    }

    /**
     * {@inheritDoc}
     * If dcName is null, then get replicas by given state from all datacenters.
     * If no routing table snapshot is found for dc name, or no resource name found for given partition, return empty list.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsByState(AmbryPartition partition, ReplicaState state, String dcName) {
      Timer.Context operationTimer = helixClusterManagerMetrics.routingTableQueryTime.time();
      long startTime = SystemTime.getInstance().milliseconds();
      Set<AmbryReplica> replicas = new HashSet<>();
      List<String> dcs = dcName != null ? Collections.singletonList(dcName)
          : dcToDcInfo.values().stream().map(dcInfo -> dcInfo.dcName).collect(Collectors.toList());
      String partitionName = partition.toPathString();
      for (String dc : dcs) {
        Set<String> resourceNames = partitionToResourceNameByDc.get(dc).get(partitionName);
        if (resourceNames == null) {
          // For newly created partitions, we might not able to find resources in other DCs
          continue;
        }
        RoutingTableSnapshot routingTableSnapshot =
            clusterMapConfig.clusterMapUseAggregatedView ? globalRoutingTableSnapshotRef.get()
                : dcToRoutingTableSnapshotRef.get(dc).get();
        int replicasBefore = replicas.size();
        for (String resourceName : resourceNames) {
          getReplicaIdsByStateInRoutingTableSnapshot(routingTableSnapshot, dc, resourceName, partitionName, state,
              replicas);
        }
        if (replicas.size() != replicasBefore) {
          continue;
        }
        logger.trace("Can't find any replicas in dc {} for partition {} at resources {} at state {}", dc, partition,
            resourceNames, state);
        if (!partitionToDuplicateResourceNameByDc.get(dc).containsKey(partitionName)) {
          logger.trace("No duplicate resource found for partition {} in dc {}, continue", partitionName, dc);
          continue;
        }
        Map<String, ExternalView> externalViewMap =
            clusterMapConfig.clusterMapUseAggregatedView ? globalResourceToExternalView
                : dcToResourceToExternalView.get(dc);
        // None of resources has instances for the partition, we try the duplicate resource
        if (!resourceNames.stream()
            .anyMatch(
                resource -> externalViewHasInstanceForPartition(externalViewMap.get(resource), dc, partitionName))) {
          String duplicateResource = partitionToDuplicateResourceNameByDc.get(dc).get(partitionName);
          logger.trace("Partition {} doesn't exist in resources {} in dc {}, try duplicate resource {}", partitionName,
              resourceNames, dc, duplicateResource);
          helixClusterManagerMetrics.resourceNameMismatchCount.inc();
          getReplicaIdsByStateInRoutingTableSnapshot(routingTableSnapshot, dc, duplicateResource, partitionName, state,
              replicas);
        }
      }
      logger.debug("Replicas for partition {} with state {} in dc {} are {}. Query time in Ms {}", partitionName,
          state.name(), dcName != null ? dcName : "", replicas, SystemTime.getInstance().milliseconds() - startTime);
      operationTimer.stop();
      return new ArrayList<>(replicas);
    }

    private boolean externalViewHasInstanceForPartition(ExternalView externalView, String dcName,
        String partitionName) {
      if (externalView == null) {
        return false;
      }
      Map<String, String> stateMap = externalView.getStateMap(partitionName);
      if (stateMap == null || stateMap.isEmpty()) {
        return false;
      }
      // state map is a map from instance to state
      return stateMap.keySet().stream().anyMatch(instance -> {
        AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instance);
        return dataNode != null && dataNode.getDatacenterName().equals(dcName);
      });
    }

    private void getReplicaIdsByStateInRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot, String dc,
        String resourceName, String partitionName, ReplicaState state, Set<AmbryReplica> replicas) {
      List<InstanceConfig> instanceConfigs =
          routingTableSnapshot.getInstancesForResource(resourceName, partitionName, state.name());
      Set<String> missingInstances = new HashSet<>();

      for (InstanceConfig instanceConfig : instanceConfigs) {
        String instanceName = instanceConfig.getInstanceName();
        if (!instanceNameToAmbryDataNode.containsKey(instanceName)) {
          missingInstances.add(instanceName);
          continue;
        }
        AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instanceName);
        if (dataNode.getDatacenterName().equals(dc)) {
          AmbryReplica replica = ambryDataNodeToAmbryReplicas.get(dataNode).get(partitionName);
          if (replica != null) {
            replicas.add(replica);
          }
        }
      }

      if (!missingInstances.isEmpty()) {
        logger.error("Instances are present in external view but missing in data node configs {}", missingInstances);
      }
    }

    @Override
    public void getReplicaIdsByStates(Map<ReplicaState, List<AmbryReplica>> replicasByState, AmbryPartition partition,
        Set<ReplicaState> states, String dcName) {
      for (ReplicaState state : states) {
        List<AmbryReplica> replicas = getReplicaIdsByState(partition, state, dcName);
        replicasByState.put(state, replicas);
      }
    }

    /**
     * Get the value counter representing the sealed state change for partitions.
     * @return the value of the counter representing the sealed state change for partitions.
     */
    @Override
    public long getSealedStateChangeCounter() {
      return sealedStateChangeCounter.get();
    }

    /**
     * @return the count of datacenters in this cluster.
     */
    long getDatacenterCount() {
      return dcToDcInfo.size();
    }

    /**
     * @return a collection of datanodes in this cluster.
     */
    Collection<AmbryDataNode> getDatanodes() {
      return getDataNodeIds();
    }

    /**
     * @return the count of the datanodes in this cluster.
     */
    long getDatanodeCount() {
      return getDatanodes().size();
    }

    /**
     * @return the count of datanodes in this cluster that are down.
     */
    long getDownDatanodesCount() {
      long count = 0;
      for (AmbryDataNode datanode : getDatanodes()) {
        if (datanode.getState() == HardwareState.UNAVAILABLE) {
          count++;
        }
      }
      return count;
    }

    @Override
    public Collection<AmbryDisk> getDisks(AmbryDataNode dataNode) {
      if (dataNode != null) {
        final Set<AmbryDisk> disks = ambryDataNodeToAmbryDisks.get(dataNode);
        return disks != null ? disks : Collections.emptyList();
      }
      List<AmbryDisk> disksToReturn = new ArrayList<>();
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        disksToReturn.addAll(disks);
      }
      return disksToReturn;
    }

    /**
     * @return the count of disks in this cluster.
     */
    long getDiskCount() {
      long count = 0;
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        count += disks.size();
      }
      return count;
    }

    /**
     * @return the count of disks in this cluster that are down.
     */
    long getDownDisksCount() {
      long count = 0;
      for (AmbryDisk disk : getDisks(null)) {
        if (disk.getState() == HardwareState.UNAVAILABLE) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return a collection of partitions in this cluster.
     */
    @Override
    public Collection<AmbryPartition> getPartitions() {
      return new ArrayList<>(partitionMap.values());
    }

    /**
     * @return the count of partitions in this cluster.
     */
    long getPartitionCount() {
      return partitionMap.size();
    }

    /**
     * @return the count of partitions in this cluster that are in read-write state.
     */
    long getPartitionReadWriteCount() {
      long count = 0;
      for (AmbryPartition partition : partitionMap.values()) {
        // TODO Efficient_Metadata_Operations_TODO Note that currently even partially read write partitions are treated
        //  as read write once we start making the distinction, we should also change this metric.
        if (partition.getPartitionState() != PartitionState.READ_ONLY) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return the count of partitions that are in sealed (read-only) state.
     */
    long getPartitionSealedCount() {
      long count = 0;
      for (AmbryPartition partition : partitionMap.values()) {
        if (partition.getPartitionState() == PartitionState.READ_ONLY) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return the count of partitions that are in partially sealed (partial-read-write) state.
     */
    long getPartitionPartiallySealedCount() {
      long count = 0;
      for (AmbryPartition partition : partitionMap.values()) {
        if (partition.getPartitionState() == PartitionState.PARTIAL_READ_WRITE) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return the cluster wide raw capacity in bytes.
     */
    long getRawCapacity() {
      return clusterWideRawCapacityBytes.get();
    }

    /**
     * @return the cluster wide allocated raw capacity in bytes.
     */
    long getAllocatedRawCapacity() {
      return clusterWideAllocatedRawCapacityBytes.get();
    }

    /**
     * @return the cluster wide allocated usable capacity in bytes.
     */
    long getAllocatedUsableCapacity() {
      return clusterWideAllocatedUsableCapacityBytes.get();
    }
  }

  /**
   * A more dynamic implementation of {@link HelixClusterChangeHandler} which supports adding new nodes/partitions at
   * runtime. It is also able to absorb replica location changes in cluster.
   */
  class HelixClusterChangeHandler
      implements DataNodeConfigChangeListener, LiveInstanceChangeListener, IdealStateChangeListener,
                 RoutingTableChangeListener, InstanceConfigChangeListener {
    // Data center for which the callback events correspond to.
    private final String dcName;
    // Helix cluster name which keeps track of Ambry topology information.
    private final String helixClusterName;
    private final Object notificationLock = new Object();
    private final Consumer<Exception> onInitializationFailure;
    private final CountDownLatch routingTableInitLatch = new CountDownLatch(1);
    private final List<ClusterMapChangeListener> clusterMapChangeListeners = new ArrayList<>();
    private volatile boolean dataNodeConfigInitialized = false;
    private volatile boolean liveStateInitialized = false;
    private volatile boolean idealStateInitialized = false;
    private volatile boolean instanceConfigInitialized = false;
    final Set<String> allInstances = ConcurrentHashMap.newKeySet();
    // Tells if this clusterChangeHandler is a listener for changes in the entire cluster via helix aggregated view
    // instead of for a particular data center.
    private final boolean isAggregatedViewHandler;

    /**
     * @param dcName the name of data center or the cluster this handler is associated with.
     * @param helixClusterName the name of helix cluster which hosts the Ambry topology information.
     * @param onInitializationFailure callback to be called if initialization fails in a listener call.
     * @param isAggregatedViewHandler indicates if this clusterChangeHandler is a listener for changes in the entire
     */
    HelixClusterChangeHandler(String dcName, String helixClusterName, Consumer<Exception> onInitializationFailure,
        boolean isAggregatedViewHandler) {
      this.dcName = dcName;
      this.helixClusterName = helixClusterName;
      this.onInitializationFailure = onInitializationFailure;
      this.isAggregatedViewHandler = isAggregatedViewHandler;
    }

    @Override
    public void onDataNodeConfigChange(Iterable<DataNodeConfig> configs) {
      handleDataNodeConfigChange(configs, dcName, helixClusterName);
    }

    @Override
    public void onDataNodeDelete(String instanceName) {
      handleDataNodeDelete(instanceName);
    }

    @Override
    public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
      handleIdealStateChange(idealState, dcName);
    }

    @Override
    public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
      handleRoutingTableChange(routingTableSnapshot);
    }

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      handleLiveInstanceChange(liveInstances);
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
      handleInstanceConfigChange(instanceConfigs);
    }

    /**
     * Handle any {@link DataNodeConfig} related change in current datacenter. Several events will trigger datanode config
     * change: (1) replica's seal or stop state has changed; (2) new node or new partition is added; (3) new replica is
     * added to existing node; (4) old replica is removed from existing node.
     * @param configs all the {@link DataNodeConfig}(s) in current data center. (Note that PreFetch is enabled by default
     *                in Helix)
     * @param dcName data center name.
     * @param sourceHelixClusterName name of helix cluster from which the data node configs are obtained.
     */
    void handleDataNodeConfigChange(Iterable<DataNodeConfig> configs, String dcName, String sourceHelixClusterName) {
      try {
        synchronized (notificationLock) {
          if (!dataNodeConfigInitialized) {
            logger.info(
                "Received initial notification for data node config change from helix cluster {} in datacenter {}",
                sourceHelixClusterName, dcName);
          } else {
            logger.info("Data node config change triggered from helix cluster {} in datacenter {}",
                sourceHelixClusterName, dcName);
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Detailed data node config from helix cluster {} in datacenter {} is: {}",
                sourceHelixClusterName, dcName, configs);
          }
          try {
            addOrUpdateInstanceInfos(configs, dcName);
          } catch (Exception e) {
            if (!dataNodeConfigInitialized) {
              logger.error("Exception occurred when initializing data nodes from helix cluster {} in datacenter {}: ",
                  sourceHelixClusterName, dcName, e);
              onInitializationFailure.accept(e);
            } else {
              logger.error(
                  "Exception occurred at runtime when handling data node config changes from helix cluster {} in datacenter {}: ",
                  sourceHelixClusterName, dcName, e);
              helixClusterManagerMetrics.instanceConfigChangeErrorCount.inc();
            }
          } finally {
            dataNodeConfigInitialized = true;
          }
          long counter = sealedStateChangeCounter.incrementAndGet();
          logger.trace("SealedStateChangeCounter increase to {}", counter);
          helixClusterManagerMetrics.dataNodeConfigChangeTriggerCount.inc();
        }
      } catch (Throwable t) {
        errorCount.incrementAndGet();
        throw t;
      }
    }

    /**
     * Handle deletion of a node from cluster.
     * @param instanceName instance that was deleted
     */
    void handleDataNodeDelete(String instanceName) {
      synchronized (notificationLock) {
        AmbryDataNode datanode = instanceNameToAmbryDataNode.get(instanceName);
        if (datanode != null) {
          // Remove disks and replicas on this node.
          List<ReplicaId> removedReplicas = new ArrayList<>();
          for (AmbryDisk disk : ambryDataNodeToAmbryDisks.get(datanode)) {
            clusterWideRawCapacityBytes.getAndAdd(-disk.getRawCapacityInBytes());
          }
          for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(datanode).values()) {
            removeReplicasFromPartition(replica.getPartitionId(), Collections.singletonList(replica));
            removedReplicas.add(replica);
          }
          ambryDataNodeToAmbryDisks.remove(datanode);
          ambryDataNodeToAmbryReplicas.remove(datanode);
          // Remove datanode
          dcToNodes.getOrDefault(datanode.getDatacenterName(), Collections.emptySet()).remove(datanode);
          instanceNameToAmbryDataNode.remove(instanceName);
          allInstances.remove(instanceName);
          // Inform about removed replicas to cluster map change listeners
          if (!removedReplicas.isEmpty()) {
            for (ClusterMapChangeListener listener : clusterMapChangeListeners) {
              listener.onReplicaAddedOrRemoved(Collections.emptyList(), removedReplicas);
            }
          }
          for (ClusterMapChangeListener listener : clusterMapChangeListeners) {
            listener.onDataNodeRemoved(datanode);
          }
          logger.info("Removed {} and its {} replicas from cluster map", instanceName, removedReplicas.size());
          // Since the replicas on deleted hosts could be sealed, increase the sealed change counter so that latest
          // partition state (RO or RW) is queried again in AmbryPartition#resolvePartitionState().
          long counter = sealedStateChangeCounter.incrementAndGet();
          logger.trace("SealedStateChangeCounter increase to {}", counter);
        } else {
          logger.info("Node {} already removed from cluster map", instanceName);
        }
        helixClusterManagerMetrics.instanceDeleteTriggerCount.inc();
      }
    }

    /**
     * Triggered whenever the IdealState in current data center has changed (for now, it is usually updated by Helix
     * Bootstrap tool).
     * @param idealStates a list of {@link IdealState} that specifies ideal location of replicas.
     * @param dcName data center name.
     */
    void handleIdealStateChange(List<IdealState> idealStates, String dcName) {
      if (!idealStateInitialized) {
        logger.info("Received initial notification for IdealState change from helix cluster {} in dc {}",
            helixClusterName, dcName);
        idealStateInitialized = true;
      } else {
        logger.info("IdealState change triggered from helix cluster {} in dc {}", helixClusterName, dcName);
      }
      logger.debug("Detailed ideal states from helix cluster {} in dc {} are: {}", helixClusterName, dcName,
          idealStates);
      updatePartitionResourceMappingFromIdealStates(idealStates, dcName);
      helixClusterManagerMetrics.idealStateChangeTriggerCount.inc();
    }

    /**
     * Triggered whenever there is a change in the list of live instances.
     * @param liveInstances the list of all live instances (not a change set) at the time of this call.
     */
    private void handleLiveInstanceChange(List<LiveInstance> liveInstances) {
      try {
        if (!liveStateInitialized) {
          logger.info("Received initial notification for live instance change from helix cluster {} in dc {}",
              helixClusterName, dcName);
          liveStateInitialized = true;
        } else {
          logger.info("Live instance change triggered from helix cluster {} in dc {}", helixClusterName, dcName);
        }
        logger.debug("Detailed live instances from helix cluster {} in dc {} are: {}", helixClusterName, dcName,
            liveInstances);
        synchronized (notificationLock) {
          updateInstanceLiveness(liveInstances);
          helixClusterManagerMetrics.liveInstanceChangeTriggerCount.inc();
        }
      } catch (Throwable t) {
        errorCount.incrementAndGet();
        throw t;
      }
    }

    /**
     * Triggered whenever the state of replica in cluster has changed. The snapshot contains up-to-date state of all
     * resources(replicas) in this data center.
     * @param routingTableSnapshot a snapshot of routing table for this data center.
     */
    private void handleRoutingTableChange(RoutingTableSnapshot routingTableSnapshot) {
      setRoutingTableSnapshot(routingTableSnapshot);
      if (routingTableInitLatch.getCount() == 1) {
        logger.info("Received initial notification for routing table change from helix cluster {} in dc {}",
            helixClusterName, dcName);
        routingTableInitLatch.countDown();
      } else {
        logger.info("Routing table change triggered from helix cluster {} in dc {}", helixClusterName, dcName);
      }

      // We should notify routing table change indication to different cluster map change listeners like replication
      // manager, etc.
      for (ClusterMapChangeListener listener : clusterMapChangeListeners) {
        listener.onRoutingTableChange();
      }

      helixClusterManagerMetrics.routingTableChangeTriggerCount.inc();
    }

    /**
     * Triggered whenever the instance configs for participants have changed. The list of {@link InstanceConfig} contains
     * all the up-to-date config for instances in local datacenter.
     * @param instanceConfigs
     */
    public void handleInstanceConfigChange(List<InstanceConfig> instanceConfigs) {
      if (!instanceConfigInitialized) {
        logger.info("Received initial notification for InstanceConfig from helix cluster {} in dc {}", helixClusterName,
            dcName);
        instanceConfigInitialized = true;
      } else {
        logger.info("InstanceConfig change triggered from helix cluster {} in dc {}", helixClusterName, dcName);
      }
      Map<String, InstanceConfig> instanceToConfig = new HashMap<>();
      Map<String, Set<String>> tagToInstances = new HashMap<>();
      for (InstanceConfig instanceConfig : instanceConfigs) {
        String instanceName = instanceConfig.getInstanceName();
        instanceToConfig.put(instanceName, instanceConfig);
        List<String> tags = instanceConfig.getTags();
        if (tags != null && !tags.isEmpty()) {
          for (String tag : tags) {
            tagToInstances.computeIfAbsent(tag, k -> new HashSet<>()).add(instanceName);
          }
        }
      }
      // Update the class member
      instanceNameToInstanceConfig.keySet().retainAll(instanceToConfig.keySet());
      tagToInstanceNames.keySet().retainAll(tagToInstances.keySet());
      instanceNameToInstanceConfig.putAll(instanceToConfig);
      tagToInstanceNames.putAll(tagToInstances);
      helixClusterManagerMetrics.instanceConfigChangeTriggerCount.inc();
    }

    /**
     * Sets the helix {@link RoutingTableSnapshot} for a given data center.
     * @param routingTableSnapshot snapshot of cluster mappings.
     */
    public void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot) {
      Map<String, ExternalView> externalViewMap = new ConcurrentHashMap<>();
      if (routingTableSnapshot != null) {
        for (ExternalView externalView : routingTableSnapshot.getExternalViews()) {
          externalViewMap.put(externalView.getResourceName(), externalView);
        }
      }
      if (isAggregatedViewHandler) {
        globalRoutingTableSnapshotRef.getAndSet(routingTableSnapshot);
        globalResourceToExternalView.putAll(externalViewMap);
        globalResourceToExternalView.keySet().retainAll(externalViewMap.keySet());
      } else {
        dcToRoutingTableSnapshotRef.computeIfAbsent(dcName, k -> new AtomicReference<>())
            .getAndSet(routingTableSnapshot);
        dcToResourceToExternalView.put(dcName, externalViewMap);
      }
    }

    /**
     * Gets the helix {@link RoutingTableSnapshot} for a given data center.
     * @param dcName data center name.
     * @return the helix {@link RoutingTableSnapshot} for a given data center.
     */
    public RoutingTableSnapshot getRoutingTableSnapshot(String dcName) {
      RoutingTableSnapshot result;
      if (dcName == null || isAggregatedViewHandler) {
        result = globalRoutingTableSnapshotRef.get();
      } else {
        result = dcToRoutingTableSnapshotRef.get(dcName).get();
      }
      return result;
    }

    public void waitForInitNotification() throws InterruptedException {
      // wait slightly more than 5 mins to ensure routerUpdater refreshes the snapshot.
      if (!routingTableInitLatch.await(320, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
            "Initial routing table change from helix cluster " + helixClusterName + "in dc " + dcName
                + " didn't come within 5 mins");
      }
    }

    public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
      clusterMapChangeListeners.add(clusterMapChangeListener);
    }

    /**
     * Update partition to resource mapping from Ideal states.
     * @param idealStates list of ideal states for various resources.
     * @param dcName data center for which this mapping corresponds to.
     */
    private void updatePartitionResourceMappingFromIdealStates(Collection<IdealState> idealStates, String dcName) {
      if (dcName != null) {
        // Rebuild the entire partition-to-resource map in current dc
        ConcurrentHashMap<String, Set<String>> partitionToResourceMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Set<String>> resourceToPartitionMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> partitionToDuplicateResourceMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Set<String>> partitionResourceToInstanceSetMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, ResourceProperty> tagToProperty = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> resourceNameToTag = new ConcurrentHashMap<>();
        for (IdealState state : idealStates) {
          String resourceName = state.getResourceName();
          String tag = state.getInstanceGroupTag();
          int numPartitions = state.getNumPartitions();
          int replicationFactor = state.getReplicaCount(DEFAULT_NUM_REPLICAS);
          if (replicationFactor == 0) {
            replicationFactor = DEFAULT_NUM_REPLICAS;
          }
          if (!Strings.isEmpty(tag)) {
            resourceNameToTag.put(resourceName, tag);
            ResourceProperty resourceProperty =
                new ResourceProperty(resourceName, numPartitions, replicationFactor, state.getRebalanceMode());
            tagToProperty.put(tag, resourceProperty);
          }
          resourceToPartitionMap.put(resourceName, new HashSet<>(state.getPartitionSet()));
          // Build the partition to resource, partition to duplicate resource and partition-resource to instance set map
          for (String partitionName : state.getPartitionSet()) {
            Set<String> instanceSet = state.getInstanceSet(partitionName);
            Set<String> resources = partitionToResourceMap.computeIfAbsent(partitionName, k -> new HashSet<>());
            if (resources.isEmpty()) {
              // there is no resource mapped to partition, just add this partition/resource in the map.
              resources.add(resourceName);
              partitionResourceToInstanceSetMap.put(partitionName + "-" + resourceName, instanceSet);
            } else {
              boolean foundDuplicate = false;
              for (String prevResource : resources) {
                Set<String> instanceSetInMap =
                    partitionResourceToInstanceSetMap.get(partitionName + "-" + prevResource);
                if (instanceSet.equals(instanceSetInMap)) {
                  foundDuplicate = true;
                  // we found the duplicate resource that has the same instance set, remove the smaller resource id
                  int currentResourceId = Integer.valueOf(resourceName);
                  int previousResourceId = Integer.valueOf(prevResource);
                  if (currentResourceId > previousResourceId) {
                    resources.remove(prevResource);
                    resources.add(resourceName);
                    partitionResourceToInstanceSetMap.remove(partitionName + "-" + prevResource);
                    partitionResourceToInstanceSetMap.put(partitionName + "-" + resourceName, instanceSet);
                    partitionToDuplicateResourceMap.put(partitionName, prevResource);
                  } else {
                    partitionToDuplicateResourceMap.put(partitionName, resourceName);
                  }
                  break;
                }
              }
              if (!foundDuplicate) {
                resources.add(resourceName);
                partitionResourceToInstanceSetMap.put(partitionName + "-" + resourceName, instanceSet);
              }
            }
          }
        }
        dcToTagToResourceProperty.put(dcName, tagToProperty);
        dcToResourceNameToTag.put(dcName, resourceNameToTag);
        partitionToResourceNameByDc.put(dcName, partitionToResourceMap);
        partitionToDuplicateResourceNameByDc.put(dcName, partitionToDuplicateResourceMap);
        resourceNameToPartitionByDc.put(dcName, resourceToPartitionMap);

        // Ideal state has changed, we need to check if the data node is now on FULL AUTO or not.
        // This call might come from frontend node, make sure we don't register any FULL AUTO metrics in frontend.
        DataNodeId currentDataNodeId = instanceNameToAmbryDataNode.get(selfInstanceName);
        if (currentDataNodeId == null || !currentDataNodeId.getDatacenterName().equals(dcName)
            || isDataNodeInFullAutoMode(currentDataNodeId) == localDataNodeInFullAuto.get()) {
          return;
        }
        localDataNodeInFullAuto.set(!localDataNodeInFullAuto.get());
        if (localDataNodeInFullAuto.get()) {
          dataNodeOnBecomingFullAuto();
        } else {
          dataNodeOnBecomingSemiAuto();
        }
      } else {
        logger.warn("Partition to resource mapping for aggregated cluster view would be built from external view");
      }
    }

    /**
     * Add new instances or update existing instances based on {@link DataNodeConfig}(s). This may also invoke callbacks
     * in some clustermap change listeners (i.e. {@link PartitionSelectionHelper}, ReplicationManager)
     * @param dataNodeConfigs the {@link DataNodeConfig}(s) used to update in-mem cluster map.
     * @param dcName data center name.
     */
    private void addOrUpdateInstanceInfos(Iterable<DataNodeConfig> dataNodeConfigs, String dcName) throws Exception {
      List<ReplicaId> totalAddedReplicas = new ArrayList<>();
      List<ReplicaId> totalRemovedReplicas = new ArrayList<>();
      for (DataNodeConfig dataNodeConfig : dataNodeConfigs) {
        Pair<List<ReplicaId>, List<ReplicaId>> addedAndRemovedReplicas;
        if (instanceNameToAmbryDataNode.containsKey(dataNodeConfig.getInstanceName())) {
          addedAndRemovedReplicas = updateInstanceInfo(dataNodeConfig, dcName);
        } else {
          addedAndRemovedReplicas = new Pair<>(createNewInstance(dataNodeConfig, dcName), new ArrayList<>());
        }
        totalAddedReplicas.addAll(addedAndRemovedReplicas.getFirst());
        totalRemovedReplicas.addAll(addedAndRemovedReplicas.getSecond());
      }
      // if this is not initial data node config change and any replicas are added or removed, we should invoke callbacks
      // for different clustermap change listeners (i.e replication manager, partition selection helper)
      logger.info(
          "DC {}: In total, {} replicas are being added and {} replicas are being removed. data node initialized: {}",
          dcName, totalAddedReplicas.size(), totalRemovedReplicas.size(), dataNodeConfigInitialized);
      if (dataNodeConfigInitialized && (!totalAddedReplicas.isEmpty() || !totalRemovedReplicas.isEmpty())) {
        for (ClusterMapChangeListener listener : clusterMapChangeListeners) {
          listener.onReplicaAddedOrRemoved(totalAddedReplicas, totalRemovedReplicas);
        }
      }
    }

    /**
     * Update info of an existing instance. This may happen in following cases: (1) new replica is added; (2) old replica
     * is removed; (3) replica's state has changed (i.e. becomes seal/unseal).
     * @param dataNodeConfig the {@link DataNodeConfig} used to update info of instance.
     * @param dcName
     * @return a pair of lists: (1) new added replicas; (2) removed old replicas, during this update.
     */
    private Pair<List<ReplicaId>, List<ReplicaId>> updateInstanceInfo(DataNodeConfig dataNodeConfig, String dcName)
        throws Exception {
      final List<ReplicaId> addedReplicas = new ArrayList<>();
      final List<ReplicaId> removedReplicas = new ArrayList<>();
      String instanceName = dataNodeConfig.getInstanceName();
      logger.info("Updating replicas info for existing node {}", instanceName);
      Set<String> sealedReplicas = dataNodeConfig.getSealedReplicas();
      Set<String> partiallySealedReplicas = dataNodeConfig.getPartiallySealedReplicas();
      Set<String> stoppedReplicas = dataNodeConfig.getStoppedReplicas();
      AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instanceName);
      ConcurrentHashMap<String, AmbryReplica> currentReplicasOnNode = ambryDataNodeToAmbryReplicas.get(dataNode);
      ConcurrentHashMap<String, AmbryReplica> replicasFromInstanceConfig = new ConcurrentHashMap<>();
      Map<String, AmbryDisk> mountPathToDisk = ambryDataNodeToAmbryDisks.get(dataNode)
          .stream()
          .collect(Collectors.toMap(AmbryDisk::getMountPath, disk -> disk));
      Map<AmbryPartition, List<AmbryReplica>> replicaToAddByPartition = new HashMap<>();
      for (Map.Entry<String, DataNodeConfig.DiskConfig> diskEntry : dataNodeConfig.getDiskConfigs().entrySet()) {
        String mountPath = diskEntry.getKey();
        DataNodeConfig.DiskConfig diskConfig = diskEntry.getValue();
        AmbryDisk disk = mountPathToDisk.getOrDefault(mountPath, null);
        if (disk == null) {
          logger.warn("{} is a new disk or unrecognizable disk which is not supported on existing node {}.", mountPath,
              instanceName);
          // TODO support dynamically adding disk in the future
          continue;
        }
        // update disk capacity if needed
        if (disk.getRawCapacityInBytes() != diskConfig.getDiskCapacityInBytes()) {
          long prevDiskCapacity = disk.getRawCapacityInBytes();
          logger.info("Capacity of disk at {} on {} has changed. Previous was: {} bytes, new capacity is {} bytes",
              mountPath, instanceName, prevDiskCapacity, diskConfig.getDiskCapacityInBytes());
          disk.setDiskCapacityInBytes(diskConfig.getDiskCapacityInBytes());
          addClusterWideRawCapacity(diskConfig.getDiskCapacityInBytes() - prevDiskCapacity);
        }
        if (disk.getState() != diskConfig.getState()) {
          logger.info("State of disk at {} on {} has changed, Previous was: {}, new stats is {}", mountPath,
              instanceName, disk.getState(), diskConfig.getState());
          disk.setState(diskConfig.getState());
        }
        for (Map.Entry<String, DataNodeConfig.ReplicaConfig> replicaEntry : diskConfig.getReplicaConfigs().entrySet()) {
          // partition name and replica name are the same.
          String partitionName = replicaEntry.getKey();
          DataNodeConfig.ReplicaConfig replicaConfig = replicaEntry.getValue();
          if (currentReplicasOnNode.containsKey(partitionName)) {
            // if replica is already present
            AmbryReplica existingReplica = currentReplicasOnNode.get(partitionName);
            // 1. directly add it into "replicasFromInstanceConfig" map
            replicasFromInstanceConfig.put(partitionName, existingReplica);
            // 2. update replica seal/stop state
            updateReplicaStateAndOverrideIfNeeded(existingReplica, sealedReplicas, partiallySealedReplicas,
                stoppedReplicas);
          } else {
            // if this is a new replica and doesn't exist on node
            logger.info("Adding new replica {} to existing node {} in {}", partitionName, instanceName, dcName);
            // this can be a brand new partition that is added to an existing node
            AmbryPartition mappedPartition =
                new AmbryPartition(Long.parseLong(partitionName), replicaConfig.getPartitionClass(),
                    helixClusterManagerQueryHelper);
            // Ensure only one AmbryPartition instance exists for specific partition.
            mappedPartition = addPartitionIfAbsent(mappedPartition, replicaConfig.getReplicaCapacityInBytes());
            ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, dataNode,
                replicaConfig.getReplicaCapacityInBytes());
            // create new replica belonging to this partition or find the existing replica from bootstrapReplicas map.
            AmbryReplica replica;
            if (selfInstanceName.equals(instanceName)) {
              // if this is a newly added replica on current instance, it should be present in bootstrapReplicas map.
              replica = fetchBootstrapReplica(mappedPartition.toPathString());
              if (replica == null) {
                logger.error("Replica {} is not present in bootstrap replica set, abort instance info update",
                    mappedPartition.toPathString());
                throw new IllegalStateException("Replica to add is not present in bootstrap replica map");
              }
            } else {
              replica = new AmbryServerReplica(clusterMapConfig, mappedPartition, disk,
                  stoppedReplicas.contains(partitionName), replicaConfig.getReplicaCapacityInBytes(),
                  resolveReplicaSealStatus(partitionName, sealedReplicas, partiallySealedReplicas));
            }
            updateReplicaStateAndOverrideIfNeeded(replica, sealedReplicas, partiallySealedReplicas, stoppedReplicas);
            // add new created replica to "replicasFromInstanceConfig" map
            replicasFromInstanceConfig.put(partitionName, replica);
            // Put new replica into partition-to-replica map temporarily (this is to avoid any exception thrown within the
            // loop before updating "ambryDataNodeToAmbryReplicas" map. If we update call addReplicasToPartition here
            // immediately, the exception may cause inconsistency between "ambryPartitionToAmbryReplicas" and
            // "ambryDataNodeToAmbryReplicas")
            replicaToAddByPartition.put(mappedPartition, Collections.singletonList(replica));
          }
        }
      }
      // update "ambryDataNodeToAmbryReplicas" map and "addedReplicas" list
      replicaToAddByPartition.forEach((k, v) -> {
        addReplicasToPartition(k, v);
        addedReplicas.addAll(v);
      });
      // update ambryDataNodeToAmbryReplicas map by adding "replicasFromInstanceConfig"
      ambryDataNodeToAmbryReplicas.put(instanceNameToAmbryDataNode.get(instanceName), replicasFromInstanceConfig);
      // Derive old replicas that are removed and delete them from partition
      currentReplicasOnNode.keySet()
          .stream()
          .filter(partitionName -> !replicasFromInstanceConfig.containsKey(partitionName))
          .forEach(pName -> {
            logger.info("Removing replica {} from existing node {}", pName, instanceName);
            AmbryReplica ambryReplica = currentReplicasOnNode.get(pName);
            removeReplicasFromPartition(ambryReplica.getPartitionId(), Collections.singletonList(ambryReplica));
            removedReplicas.add(ambryReplica);
          });
      return new Pair<>(addedReplicas, removedReplicas);
    }

    /**
     * If partition override is enabled, we override replica's {@link ReplicaSealStatus} based on partitionOverrideMap.
     * If disabled, update replica state according to the info from {@link DataNodeConfig}.
     * @param replica the {@link ReplicaId} whose states (seal,stop) should be updated.
     * @param sealedReplicas a collection of {@link ReplicaId}(s) that are in SEALED state.
     * @param partiallySealedReplicas a collection of {@link ReplicaId}(s) that are in PARTIALLY_SEALED state.
     * @param stoppedReplicas a collection of {@link ReplicaId}(s) that are in STOPPED state.
     */
    private void updateReplicaStateAndOverrideIfNeeded(AmbryReplica replica, Collection<String> sealedReplicas,
        Collection<String> partiallySealedReplicas, Collection<String> stoppedReplicas) {
      String partitionName = replica.getPartitionId().toPathString();
      replica.setSealedStatus(resolveReplicaSealStatus(partitionName, sealedReplicas, partiallySealedReplicas));
      replica.setStoppedState(stoppedReplicas.contains(partitionName));
    }

    /**
     * Create a new instance(node) and initialize disks/replicas on it.
     * @param dataNodeConfig the {@link DataNodeConfig} to create new instance
     * @param dcName data center name.
     * @return a list of newly added replicas;
     * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
     */
    private List<ReplicaId> createNewInstance(DataNodeConfig dataNodeConfig, String dcName) throws Exception {
      String instanceName = dataNodeConfig.getInstanceName();
      logger.info("Adding node {} and its disks and replicas in {}", instanceName, dcName);
      AmbryDataNode datanode =
          new AmbryServerDataNode(dataNodeConfig.getDatacenterName(), clusterMapConfig, dataNodeConfig.getHostName(),
              dataNodeConfig.getPort(), dataNodeConfig.getRackId(), dataNodeConfig.getSslPort(),
              dataNodeConfig.getHttp2Port(), DEFAULT_XID, helixClusterManagerQueryHelper);
      // for new instance, we first set it to unavailable and rely on its participation to update its liveness
      if (!instanceName.equals(selfInstanceName)) {
        datanode.setState(HardwareState.UNAVAILABLE);
      }
      List<ReplicaId> addedReplicas = initializeDisksAndReplicasOnNode(datanode, dataNodeConfig);
      instanceNameToAmbryDataNode.put(instanceName, datanode);
      dcToNodes.computeIfAbsent(datanode.getDatacenterName(), s -> ConcurrentHashMap.newKeySet()).add(datanode);
      allInstances.add(instanceName);
      return addedReplicas;
    }

    /**
     * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
     * that partition is being constructed. If partition override is enabled, the seal state of replica is determined by
     * partition info in HelixPropertyStore, if disabled, the seal state is determined by {@code dataNodeConfig}.
     * @param datanode the {@link AmbryDataNode} that is being initialized.
     * @param dataNodeConfig the {@link DataNodeConfig} associated with this datanode.
     * @return a list of newly added replicas on this node.
     * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
     */
    private List<ReplicaId> initializeDisksAndReplicasOnNode(AmbryDataNode datanode, DataNodeConfig dataNodeConfig)
        throws Exception {
      List<ReplicaId> addedReplicas = new ArrayList<>();
      Set<String> sealedReplicas = dataNodeConfig.getSealedReplicas();
      Set<String> partiallySealedReplicas = dataNodeConfig.getPartiallySealedReplicas();
      Set<String> stoppedReplicas = dataNodeConfig.getStoppedReplicas();
      ambryDataNodeToAmbryReplicas.put(datanode, new ConcurrentHashMap<>());
      ambryDataNodeToAmbryDisks.put(datanode, ConcurrentHashMap.newKeySet());
      for (Map.Entry<String, DataNodeConfig.DiskConfig> diskEntry : dataNodeConfig.getDiskConfigs().entrySet()) {
        String mountPath = diskEntry.getKey();
        DataNodeConfig.DiskConfig diskConfig = diskEntry.getValue();

        // Create disk
        AmbryDisk disk = new AmbryDisk(clusterMapConfig, datanode, mountPath, diskConfig.getState(),
            diskConfig.getDiskCapacityInBytes());
        ambryDataNodeToAmbryDisks.get(datanode).add(disk);
        addClusterWideRawCapacity(diskConfig.getDiskCapacityInBytes());

        for (Map.Entry<String, DataNodeConfig.ReplicaConfig> replicaEntry : diskConfig.getReplicaConfigs().entrySet()) {
          String partitionName = replicaEntry.getKey();
          DataNodeConfig.ReplicaConfig replicaConfig = replicaEntry.getValue();

          AmbryPartition mappedPartition =
              new AmbryPartition(Long.parseLong(partitionName), replicaConfig.getPartitionClass(),
                  helixClusterManagerQueryHelper);
          // Ensure only one AmbryPartition instance exists for specific partition.
          mappedPartition = addPartitionIfAbsent(mappedPartition, replicaConfig.getReplicaCapacityInBytes());
          ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, datanode,
              replicaConfig.getReplicaCapacityInBytes());
          // Create replica associated with this node and this partition
          AmbryReplica replica =
              new AmbryServerReplica(clusterMapConfig, mappedPartition, disk, stoppedReplicas.contains(partitionName),
                  replicaConfig.getReplicaCapacityInBytes(),
                  resolveReplicaSealStatus(partitionName, sealedReplicas, partiallySealedReplicas));
          ambryDataNodeToAmbryReplicas.get(datanode).put(mappedPartition.toPathString(), replica);
          addReplicasToPartition(mappedPartition, Collections.singletonList(replica));
          addedReplicas.add(replica);
        }
      }
      return addedReplicas;
    }

    /**
     * Update the liveness states of existing instances based on the input.
     * @param liveInstances the list of instances that are up.
     */
    private void updateInstanceLiveness(List<LiveInstance> liveInstances) {
      Set<String> liveInstancesSet = new HashSet<>();
      liveInstances.forEach(e -> liveInstancesSet.add(e.getInstanceName()));
      for (String instanceName : allInstances) {
        // Here we ignore live instance change it's about self instance. The reason is, during server's startup, current
        // node should be AVAILABLE but the list of live instances doesn't include current node since it hasn't joined yet.
        if (liveInstancesSet.contains(instanceName) || instanceName.equals(selfInstanceName)) {
          instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.AVAILABLE);
        } else {
          instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.UNAVAILABLE);
        }
      }
    }

    /**
     * Ensure that the given partition is absent on the given datanode. This is called as part of an inline validation
     * done to ensure that two replicas of the same partition do not exist on the same datanode.
     * @param partition the {@link AmbryPartition} to check.
     * @param datanode the {@link AmbryDataNode} on which to check.
     * @param expectedReplicaCapacity the capacity expected for the replicas of the partition.
     */
    private void ensurePartitionAbsenceOnNodeAndValidateCapacity(AmbryPartition partition, AmbryDataNode datanode,
        long expectedReplicaCapacity) {
      for (AmbryReplica replica : ambryPartitionToAmbryReplicas.get(partition)) {
        if (replica.getDataNodeId().equals(datanode)) {
          throw new IllegalStateException("Replica already exists on " + datanode + " for " + partition);
        } else if (replica.getCapacityInBytes() != expectedReplicaCapacity) {
          throw new IllegalStateException("Expected replica capacity " + expectedReplicaCapacity + " is different from "
              + "the capacity of an existing replica " + replica.getCapacityInBytes());
        }
      }
    }

    /**
     * Resolve the {@link ReplicaSealStatus} of the replica belonging to the partition with the specified partitionName.
     * @param partitionName Name of the partition.
     * @param sealedReplicas {@link Collection} of partition names whose replicas are sealed.
     * @param partiallySealedReplicas {@link Collection} of partition names whose replicas are partially sealed.
     * @return ReplicaSealStatus object.
     */
    ReplicaSealStatus resolveReplicaSealStatus(String partitionName, Collection<String> sealedReplicas,
        Collection<String> partiallySealedReplicas) {
      ReplicaSealStatus replicaSealStatus = ReplicaSealStatus.NOT_SEALED;
      if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(partitionName)) {
        replicaSealStatus = ClusterMapUtils.partitionStateStrToReplicaSealStatus(
            partitionOverrideInfoMap.get(partitionName).get(ClusterMapUtils.PARTITION_STATE));
        if (replicaSealStatus == ReplicaSealStatus.SEALED) {
          return replicaSealStatus;
        }
      } else {
        if (sealedReplicas.contains(partitionName)) {
          return ReplicaSealStatus.SEALED;
        }
      }
      if (partiallySealedReplicas.contains(partitionName)) {
        replicaSealStatus = ReplicaSealStatus.PARTIALLY_SEALED;
      }
      return replicaSealStatus;
    }
  }

  /**
   * Properties for Resource.
   */
  static class ResourceProperty {
    final String name;
    final int numPartitions;
    final int replicationFactor; // number of replicas
    final IdealState.RebalanceMode rebalanceMode;

    ResourceProperty(String name, int numPartitions, int replicationFactor, IdealState.RebalanceMode rebalanceMode) {
      this.numPartitions = numPartitions;
      this.name = name;
      this.replicationFactor = replicationFactor;
      this.rebalanceMode = rebalanceMode;
    }
  }

  /**
   * Interfaces and classes for returning the resource information to client.
   */

  /**
   * An identifier interface to return a list of resource names.
   */
  public interface ResourceIdentifier {
    List<String> identifyResources(HelixClusterManager helixClusterManager);
  }

  /**
   * An implementation of {@link ResourceIdentifier} to return a single resource name based
   * on the partition name.
   */
  public static class PartitionIdIdentifier implements ResourceIdentifier {
    private String partitionName;

    /**
     * Constructor to create a {@link PartitionIdIdentifier}.
     * @param partitionName
     */
    public PartitionIdIdentifier(String partitionName) {
      this.partitionName = partitionName;
    }

    /**
     * Return the resource name that the given partition belongs to.
     * @param helixClusterManager
     * @return
     */
    @Override
    public List<String> identifyResources(HelixClusterManager helixClusterManager) {
      String dcName = helixClusterManager.clusterMapConfig.clusterMapDatacenterName;
      Map<String, Set<String>> partitionToResourceMap = helixClusterManager.partitionToResourceNameByDc.get(dcName);
      if (!partitionToResourceMap.containsKey(partitionName)) {
        throw new IllegalArgumentException("Partition " + partitionName + " doesn't exist");
      }
      return new ArrayList<>(partitionToResourceMap.get(partitionName));
    }
  }

  /**
   * An implementation of {@link ResourceIdentifier} to return the resource name given by the client.
   */
  public static class ResourceNameIdentifier implements ResourceIdentifier {
    private String resourceName;

    /**
     * Constructor to create a {@link ResourceNameIdentifier}.
     * @param resourceName
     */
    public ResourceNameIdentifier(String resourceName) {
      this.resourceName = resourceName;
    }

    /**
     * Check if the given resource is valid, and then return it back.
     * @param helixClusterManager
     * @return
     */
    @Override
    public List<String> identifyResources(HelixClusterManager helixClusterManager) {
      String dcName = helixClusterManager.clusterMapConfig.clusterMapDatacenterName;
      if (!helixClusterManager.dcToResourceNameToTag.get(dcName).containsKey(resourceName)) {
        throw new IllegalArgumentException("Resource " + resourceName + " doesn't exist");
      }
      return Collections.singletonList(resourceName);
    }
  }

  /**
   * An implementation of {@link ResourceIdentifier} to return the resource name the given hostname belongs to .
   */
  public static class HostnameIdentifier implements ResourceIdentifier {
    private final String hostname;

    /**
     * Constructor to create a {@link HostnameIdentifier}.
     * @param hostname
     */
    public HostnameIdentifier(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public List<String> identifyResources(HelixClusterManager helixClusterManager) {
      String dcName = helixClusterManager.clusterMapConfig.clusterMapDatacenterName;
      DataNodeId dataNodeId = helixClusterManager.instanceNameToAmbryDataNode.get(hostname);
      if (dataNodeId == null || !dataNodeId.getDatacenterName().equals(dcName)) {
        throw new IllegalArgumentException("Host " + hostname + " doesn't exist in this datacenter");
      }
      List<String> tags = helixClusterManager.instanceNameToInstanceConfig.get(hostname).getTags();
      return tags.stream()
          .map(tag -> helixClusterManager.dcToTagToResourceProperty.get(dataNodeId.getDatacenterName()).get(tag).name)
          .collect(Collectors.toList());
    }
  }

  /**
   * An implementation to return all the resource this cluster map has.
   */
  public static class AllResourceIdentifier implements ResourceIdentifier {
    public static final AllResourceIdentifier DEFAULT = new AllResourceIdentifier();

    /**
     * Return all the resource names this cluster map has.
     * @param helixClusterManager
     * @return
     */
    @Override
    public List<String> identifyResources(HelixClusterManager helixClusterManager) {
      String dcName = helixClusterManager.clusterMapConfig.clusterMapDatacenterName;
      return new ArrayList<>(helixClusterManager.dcToResourceNameToTag.get(dcName).keySet());
    }
  }

  /**
   * Query the list of the resource names identified by the {@link ResourceIdentifier} and return a list of {@link ResourceInfo}.
   * @param identifier
   * @return
   */
  public List<ResourceInfo> queryResourceInfos(ResourceIdentifier identifier) {
    List<String> resourceNames = identifier.identifyResources(this);
    return resourceNames.stream().map(this::getResourceInfo).collect(Collectors.toList());
  }

  private ResourceInfo getResourceInfo(String resourceName) {
    String dcName = clusterMapConfig.clusterMapDatacenterName;
    List<String> allInstances = new ArrayList<>(getAllInstancesForResource(resourceName));
    List<String> liveInstances = allInstances.stream()
        .map(instanceNameToAmbryDataNode::get)
        .filter(dn -> dn.getState() == HardwareState.AVAILABLE)
        .map(ClusterMapUtils::getInstanceName).collect(Collectors.toList());
    List<String> unavailableInstances = allInstances.stream()
        .map(instanceNameToAmbryDataNode::get)
        .filter(dn -> dn.getState() == HardwareState.UNAVAILABLE)
        .map(ClusterMapUtils::getInstanceName)
        .collect(Collectors.toList());
    long totalCapacity = getResourceTotalRegisteredHostDiskCapacity(resourceName);
    long liveCapacity = getResourceAvailableRegisteredHostDiskCapacity(resourceName);
    long unavailableCapacity = totalCapacity - liveCapacity;
    int numPartitions = getNumberOfPartitionsInResource(resourceName);
    String tag = dcToResourceNameToTag.get(dcName).get(resourceName);
    int replicationFactor = dcToTagToResourceProperty.get(dcName).get(tag).replicationFactor;
    int numExpectedReplicas = numPartitions * replicationFactor;
    int numCurrentReplicas = getReplicaCountForStateInResource(null, resourceName);
    int expectedTotalReplicaWeight = getResourceExpectedTotalDiskCapacityUsage(resourceName, replicationFactor);
    int currentTotalReplicaWeight = getResourceTotalDiskCapacityUsage(resourceName);
    Map<String, Set<String>> failedDisks = new HashMap<>();
    for (String instanceName : getAllInstancesForResource(resourceName)) {
      Set<AmbryDisk> disks = ambryDataNodeToAmbryDisks.get(instanceNameToAmbryDataNode.get(instanceName));
      Set<String> failedDiskMountPath =
          disks.stream().filter(disk -> disk.getState() == HardwareState.UNAVAILABLE).map(AmbryDisk::getMountPath)
          .collect(Collectors.toSet());
      if (!failedDiskMountPath.isEmpty()) {
        failedDisks.put(instanceName, failedDiskMountPath);
      }
    }
    return new ResourceInfo(resourceName, liveInstances, unavailableInstances, liveCapacity, unavailableCapacity,
        numPartitions, numExpectedReplicas, numCurrentReplicas, expectedTotalReplicaWeight, currentTotalReplicaWeight,
        failedDisks);
  }
}

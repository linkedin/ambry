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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
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
  private final Map<String, ConcurrentHashMap<String, String>> partitionToResourceNameByDc = new ConcurrentHashMap<>();
  private final Map<String, AtomicReference<RoutingTableSnapshot>> dcToRoutingTableSnapshotRef =
      new ConcurrentHashMap<>();
  // A map of partition to resource names used in aggregated cluster view.
  private final AtomicReference<ConcurrentHashMap<String, Set<String>>> globalPartitionToResourceNamesRef =
      new AtomicReference<>(new ConcurrentHashMap<>());
  // Routing table snapshot reference used in aggregated cluster view.
  private final AtomicReference<RoutingTableSnapshot> globalRoutingTableSnapshotRef = new AtomicReference<>();
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
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStoreInLocalDc = null;
  // The current xid currently does not change after instantiation. This can change in the future, allowing the cluster
  // manager to dynamically incorporate newer changes in the cluster. This variable is atomic so that the gauge metric
  // reflects the current value.
  private final AtomicLong currentXid;
  final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private HelixAggregatedViewClusterInfo helixAggregatedViewClusterInfo = null;

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
            clusterMapConfig.clusterMapDefaultPartitionClass);
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
    return new ArrayList<>(ambryDataNodeToAmbryReplicas.get(dataNodeId).values());
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
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    return partitionSelectionHelper.getRandomWritablePartition(partitionClass, partitionsToExclude);
  }

  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    return partitionSelectionHelper.getPartitions(partitionClass);
  }

  /**
   * {@inheritDoc}
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
   *         "localhost1_17088": "/tmp/c/1",
   *         "localhost2_17088": "/tmp/d/1"
   *     },
   *     "2": {
   *         "replicaCapacityInBytes": 107374182400,
   *         "partitionClass": "max-replicas-all-datacenters",
   *         "localhost3_17088": "/tmp/e/1"
   *     }
   * }
   * </pre>
   * In above example, two bootstrap replicas of partition[1] will be added to localhost1 and localhost2 respectively.
   * The host name is followed by mount path on which the bootstrap replica should be placed.
   */
  @Override
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
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
        }
        bootstrapReplica =
            new AmbryServerReplica(clusterMapConfig, currentPartition, targetDisk, true, replicaCapacity, false);
      } catch (Exception e) {
        logger.error("Failed to create bootstrap replica for partition {} on {} due to exception: ", partitionIdStr,
            instanceName, e);
        bootstrapReplica = null;
      }
    } else {
      logger.error(
          "Either datanode or disk that associated with bootstrap replica is not found in cluster map. Cannot create the replica.");
    }
    // For now this method is only called by server which new replica will be added to. So if datanode equals to current
    // node, we temporarily add this into a map (because we don't know whether store addition in storage manager
    // succeeds or not). After store addition succeeds, current node is supposed to update InstanceConfig and will
    // receive notification from Helix afterwards. At that time, dynamic cluster change handler will move replica from
    // this map to clustermap related data structures that can be queried by other components.
    if (bootstrapReplica != null && instanceName.equals(selfInstanceName)) {
      // Note that this method might be called by several state transition threads concurrently.
      bootstrapReplicas.put(currentPartition.toPathString(), bootstrapReplica);
    }
    return bootstrapReplica;
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

    if(helixPropertyStoreInLocalDc != null){
      helixPropertyStoreInLocalDc.stop();
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
  Map<String, Map<String, String>> getPartitionToResourceMapByDC() {
    return Collections.unmodifiableMap(partitionToResourceNameByDc);
  }

  /**
   * Exposed for testing
   * @return a map of partition to its corresponding resources in all data centers.
   */
  Map<String, Set<String>> getGlobalPartitionToResourceMap() {
    return Collections.unmodifiableMap(globalPartitionToResourceNamesRef.get());
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
    return (AmbryReplica) bootstrapReplicas.remove(partitionName);
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
      return new ArrayList<>(ambryPartitionToAmbryReplicas.get(partition));
    }

    @Override
    public List<String> getResourceNamesForPartition(AmbryPartition partition) {
      if (clusterMapConfig.clusterMapUseAggregatedView) {
        return new ArrayList<>(
            globalPartitionToResourceNamesRef.get().getOrDefault(partition.toPathString(), Collections.emptySet()));
      } else {
        // go through all datacenters in case that partition resides in one datacenter only
        for (String dcName : partitionToResourceNameByDc.keySet()) {
          String resourceName = partitionToResourceNameByDc.get(dcName).get(partition.toPathString());
          if (resourceName != null) {
            return Collections.singletonList(resourceName);
          }
        }
        return Collections.emptyList();
      }
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
      if (clusterMapConfig.clusterMapUseAggregatedView) {
        String partitionPath = partition.toPathString();
        // A partition can be under different resources in different data centers. Due to that, when using aggregated
        // view, get replicas for all {Resource, partition} pairs.
        Set<String> resourceNames =
            globalPartitionToResourceNamesRef.get().getOrDefault(partitionPath, Collections.emptySet());
        RoutingTableSnapshot globalRoutingTableSnapshot = globalRoutingTableSnapshotRef.get();
        for (String resourceName : resourceNames) {
          globalRoutingTableSnapshot.getInstancesForResource(resourceName, partitionPath, state.name())
              .stream()
              .map(instanceConfig -> instanceNameToAmbryDataNode.get(instanceConfig.getInstanceName()))
              .filter(dataNode -> dcName == null || dataNode.getDatacenterName().equals(dcName))
              .map(dataNode -> ambryDataNodeToAmbryReplicas.get(dataNode).get(partition.toPathString()))
              .filter(Objects::nonNull)
              .forEach(replicas::add);
        }
      } else {
        List<String> dcs = dcName != null ? Collections.singletonList(dcName)
            : dcToDcInfo.values().stream().map(dcInfo -> dcInfo.dcName).collect(Collectors.toList());
        for (String dc : dcs) {
          String resourceName = partitionToResourceNameByDc.get(dc).get(partition.toPathString());
          RoutingTableSnapshot routingTableSnapshot = dcToRoutingTableSnapshotRef.get(dc).get();
          String partitionPath = partition.toPathString();
          routingTableSnapshot.getInstancesForResource(resourceName, partitionPath, state.name())
              .stream()
              .map(instanceConfig -> instanceNameToAmbryDataNode.get(instanceConfig.getInstanceName()))
              .map(dataNode -> ambryDataNodeToAmbryReplicas.get(dataNode).get(partition.toPathString()))
              .filter(Objects::nonNull)
              .forEach(replicas::add);
        }
      }
      logger.debug("Replicas for partition {} with state {} in dc {} are {}. Query time in Ms {}",
          partition.toPathString(), state.name(), dcName != null ? dcName : "", replicas,
          SystemTime.getInstance().milliseconds() - startTime);
      operationTimer.stop();
      return new ArrayList<>(replicas);
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
        return ambryDataNodeToAmbryDisks.get(dataNode);
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
        if (partition.getPartitionState() == PartitionState.READ_WRITE) {
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
                 RoutingTableChangeListener {
    // Data center for which the callback events correspond to.
    private final String dcName;
    // Helix cluster name which keeps track of Ambry topology information.
    private final String helixClusterName;
    private final Object notificationLock = new Object();
    private final Consumer<Exception> onInitializationFailure;
    private final CountDownLatch routingTableInitLatch = new CountDownLatch(1);
    private final List<ClusterMapChangeListener> clusterMapChangeListeners = new ArrayList<>();
    private volatile boolean instanceConfigInitialized = false;
    private volatile boolean liveStateInitialized = false;
    private volatile boolean idealStateInitialized = false;
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

    /**
     * Handle any {@link DataNodeConfig} related change in current datacenter. Several events will trigger instance config
     * change: (1) replica's seal or stop state has changed; (2) new node or new partition is added; (3) new replica is
     * added to existing node; (4) old replica is removed from existing node; (5) data node is deleted from cluster.
     * For now, we support (1)~(4). We may consider supporting (5) in the future.
     * (The ZNode path of instance config in Helix is [AmbryClusterName]/CONFIGS/PARTICIPANT/[hostname_port])
     * @param configs all the {@link DataNodeConfig}(s) in current data center. (Note that PreFetch is enabled by default
     *                in Helix, which means all instance configs under "participants" ZNode will be sent to this method)
     * @param dcName data center name.
     * @param sourceHelixClusterName name of helix cluster from which the data node configs are obtained.
     */
    void handleDataNodeConfigChange(Iterable<DataNodeConfig> configs, String dcName, String sourceHelixClusterName) {
      try {
        synchronized (notificationLock) {
          if (!instanceConfigInitialized) {
            logger.info(
                "Received initial notification for instance config change from helix cluster {} in data center {}",
                sourceHelixClusterName, dcName);
          } else {
            logger.info("Instance config change triggered from helix cluster {} in data center {}",
                sourceHelixClusterName, dcName);
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Detailed data node config from helix cluster {} in data center {} is: {}",
                sourceHelixClusterName, dcName, configs);
          }
          try {
            addOrUpdateInstanceInfos(configs, dcName);
          } catch (Exception e) {
            if (!instanceConfigInitialized) {
              logger.error("Exception occurred when initializing instances from helix cluster {} in data center {}: ",
                  sourceHelixClusterName, dcName, e);
              onInitializationFailure.accept(e);
            } else {
              logger.error(
                  "Exception occurred at runtime when handling instance config changes from helix cluster {} in data center {}: ",
                  sourceHelixClusterName, dcName, e);
              helixClusterManagerMetrics.instanceConfigChangeErrorCount.inc();
            }
          } finally {
            instanceConfigInitialized = true;
          }
          long counter = sealedStateChangeCounter.incrementAndGet();
          logger.trace("SealedStateChangeCounter increase to {}", counter);
          helixClusterManagerMetrics.instanceConfigChangeTriggerCount.inc();
        }
      } catch (Throwable t) {
        errorCount.incrementAndGet();
        throw t;
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
      // For CURRENT_STATE based Routing table provider (used in for non-aggregated view), we are not able to get
      // external view snapshot. So, we rely on IDEAL STATES to build partition-to-resource mapping.
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
     * Sets the helix {@link RoutingTableSnapshot} for a given data center.
     * @param routingTableSnapshot snapshot of cluster mappings.
     */
    public void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot) {
      if (isAggregatedViewHandler) {
        // When updating the routing table snapshot in aggregated view, we also update the partition-to-resource
        // mapping since the EXTERNAL_VIEW based RoutingTableProvider provides us this mapping. For non-aggregated view,
        // we use CURRENT_STATE based RoutingTableProvider which don't have this mapping. For it, we rely on ideal
        // states to update the mapping.
        updatePartitionResourcesMappingFromExternalView(routingTableSnapshot.getExternalViews(), null);
        globalRoutingTableSnapshotRef.getAndSet(routingTableSnapshot);
      } else {
        dcToRoutingTableSnapshotRef.computeIfAbsent(dcName, k -> new AtomicReference<>())
            .getAndSet(routingTableSnapshot);
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
        ConcurrentHashMap<String, String> partitionToResourceMap = new ConcurrentHashMap<>();
        for (IdealState state : idealStates) {
          String resourceName = state.getResourceName();
          state.getPartitionSet().forEach(partitionName -> partitionToResourceMap.put(partitionName, resourceName));
        }
        partitionToResourceNameByDc.put(dcName, partitionToResourceMap);
      } else {
        logger.warn("Partition to resource mapping for aggregated cluster view would be built from external view");
      }
    }

    /**
     * Update partition to resource mapping from External view.
     * @param externalViews list of external view for various resources.
     * @param dcName data center for which this mapping corresponds to. Since the partition to resource mapping for
     *               non-aggregated view is built from IDEAL STATES currently, this value would always be null today.
     */
    private void updatePartitionResourcesMappingFromExternalView(Collection<ExternalView> externalViews,
        String dcName) {
      if (dcName != null) {
        logger.warn(
            "Partition to resource mapping for individual dc (non-aggregated view) would be built from ideal states");
      } else {
        // Rebuild the partition-to-resource map across all data centers
        ConcurrentHashMap<String, Set<String>> partitionToResourceNames = new ConcurrentHashMap<>();
        for (ExternalView externalView : externalViews) {
          String resourceName = externalView.getResourceName();
          externalView.getPartitionSet().forEach(partitionName -> {
            Set<String> resourceNames = partitionToResourceNames.computeIfAbsent(partitionName, k -> new HashSet<>());
            resourceNames.add(resourceName);
          });
        }
        globalPartitionToResourceNamesRef.set(partitionToResourceNames);
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
      // if this is not initial InstanceConfig change and any replicas are added or removed, we should invoke callbacks
      // for different clustermap change listeners (i.e replication manager, partition selection helper)
      logger.info(
          "In total, {} replicas are being added and {} replicas are being removed. instanceConfigInitialized: {}",
          totalAddedReplicas.size(), totalRemovedReplicas.size(), instanceConfigInitialized);
      if (instanceConfigInitialized && (!totalAddedReplicas.isEmpty() || !totalRemovedReplicas.isEmpty())) {
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
            updateReplicaStateAndOverrideIfNeeded(existingReplica, sealedReplicas, stoppedReplicas);
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
                  sealedReplicas.contains(partitionName));
            }
            updateReplicaStateAndOverrideIfNeeded(replica, sealedReplicas, stoppedReplicas);
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
     * If partition override is enabled, we override replica SEAL/UNSEAL state based on partitionOverrideMap. If disabled,
     * update replica state according to the info from {@link DataNodeConfig}.
     * @param replica the {@link ReplicaId} whose states (seal,stop) should be updated.
     * @param sealedReplicas a collection of {@link ReplicaId}(s) that are in SEALED state.
     * @param stoppedReplicas a collection of {@link ReplicaId}(s) that are in STOPPED state.
     */
    private void updateReplicaStateAndOverrideIfNeeded(AmbryReplica replica, Collection<String> sealedReplicas,
        Collection<String> stoppedReplicas) {
      String partitionName = replica.getPartitionId().toPathString();
      boolean isSealed;
      if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(partitionName)) {
        isSealed = partitionOverrideInfoMap.get(partitionName)
            .get(ClusterMapUtils.PARTITION_STATE)
            .equals(ClusterMapUtils.READ_ONLY_STR);
      } else {
        isSealed = sealedReplicas.contains(partitionName);
      }
      replica.setSealedState(isSealed);
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
          boolean isSealed = sealedReplicas.contains(partitionName);
          if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(
              partitionName)) {
            // override sealed state if PartitionOverride is enabled.
            isSealed = partitionOverrideInfoMap.get(partitionName)
                .get(ClusterMapUtils.PARTITION_STATE)
                .equals(ClusterMapUtils.READ_ONLY_STR);
          }
          AmbryReplica replica =
              new AmbryServerReplica(clusterMapConfig, mappedPartition, disk, stoppedReplicas.contains(partitionName),
                  replicaConfig.getReplicaCapacityInBytes(), isSealed);
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
  }
}

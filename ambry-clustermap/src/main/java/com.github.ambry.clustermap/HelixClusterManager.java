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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
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
import java.util.concurrent.atomic.AtomicLong;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
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
  private final ConcurrentHashMap<String, DcInfo> dcToDcZkInfo = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Byte, String> dcIdToDcName = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap = new ConcurrentHashMap<>();
  private final AtomicLong clusterWideRawCapacityBytes = new AtomicLong(0);
  private final AtomicLong clusterWideAllocatedRawCapacityBytes = new AtomicLong(0);
  private final AtomicLong clusterWideAllocatedUsableCapacityBytes = new AtomicLong(0);
  private final HelixClusterManagerCallback helixClusterManagerCallback;
  private final ClusterChangeHandlerCallback clusterChangeHandlerCallback;
  private final byte localDatacenterId;
  private final ConcurrentHashMap<String, Exception> initializationFailureMap = new ConcurrentHashMap<>();
  private final AtomicLong sealedStateChangeCounter = new AtomicLong(0);
  private final PartitionSelectionHelper partitionSelectionHelper;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap = new HashMap<>();
  private ZkHelixPropertyStore<ZNRecord> helixPropertyStoreInLocalDc = null;
  // The current xid currently does not change after instantiation. This can change in the future, allowing the cluster
  // manager to dynamically incorporate newer changes in the cluster. This variable is atomic so that the gauge metric
  // reflects the current value.
  private final AtomicLong currentXid;
  final HelixClusterManagerMetrics helixClusterManagerMetrics;

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
    helixClusterManagerCallback = new HelixClusterManagerCallback();
    clusterChangeHandlerCallback = new ClusterChangeHandlerCallback();
    helixClusterManagerMetrics = new HelixClusterManagerMetrics(metricRegistry, helixClusterManagerCallback);
    Map<String, DcZkInfo> dataCenterToZkAddress = null;
    HelixManager localManager_ = null;
    try {
      dataCenterToZkAddress = parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings);
      // Make sure the HelixManager of local datacenter gets connected first and partitionOverrideInfoMap use PropertyStore
      // in local DC for initialization.
      localManager_ =
          initializeHelixManagerAndPropertyStoreInLocalDC(dataCenterToZkAddress, instanceName, helixFactory);
    } catch (Exception e) {
      initializationFailureMap.putIfAbsent(clusterMapConfig.clusterMapDatacenterName, e);
    }
    if (initializationFailureMap.get(clusterMapConfig.clusterMapDatacenterName) == null) {
      HelixManager localManager = localManager_;
      final CountDownLatch initializationAttemptComplete = new CountDownLatch(dataCenterToZkAddress.size());
      for (Map.Entry<String, DcZkInfo> entry : dataCenterToZkAddress.entrySet()) {
        String dcName = entry.getKey();
        String zkConnectStr = entry.getValue().getZkConnectStr();
        // Initialize from every remote datacenter in a separate thread to speed things up.
        Utils.newThread(() -> {
          try {
            HelixManager manager;
            if (dcName.equals(clusterMapConfig.clusterMapDatacenterName)) {
              manager = localManager;
            } else {
              manager =
                  helixFactory.getZKHelixManager(clusterName, selfInstanceName, InstanceType.SPECTATOR, zkConnectStr);
              logger.info("Connecting to Helix manager at {}", zkConnectStr);
              manager.connect();
              logger.info("Established connection to Helix manager at {}", zkConnectStr);
            }
            ClusterChangeHandler clusterChangeHandler;
            String clusterChangeHandlerType = clusterMapConfig.clusterMapClusterChangeHandlerType;
            if (clusterChangeHandlerType.equals(SimpleClusterChangeHandler.class.getSimpleName())) {
              clusterChangeHandler =
                  new SimpleClusterChangeHandler(clusterMapConfig, dcName, selfInstanceName, partitionOverrideInfoMap,
                      partitionMap, partitionNameToAmbryPartition, ambryPartitionToAmbryReplicas,
                      helixClusterManagerCallback, helixClusterManagerMetrics, initializationFailureMap,
                      sealedStateChangeCounter);
            } else if (clusterChangeHandlerType.equals(DynamicClusterChangeHandler.class.getSimpleName())) {
              clusterChangeHandler =
                  new DynamicClusterChangeHandler(clusterMapConfig, dcName, selfInstanceName, partitionOverrideInfoMap,
                      helixClusterManagerCallback, clusterChangeHandlerCallback, helixClusterManagerMetrics,
                      initializationFailureMap, sealedStateChangeCounter);
            } else {
              throw new IllegalArgumentException(
                  "Unsupported cluster change handler type: " + clusterChangeHandlerType);
            }
            // Create RoutingTableProvider of each DC to keep track of partition(replicas) state. Here, we use current
            // state based RoutingTableProvider to remove dependency on Helix's pipeline and reduce notification latency.
            logger.info("Creating routing table provider associated with Helix manager at {}", zkConnectStr);
            RoutingTableProvider routingTableProvider = new RoutingTableProvider(manager, PropertyType.CURRENTSTATES);
            logger.info("Routing table provider is created in {}", dcName);
            DcInfo dcInfo = new DcInfo(dcName, entry.getValue(), manager, clusterChangeHandler);
            dcToDcZkInfo.put(dcName, dcInfo);
            dcIdToDcName.put(dcInfo.dcZkInfo.getDcId(), dcName);
            routingTableProvider.addRoutingTableChangeListener(clusterChangeHandler, null);
            logger.info("Registered routing table change listeners in {}", dcName);

            // The initial instance config change notification is required to populate the static cluster
            // information, and only after that is complete do we want the live instance change notification to
            // come in. We do not need to do anything extra to ensure this, however, since Helix provides the initial
            // notification for a change from within the same thread that adds the listener, in the context of the add
            // call. Therefore, when the call to add a listener returns, the initial notification will have been
            // received and handled.
            manager.addInstanceConfigChangeListener(clusterChangeHandler);
            logger.info("Registered instance config change listeners for Helix manager at {}", zkConnectStr);
            manager.addIdealStateChangeListener(clusterChangeHandler);
            logger.info("Registered ideal state change listeners for Helix manager at {}", zkConnectStr);
            // Now register listeners to get notified on live instance change in every datacenter.
            manager.addLiveInstanceChangeListener(clusterChangeHandler);
            logger.info("Registered live instance change listeners for Helix manager at {}", zkConnectStr);

            // in case initial event occurs before adding routing table listener, here we explicitly set snapshot in
            // ClusterChangeHandler. The reason is, if listener missed initial event, snapshot inside routing table
            // provider should be already populated.
            clusterChangeHandler.setRoutingTableSnapshot(routingTableProvider.getRoutingTableSnapshot());
            // the initial routing table change should populate the instanceConfigs. If it's empty that means initial
            // change didn't come and thread should wait on the init latch to ensure routing table snapshot is non-empty
            if (clusterChangeHandler.getRoutingTableSnapshot().getInstanceConfigs().isEmpty()) {
              // Periodic refresh in routing table provider is enabled by default. In worst case, routerUpdater should
              // trigger routing table change within 5 minutes
              logger.info("Routing table snapshot in {} is currently empty. Waiting for initial notification.", dcName);
              clusterChangeHandler.waitForInitNotification();
            }

            if (!clusterMapConfig.clustermapListenCrossColo && manager != localManager) {
              manager.disconnect();
              logger.info("Stopped listening to cross colo ZK server {}", zkConnectStr);
            }
          } catch (Exception e) {
            initializationFailureMap.putIfAbsent(dcName, e);
          } finally {
            initializationAttemptComplete.countDown();
          }
        }, false).start();
      }
      try {
        initializationAttemptComplete.await();
      } catch (InterruptedException e) {
        initializationFailureMap.putIfAbsent(clusterMapConfig.clusterMapDatacenterName, e);
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
      if (clusterMapConfig.clusterMapClusterChangeHandlerType.equals(
          SimpleClusterChangeHandler.class.getSimpleName())) {
        // capacity stats needs to be initialized only when SimpleClusterChangeHandler is adopted.
        initializeCapacityStats();
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
    localDatacenterId = dcToDcZkInfo.get(clusterMapConfig.clusterMapDatacenterName).dcZkInfo.getDcId();
    partitionSelectionHelper =
        new PartitionSelectionHelper(helixClusterManagerCallback, clusterMapConfig.clusterMapDatacenterName,
            clusterMapConfig.clustermapWritablePartitionMinReplicaCount);
    // register partition selection helper as a listener of cluster map changes.
    dcToDcZkInfo.values()
        .forEach(info -> info.clusterChangeHandler.registerClusterMapListener(partitionSelectionHelper));
  }

  /**
   * Initialize HelixManager in local datacenter and complete subscription of HelixPropertyStore to listen for
   * PartitionOverride zNode.
   * @param dataCenterToZkAddress the map mapping each datacenter to its corresponding ZkAddress.
   * @param instanceName the String representation of the instance associated with this manager.
   * @param helixFactory the factory class to construct and get a reference to a {@link HelixManager}.
   * @return the HelixManager of local datacenter
   * @throws Exception
   */
  private HelixManager initializeHelixManagerAndPropertyStoreInLocalDC(Map<String, DcZkInfo> dataCenterToZkAddress,
      String instanceName, HelixFactory helixFactory) throws Exception {
    DcZkInfo dcZkInfo = dataCenterToZkAddress.get(clusterMapConfig.clusterMapDatacenterName);
    String zkConnectStr = dcZkInfo.getZkConnectStr();
    HelixManager manager;
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
    logger.info("Connecting to Helix manager in local zookeeper at {}", zkConnectStr);
    manager.connect();
    logger.info("Established connection to Helix manager in local zookeeper at {}", zkConnectStr);
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

  /**
   * Initialize capacity statistics.
   */
  private void initializeCapacityStats() {
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      Map<AmbryDataNode, Set<AmbryDisk>> dataNodeToDisks = dcInfo.clusterChangeHandler.getDataNodeToDisksMap();
      for (Set<AmbryDisk> disks : dataNodeToDisks.values()) {
        for (AmbryDisk disk : disks) {
          clusterWideRawCapacityBytes.getAndAdd(disk.getRawCapacityInBytes());
        }
      }
    }
    for (Set<AmbryReplica> partitionReplicas : ambryPartitionToAmbryReplicas.values()) {
      long replicaCapacity = partitionReplicas.iterator().next().getCapacityInBytes();
      clusterWideAllocatedRawCapacityBytes.getAndAdd(replicaCapacity * partitionReplicas.size());
      clusterWideAllocatedUsableCapacityBytes.getAndAdd(replicaCapacity);
    }
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dcToDcZkInfo.containsKey(datacenterName);
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
    String instanceName = getInstanceName(hostname, port);
    AmbryDataNode dataNode = null;
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      dataNode = dcInfo.clusterChangeHandler.getDataNode(instanceName);
      if (dataNode != null) {
        break;
      }
    }
    return dataNode;
  }

  @Override
  public List<AmbryReplica> getReplicaIds(DataNodeId dataNodeId) {
    if (!(dataNodeId instanceof AmbryDataNode)) {
      throw new IllegalArgumentException("Incompatible type passed in");
    }
    AmbryDataNode dataNode = (AmbryDataNode) dataNodeId;
    return dcToDcZkInfo.get(dataNode.getDatacenterName()).clusterChangeHandler.getReplicaIds(dataNode);
  }

  @Override
  public List<AmbryDataNode> getDataNodeIds() {
    List<AmbryDataNode> dataNodeList = new ArrayList<>();
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      dataNodeList.addAll(dcInfo.clusterChangeHandler.getAllDataNodes());
    }
    return dataNodeList;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    AmbryReplica replica = (AmbryReplica) replicaId;
    switch (event) {
      case Node_Response:
        replica.getDataNodeId().onNodeResponse();
        break;
      case Node_Timeout:
        replica.getDataNodeId().onNodeTimeout();
        break;
      case Disk_Error:
        replica.getDiskId().onDiskError();
        break;
      case Disk_Ok:
        replica.getDiskId().onDiskOk();
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
      dcToDcZkInfo.get(dcName).clusterChangeHandler.getAllDataNodes()
          .forEach(node -> datanodesInDc.put(node.getSnapshot()));
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
    AmbryPartition mappedPartition = partitionNameToAmbryPartition.get(partitionIdStr);
    if (mappedPartition == null) {
      logger.info("Partition {} is currently not present in cluster map, creating a new partition.", partitionIdStr);
      mappedPartition = new AmbryPartition(Long.parseLong(partitionIdStr), partitionClass, helixClusterManagerCallback);
    }
    // Check if data node or disk is in current cluster map, if not, set bootstrapReplica to null.
    ClusterChangeHandler localClusterChangeHandler =
        dcToDcZkInfo.get(clusterMapConfig.clusterMapDatacenterName).clusterChangeHandler;
    AmbryDataNode dataNode = localClusterChangeHandler.getDataNode(instanceName);
    String mountPathFromHelix = replicaInfos.get(instanceName);
    Set<AmbryDisk> disks = dataNode != null ? localClusterChangeHandler.getDisks(dataNode) : null;
    Optional<AmbryDisk> potentialDisk =
        disks != null ? disks.stream().filter(d -> d.getMountPath().equals(mountPathFromHelix)).findAny()
            : Optional.empty();
    if (potentialDisk.isPresent()) {
      try {
        bootstrapReplica =
            new AmbryReplica(clusterMapConfig, mappedPartition, potentialDisk.get(), true, replicaCapacity, false);
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

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      dcInfo.clusterChangeHandler.registerClusterMapListener(clusterMapChangeListener);
    }
  }

  /**
   * Disconnect from the HelixManagers associated with each and every datacenter.
   */
  @Override
  public void close() {
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      if (dcInfo.helixManager.isConnected()) {
        dcInfo.helixManager.disconnect();
      }
    }
    dcToDcZkInfo.clear();
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
    return dcToDcZkInfo.get(dataNodeId.getDatacenterName()).clusterChangeHandler.getReplicaId(ambryDataNode,
        partitionString);
  }

  /**
   * @return the count of errors encountered by the Cluster Manager.
   */
  long getErrorCount() {
    long totalErrorCnt = 0;
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      totalErrorCnt += dcInfo.clusterChangeHandler.getErrorCount();
    }
    return totalErrorCnt;
  }

  /**
   * @return a map of datacenter names to {@link DcInfo}
   */
  Map<String, DcInfo> getDcInfosMap() {
    return Collections.unmodifiableMap(dcToDcZkInfo);
  }

  /**
   * @return a map of partition to its corresponding resource grouped by data center
   */
  Map<String, Map<String, String>> getPartitionToResourceMap() {
    Map<String, Map<String, String>> partitionToResourceNameByDc = new HashMap<>();
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      partitionToResourceNameByDc.put(dcInfo.dcName, dcInfo.clusterChangeHandler.getPartitionToResourceMap());
    }
    return partitionToResourceNameByDc;
  }

  /**
   * @return a map of data center to its data nodes
   */
  Map<String, Set<AmbryDataNode>> getDcToDataNodesMap() {
    Map<String, Set<AmbryDataNode>> dcToNodes = new HashMap<>();
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      dcToNodes.put(dcInfo.dcName, new HashSet<>(dcInfo.clusterChangeHandler.getAllDataNodes()));
    }
    return Collections.unmodifiableMap(dcToNodes);
  }

  /**
   * @return a map of data center to its {@link RoutingTableSnapshot}
   */
  Map<String, RoutingTableSnapshot> getRoutingTableSnapshots() {
    Map<String, RoutingTableSnapshot> dcToRoutingTableSnapshot = new HashMap<>();
    for (DcInfo dcInfo : dcToDcZkInfo.values()) {
      dcToRoutingTableSnapshot.put(dcInfo.dcName, dcInfo.clusterChangeHandler.getRoutingTableSnapshot());
    }
    return Collections.unmodifiableMap(dcToRoutingTableSnapshot);
  }

  /**
   * @return {@link HelixClusterManagerCallback} associated with this cluster manager.
   */
  HelixClusterManagerCallback getManagerCallback() {
    return helixClusterManagerCallback;
  }

  /**
   * Class that stores all the information associated with a datacenter.
   */
  static class DcInfo {
    final String dcName;
    final DcZkInfo dcZkInfo;
    final HelixManager helixManager;
    final ClusterChangeHandler clusterChangeHandler;

    /**
     * Construct a DcInfo object with the given parameters.
     * @param dcName the associated datacenter name.
     * @param dcZkInfo the {@link DcZkInfo} associated with the DC.
     * @param helixManager the associated {@link HelixManager} for this datacenter.
     * @param clusterChangeHandler the associated {@link ClusterChangeHandler} for this datacenter.
     */
    DcInfo(String dcName, DcZkInfo dcZkInfo, HelixManager helixManager, ClusterChangeHandler clusterChangeHandler) {
      this.dcName = dcName;
      this.dcZkInfo = dcZkInfo;
      this.helixManager = helixManager;
      this.clusterChangeHandler = clusterChangeHandler;
    }
  }

  /**
   * A callback class for {@link ClusterChangeHandler} in each dc to update cluster-wide info (i.e partition-to-replica
   * mapping, cluster-wide capacity)
   */
  class ClusterChangeHandlerCallback {
    /**
     * Add partition if it's not present in cluster-wide partition map and also update cluster-wide allocated usable
     * capacity. If the partition already exists, skip addition and return current partition.
     * @param partition the {@link AmbryPartition} to add (if not present)
     * @param capacityBytes the capacity of partition in bytes
     * @return the current {@link AmbryPartition} present in the map.
     */
    AmbryPartition addPartitionIfAbsent(AmbryPartition partition, long capacityBytes) {
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
    void addReplicasToPartition(AmbryPartition partition, List<AmbryReplica> replicas) {
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
    void removeReplicasFromPartition(AmbryPartition partition, List<AmbryReplica> replicas) {
      ambryPartitionToAmbryReplicas.computeIfPresent(partition, (k, v) -> {
        v.removeAll(replicas);
        clusterWideAllocatedRawCapacityBytes.getAndAdd(-1 * replicas.get(0).getCapacityInBytes() * replicas.size());
        return v;
      });
    }

    /**
     * Add cluster-wide raw capacity. This is called when new disk is created and added to cluster. We update cluster-wide
     * raw capacity by adding the disk capacity.
     * @param diskRawCapacityBytes raw disk capacity to be added.
     */
    void addClusterWideRawCapacity(long diskRawCapacityBytes) {
      clusterWideRawCapacityBytes.getAndAdd(diskRawCapacityBytes);
    }
  }

  /**
   * A callback class used to query information from the {@link HelixClusterManager}
   */
  class HelixClusterManagerCallback implements ClusterManagerCallback {
    /**
     * Get all replica ids associated with the given {@link AmbryPartition}
     * @param partition the {@link AmbryPartition} for which to get the list of replicas.
     * @return the list of {@link AmbryReplica}s associated with the given partition.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(PartitionId partition) {
      AmbryPartition ambryPartition = (AmbryPartition) partition;
      return new ArrayList<>(ambryPartitionToAmbryReplicas.get(ambryPartition));
    }

    /**
     * {@inheritDoc}
     * If dcName is null, then get replicas by given state from all datacenters.
     * If no routing table snapshot is found for dc name, or no resource name found for given partition, return empty list.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsByState(PartitionId partition, ReplicaState state, String dcName) {
      List<AmbryReplica> replicas = new ArrayList<>();
      for (DcInfo dcInfo : dcToDcZkInfo.values()) {
        String dc = dcInfo.dcName;
        if (dcName == null || dcName.equals(dc)) {
          ClusterChangeHandler clusterChangeHandler = dcInfo.clusterChangeHandler;
          String resourceName = clusterChangeHandler.getPartitionToResourceMap().get(partition.toPathString());
          List<InstanceConfig> instanceConfigs = clusterChangeHandler.getRoutingTableSnapshot()
              .getInstancesForResource(resourceName, partition.toPathString(), state.name());
          instanceConfigs.forEach((e) -> {
            AmbryDataNode dataNode = clusterChangeHandler.getDataNode(e.getInstanceName());
            replicas.add(clusterChangeHandler.getReplicaId(dataNode, partition.toPathString()));
          });
        }
      }
      return replicas;
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
      return dcToDcZkInfo.size();
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
    public Collection<AmbryDisk> getDisks(DataNodeId dataNode) {
      if (dataNode != null) {
        return dcToDcZkInfo.get(dataNode.getDatacenterName()).clusterChangeHandler.getDisks((AmbryDataNode) dataNode);
      }
      List<AmbryDisk> disksToReturn = new ArrayList<>();
      for (DcInfo dcInfo : dcToDcZkInfo.values()) {
        for (Set<AmbryDisk> disks : dcInfo.clusterChangeHandler.getDataNodeToDisksMap().values()) {
          disksToReturn.addAll(disks);
        }
      }
      return disksToReturn;
    }

    /**
     * @return the count of disks in this cluster.
     */
    long getDiskCount() {
      long count = 0;
      for (DcInfo dcInfo : dcToDcZkInfo.values()) {
        for (Set<AmbryDisk> disks : dcInfo.clusterChangeHandler.getDataNodeToDisksMap().values()) {
          count += disks.size();
        }
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
}

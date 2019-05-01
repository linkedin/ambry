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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
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
class HelixClusterManager implements ClusterMap {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String clusterName;
  private final String selfInstanceName;
  private final MetricRegistry metricRegistry;
  private final ClusterMapConfig clusterMapConfig;
  private final ConcurrentHashMap<String, DcInfo> dcToDcZkInfo = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<AmbryDataNode>> dcToNodes = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Byte, String> dcIdToDcName = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AmbryDataNode> instanceNameToAmbryDataNode = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryReplica>> ambryDataNodeToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryDisk>> ambryDataNodeToAmbryDisks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap = new ConcurrentHashMap<>();
  private long clusterWideRawCapacityBytes;
  private long clusterWideAllocatedRawCapacityBytes;
  private long clusterWideAllocatedUsableCapacityBytes;
  private final HelixClusterManagerCallback helixClusterManagerCallback;
  private final byte localDatacenterId;
  private final ConcurrentHashMap<String, Exception> initializationFailureMap = new ConcurrentHashMap<>();
  private final AtomicLong sealedStateChangeCounter = new AtomicLong(0);
  final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private final PartitionSelectionHelper partitionSelectionHelper;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap = new HashMap<>();
  // The current xid currently does not change after instantiation. This can change in the future, allowing the cluster
  // manager to dynamically incorporate newer changes in the cluster. This variable is atomic so that the gauge metric
  // reflects the current value.
  private final AtomicLong currentXid;
  private final AtomicLong errorCount = new AtomicLong(0);

  /**
   * Instantiate a HelixClusterManager.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this manager.
   * @param instanceName the String representation of the instance associated with this manager.
   * @param helixFactory the factory class to construct and get a reference to a {@link HelixManager}.
   * @param metricRegistry the registry of metric instances associated with this manager.
   * @throws IOException if there is an error in parsing the clusterMapConfig or in connecting with the associated
   *                     remote Zookeeper services.
   */
  HelixClusterManager(ClusterMapConfig clusterMapConfig, String instanceName, HelixFactory helixFactory,
      MetricRegistry metricRegistry) throws IOException {
    this.clusterMapConfig = clusterMapConfig;
    currentXid = new AtomicLong(clusterMapConfig.clustermapCurrentXid);
    this.metricRegistry = metricRegistry;
    clusterName = clusterMapConfig.clusterMapClusterName;
    selfInstanceName = instanceName;
    helixClusterManagerCallback = new HelixClusterManagerCallback();
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
        ClusterChangeHandler clusterChangeHandler = new ClusterChangeHandler(dcName);
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
            DcInfo dcInfo = new DcInfo(dcName, entry.getValue(), manager, clusterChangeHandler);
            dcToDcZkInfo.put(dcName, dcInfo);
            dcIdToDcName.put(dcInfo.dcZkInfo.getDcId(), dcName);

            // The initial instance config change notification is required to populate the static cluster
            // information, and only after that is complete do we want the live instance change notification to
            // come in. We do not need to do anything extra to ensure this, however, since Helix provides the initial
            // notification for a change from within the same thread that adds the listener, in the context of the add
            // call. Therefore, when the call to add a listener returns, the initial notification will have been
            // received and handled.
            manager.addInstanceConfigChangeListener(clusterChangeHandler);
            logger.info("Registered instance config change listeners for Helix manager at {}", zkConnectStr);
            // Now register listeners to get notified on live instance change in every datacenter.
            manager.addLiveInstanceChangeListener(clusterChangeHandler);
            logger.info("Registered live instance change listeners for Helix manager at {}", zkConnectStr);
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
      initializeCapacityStats();
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
        new PartitionSelectionHelper(partitionMap.values(), clusterMapConfig.clusterMapDatacenterName);
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
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore;
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
    logger.info("Connecting to Helix manager in local zookeeper at {}", zkConnectStr);
    manager.connect();
    logger.info("Established connection to Helix manager in local zookeeper at {}", zkConnectStr);
    helixPropertyStore = manager.getHelixPropertyStore();
    logger.info("HelixPropertyStore from local datacenter {} is: {}", dcZkInfo.getDcName(), helixPropertyStore);
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
    helixPropertyStore.subscribeDataChanges(ClusterMapUtils.ZNODE_PATH, dataListener);
    logger.info("Getting ZNRecord from HelixPropertyStore");
    ZNRecord zNRecord = helixPropertyStore.get(ClusterMapUtils.ZNODE_PATH, null, AccessOption.PERSISTENT);
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
    for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
      for (AmbryDisk disk : disks) {
        clusterWideRawCapacityBytes += disk.getRawCapacityInBytes();
      }
    }
    for (Set<AmbryReplica> partitionReplicas : ambryPartitionToAmbryReplicas.values()) {
      long replicaCapacity = partitionReplicas.iterator().next().getCapacityInBytes();
      clusterWideAllocatedRawCapacityBytes += replicaCapacity * partitionReplicas.size();
      clusterWideAllocatedUsableCapacityBytes += replicaCapacity;
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
    return instanceNameToAmbryDataNode.get(getInstanceName(hostname, port));
  }

  @Override
  public List<AmbryReplica> getReplicaIds(DataNodeId dataNodeId) {
    if (!(dataNodeId instanceof AmbryDataNode)) {
      throw new IllegalArgumentException("Incompatible type passed in");
    }
    AmbryDataNode datanode = (AmbryDataNode) dataNodeId;
    return new ArrayList<>(ambryDataNodeToAmbryReplicas.get(datanode));
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
      throw new IOException("Partition id from stream is null");
    }
    return partition;
  }

  @Override
  public List<PartitionId> getWritablePartitionIds(String partitionClass) {
    return partitionSelectionHelper.getWritablePartitions(partitionClass);
  }

  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    return partitionSelectionHelper.getPartitions(partitionClass);
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
   * @param hostname the hostname associated with the {@link AmbryDataNode}.
   * @param port the port associated with the {@link AmbryDataNode}.
   * @param partitionString the partition id string associated with the {@link AmbryPartition}.
   * @return the {@link AmbryReplica} associated with the given parameters.
   */
  AmbryReplica getReplicaForPartitionOnNode(String hostname, int port, String partitionString) {
    for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(getDataNodeId(hostname, port))) {
      if (replica.getPartitionId().toString().equals(partitionString)) {
        return replica;
      }
    }
    return null;
  }

  /**
   * @return a map of datacenter names to {@link HelixManager}
   */
  Map<String, HelixManager> getHelixManagerMap() {
    return dcToDcZkInfo.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().helixManager));
  }

  /**
   * @return the count of errors encountered by the Cluster Manager.
   */
  long getErrorCount() {
    return errorCount.get();
  }

  /**
   * An instance of this object is used to register as listener for Helix related changes in each datacenter. This
   * class is also responsible for handling events received.
   */
  private class ClusterChangeHandler implements InstanceConfigChangeListener, LiveInstanceChangeListener {
    private final String dcName;
    final Set<String> allInstances = new HashSet<>();
    private final Object notificationLock = new Object();
    private final AtomicBoolean instanceConfigInitialized = new AtomicBoolean(false);
    private final AtomicBoolean liveStateInitialized = new AtomicBoolean(false);

    /**
     * Initialize a ClusterChangeHandler in the given datacenter.
     * @param dcName the datacenter associated with this ClusterChangeHandler.
     */
    ClusterChangeHandler(String dcName) {
      this.dcName = dcName;
    }

    @Override
    public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
      try {
        logger.trace("InstanceConfig change triggered in {} with: {}", dcName, configs);
        synchronized (notificationLock) {
          if (!instanceConfigInitialized.get()) {
            logger.info("Received initial notification for instance config change from {}", dcName);
            try {
              initializeInstances(configs);
            } catch (Exception e) {
              initializationFailureMap.putIfAbsent(dcName, e);
            }
            instanceConfigInitialized.set(true);
          } else {
            updateStateOfReplicas(configs);
          }
          sealedStateChangeCounter.incrementAndGet();
          helixClusterManagerMetrics.instanceConfigChangeTriggerCount.inc();
        }
      } catch (Throwable t) {
        errorCount.incrementAndGet();
        throw t;
      }
    }

    /**
     * Populate the initial data from the admin connection. Create nodes, disks, partitions and replicas for the entire
     * cluster. An {@link InstanceConfig} will only be looked at if the xid in it is <= currentXid.
     * @param instanceConfigs the list of {@link InstanceConfig}s containing the information about the sealed states of replicas.
     * @throws Exception if creation of {@link AmbryDataNode}s or {@link AmbryDisk}s throw an Exception.
     */
    private void initializeInstances(List<InstanceConfig> instanceConfigs) throws Exception {
      logger.info("Initializing cluster information from {}", dcName);
      for (InstanceConfig instanceConfig : instanceConfigs) {
        int schemaVersion = getSchemaVersion(instanceConfig);
        switch (schemaVersion) {
          case 0:
            String instanceName = instanceConfig.getInstanceName();
            long instanceXid = getXid(instanceConfig);
            if (instanceName.equals(selfInstanceName) || instanceXid <= currentXid.get()) {
              logger.info("Adding node {} and its disks and replicas", instanceName);
              AmbryDataNode datanode =
                  new AmbryDataNode(getDcName(instanceConfig), clusterMapConfig, instanceConfig.getHostName(),
                      Integer.valueOf(instanceConfig.getPort()), getRackId(instanceConfig),
                      getSslPortStr(instanceConfig), instanceXid, helixClusterManagerCallback);
              initializeDisksAndReplicasOnNode(datanode, instanceConfig);
              instanceNameToAmbryDataNode.put(instanceName, datanode);
              dcToNodes.computeIfAbsent(datanode.getDatacenterName(), s -> new HashSet<>()).add(datanode);
              allInstances.add(instanceName);
            } else {
              logger.info(
                  "Ignoring instanceConfig for {} because the xid associated with it ({}) is later than current xid ({})",
                  instanceName, instanceXid, currentXid.get());
              helixClusterManagerMetrics.ignoredUpdatesCount.inc();
            }
            break;
          default:
            logger.error("Unknown InstanceConfig schema version: {}, ignoring.", schemaVersion);
            break;
        }
      }
      logger.info("Initialized cluster information from {}", dcName);
    }

    /**
     * Go over the given list of {@link InstanceConfig}s and update the both sealed and stopped states of replicas.
     * An {@link InstanceConfig} will only be looked at if the xid in it is <= currentXid.
     * @param instanceConfigs the list of {@link InstanceConfig}s containing the up-to-date information about the
     *                        sealed states of replicas.
     */
    private void updateStateOfReplicas(List<InstanceConfig> instanceConfigs) {
      for (InstanceConfig instanceConfig : instanceConfigs) {
        int schemaVersion = getSchemaVersion(instanceConfig);
        switch (schemaVersion) {
          case 0:
            String instanceName = instanceConfig.getInstanceName();
            long instanceXid = getXid(instanceConfig);
            AmbryDataNode node = instanceNameToAmbryDataNode.get(instanceName);
            if (instanceName.equals(selfInstanceName) || instanceXid <= currentXid.get()) {
              if (node == null) {
                logger.trace("Dynamic addition of new nodes is not yet supported, ignoring InstanceConfig {}",
                    instanceConfig);
              } else {
                Set<String> sealedReplicas = new HashSet<>(getSealedReplicas(instanceConfig));
                Set<String> stoppedReplicas = new HashSet<>(getStoppedReplicas(instanceConfig));
                for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(node)) {
                  String partitionId = replica.getPartitionId().toPathString();
                  if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(
                      partitionId)) {
                    logger.trace(
                        "Ignoring instanceConfig change for partition {} on instance {} because partition override is enabled",
                        partitionId, instanceName);
                    helixClusterManagerMetrics.ignoredUpdatesCount.inc();
                  } else {
                    replica.setSealedState(sealedReplicas.contains(partitionId));
                    replica.setStoppedState(stoppedReplicas.contains(partitionId));
                  }
                }
              }
            } else {
              logger.trace(
                  "Ignoring instanceConfig change for {} because the xid associated with it ({}) is later than current xid ({})",
                  instanceName, instanceXid, currentXid.get());
              helixClusterManagerMetrics.ignoredUpdatesCount.inc();
            }
            break;
          default:
            logger.error("Unknown InstanceConfig schema version: {}, ignoring.", schemaVersion);
        }
      }
    }

    /**
     * Triggered whenever there is a change in the list of live instances.
     * @param liveInstances the list of all live instances (not a change set) at the time of this call.
     * @param changeContext the {@link NotificationContext} associated.
     */
    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      try {
        logger.trace("Live instance change triggered from {} with: {}", dcName, liveInstances);
        updateInstanceLiveness(liveInstances);
        if (!liveStateInitialized.get()) {
          logger.info("Received initial notification for live instance change from {}", dcName);
          liveStateInitialized.set(true);
        }
        helixClusterManagerMetrics.liveInstanceChangeTriggerCount.inc();
      } catch (Throwable t) {
        errorCount.incrementAndGet();
        throw t;
      }
    }

    /**
     * Update the liveness states of existing instances based on the input.
     * @param liveInstances the list of instances that are up.
     */
    private void updateInstanceLiveness(List<LiveInstance> liveInstances) {
      synchronized (notificationLock) {
        Set<String> liveInstancesSet = new HashSet<>();
        for (LiveInstance liveInstance : liveInstances) {
          liveInstancesSet.add(liveInstance.getInstanceName());
        }
        for (String instanceName : allInstances) {
          if (liveInstancesSet.contains(instanceName)) {
            instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.AVAILABLE);
          } else {
            instanceNameToAmbryDataNode.get(instanceName).setState(HardwareState.UNAVAILABLE);
          }
        }
      }
    }

    /**
     * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
     * that partition is being constructed. If partition override is enabled, the seal state of replica is determined by
     * partition info in HelixPropertyStore, if disabled, the seal state is determined by instanceConfig.
     * @param datanode the {@link AmbryDataNode} that is being initialized.
     * @param instanceConfig the {@link InstanceConfig} associated with this datanode.
     * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
     */
    private void initializeDisksAndReplicasOnNode(AmbryDataNode datanode, InstanceConfig instanceConfig)
        throws Exception {
      ambryDataNodeToAmbryReplicas.put(datanode, new HashSet<AmbryReplica>());
      ambryDataNodeToAmbryDisks.put(datanode, new HashSet<AmbryDisk>());
      List<String> sealedReplicas = getSealedReplicas(instanceConfig);
      List<String> stoppedReplicas = getStoppedReplicas(instanceConfig);
      Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
      for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
        String mountPath = entry.getKey();
        Map<String, String> diskInfo = entry.getValue();
        long capacityBytes = Long.valueOf(diskInfo.get(DISK_CAPACITY_STR));
        HardwareState diskState =
            diskInfo.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE;
        String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);

        // Create disk
        AmbryDisk disk = new AmbryDisk(clusterMapConfig, datanode, mountPath, diskState, capacityBytes);
        ambryDataNodeToAmbryDisks.get(datanode).add(disk);

        if (!replicasStr.isEmpty()) {
          List<String> replicaInfoList = Arrays.asList(replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR));
          for (String replicaInfo : replicaInfoList) {
            String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
            // partition name and replica name are the same.
            String partitionName = info[0];
            long replicaCapacity = Long.valueOf(info[1]);
            String partitionClass = clusterMapConfig.clusterMapDefaultPartitionClass;
            if (info.length > 2) {
              partitionClass = info[2];
            }
            AmbryPartition mappedPartition =
                new AmbryPartition(Long.valueOf(partitionName), partitionClass, helixClusterManagerCallback);
            // Ensure only one AmbryPartition entry goes in to the mapping based on the name.
            AmbryPartition existing = partitionNameToAmbryPartition.putIfAbsent(partitionName, mappedPartition);
            if (existing != null) {
              mappedPartition = existing;
            }
            // mappedPartition is now the final mapped AmbryPartition object for this partition.
            synchronized (mappedPartition) {
              if (!ambryPartitionToAmbryReplicas.containsKey(mappedPartition)) {
                ambryPartitionToAmbryReplicas.put(mappedPartition,
                    Collections.newSetFromMap(new ConcurrentHashMap<>()));
                partitionMap.put(ByteBuffer.wrap(mappedPartition.getBytes()), mappedPartition);
              }
            }
            ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, datanode, replicaCapacity);
            // Create replica associated with this node.
            boolean isSealed;
            if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(
                partitionName)) {
              isSealed = partitionOverrideInfoMap.get(partitionName)
                  .get(ClusterMapUtils.PARTITION_STATE)
                  .equals(ClusterMapUtils.READ_ONLY_STR);
            } else {
              isSealed = sealedReplicas.contains(partitionName);
            }
            AmbryReplica replica =
                new AmbryReplica(clusterMapConfig, mappedPartition, disk, stoppedReplicas.contains(partitionName),
                    replicaCapacity, isSealed);
            ambryPartitionToAmbryReplicas.get(mappedPartition).add(replica);
            ambryDataNodeToAmbryReplicas.get(datanode).add(replica);
          }
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

  /**
   * Class that stores all the information associated with a datacenter.
   */
  private static class DcInfo {
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
   * A callback class used to query information from the {@link HelixClusterManager}
   */
  class HelixClusterManagerCallback implements ClusterManagerCallback {
    /**
     * Get all replica ids associated with the given {@link AmbryPartition}
     * @param partition the {@link AmbryPartition} for which to get the list of replicas.
     * @return the list of {@link AmbryReplica}s associated with the given partition.
     */
    @Override
    public List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      return new ArrayList<>(ambryPartitionToAmbryReplicas.get(partition));
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
      return new ArrayList<>(instanceNameToAmbryDataNode.values());
    }

    /**
     * @return the count of the datanodes in this cluster.
     */
    long getDatanodeCount() {
      return instanceNameToAmbryDataNode.values().size();
    }

    /**
     * @return the count of datanodes in this cluster that are down.
     */
    long getDownDatanodesCount() {
      long count = 0;
      for (AmbryDataNode datanode : instanceNameToAmbryDataNode.values()) {
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
      for (Set<AmbryDisk> disks : ambryDataNodeToAmbryDisks.values()) {
        for (AmbryDisk disk : disks) {
          if (disk.getState() == HardwareState.UNAVAILABLE) {
            count++;
          }
        }
      }
      return count;
    }

    /**
     * @return a collection of partitions in this cluster.
     */
    Collection<AmbryPartition> getPartitions() {
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
      return clusterWideRawCapacityBytes;
    }

    /**
     * @return the cluster wide allocated raw capacity in bytes.
     */
    long getAllocatedRawCapacity() {
      return clusterWideAllocatedRawCapacityBytes;
    }

    /**
     * @return the cluster wide allocated usable capacity in bytes.
     */
    long getAllocatedUsableCapacity() {
      return clusterWideAllocatedUsableCapacityBytes;
    }
  }
}


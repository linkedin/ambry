/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * A more dynamic implementation of {@link HelixClusterChangeHandler} which supports adding new nodes/partitions at
 * runtime. It is also able to absorb replica location changes in cluster.
 */
public class DynamicClusterChangeHandler implements HelixClusterChangeHandler {
  private final String dcName;
  private final Object notificationLock = new Object();
  private final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private final AtomicLong sealedStateChangeCounter;
  private final Consumer<Exception> onInitializationFailure;
  private final ClusterMapConfig clusterMapConfig;
  private final String selfInstanceName;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap;
  private final HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback;
  private final HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback;
  private final CountDownLatch routingTableInitLatch = new CountDownLatch(1);
  // A map whose key is ambry datanode and value is a map of partitionName to corresponding replica on this datanode
  // Note: the partitionName (inner map key) comes from partitionId.toString() not partitionId.toPathString()
  private final ConcurrentHashMap<AmbryDataNode, ConcurrentHashMap<String, AmbryReplica>> ambryDataNodeToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryDisk>> ambryDataNodeToAmbryDisks = new ConcurrentHashMap<>();
  private final List<ClusterMapChangeListener> clusterMapChangeListeners = new ArrayList<>();
  private final AtomicLong errorCount = new AtomicLong(0);

  private volatile boolean instanceConfigInitialized = false;
  private volatile boolean liveStateInitialized = false;
  private volatile boolean idealStateInitialized = false;
  private volatile ConcurrentHashMap<String, String> partitionNameToResource = new ConcurrentHashMap<>();
  private AtomicReference<RoutingTableSnapshot> routingTableSnapshotRef = new AtomicReference<>();
  private ConcurrentHashMap<String, AmbryDataNode> instanceNameToAmbryDataNode = new ConcurrentHashMap<>();
  private static final Logger logger = LoggerFactory.getLogger(DynamicClusterChangeHandler.class);

  /**
   * Constructor for {@link DynamicClusterChangeHandler}
   * @param clusterMapConfig the {@link ClusterMapConfig} used to define some behavior of cluster change handler.
   * @param dcName the name of data center this handler is associated with.
   * @param selfInstanceName instance name of current node.
   * @param partitionOverrideInfoMap a map that records partitions and states they should be overridden to.
   * @param helixClusterManagerCallback a call back used to query cluster-wide info.
   * @param clusterChangeHandlerCallback a call back that allows current handler to update cluster-wide info.
   * @param helixClusterManagerMetrics metrics to keep track of changes and status of {@link HelixClusterManager}.
   * @param onInitializationFailure callback to be called if initialization fails in a listener call.
   * @param sealedStateChangeCounter a counter indicating if sealed state of any partition has changed.
   */
  DynamicClusterChangeHandler(ClusterMapConfig clusterMapConfig, String dcName, String selfInstanceName,
      Map<String, Map<String, String>> partitionOverrideInfoMap,
      HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback,
      HelixClusterManager.ClusterChangeHandlerCallback clusterChangeHandlerCallback,
      HelixClusterManagerMetrics helixClusterManagerMetrics, Consumer<Exception> onInitializationFailure,
      AtomicLong sealedStateChangeCounter) {
    this.clusterMapConfig = clusterMapConfig;
    this.dcName = dcName;
    this.selfInstanceName = selfInstanceName;
    this.partitionOverrideInfoMap = partitionOverrideInfoMap;
    this.helixClusterManagerCallback = helixClusterManagerCallback;
    this.clusterChangeHandlerCallback = clusterChangeHandlerCallback;
    this.helixClusterManagerMetrics = helixClusterManagerMetrics;
    this.onInitializationFailure = onInitializationFailure;
    this.sealedStateChangeCounter = sealedStateChangeCounter;
  }

  /**
   * Handle any {@link InstanceConfig} related change in current datacenter. Several events will trigger instance config
   * change: (1) replica's seal or stop state has changed; (2) new node or new partition is added; (3) new replica is
   * added to existing node; (4) old replica is removed from existing node; (5) data node is deleted from cluster.
   * For now, {@link DynamicClusterChangeHandler} supports (1)~(4). We may consider supporting (5) in the future.
   * (The ZNode path of instance config in Helix is [AmbryClusterName]/CONFIGS/PARTICIPANT/[hostname_port])
   * @param configs all the {@link InstanceConfig}(s) in current data center. (Note that PreFetch is enabled by default
   *                in Helix, which means all instance configs under "participants" ZNode will be sent to this method)
   * @param changeContext the {@link NotificationContext} associated.
   */
  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    try {
      synchronized (notificationLock) {
        if (!instanceConfigInitialized) {
          logger.info("Received initial notification for instance config change from {}", dcName);
        } else {
          logger.info("Instance config change triggered from {}", dcName);
        }
        logger.debug("Detailed instance configs in {} are: {}", dcName, configs);
        try {
          addOrUpdateInstanceInfos(configs);
        } catch (Exception e) {
          if (!instanceConfigInitialized) {
            logger.error("Exception occurred when initializing instances in {}: ", dcName, e);
            onInitializationFailure.accept(e);
          } else {
            logger.error("Exception occurred at runtime when handling instance config changes in {}: ", dcName, e);
            helixClusterManagerMetrics.instanceConfigChangeErrorCount.inc();
          }
        } finally {
          instanceConfigInitialized = true;
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
   * Triggered whenever the IdealState in current data center has changed (for now, it is usually updated by Helix
   * Bootstrap tool).
   * @param idealState a list of {@link IdealState} that specifies ideal location of replicas.
   * @param changeContext the {@link NotificationContext} associated.
   */
  @Override
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
    if (!idealStateInitialized) {
      logger.info("Received initial notification for IdealState change from {}", dcName);
      idealStateInitialized = true;
    } else {
      logger.info("IdealState change triggered from {}", dcName);
    }
    logger.debug("Detailed ideal states in {} are: {}", dcName, idealState);
    // rebuild the entire partition-to-resource map in current dc
    ConcurrentHashMap<String, String> partitionToResourceMap = new ConcurrentHashMap<>();
    for (IdealState state : idealState) {
      String resourceName = state.getResourceName();
      state.getPartitionSet().forEach(partitionName -> partitionToResourceMap.put(partitionName, resourceName));
    }
    partitionNameToResource = partitionToResourceMap;
    helixClusterManagerMetrics.idealStateChangeTriggerCount.inc();
  }

  /**
   * Triggered whenever there is a change in the list of live instances.
   * @param liveInstances the list of all live instances (not a change set) at the time of this call.
   * @param changeContext the {@link NotificationContext} associated.
   */
  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
    try {
      if (!liveStateInitialized) {
        logger.info("Received initial notification for live instance change from {}", dcName);
        liveStateInitialized = true;
      } else {
        logger.info("Live instance change triggered from {}", dcName);
      }
      logger.debug("Detailed live instances in {} are: {}", dcName, liveInstances);
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
   * @param context additional context associated with this change.
   */
  @Override
  public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
    routingTableSnapshotRef.getAndSet(routingTableSnapshot);
    if (routingTableInitLatch.getCount() == 1) {
      logger.info("Received initial notification for routing table change from {}", dcName);
      routingTableInitLatch.countDown();
    } else {
      logger.info("Routing table change triggered from {}", dcName);
    }
    helixClusterManagerMetrics.routingTableChangeTriggerCount.inc();
  }

  @Override
  public void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot) {
    routingTableSnapshotRef.getAndSet(routingTableSnapshot);
  }

  @Override
  public RoutingTableSnapshot getRoutingTableSnapshot() {
    return routingTableSnapshotRef.get();
  }

  @Override
  public Map<AmbryDataNode, Set<AmbryDisk>> getDataNodeToDisksMap() {
    return Collections.unmodifiableMap(ambryDataNodeToAmbryDisks);
  }

  @Override
  public AmbryDataNode getDataNode(String instanceName) {
    return instanceNameToAmbryDataNode.get(instanceName);
  }

  @Override
  public AmbryReplica getReplicaId(AmbryDataNode ambryDataNode, String partitionName) {
    return ambryDataNodeToAmbryReplicas.getOrDefault(ambryDataNode, new ConcurrentHashMap<>()).get(partitionName);
  }

  @Override
  public List<AmbryReplica> getReplicaIds(AmbryDataNode ambryDataNode) {
    return new ArrayList<>(ambryDataNodeToAmbryReplicas.get(ambryDataNode).values());
  }

  @Override
  public List<AmbryDataNode> getAllDataNodes() {
    return new ArrayList<>(instanceNameToAmbryDataNode.values());
  }

  @Override
  public Set<AmbryDisk> getDisks(AmbryDataNode ambryDataNode) {
    return ambryDataNodeToAmbryDisks.get(ambryDataNode);
  }

  @Override
  public Map<String, String> getPartitionToResourceMap() {
    return Collections.unmodifiableMap(partitionNameToResource);
  }

  @Override
  public long getErrorCount() {
    return errorCount.get();
  }

  @Override
  public void waitForInitNotification() throws InterruptedException {
    // wait slightly more than 5 mins to ensure routerUpdater refreshes the snapshot.
    if (!routingTableInitLatch.await(320, TimeUnit.SECONDS)) {
      throw new IllegalStateException("Initial routing table change from " + dcName + " didn't come within 5 mins");
    }
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    clusterMapChangeListeners.add(clusterMapChangeListener);
  }

  /**
   * Add new instance or update existing instance based on {@link InstanceConfig}(s). This may also invoke callbacks in
   * some clustermap change listeners (i.e. {@link PartitionSelectionHelper}, ReplicationManager)
   * @param instanceConfigs the {@link InstanceConfig}(s) used to update in-mem cluster map.
   * @throws Exception
   */
  private void addOrUpdateInstanceInfos(List<InstanceConfig> instanceConfigs) throws Exception {
    List<ReplicaId> totalAddedReplicas = new ArrayList<>();
    List<ReplicaId> totalRemovedReplicas = new ArrayList<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      int schemaVersion = getSchemaVersion(instanceConfig);
      if (schemaVersion != 0) {
        logger.error("Unknown InstanceConfig schema version: {}, ignoring.", schemaVersion);
        continue;
      }
      Pair<List<ReplicaId>, List<ReplicaId>> addedAndRemovedReplicas;
      if (instanceNameToAmbryDataNode.containsKey(instanceConfig.getInstanceName())) {
        addedAndRemovedReplicas = updateInstanceInfo(instanceConfig);
      } else {
        addedAndRemovedReplicas = new Pair<>(createNewInstance(instanceConfig), new ArrayList<>());
      }
      totalAddedReplicas.addAll(addedAndRemovedReplicas.getFirst());
      totalRemovedReplicas.addAll(addedAndRemovedReplicas.getSecond());
    }
    // if this is not initial InstanceConfig change and any replicas are added or removed, we should invoke callbacks
    // for different clustermap change listeners (i.e replication manager, partition selection helper)
    if (instanceConfigInitialized && (!totalAddedReplicas.isEmpty() || !totalRemovedReplicas.isEmpty())) {
      for (ClusterMapChangeListener listener : clusterMapChangeListeners) {
        listener.onReplicaAddedOrRemoved(totalAddedReplicas, totalRemovedReplicas);
      }
    }
  }

  /**
   * Update info of an existing instance. This may happen in following cases: (1) new replica is added; (2) old replica
   * is removed; (3) replica's state has changed (i.e. becomes seal/unseal).
   * @param instanceConfig the {@link InstanceConfig} used to update info of instance.
   * @return a pair of lists: (1) new added replicas; (2) removed old replicas, during this update.
   */
  private Pair<List<ReplicaId>, List<ReplicaId>> updateInstanceInfo(InstanceConfig instanceConfig) throws Exception {
    final List<ReplicaId> addedReplicas = new ArrayList<>();
    final List<ReplicaId> removedReplicas = new ArrayList<>();
    String instanceName = instanceConfig.getInstanceName();
    logger.info("Updating replicas info for existing node {}", instanceName);
    List<String> sealedReplicas = getSealedReplicas(instanceConfig);
    List<String> stoppedReplicas = getStoppedReplicas(instanceConfig);
    AmbryDataNode dataNode = instanceNameToAmbryDataNode.get(instanceName);
    ConcurrentHashMap<String, AmbryReplica> currentReplicasOnNode = ambryDataNodeToAmbryReplicas.get(dataNode);
    ConcurrentHashMap<String, AmbryReplica> replicasFromInstanceConfig = new ConcurrentHashMap<>();
    Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
    Map<String, AmbryDisk> mountPathToDisk = ambryDataNodeToAmbryDisks.get(dataNode)
        .stream()
        .collect(Collectors.toMap(AmbryDisk::getMountPath, disk -> disk));
    Map<AmbryPartition, List<AmbryReplica>> replicaToAddByPartition = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
      String mountPath = entry.getKey();
      Map<String, String> diskInfo = entry.getValue();
      AmbryDisk disk = mountPathToDisk.getOrDefault(mountPath, null);
      if (disk == null) {
        logger.warn("{} is a new disk or unrecognizable disk which is not supported on existing node {}.", mountPath,
            instanceName);
        // TODO support dynamically adding disk in the future
        continue;
      }
      String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);
      if (!replicasStr.isEmpty()) {
        for (String replicaInfo : replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR)) {
          String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
          // partition name and replica name are the same.
          String partitionName = info[0];
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
            long replicaCapacity = Long.parseLong(info[1]);
            String partitionClass = info.length > 2 ? info[2] : clusterMapConfig.clusterMapDefaultPartitionClass;
            // this can be a brand new partition that is added to an existing node
            AmbryPartition mappedPartition =
                new AmbryPartition(Long.parseLong(partitionName), partitionClass, helixClusterManagerCallback);
            // Ensure only one AmbryPartition instance exists for specific partition.
            mappedPartition = clusterChangeHandlerCallback.addPartitionIfAbsent(mappedPartition, replicaCapacity);
            ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, dataNode, replicaCapacity);
            // create new replica belonging to this partition or find the existing replica from bootstrapReplicas map.
            AmbryReplica replica;
            if (selfInstanceName.equals(instanceName)) {
              // if this is a newly added replica on current instance, it should be present in bootstrapReplicas map.
              replica = clusterChangeHandlerCallback.fetchBootstrapReplica(mappedPartition.toPathString());
              if (replica == null) {
                logger.error("Replica {} is not present in bootstrap replica set, abort instance info update",
                    mappedPartition.toPathString());
                throw new IllegalStateException("Replica to add is not present in bootstrap replica map");
              }
            } else {
              replica = new AmbryServerReplica(clusterMapConfig, mappedPartition, disk,
                  stoppedReplicas.contains(partitionName), replicaCapacity, sealedReplicas.contains(partitionName));
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
    }
    // update "ambryDataNodeToAmbryReplicas" map and "addedReplicas" list
    replicaToAddByPartition.forEach((k, v) -> {
      clusterChangeHandlerCallback.addReplicasToPartition(k, v);
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
          clusterChangeHandlerCallback.removeReplicasFromPartition(ambryReplica.getPartitionId(),
              Collections.singletonList(ambryReplica));
          removedReplicas.add(ambryReplica);
        });
    return new Pair<>(addedReplicas, removedReplicas);
  }

  /**
   * If partition override is enabled, we override replica SEAL/UNSEAL state based on partitionOverrideMap. If disabled,
   * update replica state according to the info from {@link InstanceConfig}.
   * @param replica the {@link ReplicaId} whose states (seal,stop) should be updated.
   * @param sealedReplicas a list of {@link ReplicaId}(s) that are in SEALED state.
   * @param stoppedReplicas a list of {@link ReplicaId}(s) that are in STOPPED state.
   */
  private void updateReplicaStateAndOverrideIfNeeded(AmbryReplica replica, List<String> sealedReplicas,
      List<String> stoppedReplicas) {
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
   * @param instanceConfig the {@link InstanceConfig} to create new instance
   * @return a list of newly added replicas;
   * @throws Exception
   */
  private List<ReplicaId> createNewInstance(InstanceConfig instanceConfig) throws Exception {
    String instanceName = instanceConfig.getInstanceName();
    logger.info("Adding node {} and its disks and replicas in {}", instanceName, dcName);
    AmbryDataNode datanode =
        new AmbryServerDataNode(getDcName(instanceConfig), clusterMapConfig, instanceConfig.getHostName(),
            Integer.parseInt(instanceConfig.getPort()), getRackId(instanceConfig), getSslPortStr(instanceConfig),
            getHttp2PortStr(instanceConfig), getXid(instanceConfig), helixClusterManagerCallback);
    // for new instance, we first set it to unavailable and rely on its participation to update its liveness
    if (!instanceName.equals(selfInstanceName)) {
      datanode.setState(HardwareState.UNAVAILABLE);
    }
    List<ReplicaId> addedReplicas = initializeDisksAndReplicasOnNode(datanode, instanceConfig);
    instanceNameToAmbryDataNode.put(instanceName, datanode);
    return addedReplicas;
  }

  /**
   * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
   * that partition is being constructed. If partition override is enabled, the seal state of replica is determined by
   * partition info in HelixPropertyStore, if disabled, the seal state is determined by instanceConfig.
   * @param datanode the {@link AmbryDataNode} that is being initialized.
   * @param instanceConfig the {@link InstanceConfig} associated with this datanode.
   * @return a list of newly added replicas on this node.
   * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
   */
  private List<ReplicaId> initializeDisksAndReplicasOnNode(AmbryDataNode datanode, InstanceConfig instanceConfig)
      throws Exception {
    List<ReplicaId> addedReplicas = new ArrayList<>();
    List<String> sealedReplicas = getSealedReplicas(instanceConfig);
    List<String> stoppedReplicas = getStoppedReplicas(instanceConfig);
    ambryDataNodeToAmbryReplicas.put(datanode, new ConcurrentHashMap<>());
    ambryDataNodeToAmbryDisks.put(datanode, new HashSet<>());
    Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
      String mountPath = entry.getKey();
      Map<String, String> diskInfo = entry.getValue();
      if (diskInfo.get(DISK_STATE) == null) {
        // This may happen when Helix controller adds partitions in ERROR state to InstanceConfig. (The additional string
        // is not recognizable for current HelixClusterManager)
        logger.warn("{} is invalid disk info on {}. Skip it and continue on next one.", mountPath,
            instanceConfig.getInstanceName());
        continue;
      }
      HardwareState diskState =
          diskInfo.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE;
      long capacityBytes = Long.parseLong(diskInfo.get(DISK_CAPACITY_STR));

      // Create disk
      AmbryDisk disk = new AmbryDisk(clusterMapConfig, datanode, mountPath, diskState, capacityBytes);
      ambryDataNodeToAmbryDisks.get(datanode).add(disk);
      clusterChangeHandlerCallback.addClusterWideRawCapacity(capacityBytes);

      String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);
      if (!replicasStr.isEmpty()) {
        for (String replicaInfo : replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR)) {
          String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
          // partition name and replica name are the same.
          String partitionName = info[0];
          long replicaCapacity = Long.parseLong(info[1]);
          String partitionClass = info.length > 2 ? info[2] : clusterMapConfig.clusterMapDefaultPartitionClass;

          AmbryPartition mappedPartition =
              new AmbryPartition(Long.parseLong(partitionName), partitionClass, helixClusterManagerCallback);
          // Ensure only one AmbryPartition instance exists for specific partition.
          mappedPartition = clusterChangeHandlerCallback.addPartitionIfAbsent(mappedPartition, replicaCapacity);
          ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, datanode, replicaCapacity);
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
                  replicaCapacity, isSealed);
          ambryDataNodeToAmbryReplicas.get(datanode).put(mappedPartition.toPathString(), replica);
          clusterChangeHandlerCallback.addReplicasToPartition(mappedPartition, Collections.singletonList(replica));
          addedReplicas.add(replica);
        }
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
    for (String instanceName : instanceNameToAmbryDataNode.keySet()) {
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
    for (AmbryReplica replica : helixClusterManagerCallback.getReplicaIdsForPartition(partition)) {
      if (replica.getDataNodeId().equals(datanode)) {
        throw new IllegalStateException("Replica already exists on " + datanode + " for " + partition);
      } else if (replica.getCapacityInBytes() != expectedReplicaCapacity) {
        throw new IllegalStateException("Expected replica capacity " + expectedReplicaCapacity + " is different from "
            + "the capacity of an existing replica " + replica.getCapacityInBytes());
      }
    }
  }
}

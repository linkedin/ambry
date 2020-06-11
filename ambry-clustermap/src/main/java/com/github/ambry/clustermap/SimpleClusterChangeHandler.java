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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link HelixClusterChangeHandler} to register as listener for Helix related changes in each
 * datacenter. This class is also responsible for handling events received.
 */
public class SimpleClusterChangeHandler implements HelixClusterChangeHandler {
  private final String dcName;
  private final Object notificationLock = new Object();
  private final AtomicBoolean instanceConfigInitialized = new AtomicBoolean(false);
  private final AtomicBoolean liveStateInitialized = new AtomicBoolean(false);
  private final AtomicBoolean idealStateInitialized = new AtomicBoolean(false);
  private final HelixClusterManagerMetrics helixClusterManagerMetrics;
  private final AtomicLong sealedStateChangeCounter;
  private final Consumer<Exception> onInitializationFailure;
  private final ClusterMapConfig clusterMapConfig;
  private final String selfInstanceName;
  private final AtomicLong currentXid;
  private final Map<String, Map<String, String>> partitionOverrideInfoMap;
  private final ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap;
  private final ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition;
  private final ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas;
  private final HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback;
  private final CountDownLatch routingTableInitLatch = new CountDownLatch(1);

  private volatile ConcurrentHashMap<String, String> partitionNameToResource = new ConcurrentHashMap<>();
  private AtomicReference<RoutingTableSnapshot> routingTableSnapshotRef = new AtomicReference<>();
  private ConcurrentHashMap<String, AmbryDataNode> instanceNameToAmbryDataNode = new ConcurrentHashMap<>();
  // A map whose key is ambry datanode and value is a map of partitionId to corresponding replica associated with this datanode
  // Note: the partitionName (inner map key) comes from partitionId.toString() not partitionId.toPathString()
  private final ConcurrentHashMap<AmbryDataNode, ConcurrentHashMap<String, AmbryReplica>> ambryDataNodeToAmbryReplicas =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<AmbryDataNode, Set<AmbryDisk>> ambryDataNodeToAmbryDisks = new ConcurrentHashMap<>();
  private final AtomicLong errorCount = new AtomicLong(0);

  private static final Logger logger = LoggerFactory.getLogger(SimpleClusterChangeHandler.class);

  /**
   * Initialize a ClusterChangeHandler in the given datacenter.
   * @param clusterMapConfig {@link ClusterMapConfig} to help some admin operations
   * @param dcName the name of dc this {@link HelixClusterChangeHandler} associates with
   * @param selfInstanceName the name of instance on which {@link HelixClusterManager} resides.
   * @param partitionOverrideInfoMap a map specifying partitions whose state should be overridden.
   * @param partitionMap a map from serialized bytes to corresponding partition.
   * @param partitionNameToAmbryPartition a map from partition name to {@link AmbryPartition} object.
   * @param ambryPartitionToAmbryReplicas a map from {@link AmbryPartition} to its replicas.
   * @param helixClusterManagerCallback a help class to get cluster state from all DCs.
   * @param helixClusterManagerMetrics metrics that help track of cluster changes and infos.
   * @param onInitializationFailure callback to be called if initialization fails in a listener call.
   * @param sealedStateChangeCounter a counter that records event when replica is sealed or unsealed
   */
  SimpleClusterChangeHandler(ClusterMapConfig clusterMapConfig, String dcName, String selfInstanceName,
      Map<String, Map<String, String>> partitionOverrideInfoMap,
      ConcurrentHashMap<ByteBuffer, AmbryPartition> partitionMap,
      ConcurrentHashMap<String, AmbryPartition> partitionNameToAmbryPartition,
      ConcurrentHashMap<AmbryPartition, Set<AmbryReplica>> ambryPartitionToAmbryReplicas,
      HelixClusterManager.HelixClusterManagerCallback helixClusterManagerCallback,
      HelixClusterManagerMetrics helixClusterManagerMetrics, Consumer<Exception> onInitializationFailure,
      AtomicLong sealedStateChangeCounter) {
    this.clusterMapConfig = clusterMapConfig;
    this.dcName = dcName;
    this.selfInstanceName = selfInstanceName;
    this.partitionOverrideInfoMap = partitionOverrideInfoMap;
    this.partitionMap = partitionMap;
    this.partitionNameToAmbryPartition = partitionNameToAmbryPartition;
    this.ambryPartitionToAmbryReplicas = ambryPartitionToAmbryReplicas;
    this.helixClusterManagerCallback = helixClusterManagerCallback;
    this.helixClusterManagerMetrics = helixClusterManagerMetrics;
    this.onInitializationFailure = onInitializationFailure;
    this.sealedStateChangeCounter = sealedStateChangeCounter;
    currentXid = new AtomicLong(clusterMapConfig.clustermapCurrentXid);
  }

  @Override
  public void onDataNodeConfigChange(Iterable<DataNodeConfig> configs) {
    try {
      logger.debug("InstanceConfig change triggered in {} with: {}", dcName, configs);
      synchronized (notificationLock) {
        if (!instanceConfigInitialized.get()) {
          logger.info("Received initial notification for instance config change from {}", dcName);
          try {
            initializeInstances(configs);
          } catch (Exception e) {
            logger.error("Exception occurred when initializing instances in {}: ", dcName, e);
            onInitializationFailure.accept(e);
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
   * Triggered whenever the IdealState in current data center has changed (for now, it is usually updated by Helix
   * Bootstrap tool).
   * @param idealState a list of {@link IdealState} that specifies ideal location of replicas.
   * @param changeContext the {@link NotificationContext} associated.
   * @throws InterruptedException
   */
  @Override
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext)
      throws InterruptedException {
    if (!idealStateInitialized.get()) {
      logger.info("Received initial notification for IdealState change from {}", dcName);
      idealStateInitialized.set(true);
    } else {
      logger.info("IdealState change triggered from {}", dcName);
    }
    // rebuild the entire partition-to-resource map in current dc
    ConcurrentHashMap<String, String> newPartitionToResourceMap = new ConcurrentHashMap<>();
    for (IdealState state : idealState) {
      String resourceName = state.getResourceName();
      for (String partitionStr : state.getPartitionSet()) {
        newPartitionToResourceMap.put(partitionStr, resourceName);
      }
    }
    partitionNameToResource = newPartitionToResourceMap;
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
      logger.debug("Live instance change triggered from {} with: {}", dcName, liveInstances);
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
   * Triggered whenever the state of replica in cluster has changed. The snapshot contains up-to-date state of all
   * resources(replicas) in this data center.
   * @param routingTableSnapshot a snapshot of routing table for this data center.
   * @param context additional context associated with this change.
   */
  @Override
  public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
    if (routingTableInitLatch.getCount() == 1) {
      logger.info("Received initial notification for routing table change from {}", dcName);
      routingTableSnapshotRef.getAndSet(routingTableSnapshot);
      routingTableInitLatch.countDown();
    } else {
      logger.info("Routing table change triggered from {}", dcName);
      routingTableSnapshotRef.getAndSet(routingTableSnapshot);
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
    // no-op for SimpleClusterChangeHandler because it doesn't supporting adding/removing replicas dynamically
  }

  /**
   * Populate the initial data from the admin connection. Create nodes, disks, partitions and replicas for the entire
   * cluster. A {@link DataNodeConfig} will only be looked at if the xid in it is <= currentXid.
   * @param dataNodeConfigs the list of {@link DataNodeConfig}s containing the information about the sealed states of
   *                        replicas.
   * @throws Exception if creation of {@link AmbryDataNode}s or {@link AmbryDisk}s throw an Exception.
   */
  private void initializeInstances(Iterable<DataNodeConfig> dataNodeConfigs) throws Exception {
    logger.info("Initializing cluster information from {}", dcName);
    for (DataNodeConfig dataNodeConfig : dataNodeConfigs) {
      String instanceName = dataNodeConfig.getInstanceName();
      long instanceXid = dataNodeConfig.getXid();
      if (instanceName.equals(selfInstanceName) || instanceXid <= currentXid.get()) {
        logger.info("Adding node {} and its disks and replicas", instanceName);
        AmbryDataNode datanode =
            new AmbryServerDataNode(dataNodeConfig.getDatacenterName(), clusterMapConfig, dataNodeConfig.getHostName(),
                dataNodeConfig.getPort(), dataNodeConfig.getRackId(), dataNodeConfig.getSslPort(),
                dataNodeConfig.getHttp2Port(), instanceXid, helixClusterManagerCallback);
        initializeDisksAndReplicasOnNode(datanode, dataNodeConfig);
        instanceNameToAmbryDataNode.put(instanceName, datanode);
      } else {
        logger.info(
            "Ignoring instanceConfig for {} because the xid associated with it ({}) is later than current xid ({})",
            instanceName, instanceXid, currentXid.get());
        helixClusterManagerMetrics.ignoredUpdatesCount.inc();
      }
    }
    logger.info("Initialized cluster information from {}", dcName);
  }

  /**
   * Go over the given list of {@link DataNodeConfig}s and update the both sealed and stopped states of replicas.
   * A {@link DataNodeConfig} will only be looked at if the xid in it is <= currentXid.
   * @param dataNodeConfigs the list of {@link DataNodeConfig}s containing the up-to-date information about the
   *                        sealed states of replicas.
   */
  private void updateStateOfReplicas(Iterable<DataNodeConfig> dataNodeConfigs) {
    for (DataNodeConfig dataNodeConfig : dataNodeConfigs) {
      String instanceName = dataNodeConfig.getInstanceName();
      long instanceXid = dataNodeConfig.getXid();
      AmbryDataNode node = instanceNameToAmbryDataNode.get(instanceName);
      if (instanceName.equals(selfInstanceName) || instanceXid <= currentXid.get()) {
        if (node == null) {
          logger.trace("Dynamic addition of new nodes is not yet supported, ignoring InstanceConfig {}",
              dataNodeConfig);
        } else {
          Set<String> sealedReplicas = dataNodeConfig.getSealedReplicas();
          Set<String> stoppedReplicas = dataNodeConfig.getStoppedReplicas();
          for (AmbryReplica replica : ambryDataNodeToAmbryReplicas.get(node).values()) {
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
  }

  /**
   * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
   * that partition is being constructed. If partition override is enabled, the seal state of replica is determined by
   * partition info in HelixPropertyStore, if disabled, the seal state is determined by {@code dataNodeConfig}.
   * @param datanode the {@link AmbryDataNode} that is being initialized.
   * @param dataNodeConfig the {@link DataNodeConfig} associated with this datanode.
   * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
   */
  private void initializeDisksAndReplicasOnNode(AmbryDataNode datanode, DataNodeConfig dataNodeConfig)
      throws Exception {
    ambryDataNodeToAmbryReplicas.put(datanode, new ConcurrentHashMap<>());
    ambryDataNodeToAmbryDisks.put(datanode, new HashSet<>());
    Set<String> sealedReplicas = dataNodeConfig.getSealedReplicas();
    Set<String> stoppedReplicas = dataNodeConfig.getStoppedReplicas();
    for (Map.Entry<String, DataNodeConfig.DiskConfig> diskEntry : dataNodeConfig.getDiskConfigs().entrySet()) {
      String mountPath = diskEntry.getKey();
      DataNodeConfig.DiskConfig diskConfig = diskEntry.getValue();

      // Create disk
      AmbryDisk disk =
          new AmbryDisk(clusterMapConfig, datanode, mountPath, diskConfig.getState(), diskConfig.getDiskCapacityInBytes());
      ambryDataNodeToAmbryDisks.get(datanode).add(disk);
      for (Map.Entry<String, DataNodeConfig.ReplicaConfig> replicaEntry : diskConfig.getReplicaConfigs().entrySet()) {
        String partitionName = replicaEntry.getKey();
        DataNodeConfig.ReplicaConfig replicaConfig = replicaEntry.getValue();

        AmbryPartition mappedPartition =
            new AmbryPartition(Long.parseLong(partitionName), replicaConfig.getPartitionClass(),
                helixClusterManagerCallback);
        // Ensure only one AmbryPartition entry goes in to the mapping based on the name.
        AmbryPartition existing = partitionNameToAmbryPartition.putIfAbsent(partitionName, mappedPartition);
        if (existing != null) {
          mappedPartition = existing;
        }
        // mappedPartition is now the final mapped AmbryPartition object for this partition.
        synchronized (mappedPartition) {
          if (!ambryPartitionToAmbryReplicas.containsKey(mappedPartition)) {
            ambryPartitionToAmbryReplicas.put(mappedPartition, ConcurrentHashMap.newKeySet());
            partitionMap.put(ByteBuffer.wrap(mappedPartition.getBytes()), mappedPartition);
          }
        }
        ensurePartitionAbsenceOnNodeAndValidateCapacity(mappedPartition, datanode, replicaConfig.getReplicaCapacityInBytes());
        // Create replica associated with this node.
        boolean isSealed;
        if (clusterMapConfig.clusterMapEnablePartitionOverride && partitionOverrideInfoMap.containsKey(partitionName)) {
          isSealed = partitionOverrideInfoMap.get(partitionName)
              .get(ClusterMapUtils.PARTITION_STATE)
              .equals(ClusterMapUtils.READ_ONLY_STR);
        } else {
          isSealed = sealedReplicas.contains(partitionName);
        }
        AmbryReplica replica =
            new AmbryServerReplica(clusterMapConfig, mappedPartition, disk, stoppedReplicas.contains(partitionName),
                replicaConfig.getReplicaCapacityInBytes(), isSealed);
        ambryPartitionToAmbryReplicas.get(mappedPartition).add(replica);
        ambryDataNodeToAmbryReplicas.get(datanode).put(mappedPartition.toPathString(), replica);
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

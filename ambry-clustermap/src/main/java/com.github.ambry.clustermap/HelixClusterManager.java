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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An implementation of {@link ClusterMap} that makes use of Helix to dynamically manage the cluster information.
 *
 * @see <a href="http://helix.apache.org">http://helix.apache.org</a>
 */
class HelixClusterManager implements ClusterMap {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String clusterName;
  private final MetricRegistry metricRegistry;
  private final ClusterMapConfig clusterMapConfig;
  private final Map<String, DcZkInfo> dcToDcZkInfo = new HashMap<>();
  private final Map<String, AmbryPartition> partitionNameToPartitionIdMap = new ConcurrentHashMap<>();
  private final Map<String, AmbryDataNode> instanceNameToDataNodeIdMap = new ConcurrentHashMap<>();
  private final Map<AmbryPartition, Set<AmbryReplica>> partitionIdToReplicaIds = new ConcurrentHashMap<>();
  private final Map<AmbryDataNode, Set<AmbryReplica>> dataNodeIdToReplicaIds = new ConcurrentHashMap<>();
  private final Map<AmbryDataNode, Set<AmbryDisk>> dataNodeIdToDiskIds = new ConcurrentHashMap<>();
  private final Map<ByteBuffer, AmbryPartition> partitionMap = new ConcurrentHashMap<>();
  private final HelixClusterManagerCallback helixClusterManagerCallback;
  private long clusterWideRawCapacityBytes;
  private long clusterWideAllocatedRawCapacityBytes;
  private long clusterWiseAllocatedUsableCapacityBytes;
  final HelixClusterManagerMetrics helixClusterManagerMetrics;

  /**
   * Instantiate a HelixClusterManager.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this manager.
   * @param instanceName the String representation of the instance associated with this manager.
   * @throws IOException if there is an error in parsing the clusterMapConfig or in connecting with the associated
   *                     remote Zookeeper services.
   */
  HelixClusterManager(ClusterMapConfig clusterMapConfig, String instanceName) throws IOException {
    this.clusterMapConfig = clusterMapConfig;
    clusterName = clusterMapConfig.clusterMapClusterName;
    metricRegistry = new MetricRegistry();
    helixClusterManagerCallback = new HelixClusterManagerCallback();
    helixClusterManagerMetrics = new HelixClusterManagerMetrics(metricRegistry, helixClusterManagerCallback);
    try {
      Map<String, String> dataCenterToZkAddress =
          parseZkJsonAndPopulateZkInfo(clusterMapConfig.clusterMapDcsZkConnectStrings);
      for (Map.Entry<String, String> entry : dataCenterToZkAddress.entrySet()) {
        String dcName = entry.getKey();
        String zkConnectStr = entry.getValue();
        HelixManager manager =
            HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
        manager.connect();
        ClusterChangeListener clusterChangeListener = new ClusterChangeListener();
        DcZkInfo dcZkInfo = new DcZkInfo(dcName, zkConnectStr, manager, clusterChangeListener);
        dcToDcZkInfo.put(dcName, dcZkInfo);
      }
      // Now initialize by pulling information from Helix.
      initialize();
      // Now register listeners to get notified on change.
      for (DcZkInfo dcZkInfo : dcToDcZkInfo.values()) {
        dcZkInfo.helixManager.addExternalViewChangeListener(dcZkInfo.clusterChangeListener);
        dcZkInfo.helixManager.addConfigChangeListener(dcZkInfo.clusterChangeListener);
        dcZkInfo.helixManager.addLiveInstanceChangeListener(dcZkInfo.clusterChangeListener);
        dcZkInfo.clusterChangeListener.waitForInitialization();
      }
    } catch (Exception e) {
      helixClusterManagerMetrics.initializeInstantiationMetric(false);
      throw new IOException("Encountered exception while parsing json, connecting to Helix or initializing", e);
    }
    helixClusterManagerMetrics.initializeInstantiationMetric(true);
    helixClusterManagerMetrics.initializeDatacenterMetrics();
    helixClusterManagerMetrics.initializeDataNodeMetrics();
    helixClusterManagerMetrics.initializeDiskMetrics();
    helixClusterManagerMetrics.initializePartitionMetrics();
    helixClusterManagerMetrics.initializeCapacityMetrics();
  }

  /**
   * Populate the initial data from the admin connection. Create nodes, disks, partitions and replicas for the entire
   * cluster.
   * @throws Exception if creation of {@link AmbryDataNode}s or {@link AmbryDisk}s throw an Exception.
   */
  private void initialize() throws Exception {
    for (DcZkInfo dcZkInfo : dcToDcZkInfo.values()) {
      HelixAdmin admin = dcZkInfo.helixManager.getClusterManagmentTool();
      for (String instanceName : admin.getInstancesInCluster(clusterName)) {
        InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
        AmbryDataNode datanode =
            new AmbryDataNode(dcZkInfo.dcName, clusterMapConfig, instanceConfig.getHostName(), instanceConfig.getPort(),
                getRackIdStr(instanceConfig), getSslPortStr(instanceConfig));
        initializeDisksAndReplicasOnNode(datanode, instanceConfig);
        instanceNameToDataNodeIdMap.put(instanceName, datanode);
        dcZkInfo.clusterChangeListener.allInstances.add(instanceName);
      }
    }
    for (Set<AmbryDisk> disks : dataNodeIdToDiskIds.values()) {
      for (AmbryDisk disk : disks) {
        clusterWideRawCapacityBytes += disk.getRawCapacityInBytes();
      }
    }
    for (Set<AmbryReplica> partitionReplicas : partitionIdToReplicaIds.values()) {
      long replicaCapacity = partitionReplicas.iterator().next().getCapacityInBytes();
      clusterWideAllocatedRawCapacityBytes += replicaCapacity * partitionReplicas.size();
      clusterWiseAllocatedUsableCapacityBytes += replicaCapacity;
    }
  }

  /**
   * Initialize the disks and replicas on the given node. Create partitions if this is the first time a replica of
   * that partition is being constructed.
   * @param datanode the {@link AmbryDataNode} that is being initialized.
   * @param instanceConfig the {@link InstanceConfig} associated with this datanode.
   * @throws Exception if creation of {@link AmbryDisk} throws an Exception.
   */
  private void initializeDisksAndReplicasOnNode(AmbryDataNode datanode, InstanceConfig instanceConfig)
      throws Exception {
    dataNodeIdToReplicaIds.put(datanode, new HashSet<AmbryReplica>());
    dataNodeIdToDiskIds.put(datanode, new HashSet<AmbryDisk>());
    List<String> sealedReplicas = getSealedReplicas(instanceConfig);
    Map<String, Map<String, String>> diskInfos = instanceConfig.getRecord().getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : diskInfos.entrySet()) {
      String mountPath = entry.getKey();
      Map<String, String> diskInfo = entry.getValue();
      long capacityBytes = Long.valueOf(diskInfo.get(DISK_CAPACITY_STR));
      HardwareState state =
          diskInfo.get(DISK_STATE).equals(AVAILABLE_STR) ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE;
      String replicasStr = diskInfo.get(ClusterMapUtils.REPLICAS_STR);

      // Create disk
      AmbryDisk disk = new AmbryDisk(clusterMapConfig, datanode, mountPath, state, capacityBytes);
      dataNodeIdToDiskIds.get(datanode).add(disk);

      if (!replicasStr.isEmpty()) {
        List<String> replicaInfoList = Arrays.asList(replicasStr.split(ClusterMapUtils.REPLICAS_DELIM_STR));
        for (String replicaInfo : replicaInfoList) {
          String[] info = replicaInfo.split(ClusterMapUtils.REPLICAS_STR_SEPARATOR);
          // partition name and replica name are the same.
          String partitionName = info[0];
          long replicaCapacity = Long.valueOf(info[1]);
          AmbryPartition partition = partitionNameToPartitionIdMap.get(partitionName);
          if (partition == null) {
            // Create partition
            partition = new AmbryPartition(Long.valueOf(partitionName), helixClusterManagerCallback);
            partitionNameToPartitionIdMap.put(partitionName, partition);
            partitionIdToReplicaIds.put(partition, new HashSet<AmbryReplica>());
            partitionMap.put(ByteBuffer.wrap(partition.getBytes()), partition);
          } else {
            ensurePartitionAbsenceOnNodeAndValidateCapacity(partition, datanode, replicaCapacity);
          }
          if (sealedReplicas.contains(partitionName)) {
            partition.setState(PartitionState.READ_ONLY);
          }
          // Create replica
          AmbryReplica replica = new AmbryReplica(partition, disk, replicaCapacity);
          partitionIdToReplicaIds.get(partition).add(replica);
          dataNodeIdToReplicaIds.get(datanode).add(replica);
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
    for (AmbryReplica replica : partitionIdToReplicaIds.get(partition)) {
      if (replica.getDataNodeId().equals(datanode)) {
        throw new IllegalStateException("Replica already exists on " + datanode + " for " + partition);
      } else if (replica.getCapacityInBytes() != expectedReplicaCapacity) {
        throw new IllegalStateException("Expected replica capacity " + expectedReplicaCapacity + " is different from "
            + "the capacity of an existing replica " + replica.getCapacityInBytes());
      }
    }
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return dcToDcZkInfo.containsKey(datacenterName);
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return instanceNameToDataNodeIdMap.get(getInstanceName(hostname, port));
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    if (!(dataNodeId instanceof AmbryDataNode)) {
      throw new IllegalStateException("Incompatible type passed in");
    }
    AmbryDataNode datanode = (AmbryDataNode) dataNodeId;
    return new ArrayList<>(dataNodeIdToReplicaIds.get(datanode));
  }

  @Override
  public List<? extends DataNodeId> getDataNodeIds() {
    return new ArrayList<>(instanceNameToDataNodeIdMap.values());
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
        ((AmbryDataNode) replica.getDataNodeId()).onNodeResponse();
        break;
      case Node_Timeout:
        ((AmbryDataNode) replica.getDataNodeId()).onNodeTimeout();
        break;
      case Disk_Error:
        ((AmbryDisk) replica.getDiskId()).onDiskError();
        break;
      case Disk_Ok:
        ((AmbryDisk) replica.getDiskId()).onDiskOk();
        break;
      case Partition_ReadOnly:
        ((AmbryPartition) replica.getPartitionId()).onPartitionReadOnly();
        break;
    }
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

  /**
   * @return list of partition ids that are in {@link PartitionState#READ_WRITE}.
   */
  @Override
  public List<PartitionId> getWritablePartitionIds() {
    List<PartitionId> writablePartitions = new ArrayList<>();
    List<PartitionId> healthyWritablePartitions = new ArrayList<>();
    for (AmbryPartition partition : partitionNameToPartitionIdMap.values()) {
      if (partition.getPartitionState() == PartitionState.READ_WRITE) {
        writablePartitions.add(partition);
        if (areAllReplicasForPartitionUp(partition)) {
          healthyWritablePartitions.add(partition);
        }
      }
    }
    return healthyWritablePartitions.isEmpty() ? writablePartitions : healthyWritablePartitions;
  }

  /**
   * Check whether all replicas of the given {@link AmbryPartition} are up.
   * @param partition the {@link AmbryPartition} to check.
   * @return true if all associated replicas are up; false otherwise.
   */
  private boolean areAllReplicasForPartitionUp(AmbryPartition partition) {
    for (AmbryReplica replica : partitionIdToReplicaIds.get(partition)) {
      if (replica.isDown()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the unique {@link AmbryReplica} for a {@link AmbryPartition} on a {@link AmbryDataNode}.
   * @param hostname the hostname associated with the {@link AmbryDataNode}.
   * @param port the port associated with the {@link AmbryDataNode}.
   * @param partitionString the partition id string associated with the {@link AmbryPartition}.
   * @return the {@link AmbryReplica} associated with the given parameters.
   */
  AmbryReplica getReplicaForPartitionOnNode(String hostname, int port, String partitionString) {
    for (AmbryReplica replica : dataNodeIdToReplicaIds.get(getDataNodeId(hostname, port))) {
      if (replica.getPartitionId().toString().equals(partitionString)) {
        return replica;
      }
    }
    return null;
  }

  /**
   * An instance of this object is used to register as listener for Helix related changes in each datacenter.
   */
  private class ClusterChangeListener implements ExternalViewChangeListener, ConfigChangeListener, LiveInstanceChangeListener {
    final Set<String> allInstances = new HashSet<>();
    private boolean liveInstanceChangeTriggered = false;
    private boolean externalViewChangeTriggered = false;
    private boolean configChangeTriggered = false;
    private CountDownLatch initialized = new CountDownLatch(3);

    /**
     * Triggered whenever there is a change in the list of live instances.
     * @param liveInstances the list of all live instances (not a change set) at the time of this call.
     * @param changeContext the {@link NotificationContext} associated.
     */
    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      logger.info("Live instance change triggered with: " + liveInstances);
      Set<String> liveInstancesSet = new HashSet<>();
      for (LiveInstance liveInstance : liveInstances) {
        liveInstancesSet.add(liveInstance.getInstanceName());
      }
      for (String instanceName : allInstances) {
        if (liveInstancesSet.contains(instanceName)) {
          instanceNameToDataNodeIdMap.get(instanceName).setState(HardwareState.AVAILABLE);
        } else {
          instanceNameToDataNodeIdMap.get(instanceName).setState(HardwareState.UNAVAILABLE);
        }
      }
      if (!liveInstanceChangeTriggered) {
        liveInstanceChangeTriggered = true;
        initialized.countDown();
      }
      helixClusterManagerMetrics.liveInstanceChangeTriggerCount.inc();
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
      logger.info("ExternalView change triggered with: " + externalViewList);

      // No action taken at this time.

      if (!externalViewChangeTriggered) {
        externalViewChangeTriggered = true;
        initialized.countDown();
      }
      helixClusterManagerMetrics.externalViewChangeTriggerCount.inc();
    }

    @Override
    public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
      logger.info("Config change triggered with: ", configs);

      // No action taken at this time. Going forward, changes like marking partitions back to read-write will go here.

      if (!configChangeTriggered) {
        configChangeTriggered = true;
        initialized.countDown();
      }
      helixClusterManagerMetrics.instanceConfigChangeTriggerCount.inc();
    }

    /**
     * Wait until the first set of notifications come in for live instance change, external view change and config
     * changes.
     * @throws InterruptedException if the wait gets interrupted unexpectedly.
     */
    void waitForInitialization() throws InterruptedException {
      initialized.await();
    }
  }

  /**
   * Class that stores all ZK related information associated with a datacenter.
   */
  private static class DcZkInfo {
    final String dcName;
    final String zkConnectStr;
    final HelixManager helixManager;
    final ClusterChangeListener clusterChangeListener;

    /**
     * Construct a DcZkInfo object with the given parameters.
     * @param dcName the associated datacenter name.
     * @param zkConnectStr the associated ZK connect string for this datacenter.
     * @param helixManager the associated {@link HelixManager} for this datacenter.
     * @param clusterChangeListener the associated {@link ClusterChangeListener} for this datacenter.
     */
    DcZkInfo(String dcName, String zkConnectStr, HelixManager helixManager,
        ClusterChangeListener clusterChangeListener) {
      this.dcName = dcName;
      this.zkConnectStr = zkConnectStr;
      this.helixManager = helixManager;
      this.clusterChangeListener = clusterChangeListener;
    }
  }

  /**
   * A callback class used to query information from the {@link HelixClusterManager}
   */
  class HelixClusterManagerCallback {
    /**
     * Get all replica ids associated with the given {@link AmbryPartition}
     * @param partition the {@link AmbryPartition} for which to get the list of replicas.
     * @return the list of {@link AmbryReplica}s associated with the given partition.
     */
    List<AmbryReplica> getReplicaIdsForPartition(AmbryPartition partition) {
      return new ArrayList<>(partitionIdToReplicaIds.get(partition));
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
      return instanceNameToDataNodeIdMap.values();
    }

    /**
     * @return the count of the datanodes in this cluster.
     */
    long getDatanodeCount() {
      return instanceNameToDataNodeIdMap.values().size();
    }

    /**
     * @return the count of datanodes in this cluster that are down.
     */
    long getDownDatanodesCount() {
      long count = 0;
      for (AmbryDataNode datanode : instanceNameToDataNodeIdMap.values()) {
        if (datanode.getState() == HardwareState.UNAVAILABLE) {
          count++;
        }
      }
      return count;
    }

    /**
     * @return a collection of all the disks in this datacenter.
     */
    Collection<AmbryDisk> getDisks() {
      List<AmbryDisk> disksToReturn = new ArrayList<>();
      for (Set<AmbryDisk> disks : dataNodeIdToDiskIds.values()) {
        disksToReturn.addAll(disks);
      }
      return disksToReturn;
    }

    /**
     * @return the count of disks in this cluster.
     */
    long getDiskCount() {
      long count = 0;
      for (Set<AmbryDisk> disks : dataNodeIdToDiskIds.values()) {
        count += disks.size();
      }
      return count;
    }

    /**
     * @return the count of disks in this cluster that are down.
     */
    long getDownDisksCount() {
      long count = 0;
      for (Set<AmbryDisk> disks : dataNodeIdToDiskIds.values()) {
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
      return partitionMap.values();
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
      return clusterWiseAllocatedUsableCapacityBytes;
    }
  }
}


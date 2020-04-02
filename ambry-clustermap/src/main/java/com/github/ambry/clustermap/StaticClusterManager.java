/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.utils.Utils.*;


/**
 * StaticClusterManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
class StaticClusterManager implements ClusterMap {
  protected final HardwareLayout hardwareLayout;
  protected final PartitionLayout partitionLayout;
  private final MetricRegistry metricRegistry;
  private final ClusterMapMetrics clusterMapMetrics;
  private final byte localDatacenterId;

  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * How many data nodes to put in a random sample for partition allocation. 2 node samples provided the best balance
   * of speed and allocation quality in testing. Larger samples (ie 3 nodes) took longer to generate but did not
   * improve the quality of the allocated partitions.
   */
  private static final int NUM_CHOICES = 2;

  StaticClusterManager(PartitionLayout partitionLayout, String localDatacenterName, MetricRegistry metricRegistry) {
    if (logger.isTraceEnabled()) {
      logger.trace("StaticClusterManager " + partitionLayout);
    }
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    this.partitionLayout = partitionLayout;
    this.metricRegistry = metricRegistry;
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
    localDatacenterId = localDatacenterName != null && !localDatacenterName.isEmpty() ? hardwareLayout.findDatacenter(
        localDatacenterName).getId() : ClusterMapUtils.UNKNOWN_DATACENTER_ID;
  }

  void persist(String hardwareLayoutPath, String partitionLayoutPath) throws IOException, JSONException {
    logger.trace("persist " + hardwareLayoutPath + ", " + partitionLayoutPath);
    writeJsonObjectToFile(hardwareLayout.toJSONObject(), hardwareLayoutPath);
    writeJsonObjectToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
  }

  // Implementation of ClusterMap interface
  // --------------------------------------

  @Override
  public List<PartitionId> getWritablePartitionIds(String partitionClass) {
    return partitionLayout.getWritablePartitions(partitionClass);
  }

  @Override
  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> partitionsToExclude) {
    return partitionLayout.getRandomWritablePartition(partitionClass, partitionsToExclude);
  }

  @Override
  public List<PartitionId> getAllPartitionIds(String partitionClass) {
    return partitionLayout.getPartitions(partitionClass);
  }

  @Override
  public PartitionId getPartitionIdFromStream(InputStream stream) throws IOException {
    PartitionId partitionId = partitionLayout.getPartition(stream);
    if (partitionId == null) {
      throw new IOException("Partition id from stream is null");
    }
    return partitionId;
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return hardwareLayout.findDatacenter(datacenterName) != null;
  }

  @Override
  public byte getLocalDatacenterId() {
    return localDatacenterId;
  }

  @Override
  public String getDatacenterName(byte id) {
    Datacenter datacenter = hardwareLayout.findDatacenter(id);
    return datacenter == null ? null : datacenter.getName();
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<ReplicaId> replicas = getReplicas(dataNodeId);
    return new ArrayList<ReplicaId>(replicas);
  }

  List<ReplicaId> getReplicas(DataNodeId dataNodeId) {
    List<ReplicaId> replicas = new ArrayList<ReplicaId>();
    for (PartitionId partition : partitionLayout.getPartitions(null)) {
      for (ReplicaId replica : partition.getReplicaIds()) {
        if (replica.getDataNodeId().equals(dataNodeId)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> dataNodeIds = new ArrayList<DataNodeId>();
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      dataNodeIds.addAll(datacenter.getDataNodes());
    }
    return dataNodeIds;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener) {
    // no op for static cluster manager
  }

  // Administrative API
  // -----------------------

  long getRawCapacityInBytes() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  long getAllocatedRawCapacityInBytes() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  long getAllocatedUsableCapacityInBytes() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }

  long getAllocatedRawCapacityInBytes(Datacenter datacenter) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions(null)) {
      for (ReplicaId replica : partition.getReplicaIds()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  long getAllocatedRawCapacityInBytes(DataNodeId dataNode) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions(null)) {
      for (ReplicaId replica : partition.getReplicaIds()) {
        Disk disk = (Disk) replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  long getAllocatedRawCapacityInBytes(Disk disk) {
    long allocatedRawCapacityInBytes = 0;
    for (PartitionId partition : partitionLayout.getPartitions(null)) {
      for (ReplicaId replica : partition.getReplicaIds()) {
        Disk currentDisk = (Disk) replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  long getUnallocatedRawCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedRawCapacityInBytes();
  }

  long getUnallocatedRawCapacityInBytes(Datacenter datacenter) {
    return datacenter.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(datacenter);
  }

  long getUnallocatedRawCapacityInBytes(DataNode dataNode) {
    return dataNode.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(dataNode);
  }

  long getUnallocatedRawCapacityInBytes(Disk disk) {
    return disk.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(disk);
  }

  DataNode getDataNodeWithMostUnallocatedRawCapacity(Datacenter dc, Set nodesToExclude) {
    DataNode maxCapacityNode = null;
    List<DataNode> dataNodes = dc.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      if (!nodesToExclude.contains(dataNode) && (maxCapacityNode == null
          || getUnallocatedRawCapacityInBytes(dataNode) > getUnallocatedRawCapacityInBytes(maxCapacityNode))) {
        maxCapacityNode = dataNode;
      }
    }
    return maxCapacityNode;
  }

  Disk getDiskWithMostUnallocatedRawCapacity(DataNode node, long minCapacity) {
    Disk maxCapacityDisk = null;
    List<Disk> disks = node.getDisks();
    for (Disk disk : disks) {
      if ((maxCapacityDisk == null || getUnallocatedRawCapacityInBytes(disk) > getUnallocatedRawCapacityInBytes(
          maxCapacityDisk)) && getUnallocatedRawCapacityInBytes(disk) >= minCapacity) {
        maxCapacityDisk = disk;
      }
    }
    return maxCapacityDisk;
  }

  PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes, String partitionClass) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes, partitionClass);
  }

  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean checkEnoughUnallocatedRawCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getUnallocatedRawCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient unallocated space in datacenter {} ({} bytes unallocated)", datacenter.getName(),
            getUnallocatedRawCapacityInBytes(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getUnallocatedRawCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with unallocated space in datacenter {} for {} Replicas)", rcpd,
            datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }

    return true;
  }

  /**
   * Get a sampling of {@code numChoices} random disks from a list of {@link DataNode}s and choose the
   * {@link Disk} on the {@link DataNode} with the most free space.
   * NOTE 1: This method will change the ordering of the nodes in {@code dataNodes}
   * NOTE 2: This method can return null, if a disk with enough free space could not be found.
   *
   * @param dataNodes the list of {@link DataNode}s to sample from
   * @param dataNodesUsed the set of {@link DataNode}s to exclude from the sample
   * @param replicaCapacityInBytes the minimum amount of free space that a disk in the sample should have
   * @param rackAware if {@code true}, only return disks in nodes that do not share racks with the nodes
   *                  in {@code dataNodesUsed}
   * @param numChoices how many disks in the sample to choose between
   * @return The {@link Disk} on the {@link DataNode} in the sample with the most free space, or {@code null } if a disk
   *         with enough free space could not be found.
   */
  private Disk getBestDiskCandidate(List<DataNode> dataNodes, Set<DataNode> dataNodesUsed, long replicaCapacityInBytes,
      boolean rackAware, int numChoices) {
    Set<String> rackIdsUsed = new HashSet<>();
    if (rackAware) {
      for (DataNode dataNode : dataNodesUsed) {
        rackIdsUsed.add(dataNode.getRackId());
      }
    }
    int numFound = 0;
    int selectionBound = dataNodes.size();
    Random randomGen = new Random();
    Disk bestDisk = null;
    while ((selectionBound > 0) && (numFound < numChoices)) {
      int selectionIndex = randomGen.nextInt(selectionBound);
      DataNode candidate = dataNodes.get(selectionIndex);
      if (!dataNodesUsed.contains(candidate) && !rackIdsUsed.contains(candidate.getRackId())) {
        Disk diskCandidate = getDiskWithMostUnallocatedRawCapacity(candidate, replicaCapacityInBytes);
        if (diskCandidate != null) {
          if ((bestDisk == null) || (getUnallocatedRawCapacityInBytes(diskCandidate.getDataNode())
              >= getUnallocatedRawCapacityInBytes(bestDisk.getDataNode()))) {
            bestDisk = diskCandidate;
          }
          numFound++;
        }
      }
      selectionBound--;
      Collections.swap(dataNodes, selectionIndex, selectionBound);
    }
    return bestDisk;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}.  Does not retry if fewer than
   * {@code replicaCountPerDatacenter} disks cannot be allocated.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param rackAware if {@code true}, attempt a rack-aware allocation
   * @return A list of {@link Disk}s
   */
  private List<Disk> getDiskCandidatesForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      Datacenter datacenter, boolean rackAware) {
    ArrayList<Disk> disksToAllocate = new ArrayList<Disk>();
    Set<DataNode> nodesToExclude = new HashSet<>();
    List<DataNode> dataNodes = new ArrayList<>(datacenter.getDataNodes());
    for (int i = 0; i < replicaCountPerDatacenter; i++) {
      Disk bestDisk = getBestDiskCandidate(dataNodes, nodesToExclude, replicaCapacityInBytes, rackAware, NUM_CHOICES);
      if (bestDisk != null) {
        disksToAllocate.add(bestDisk);
        nodesToExclude.add(bestDisk.getDataNode());
      } else {
        break;
      }
    }
    return disksToAllocate;
  }

  /**
   * Return a list of disks for a new partition in the specified {@link Datacenter}. Retry a non rack-aware allocation
   * in certain cases described below if {@code attemptNonRackAwareOnFailure} is enabled.
   *
   * @param replicaCountPerDatacenter how many replicas to attempt to allocate in the datacenter
   * @param replicaCapacityInBytes the minimum amount of free space on a disk for a replica
   * @param datacenter the {@link Datacenter} to allocate replicas in
   * @param attemptNonRackAwareOnFailure {@code true} if we should attempt a non rack-aware allocation if a rack-aware
   *                                     one is not possible.
   * @return a list of {@code replicaCountPerDatacenter} or fewer disks that can be allocated for a new partition in
   *         the specified datacenter
   */
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes,
      Datacenter datacenter, boolean attemptNonRackAwareOnFailure) {
    List<Disk> disks;
    if (datacenter.isRackAware()) {
      disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, true);
      if ((disks.size() < replicaCountPerDatacenter) && attemptNonRackAwareOnFailure) {
        System.err.println("Rack-aware allocation failed for a partition on datacenter:" + datacenter.getName()
            + "; attempting to perform a non rack-aware allocation.");
        disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
      }
    } else if (!attemptNonRackAwareOnFailure) {
      throw new IllegalArgumentException(
          "attemptNonRackAwareOnFailure is false, but the datacenter: " + datacenter.getName()
              + " does not have rack information");
    } else {
      disks = getDiskCandidatesForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter, false);
    }

    if (disks.size() < replicaCountPerDatacenter) {
      System.err.println(
          "Could only allocate " + disks.size() + "/" + replicaCountPerDatacenter + " replicas in datacenter: "
              + datacenter.getName());
    }
    return disks;
  }

  /**
   * Allocate partitions for {@code numPartitions} new partitions on all datacenters.
   *
   * @param numPartitions How many partitions to allocate.
   * @param partitionClass the partition class that the created partitions must be tagged with
   * @param replicaCountPerDatacenter The number of replicas per partition on each datacenter
   * @param replicaCapacityInBytes How large each replica (of a partition) should be
   * @param attemptNonRackAwareOnFailure {@code true} if we should attempt a non rack-aware allocation if a rack-aware
   *                                     one is not possible.
   * @return A list of the new {@link PartitionId}s.
   */
  List<PartitionId> allocatePartitions(int numPartitions, String partitionClass, int replicaCountPerDatacenter,
      long replicaCapacityInBytes, boolean attemptNonRackAwareOnFailure) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);
    int partitionsAllocated = 0;
    while (checkEnoughUnallocatedRawCapacity(replicaCountPerDatacenter, replicaCapacityInBytes)
        && partitionsAllocated < numPartitions) {
      List<Disk> disksToAllocate = new ArrayList<>();
      for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes, datacenter,
            attemptNonRackAwareOnFailure);
        disksToAllocate.addAll(disks);
      }
      partitions.add(partitionLayout.addNewPartition(disksToAllocate, replicaCapacityInBytes, partitionClass));
      partitionsAllocated++;
      System.out.println("Allocated " + partitionsAllocated + " new partitions so far.");
    }
    return partitions;
  }

  /**
   * Add a set of replicas on a new datacenter for an existing partition.
   *
   * @param partitionId The partition to add to the new datacenter
   * @param dataCenterName The name of the new datacenter
   * @param attemptNonRackAwareOnFailure {@code true} if a non rack-aware allocation should be attempted if a rack-aware one
   *                            is not possible.
   */
  void addReplicas(PartitionId partitionId, String dataCenterName, boolean attemptNonRackAwareOnFailure) {
    List<? extends ReplicaId> replicaIds = partitionId.getReplicaIds();
    Map<String, Integer> replicaCountByDatacenter = new HashMap<String, Integer>();
    long capacityOfReplicasInBytes = 0;
    // we ensure that the datacenter provided does not have any replicas
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.getDataNodeId().getDatacenterName().compareToIgnoreCase(dataCenterName) == 0) {
        throw new IllegalArgumentException(
            "Data center " + dataCenterName + " provided already contains replica for partition " + partitionId);
      }
      capacityOfReplicasInBytes = replicaId.getCapacityInBytes();
      int numberOfReplicas =
          replicaCountByDatacenter.getOrDefault(replicaId.getDataNodeId().getDatacenterName(), 0) + 1;
      replicaCountByDatacenter.put(replicaId.getDataNodeId().getDatacenterName(), numberOfReplicas);
    }
    if (replicaCountByDatacenter.size() == 0) {
      throw new IllegalArgumentException("No existing replicas present for partition " + partitionId + " in cluster.");
    }
    // verify that all data centers have the same replica
    int numberOfReplicasPerDatacenter = 0;
    int index = 0;
    for (Map.Entry<String, Integer> entry : replicaCountByDatacenter.entrySet()) {
      if (index == 0) {
        numberOfReplicasPerDatacenter = entry.getValue();
      }
      if (numberOfReplicasPerDatacenter != entry.getValue()) {
        throw new IllegalStateException("Datacenters have different replicas for partition " + partitionId);
      }
      index++;
    }
    Datacenter datacenterToAdd = hardwareLayout.findDatacenter(dataCenterName);
    List<Disk> disksForReplicas =
        allocateDisksForPartition(numberOfReplicasPerDatacenter, capacityOfReplicasInBytes, datacenterToAdd,
            attemptNonRackAwareOnFailure);
    partitionLayout.addNewReplicas((Partition) partitionId, disksForReplicas);
    System.out.println("Added partition " + partitionId + " to datacenter " + dataCenterName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticClusterManager that = (StaticClusterManager) o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null) {
      return false;
    }
    return !(partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null);
  }

  @Override
  public void onReplicaEvent(ReplicaId replicaId, ReplicaEventType event) {
    switch (event) {
      case Disk_Error:
        if (replicaId.getReplicaType() == ReplicaType.DISK_BACKED) {
          ((Disk) replicaId.getDiskId()).onDiskError();
        }
        break;
      case Disk_Ok:
        if (replicaId.getReplicaType() == ReplicaType.DISK_BACKED) {
          ((Disk) replicaId.getDiskId()).onDiskOk();
        }
        break;
      case Node_Timeout:
        ((DataNode) replicaId.getDataNodeId()).onNodeTimeout();
        break;
      case Node_Response:
        ((DataNode) replicaId.getDataNodeId()).onNodeResponse();
        break;
      case Partition_ReadOnly:
        ((Partition) replicaId.getPartitionId()).onPartitionReadOnly();
        break;
      case Replica_Unavailable:
        ((Replica) replicaId).onReplicaUnavailable();
        break;
      case Replica_Available:
        ((Replica) replicaId).onReplicaResponse();
        break;
    }
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(IMPLEMENTATION, StaticClusterManager.class.getName());
    snapshot.put(CLUSTER_NAME, partitionLayout.getClusterName());
    snapshot.put(TIMESTAMP_MS, SystemTime.getInstance().milliseconds());
    JSONArray datacentersJsonArray = new JSONArray();
    hardwareLayout.getDatacenters().forEach(dc -> {
      JSONObject data = new JSONObject();
      data.put(DATACENTER_NAME, dc.getName());
      data.put(DATACENTER_ID, dc.getId());
      JSONArray datanodesInDc = new JSONArray();
      dc.getDataNodes().forEach(node -> datanodesInDc.put(node.getSnapshot()));
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
  public ReplicaId getBootstrapReplica(String partitionIdStr, DataNodeId dataNodeId) {
    throw new UnsupportedOperationException("Adding new replica is currently not supported in static cluster manager.");
  }

  @Override
  public void close() {
    // No-op.
  }
}

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

import com.github.ambry.config.ClusterMapConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PartitionLayout of {@link Partition}s and {@link Replica}s on an Ambry cluster (see {@link HardwareLayout}).
 */
class PartitionLayout {
  private static final long MinPartitionId = 0;

  private final HardwareLayout hardwareLayout;
  private final String clusterName;
  private final long version;
  private final Map<ByteBuffer, Partition> partitionMap;

  private long maxPartitionId;
  private long allocatedRawCapacityInBytes;
  private long allocatedUsableCapacityInBytes;
  private final String localDatacenterName;
  private final ClusterMapUtils.PartitionSelectionHelper partitionSelectionHelper;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a PartitionLayout
   * @param hardwareLayout the {@link HardwareLayout} to use.
   * @param jsonObject the {@link JSONObject} that represents the partition layout
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @throws JSONException
   */
  public PartitionLayout(HardwareLayout hardwareLayout, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("PartitionLayout " + hardwareLayout + ", " + jsonObject.toString());
    }
    this.hardwareLayout = hardwareLayout;
    this.localDatacenterName = clusterMapConfig.clusterMapDatacenterName;
    this.clusterName = jsonObject.getString("clusterName");
    this.version = jsonObject.getLong("version");
    this.partitionMap = new HashMap<>();
    for (int i = 0; i < jsonObject.getJSONArray("partitions").length(); ++i) {
      addPartition(new Partition(this, jsonObject.getJSONArray("partitions").getJSONObject(i)));
    }

    validate();
    partitionSelectionHelper =
        new ClusterMapUtils.PartitionSelectionHelper(new StaticClusterManagerCallback(), localDatacenterName,
            clusterMapConfig.clustermapWritablePartitionMinReplicaCount);
  }

  /**
   * Constructor for initial PartitionLayout
   * @param hardwareLayout the {@link JSONObject} that represents the partition layout
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   */
  public PartitionLayout(HardwareLayout hardwareLayout, ClusterMapConfig clusterMapConfig) {
    if (logger.isTraceEnabled()) {
      logger.trace("PartitionLayout " + hardwareLayout);
    }
    this.hardwareLayout = hardwareLayout;
    this.localDatacenterName = clusterMapConfig.clusterMapDatacenterName;
    this.clusterName = hardwareLayout.getClusterName();
    this.version = 1;
    this.maxPartitionId = MinPartitionId;
    this.partitionMap = new HashMap<>();
    validate();
    partitionSelectionHelper =
        new ClusterMapUtils.PartitionSelectionHelper(new StaticClusterManagerCallback(), localDatacenterName,
            clusterMapConfig.clustermapWritablePartitionMinReplicaCount);
  }

  public HardwareLayout getHardwareLayout() {
    return hardwareLayout;
  }

  public String getClusterName() {
    return clusterName;
  }

  public long getVersion() {
    return version;
  }

  public long getPartitionCount() {
    return partitionMap.size();
  }

  public long getPartitionInStateCount(PartitionState partitionState) {
    int count = 0;
    for (Partition partition : partitionMap.values()) {
      if (partition.getPartitionState() == partitionState) {
        count++;
      }
    }
    return count;
  }

  public List<PartitionId> getPartitions(String partitionClass) {
    return partitionSelectionHelper.getPartitions(partitionClass);
  }

  public List<PartitionId> getWritablePartitions(String partitionClass) {
    return partitionSelectionHelper.getWritablePartitions(partitionClass);
  }

  public PartitionId getRandomWritablePartition(String partitionClass, List<PartitionId> toExclude) {
    return partitionSelectionHelper.getRandomWritablePartition(partitionClass, toExclude);
  }

  public long getAllocatedRawCapacityInBytes() {
    return allocatedRawCapacityInBytes;
  }

  private long calculateAllocatedRawCapacityInBytes() {
    long allocatedRawCapacityInBytes = 0;
    for (Partition partition : partitionMap.values()) {
      allocatedRawCapacityInBytes += partition.getAllocatedRawCapacityInBytes();
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedUsableCapacityInBytes() {
    return allocatedUsableCapacityInBytes;
  }

  private long calculateAllocatedUsableCapacityInBytes() {
    long allocatedUsableCapacityInBytes = 0;
    for (Partition partition : partitionMap.values()) {
      allocatedUsableCapacityInBytes += partition.getReplicaCapacityInBytes();
    }
    return allocatedUsableCapacityInBytes;
  }

  /**
   * Adds Partition to and validates Partition is unique. A duplicate Partition results in an exception.
   */
  private void addPartition(Partition partition) {
    if (partitionMap.put(ByteBuffer.wrap(partition.getBytes()), partition) != null) {
      throw new IllegalStateException("Duplicate Partition detected: " + partition.toString());
    }
    if (partition.getId() >= maxPartitionId) {
      maxPartitionId = partition.getId() + 1;
    }
  }

  private void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("ClusterName cannot be null.");
    }
    if (!hardwareLayout.getClusterName().equals(clusterName)) {
      throw new IllegalStateException(
          "PartitionLayout cluster name does not match that of HardwareLayout: " + clusterName + " != " + hardwareLayout
              .getClusterName());
    }
  }

  private void validatePartitionIds() {
    for (Partition partition : partitionMap.values()) {
      long partitionId = partition.getId();
      if (partitionId < MinPartitionId) {
        throw new IllegalStateException("Partition has invalid ID: Less than " + MinPartitionId);
      }
      if (partitionId >= maxPartitionId) {
        throw new IllegalStateException("Partition has invalid ID: Greater than or equal to " + maxPartitionId);
      }
    }
  }

  private void validateUniqueness() {
    // Validate uniqueness of each logical component. Partition uniqueness is validated by method addPartition.
    Set<ReplicaId> replicaSet = new HashSet<>();

    for (Partition partition : partitionMap.values()) {
      for (ReplicaId replica : partition.getReplicaIds()) {
        if (!replicaSet.add(replica)) {
          throw new IllegalStateException("Duplicate Replica detected: " + replica.toString());
        }
      }
    }
  }

  private void validate() {
    logger.trace("begin validate.");
    validateClusterName();
    validatePartitionIds();
    validateUniqueness();
    this.allocatedRawCapacityInBytes = calculateAllocatedRawCapacityInBytes();
    this.allocatedUsableCapacityInBytes = calculateAllocatedUsableCapacityInBytes();
    if (localDatacenterName != null && !localDatacenterName.isEmpty()
        && hardwareLayout.findDatacenter(localDatacenterName) == null) {
      throw new IllegalArgumentException("Clustermap has no datacenter named " + localDatacenterName);
    }
    logger.trace("complete validate.");
  }

  private long getNewPartitionId() {
    long currentPartitionId = maxPartitionId;
    maxPartitionId++;
    return currentPartitionId;
  }

  // Creates a Partition and corresponding Replicas for each specified disk
  public Partition addNewPartition(List<Disk> disks, long replicaCapacityInBytes, String partitionClass) {
    if (disks == null || disks.size() == 0) {
      throw new IllegalArgumentException("Disks either null or of zero length.");
    }

    Partition partition =
        new Partition(getNewPartitionId(), partitionClass, PartitionState.READ_WRITE, replicaCapacityInBytes);
    for (Disk disk : disks) {
      partition.addReplica(new Replica(partition, disk, hardwareLayout.getClusterMapConfig()));
    }
    addPartition(partition);
    validate();
    partitionSelectionHelper.updatePartitions(partitionMap.values(), localDatacenterName);

    return partition;
  }

  // Adds replicas to the partition for each specified disk
  public void addNewReplicas(Partition partition, List<Disk> disks) {
    if (partition == null || disks == null || disks.size() == 0) {
      throw new IllegalArgumentException("Partition or disks is null or disks is of zero length");
    }
    for (Disk disk : disks) {
      partition.addReplica(new Replica(partition, disk, hardwareLayout.getClusterMapConfig()));
    }
    validate();
    partitionSelectionHelper.updatePartitions(partitionMap.values(), localDatacenterName);
  }

  /**
   * Gets Partition with specified byte-serialized ID.
   *
   * @param stream byte-serialized partition ID
   * @return requested Partition else null.
   */
  public Partition getPartition(InputStream stream) throws IOException {
    byte[] partitionBytes = Partition.readPartitionBytesFromStream(stream);
    return partitionMap.get(ByteBuffer.wrap(partitionBytes));
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject().put("clusterName", hardwareLayout.getClusterName())
        .put("version", version)
        .put("partitions", new JSONArray());
    for (Partition partition : partitionMap.values()) {
      jsonObject.accumulate("partitions", partition.toJSONObject());
    }
    return jsonObject;
  }

  public List<String> getAllPartitionNames() {
    return partitionMap.values().stream().map(Partition::toPathString).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString(2);
    } catch (JSONException e) {
      logger.error("JSONException caught in toString: ", e.getCause());
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PartitionLayout that = (PartitionLayout) o;

    if (!clusterName.equals(that.clusterName)) {
      return false;
    }
    return hardwareLayout.equals(that.hardwareLayout);
  }

  /**
   * An implementation of {@link ClusterManagerCallback} that supports getting resource from static clustermap.
   */
  class StaticClusterManagerCallback implements ClusterManagerCallback<Replica, Disk, Partition, DataNode> {

    @Override
    public List<Replica> getReplicaIdsForPartition(Partition partition) {
      throw new UnsupportedOperationException("Not supported in static cluster map");
    }

    @Override
    public List<Replica> getReplicaIdsByState(Partition partition, ReplicaState state, String dcName) {
      throw new UnsupportedOperationException("Not supported in static cluster map");
    }

    @Override
    public long getSealedStateChangeCounter() {
      throw new UnsupportedOperationException("Not supported in static cluster map");
    }

    @Override
    public Collection<Disk> getDisks(DataNode dataNode) {
      throw new UnsupportedOperationException("Not supported in static cluster map");
    }

    /**
     * @return a collection of partitions in this cluster.
     */
    @Override
    public Collection<Partition> getPartitions() {
      return partitionMap.values();
    }
  }
}

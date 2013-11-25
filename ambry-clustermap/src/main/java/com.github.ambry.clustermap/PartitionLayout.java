package com.github.ambry.clustermap;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PartitionLayout of {@link Partition}s and {@link Replica}s on an Ambry cluster (see {@link HardwareLayout}).
 */
public class PartitionLayout {
  private static final long MinPartitionId = 0;

  private HardwareLayout hardwareLayout;
  private String clusterName;
  private long partitionIdFactory;
  private Map<ByteBuffer, Partition> partitionMap;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public PartitionLayout(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    this.hardwareLayout = hardwareLayout;

    this.clusterName = jsonObject.getString("clusterName");
    this.partitionIdFactory = jsonObject.getLong("partitionIdFactory");
    this.partitionMap = new HashMap<ByteBuffer, Partition>();

    for (int i = 0; i < jsonObject.getJSONArray("partitions").length(); ++i) {
      addPartition(new Partition(this, jsonObject.getJSONArray("partitions").getJSONObject(i)));
    }

    validate();
  }

  // Constructor for initial PartitionLayout.
  public PartitionLayout(HardwareLayout hardwareLayout) {
    this.hardwareLayout = hardwareLayout;

    this.clusterName = hardwareLayout.getClusterName();
    this.partitionIdFactory = MinPartitionId;
    this.partitionMap = new HashMap<ByteBuffer, Partition>();

    validate();
  }

  public HardwareLayout getHardwareLayout() {
    return hardwareLayout;
  }

  public String getClusterName() {
    return clusterName;
  }

  public List<Partition> getPartitions() {
    return new ArrayList<Partition>(partitionMap.values());
  }

  public List<Partition> getWritablePartitions() {
    List<Partition> writablePartitions = new ArrayList<Partition>();

    for (Partition partition : partitionMap.values()) {
      if (partition.getPartitionState() == PartitionState.READ_WRITE) {
        writablePartitions.add(partition);
      }
    }

    return writablePartitions;
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for (Partition partition : partitionMap.values()) {
      capacityGB += partition.getCapacityGB();
    }
    return capacityGB;
  }

  /**
   * Adds Partition to and validates Partition is unique. A duplicate Partition results in an exception.
   */
  private void addPartition(Partition partition) {
    if(partitionMap.put(ByteBuffer.wrap(partition.getBytes()), partition) != null ) {
      throw new IllegalStateException("Duplicate Partition detected: " + partition.toString());
    }
  }

  protected void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("ClusterName cannot be null.");
    }
    if (!hardwareLayout.getClusterName().equals(clusterName)) {
      throw new IllegalStateException("PartitionLayout cluster name does not match that of HardwareLayout: "
              + clusterName + " != " + hardwareLayout.getClusterName());
    }
  }

  protected void validatePartitionIdFactory() {
    if(partitionIdFactory < MinPartitionId) {
      throw new IllegalStateException("Partition ID factory is " + partitionIdFactory
              + " and should not be less than " + MinPartitionId);
    }
  }

  protected void validatePartitionIds() {
    for (Partition partition : partitionMap.values()) {
      long partitionId = ByteBuffer.wrap(partition.getBytes()).getLong();
      if(partitionId < MinPartitionId) {
        throw new IllegalStateException("Partition has invalid ID: Less than " + MinPartitionId);
      }
      if(partitionId >= partitionIdFactory) {
        throw new IllegalStateException("Partition has invalid ID: Greater than or equal to " + partitionIdFactory);
      }
    }
  }

  protected void validateUniqueness() {
    // Validate uniqueness of each logical component. Partition uniqueness is validated by method  addPartition.
    Set<Replica> replicaSet = new HashSet<Replica>();

    for (Partition partition : partitionMap.values()) {
      for (Replica replica : partition.getReplicas()) {
        if (!replicaSet.add(replica)) {
          throw new IllegalStateException("Duplicate Replica detected: " + replica.toString());
        }
      }
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateClusterName();
    validatePartitionIdFactory();
    validatePartitionIds();
    validateUniqueness();
    logger.trace("complete validate.");
  }

  protected long getNewPartitionId() {
    long currentPartitionId = partitionIdFactory;
    partitionIdFactory++;
    return currentPartitionId;
  }

  // Creates a Partition and corresponding Replicas for each specified disk
  public Partition addNewPartition(List<Disk> disks, long replicaCapacityGB) {
    if (disks == null || disks.size() == 0) {
      throw new IllegalArgumentException("Disks either null or of zero length.");
    }

    Partition partition = new Partition(getNewPartitionId(), PartitionState.READ_WRITE, replicaCapacityGB);
    for(Disk disk : disks) {
      partition.addReplica(new Replica(partition, disk));
    }
    addPartition(partition);
    validate();

    return partition;
  }

  /**
   * Gets Partition with specified byte-serialized ID.
   *
   * @param bytes byte-serialized partition ID
   * @return requested Partition else null.
   */
  public Partition getPartition(byte[] bytes) {
    return partitionMap.get(ByteBuffer.wrap(bytes));
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("clusterName", hardwareLayout.getClusterName())
            .put("partitionIdFactory", partitionIdFactory)
            .put("partitions", new JSONArray());
    for (Partition partition : partitionMap.values()) {
      jsonObject.accumulate("partitions", partition.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString(2);
    } catch (JSONException e) {
      logger.error("JSONException caught in toString: {}",  e.getCause());
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PartitionLayout that = (PartitionLayout) o;

    if (partitionIdFactory != that.partitionIdFactory) return false;
    if (!clusterName.equals(that.clusterName)) return false;
    if (!hardwareLayout.equals(that.hardwareLayout)) return false;

    return true;
  }
}

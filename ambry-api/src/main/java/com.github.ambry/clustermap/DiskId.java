package com.github.ambry.clustermap;

/**
 * A DiskId stores {@link ReplicaId}s. Each DiskId is hosted on one specific {@link DataNodeId}. Each DiskId is uniquely
 * identified by its DataNodeId and mount path (the path to this Disk's device on its DataNode).
 */
public interface DiskId {

  /**
   * Gets the absolute path to the mounted device
   *
   * @return absolute mount path.
   */
  public String getMountPath();

  /**
   * Gets the state of the DiskId.
   *
   * @return state of the DiskId.
   */
  public HardwareState getState();

  /**
   * Gets the capacity in bytes for this DiskId.
   * @return the capacity in bytes
   */
  public long getCapacityInBytes();
}

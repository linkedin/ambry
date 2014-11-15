package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * A Disk stores {@link Replica}s. Each Disk is hosted on one specific {@link DataNode}. Each Disk is uniquely
 * identified by its DataNode and mount path (the path to this Disk's device on its DataNode).
 */
public class Disk implements DiskId {
  // Hard-code disk capacity limits in bytes for validation
  private static final long MinCapacityInBytes = 10 * 1024 * 1024 * 1024L;
  private static final long MaxCapacityInBytes = 1024 * 1024 * 1024 * 1024L; // 1 PB

  private final DataNode dataNode;
  private final String mountPath;
  private final DiskStatePolicy diskStatePolicy;
  private long capacityInBytes;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public Disk(DataNode dataNode, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("Disk " + jsonObject.toString());
    }
    this.dataNode = dataNode;
    this.mountPath = jsonObject.getString("mountPath");
    this.diskStatePolicy = new DiskStatePolicy(HardwareState.valueOf(jsonObject.getString("hardwareState")),
        clusterMapConfig.clusterMapDiskWindowMs, clusterMapConfig.clusterMapDiskErrorThreshold,
        clusterMapConfig.clusterMapDiskRetryBackoffMs);
    this.capacityInBytes = jsonObject.getLong("capacityInBytes");
    validate();
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public HardwareState getState() {
    // A Disk is unavailable if its DataNode is unavailable.
    return dataNode.getState() == HardwareState.AVAILABLE && diskStatePolicy.getState() == HardwareState.AVAILABLE
        ? HardwareState.AVAILABLE : HardwareState.UNAVAILABLE;
  }

  public boolean isSoftDown() {
    return diskStatePolicy.isSoftDown();
  }

  public boolean isHardDown() {
    return diskStatePolicy.isHardDown();
  }

  @Override
  public long getRawCapacityInBytes() {
    return capacityInBytes;
  }

  public DataNode getDataNode() {
    return dataNode;
  }

  public HardwareState getHardState() {
    return diskStatePolicy.isHardDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }

  protected void validateDataNode() {
    if (dataNode == null) {
      throw new IllegalStateException("DataNode cannot be null.");
    }
  }

  protected void validateMountPath() {
    if (mountPath == null) {
      throw new IllegalStateException("Mount path cannot be null.");
    }
    if (mountPath.length() == 0) {
      throw new IllegalStateException("Mount path cannot be zero-length string.");
    }
    File mountPathFile = new File(mountPath);
    if (!mountPathFile.isAbsolute()) {
      throw new IllegalStateException("Mount path has to be an absolute path.");
    }
  }

  protected void validateCapacity() {
    if (capacityInBytes < MinCapacityInBytes) {
      throw new IllegalStateException("Invalid disk capacity: " + capacityInBytes + " is less than " +
          MinCapacityInBytes);
    } else if (capacityInBytes > MaxCapacityInBytes) {
      throw new IllegalStateException("Invalid disk capacity: " + capacityInBytes + " is more than " +
          MaxCapacityInBytes);
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateDataNode();
    validateMountPath();
    validateCapacity();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject()
      throws JSONException {
    return new JSONObject().put("mountPath", mountPath).put("hardwareState", getHardState())
        .put("capacityInBytes", capacityInBytes);
  }

  @Override
  public String toString() {
    return "Disk[" + dataNode.getHostname() + ":" + dataNode.getPort() + ":" + getMountPath() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Disk disk = (Disk) o;

    if (!dataNode.equals(disk.dataNode)) {
      return false;
    }
    return mountPath.equals(disk.mountPath);
  }

  @Override
  public int hashCode() {
    int result = dataNode.hashCode();
    result = 31 * result + mountPath.hashCode();
    return result;
  }

  public void onDiskError() {
    String diskStr;
    try {
      diskStr = toJSONObject().toString();
    } catch (JSONException e) {
      diskStr = null;
    }
    logger.info("Disk error, informing resource state for disk [" + diskStr + "]");
    if (diskStatePolicy.onError()) {
      logger.info("Disk [" + diskStr + "] has been determined as down: ");
    }
  }
}

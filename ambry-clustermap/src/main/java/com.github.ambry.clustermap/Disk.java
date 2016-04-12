/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.Utils;
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
  private static final long MinCapacityInBytes = 10L * 1024 * 1024 * 1024;
  private static final long MaxCapacityInBytes = 10L * 1024 * 1024 * 1024 * 1024; // 10 TB

  private final DataNode dataNode;
  private final String mountPath;
  private final ResourceStatePolicy diskStatePolicy;
  private long capacityInBytes;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public Disk(DataNode dataNode, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("Disk " + jsonObject.toString());
    }
    this.dataNode = dataNode;
    this.mountPath = jsonObject.getString("mountPath");
    try {
      ResourceStatePolicyFactory resourceStatePolicyFactory = Utils
          .getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this,
              HardwareState.valueOf(jsonObject.getString("hardwareState")), clusterMapConfig);
      this.diskStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    } catch (Exception e) {
      logger.error("Error creating resource state policy when instantiating a disk " + e);
      throw new IllegalStateException("Error creating resource state policy when instantiating a disk: " + mountPath,
          e);
    }
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
    return dataNode.getState() == HardwareState.AVAILABLE && !diskStatePolicy.isDown() ? HardwareState.AVAILABLE
        : HardwareState.UNAVAILABLE;
  }

  public boolean isDown() {
    return diskStatePolicy.isDown();
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
    String dataNodeStr = dataNode == null ? "" : dataNode.getHostname() + ":" + dataNode.getPort() + ":";
    return "Disk[" + dataNodeStr + getMountPath() + "]";
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
    diskStatePolicy.onError();
  }

  public void onDiskOk() {
    diskStatePolicy.onSuccess();
  }
}

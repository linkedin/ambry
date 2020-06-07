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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.io.File;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * {@link DiskId} implementation to use within dynamic cluster managers.
 */
class AmbryDisk implements DiskId, Resource {
  private final AmbryDataNode datanode;
  private final String mountPath;
  private final long rawCapacityBytes;
  private final ResourceStatePolicy resourceStatePolicy;

  /**
   * Instantiate an AmbryDisk object.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param datanode the {@link AmbryDataNode} on which this disk resides.
   * @param mountPath the mount path at which this disk is mounted on the datanode.
   * @param state the initial {@link HardwareState} of this disk.
   * @param rawCapacityBytes the capacity in bytes.
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  AmbryDisk(ClusterMapConfig clusterMapConfig, AmbryDataNode datanode, String mountPath, HardwareState state,
      long rawCapacityBytes) throws Exception {
    this.datanode = datanode;
    this.mountPath = mountPath;
    this.rawCapacityBytes = rawCapacityBytes;
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, state, clusterMapConfig);
    this.resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    validate();
  }

  /**
   * Validate the newly constructed AmbryDisk instance.
   */
  private void validate() {
    if (datanode == null || mountPath == null) {
      throw new IllegalStateException("datanode and mount path cannot be null.");
    }
    if (mountPath.length() == 0) {
      throw new IllegalStateException("Mount path cannot be zero-length string.");
    }
    File mountPathFile = new File(mountPath);
    if (!mountPathFile.isAbsolute()) {
      throw new IllegalStateException("Mount path has to be an absolute path.");
    }
    ClusterMapUtils.validateDiskCapacity(rawCapacityBytes);
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public HardwareState getState() {
    return resourceStatePolicy.isDown() || datanode.getState() == HardwareState.UNAVAILABLE ? HardwareState.UNAVAILABLE
        : HardwareState.AVAILABLE;
  }

  @Override
  public long getRawCapacityInBytes() {
    return rawCapacityBytes;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(DISK_NODE, datanode.getHostname() + ":" + datanode.getPort());
    snapshot.put(DISK_MOUNT_PATH, getMountPath());
    snapshot.put(CAPACITY_BYTES, getRawCapacityInBytes());
    String liveness = UP;
    if (datanode.getState() == HardwareState.UNAVAILABLE) {
      liveness = NODE_DOWN;
    } else if (resourceStatePolicy.isHardDown()) {
      liveness = DISK_DOWN;
    } else if (resourceStatePolicy.isDown()) {
      liveness = SOFT_DOWN;
    }
    snapshot.put(LIVENESS, liveness);
    return snapshot;
  }

  @Override
  public String toString() {
    return "Disk[" + datanode.getHostname() + ":" + datanode.getPort() + ":" + getMountPath() + "]";
  }

  /**
   * @return the {@link AmbryDataNode} associated with this disk.
   */
  AmbryDataNode getDataNode() {
    return datanode;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return datanode;
  }

  /**
   * Set the hard state of this disk dynamically.
   * @param newState the updated {@link HardwareState}
   */
  void setState(HardwareState newState) {
    if (newState == HardwareState.AVAILABLE) {
      resourceStatePolicy.onHardUp();
    } else {
      resourceStatePolicy.onHardDown();
    }
  }

  /**
   * Take actions, if any, when an error is received for this disk.
   */
  void onDiskError() {
    resourceStatePolicy.onError();
  }

  /**
   * Take actions, if any, when this disk is back in a good state.
   */
  void onDiskOk() {
    resourceStatePolicy.onSuccess();
  }
}


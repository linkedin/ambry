/*
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

import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


public class MockDiskId implements DiskId {
  String mountPath;
  MockDataNodeId dataNode;
  HardwareState state = HardwareState.AVAILABLE;
  private boolean allowChangesThroughClustermap = true;

  public MockDiskId(MockDataNodeId dataNode, String mountPath) {
    this.mountPath = mountPath;
    this.dataNode = dataNode;
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public HardwareState getState() {
    return state;
  }

  @Override
  public long getRawCapacityInBytes() {
    return 100000;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(DISK_NODE, dataNode.getHostname() + ":" + dataNode.getPort());
    snapshot.put(DISK_MOUNT_PATH, mountPath);
    snapshot.put(LIVENESS, dataNode.getState() == HardwareState.UNAVAILABLE ? NODE_DOWN
        : state == HardwareState.UNAVAILABLE ? DISK_DOWN : UP);
    return snapshot;
  }

  public synchronized void onDiskError() {
    if (allowChangesThroughClustermap) {
      state = HardwareState.UNAVAILABLE;
    }
  }

  public synchronized void onDiskOk() {
    if (allowChangesThroughClustermap) {
      state = HardwareState.AVAILABLE;
    }
  }

  /**
   * Forcibly set disk state {@link HardwareState} and specify whether ClusterMap is allowed to change disk state.
   * @param state the hardware state of disk
   * @param allowChangesThroughClusterMap whether the ClusterMap is allowed to change disk state
   */
  public synchronized void setDiskState(HardwareState state, boolean allowChangesThroughClusterMap) {
    this.allowChangesThroughClustermap = allowChangesThroughClusterMap;
    this.state = state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockDiskId that = (MockDiskId) o;

    if (!mountPath.equals(that.mountPath)) {
      return false;
    }
    return dataNode.equals(that.dataNode);
  }

  @Override
  public int hashCode() {
    int result = mountPath.hashCode();
    result = 31 * result + dataNode.hashCode();
    return result;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return dataNode;
  }
}

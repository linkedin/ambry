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

public class MockDiskId implements DiskId {
  String mountPath;
  MockDataNodeId dataNode;
  HardwareState state = HardwareState.AVAILABLE;

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

  public void onDiskError() {
    state = HardwareState.UNAVAILABLE;
  }

  public void onDiskOk() {
    state = HardwareState.AVAILABLE;
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
}

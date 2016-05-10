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
    return HardwareState.AVAILABLE;
  }

  @Override
  public long getRawCapacityInBytes() {
    return 100000;
  }

  public void onDiskError() {
    /* no-op for now */
  }

  public void onDiskOk() {
    /* no-op for now */
  }
}

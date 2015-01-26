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

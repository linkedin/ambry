package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;


public class FixedBackoffResourceStatePolicyFactory implements ResourceStatePolicyFactory {
  private ResourceStatePolicy resourceStatePolicy;

  public FixedBackoffResourceStatePolicyFactory(Resource resource, HardwareState initialState,
      ClusterMapConfig clusterMapConfig)
      throws InstantiationError {
    resourceStatePolicy = null;
    if (resource instanceof DataNodeId) {
      resourceStatePolicy = new FixedBackoffResourceStatePolicy(resource, initialState == HardwareState.UNAVAILABLE,
          clusterMapConfig.clusterMapFixedTimeoutDatanodeWindowMs,
          clusterMapConfig.clusterMapFixedTimeoutDatanodeErrorThreshold,
          clusterMapConfig.clusterMapFixedTimeoutDataNodeRetryBackoffMs);
    } else if (resource instanceof DiskId) {
      resourceStatePolicy = new FixedBackoffResourceStatePolicy(resource, initialState == HardwareState.UNAVAILABLE,
          clusterMapConfig.clusterMapFixedTimeoutDiskWindowMs,
          clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold,
          clusterMapConfig.clusterMapFixedTimeoutDiskRetryBackoffMs);
    }

    if (resourceStatePolicy == null) {
      throw new InstantiationError("Unknown resource type, cannot get resource state policy.");
    }
  }

  @Override
  public ResourceStatePolicy getResourceStatePolicy() {
    return resourceStatePolicy;
  }
}

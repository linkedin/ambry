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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.SystemTime;


public class FixedBackoffResourceStatePolicyFactory implements ResourceStatePolicyFactory {
  private ResourceStatePolicy resourceStatePolicy;

  public FixedBackoffResourceStatePolicyFactory(Resource resource, HardwareState initialState,
      ClusterMapConfig clusterMapConfig) throws InstantiationError {
    resourceStatePolicy = null;
    if (resource instanceof DataNodeId) {
      resourceStatePolicy = new FixedBackoffResourceStatePolicy(resource, initialState == HardwareState.UNAVAILABLE,
          clusterMapConfig.clusterMapFixedTimeoutDatanodeErrorThreshold,
          clusterMapConfig.clusterMapFixedTimeoutDataNodeRetryBackoffMs, SystemTime.getInstance());
    } else if (resource instanceof DiskId) {
      resourceStatePolicy = new FixedBackoffResourceStatePolicy(resource, initialState == HardwareState.UNAVAILABLE,
          clusterMapConfig.clusterMapFixedTimeoutDiskErrorThreshold,
          clusterMapConfig.clusterMapFixedTimeoutDiskRetryBackoffMs, SystemTime.getInstance());
    } else if (resource instanceof ReplicaId) {
      resourceStatePolicy = new FixedBackoffResourceStatePolicy(resource, initialState == HardwareState.UNAVAILABLE,
          clusterMapConfig.clusterMapFixedTimeoutReplicaErrorThreshold,
          clusterMapConfig.clusterMapFixedTimeoutReplicaRetryBackoffMs, SystemTime.getInstance());
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

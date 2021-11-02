/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;


/**
 * Implementation of {@link StorageQuotaEnforcementPolicyFactory} to return a {@link AdaptiveStorageQuotaEnforcementPolicy}.
 */
public class AdaptiveStorageQuotaEnforcementPolicyFactory implements StorageQuotaEnforcementPolicyFactory {
  private final AdaptiveStorageQuotaEnforcementPolicy policy;

  /**
   * Constructor to instantiate {@link AdaptiveStorageQuotaEnforcementPolicyFactory}.
   * @param clusterMap The {@link ClusterMap} object.
   * @param config The {@link StorageQuotaConfig} object.
   */
  public AdaptiveStorageQuotaEnforcementPolicyFactory(ClusterMap clusterMap, StorageQuotaConfig config) {
    policy = new AdaptiveStorageQuotaEnforcementPolicy(clusterMap, SystemTime.getInstance(), config);
  }

  @Override
  public StorageQuotaEnforcementPolicy getPolicy() {
    return policy;
  }
}

class AdaptiveStorageQuotaEnforcementPolicy implements StorageQuotaEnforcementPolicy {
  private final ClusterMap clusterMap;
  private final StorageQuotaConfig config;
  private final Time time;
  private volatile long lastClusterMapUpdateTimeMs;
  private volatile int currentClusterMapWritablePartitionPercentage;

  public AdaptiveStorageQuotaEnforcementPolicy(ClusterMap clusterMap, Time time, StorageQuotaConfig config) {
    this.clusterMap = clusterMap;
    this.time = time;
    this.config = config;
    this.lastClusterMapUpdateTimeMs = time.milliseconds();
    updateClusterMapWritablePartitionPercentage();
  }

  @Override
  public boolean shouldThrottleRequest(long storageQuota, long currentUsage) {
    synchronized (this) {
      if (time.milliseconds() - lastClusterMapUpdateTimeMs >= config.adaptiveEnforcementClusterMapUpdateIntervalMs) {
        lastClusterMapUpdateTimeMs = time.milliseconds();
        updateClusterMapWritablePartitionPercentage();
      }
    }
    return currentUsage >= storageQuota
        && currentClusterMapWritablePartitionPercentage <= config.adaptiveEnforcementWritablePartitionThreshold;
  }

  private void updateClusterMapWritablePartitionPercentage() {
    this.currentClusterMapWritablePartitionPercentage =
        100 * clusterMap.getWritablePartitionIds(null).size() / clusterMap.getAllPartitionIds(null).size();
  }
}
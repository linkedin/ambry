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
import java.util.concurrent.atomic.AtomicLong;


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

/**
 * An implementation of {@link StorageQuotaEnforcementPolicy} that takes current cluster capacity into consideration when
 * deciding whether to throttle upload requests. This implementation would compare the percentage of writable partitions
 * to all partitions in the cluster, with the threshold provided in the {@link StorageQuotaConfig}, if the writable
 * partitions percentage is larger then the threshold, we don't throttle even if current usage is bigger than storage quota.
 */
class AdaptiveStorageQuotaEnforcementPolicy implements StorageQuotaEnforcementPolicy {
  private final ClusterMap clusterMap;
  private final StorageQuotaConfig config;
  private final Time time;
  private AtomicLong lastClusterMapUpdateTimeMs = new AtomicLong();
  private volatile int currentClusterMapWritablePartitionPercentage;

  /**
   * Constructor to instantiate an {@link AdaptiveStorageQuotaEnforcementPolicy}.
   * @param clusterMap The {@link ClusterMap} object.
   * @param time The {@link Time} object.
   * @param config The {@link StorageQuotaConfig}.
   */
  public AdaptiveStorageQuotaEnforcementPolicy(ClusterMap clusterMap, Time time, StorageQuotaConfig config) {
    this.clusterMap = clusterMap;
    this.time = time;
    this.config = config;
    this.lastClusterMapUpdateTimeMs.set(time.milliseconds());
    updateClusterMapWritablePartitionPercentage();
  }

  @Override
  public boolean shouldThrottleRequest(long storageQuota, long currentUsage) {
    long lastUpdateTimeMs = lastClusterMapUpdateTimeMs.get();
    if (time.milliseconds() - lastUpdateTimeMs >= config.adaptiveEnforcementClusterMapUpdateIntervalMs) {
      if (lastClusterMapUpdateTimeMs.compareAndSet(lastUpdateTimeMs, time.milliseconds())) {
        // Only one thread need to update the percentage. The other threads can just use old percentage value.
        updateClusterMapWritablePartitionPercentage();
      }
    }
    return currentUsage >= storageQuota
        && currentClusterMapWritablePartitionPercentage <= config.adaptiveEnforcementWritablePartitionThreshold;
  }

  /**
   * Update the current writable partition percentage in the cluster.
   */
  private void updateClusterMapWritablePartitionPercentage() {
    int numberOfAllPartitionIds = clusterMap.getAllPartitionIds(null).size();
    // If somehow the number of all partition ids is 0, then set writable partition percentage to 0 so we would fall back
    // to simple throttling policy.
    this.currentClusterMapWritablePartitionPercentage =
        numberOfAllPartitionIds != 0 ? 100 * clusterMap.getWritablePartitionIds(null).size() / numberOfAllPartitionIds
            : 0;
  }
}
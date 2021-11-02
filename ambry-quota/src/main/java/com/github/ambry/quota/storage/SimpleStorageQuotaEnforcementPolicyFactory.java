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


/**
 * Implementation of {@link StorageQuotaEnforcementPolicyFactory} to return a {@link SimpleStorageQuotaEnforcementPolicy}.
 */
public class SimpleStorageQuotaEnforcementPolicyFactory implements StorageQuotaEnforcementPolicyFactory {
  private final SimpleStorageQuotaEnforcementPolicy policy;

  /**
   * Constructor to instantiate {@link SimpleStorageQuotaEnforcementPolicyFactory}.
   * @param clusterMap The {@link ClusterMap} object.
   * @param config The {@link StorageQuotaConfig} object.
   */
  public SimpleStorageQuotaEnforcementPolicyFactory(ClusterMap clusterMap, StorageQuotaConfig config) {
    policy = new SimpleStorageQuotaEnforcementPolicy();
  }

  @Override
  public StorageQuotaEnforcementPolicy getPolicy() {
    return policy;
  }
}

/**
 * A simple {@link StorageQuotaEnforcementPolicy} implementation. When the current usage is equal or greater than the
 * quota, we start throttling.
 */
class SimpleStorageQuotaEnforcementPolicy implements StorageQuotaEnforcementPolicy {
  @Override
  public boolean shouldThrottleRequest(long storageQuota, long currentUsage) {
    return currentUsage >= storageQuota;
  }
}

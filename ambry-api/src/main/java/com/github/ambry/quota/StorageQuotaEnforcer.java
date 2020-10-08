/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

import java.util.Map;


/**
 * {@link StorageQuotaEnforcer} enforces the traffic throttling based on the storage quota and the current storage usage.
 *
 * Each traffic that changes storage usage is targeted at a specific account and container. Enforcer enforces storage
 * quota on each container. Before evaluating any traffic, enforcer has to retrieve storage quota of each container from
 * {@link StorageQuotaSource} and current storage uasage of each container from {@link StorageUsageRefresher}, by calling
 * {@link #initStorageQuota} and {@link #initStorageUsage} respectively.
 *
 * Container storage quota can be dynamic, it can be updated to increase or decrease the quota for specific containers.
 * To listen on these changes, Enforcer would return a {@link StorageQuotaSource.Listener}.
 *
 * Container storage usage is changing all the the time because of expired blobs and compacted deleted blobs. That's the
 * reason why relying on the incoming traffic won't give you a correct answer about the current storage usage. For instance,
 * if 1GB blob is uploaded to containerA and the TTL for this blob is one day. Then one day later, without any traffic
 * from client, the storage usage for containerA becomes 0. Since the storage usage from {@link StorageUsageRefresher} is
 * the source of the truth, enforcer has to listen on the changes for storage usage and replace the value in memory with
 * the value from {@link StorageUsageRefresher}.
 */
public interface StorageQuotaEnforcer {

  /**
   * Initialize the storage usage in {@link StorageQuotaEnforcer}.
   * @param usage The initial storage usage from {@link StorageUsageRefresher}.
   */
  void initStorageUsage(Map<String, Map<String, Long>> usage);

  /**
   * Return a {@link StorageUsageRefresher.Listener} to listen on the change of current storage usage.
   * Return null if {@link StorageQuotaEnforcer} doesn't want to listen on the changes.
   * @return The {@link StorageUsageRefresher.Listener}.
   */
  StorageUsageRefresher.Listener getUsageRefresherListener();

  /**
   * Initialize the storage quota in {@link StorageQuotaSource}.
   * @param quota The initial quota from {@link StorageQuotaSource}.
   */
  void initStorageQuota(Map<String, Map<String, Long>> quota);

  /**
   * Return a {@link StorageQuotaSource.Listener} to listen on the change of storage quota.
   * Return null if {@link StorageQuotaEnforcer} doesn't want to listen on the changes.
   * @return The {@link StorageQuotaSource.Listener}.
   */
  StorageQuotaSource.Listener getQuotaSourceListener();

  /**
   * Return true if the given {@link Operation} should be throttled.
   * @param accountId The accountId of this operation.
   * @param containerId The containerId of this operation.
   * @param op The {@link Operation}.
   * @param size The size of this operation. eg, if the op is Upload, size if the size of the content.
   * @return True is the given {@link Operation} should be throttled.
   */
  boolean shouldThrottle(short accountId, short containerId, Operation op, long size);

  /**
   * Change the {@link StorageQuotaEnforcer}'s mode to the given value. If the mode is {@link Mode#Tracking}, then {@link StorageQuotaEnforcer}
   * should never return true in {@link #shouldThrottle} method.
   * @param mode The new value for {@link Mode}.
   */
  void setMode(Mode mode);
}

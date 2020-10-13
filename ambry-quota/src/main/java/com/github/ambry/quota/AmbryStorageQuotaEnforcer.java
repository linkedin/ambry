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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A {@link StorageQuotaEnforcer} implementation. It keeps an in-memory map for storage quota and listens on the quota change
 * to update this in-memory map. It also keeps another in-memory map for storage usage of each container and listens on usage
 * change from {@link StorageUsageRefresher}.
 *
 * This implementation checks whether to throttle the operation only if the operation is {@link QuotaOperation#Post}. And when the
 * targeted account and container doesn't have a quota specified, it doesn't throttle the operation at all. Any legitimate
 * uploads would also increase the storage usage in the in-memory map.
 */
public class AmbryStorageQuotaEnforcer implements StorageQuotaEnforcer {
  private volatile QuotaMode mode = QuotaMode.Tracking;
  private volatile Map<String, Map<String, Long>> storageQuota;
  private volatile Map<String, Map<String, Long>> storageUsage;

  @Override
  public boolean shouldThrottle(short accountId, short containerId, QuotaOperation op, long size) {
    if (op != QuotaOperation.Post) {
      return false;
    }
    long quota = storageQuota.getOrDefault(String.valueOf(accountId), new HashMap<>())
        .getOrDefault(String.valueOf(containerId), Long.MAX_VALUE);

    AtomicBoolean exceedQuota = new AtomicBoolean(false);
    storageUsage.computeIfAbsent(String.valueOf(accountId), k -> new ConcurrentHashMap<>())
        .compute(String.valueOf(containerId), (k, v) -> {
          if (v == null) {
            return size;
          }
          if (v + size < quota) {
            return v + size;
          } else {
            exceedQuota.set(true);
            return v;
          }
        });
    return mode == QuotaMode.Throttling ? exceedQuota.get() : false;
  }

  @Override
  public void setQuotaMode(QuotaMode mode) {
    this.mode = mode;
  }

  @Override
  public void initStorageUsage(Map<String, Map<String, Long>> usage) {
    storageUsage = new ConcurrentHashMap<>();
    initMap(usage, storageUsage, true);
  }

  @Override
  public void initStorageQuota(Map<String, Map<String, Long>> quota) {
    storageQuota = new HashMap<>();
    initMap(quota, storageQuota, false);
  }

  @Override
  public StorageUsageRefresher.Listener getUsageRefresherListener() {
    return new StorageUsageRefresher.Listener() {
      @Override
      public void onNewContainerStorageUsage(Map<String, Map<String, Long>> containerStorageUsage) {
        initMap(containerStorageUsage, storageUsage, true);
      }
    };
  }

  @Override
  public StorageQuotaSource.Listener getQuotaSourceListener() {
    return new StorageQuotaSource.Listener() {
      @Override
      public void onNewContainerStorageQuota(Map<String, Map<String, Long>> containerStorageQuota) {
        Map<String, Map<String, Long>> newQuota = new HashMap<>();
        initMap(containerStorageQuota, newQuota, false);
        storageQuota = newQuota;
      }
    };
  }

  private void initMap(Map<String, Map<String, Long>> mapWithValue, Map<String, Map<String, Long>> mapToInit,
      boolean concurrentMap) {
    for (Map.Entry<String, Map<String, Long>> mapEntry : mapWithValue.entrySet()) {
      Map<String, Long> innerMap = mapToInit.computeIfAbsent(mapEntry.getKey(),
          k -> concurrentMap ? new ConcurrentHashMap<>() : new HashMap<>());
      for (Map.Entry<String, Long> innerMapEntry : mapEntry.getValue().entrySet()) {
        // Replace the value in the map anyway.
        innerMap.put(innerMapEntry.getKey(), innerMapEntry.getValue());
      }
    }
  }

  Map<String, Map<String, Long>> getStorageQuota() {
    return storageQuota;
  }

  Map<String, Map<String, Long>> getStorageUsage() {
    return storageUsage;
  }
}

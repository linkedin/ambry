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
package com.github.ambry.quota.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger logger = LoggerFactory.getLogger(AmbryStorageQuotaEnforcer.class);
  private volatile QuotaMode mode = QuotaMode.Tracking;
  private volatile Map<String, Map<String, Long>> storageQuota;
  private volatile Map<String, Map<String, Long>> storageUsage;
  private final QuotaExceededCallback quotaExceededCallback;

  /**
   * Constructor to instantiate {@link AmbryStorageQuotaEnforcer}.
   * @param quotaExceededCallback The {@link QuotaExceededCallback}
   */
  public AmbryStorageQuotaEnforcer(QuotaExceededCallback quotaExceededCallback) {
    this.quotaExceededCallback = quotaExceededCallback;
  }

  @Override
  public boolean shouldThrottle(short accountId, short containerId, QuotaOperation op, long size) {
    if (op != QuotaOperation.Post) {
      return false;
    }
    long quota = storageQuota.getOrDefault(String.valueOf(accountId), new HashMap<>())
        .getOrDefault(String.valueOf(containerId), Long.MAX_VALUE);

    AtomicBoolean exceedQuota = new AtomicBoolean(false);
    AtomicLong existingUsage = new AtomicLong();
    storageUsage.computeIfAbsent(String.valueOf(accountId), k -> new ConcurrentHashMap<>())
        .compute(String.valueOf(containerId), (k, v) -> {
          existingUsage.set(v == null ? 0 : v);
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
    if (exceedQuota.get() && quota != Long.MAX_VALUE && quotaExceededCallback != null) {
      quotaExceededCallback.onQuotaExceeded(mode, accountId, containerId, op, quota, existingUsage.get(), size);
    }
    return mode == QuotaMode.Throttling ? exceedQuota.get() : false;
  }

  @Override
  public void setQuotaMode(QuotaMode mode) {
    logger.info("Setting Quota mode to {}", mode.name());
    this.mode = mode;
  }

  @Override
  public void initStorageUsage(Map<String, Map<String, Long>> usage) {
    logger.info("Initializing storage usage for {} accounts", usage.size());
    storageUsage = new ConcurrentHashMap<>();
    initMap(usage, storageUsage, true);
  }

  @Override
  public void initStorageQuota(Map<String, Map<String, Long>> quota) {
    logger.info("Initializing storage quota for {} accounts");
    storageQuota = new HashMap<>();
    initMap(quota, storageQuota, false);
  }

  @Override
  public void registerListeners(StorageQuotaSource storageQuotaSource, StorageUsageRefresher storageUsageRefresher) {
    logger.info("Register quota source and usage refresher listeners");
    storageQuotaSource.registerListener(getQuotaSourceListener());
    storageUsageRefresher.registerListener(getUsageRefresherListener());
  }

  /**
   * Return a {@link StorageQuotaSource.Listener}, only used in test.
   * @return {@link StorageQuotaSource.Listener}.
   */
  StorageQuotaSource.Listener getQuotaSourceListener() {
    return containerStorageQuota -> {
      logger.trace("QuotaSourceListener invoked with new container storage quota");
      logger.debug("New quota: {}", containerStorageQuota);
      Map<String, Map<String, Long>> newQuota = new HashMap<>();
      initMap(containerStorageQuota, newQuota, false);
      storageQuota = newQuota;
    };
  }

  /**
   * Return a {@link StorageUsageRefresher.Listener}, only used in test.
   * @return {@link StorageUsageRefresher.Listener}.
   */
  StorageUsageRefresher.Listener getUsageRefresherListener() {
    return containerStorageUsage -> {
      logger.trace("UsageRefresherListener invoked with new container storage usage");
      logger.debug("New usage: {}", containerStorageUsage);
      initMap(containerStorageUsage, storageUsage, true);
    };
  }

  /**
   * Initialize the map with another given map and replace the value in the map with the value from given map.
   * @param mapWithValue The given map used to initialize a different map.
   * @param mapToInit The map to be initialized.
   * @param concurrentMap If true, then create a concurent hashmap for the inner map when it doesn't exist in the map
   *                      to be initialized.
   */
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

  /**
   * Return the in-memory copy of the storage quota, only used in test.
   * @return The in-memory copy of the storage quota.
   */
  Map<String, Map<String, Long>> getStorageQuota() {
    return storageQuota;
  }

  /**
   * Return the in-memory copy of the storage usage, only used in test.
   * @return The in-memory copy of the storage usage.
   */
  Map<String, Map<String, Long>> getStorageUsage() {
    return storageUsage;
  }
}

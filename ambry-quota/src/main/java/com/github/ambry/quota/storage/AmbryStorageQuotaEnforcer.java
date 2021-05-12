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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.quota.QuotaMode;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
  private final QuotaExceededCallback quotaExceededCallback;
  private final StorageQuotaServiceMetrics metrics;
  private volatile QuotaMode mode = QuotaMode.TRACKING;
  private volatile Map<String, Map<String, Long>> storageQuota;
  private volatile Map<String, Map<String, Long>> storageUsage;

  /**
   * Constructor to instantiate {@link AmbryStorageQuotaEnforcer}.
   * @param quotaExceededCallback The {@link QuotaExceededCallback}
   * @param metrics The {@link StorageQuotaServiceMetrics}.
   */
  public AmbryStorageQuotaEnforcer(QuotaExceededCallback quotaExceededCallback, StorageQuotaServiceMetrics metrics) {
    this.quotaExceededCallback = quotaExceededCallback;
    this.metrics = Objects.requireNonNull(metrics, "StorageQuotaServiceMetrics is null");
  }

  @Override
  public boolean shouldThrottle(RestRequest restRequest) {
    boolean rst = false;
    logger.debug("RestRequest: method {}, size {}", restRequest.getRestMethod(), restRequest.getSize());
    if (restRequest.getRestMethod() == RestMethod.POST && restRequest.getSize() > 0) {
      try {
        Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
        Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
        logger.debug("Account id: {}, Container id: {}, size: {}", account.getId(), container.getId(),
            restRequest.getSize());
        rst = shouldThrottle(account.getId(), container.getId(), QuotaOperation.Post, restRequest.getSize());
      } catch (Exception e) {
        logger.error("Failed to apply shouldThrottle logic to RestRequest {}", restRequest, e);
        rst = false;
      }
    }
    return rst;
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

  boolean shouldThrottle(short accountId, short containerId, QuotaOperation op, long size) {
    if (op != QuotaOperation.Post) {
      return false;
    }
    long startTimeMs = System.currentTimeMillis();
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
    logger.debug("Account id {} container id {} quota {}, existing usage {} new size {}, new usage {}", accountId,
        containerId, quota == Long.MAX_VALUE ? "MAX_VALUE" : String.valueOf(quota), existingUsage.get(), size,
        storageUsage.get(String.valueOf(accountId)).get(String.valueOf(containerId)));
    if (exceedQuota.get()) {
      metrics.quotaExceededCount.inc();
    }
    if (exceedQuota.get() && quota != Long.MAX_VALUE && quotaExceededCallback != null) {
      long cbStartTimeMs = System.currentTimeMillis();
      quotaExceededCallback.onQuotaExceeded(mode, accountId, containerId, op, quota, existingUsage.get(), size);
      metrics.quotaExceededCallbackTimeMs.update(System.currentTimeMillis() - cbStartTimeMs);
    }
    metrics.shouldThrottleTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return mode == QuotaMode.THROTTLING && exceedQuota.get();
  }

  /**
   * Return quota and current usage for the account/container carried in the given {@code restRequest}.
   * If there is no account and container found in the {@code restRequest}, this method would return -1
   * for quota. If there is no quota found for the account/container, this method would return -1 for
   * quota as well.
   * @param restRequest the {@link RestRequest} that carries account and container in the header.
   * @return A {@link Pair} whose first element is quota the second element is current storage usage.
   */
  Pair<Long, Long> getQuotaAndUsage(RestRequest restRequest) {
    long quotaValue = -1L;
    long currentUsage = 0L;
    try {
      short accountId = RestUtils.getAccountFromArgs(restRequest.getArgs()).getId();
      short containerId = RestUtils.getContainerFromArgs(restRequest.getArgs()).getId();
      quotaValue = storageQuota.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .getOrDefault(String.valueOf(containerId), -1L);
      if (quotaValue != -1L) {
        currentUsage = storageUsage.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
            .getOrDefault(String.valueOf(containerId), 0L);
      }
    } catch (Exception e) {
      logger.error("Failed to getQuotaAndUsage logic to RestRequest {}", restRequest, e);
    }
    return new Pair<>(quotaValue, currentUsage);
  }

  /**
   * Add given {@code usage} to the current storage usage of account/container carried in {@code restRequest} even
   * if the result exceeds quota for the target account/container. If there is no account and container found in
   * {@code restRequest}, then this is a no-op. If there is no quota found for the account/container, then this is
   * a no-op. A {@link Pair} whose first element is quota and second element is the storage usage after charge.
   * @param restRequest the {@link RestRequest} that carries account and container in the header.
   * @param usage the usage to charge
   * @return A {@link Pair} whose first element is quota and second element is the storage usage after charge.
   */
  Pair<Long, Long> charge(RestRequest restRequest, long usage) {
    long quotaValue = -1L;
    long usageAfterCharge = 0L;
    try {
      short accountId = RestUtils.getAccountFromArgs(restRequest.getArgs()).getId();
      short containerId = RestUtils.getContainerFromArgs(restRequest.getArgs()).getId();
      quotaValue = storageQuota.getOrDefault(String.valueOf(accountId), new HashMap<>())
          .getOrDefault(String.valueOf(containerId), -1L);
      if (quotaValue != -1L) {
        AtomicLong existingUsage = new AtomicLong();
        storageUsage.computeIfAbsent(String.valueOf(accountId), k -> new ConcurrentHashMap<>())
            .compute(String.valueOf(containerId), (k, v) -> {
              existingUsage.set(v == null ? 0 : v);
              if (v == null) {
                return usage;
              }
              return v + usage;
            });
        usageAfterCharge = existingUsage.addAndGet(usage);
      }
    } catch (Exception e) {
      logger.error("Failed to getQuotaAndUsage logic to RestRequest {}", restRequest, e);
    }
    return new Pair<>(quotaValue, usageAfterCharge);
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

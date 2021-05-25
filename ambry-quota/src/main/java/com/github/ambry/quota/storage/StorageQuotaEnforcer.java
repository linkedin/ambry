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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link QuotaEnforcer} implementation on each container's storage usage.
 */
public class StorageQuotaEnforcer implements QuotaEnforcer {
  private static final Logger logger = LoggerFactory.getLogger(StorageQuotaEnforcer.class);
  private static final String STORAGE_QUOTA_SERVICE_PREFIX = "storage-quota-enforcer";
  private static final int HTTP_STATUS_THROTTLE = 429;
  private static final int HTTP_STATUS_ALLOW = 200;
  private static final long NO_RETRY = -1L;
  private static final double BYTES_IN_GB = 1024 * 1024 * 1024;
  private final StorageUsageRefresher storageUsageRefresher;
  private final StorageQuotaSource storageQuotaSource;
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;
  private final StorageQuotaServiceMetrics metrics;

  private volatile Map<String, Map<String, Long>> storageQuota;
  private volatile Map<String, Map<String, Long>> storageUsage;

  // The quota recommendation returned when there is no quota found for the given account/container.
  private static final QuotaRecommendation NO_QUOTA_VALUE_RECOMMENDATION =
      new QuotaRecommendation(false, 0.0f, QuotaName.STORAGE_IN_GB, HTTP_STATUS_ALLOW, NO_RETRY);

  /**
   * Constructor to instantiate a new {@link StorageQuotaEnforcer}.
   * @param storageQuotaConfig the {@link StorageQuotaConfig}.
   * @param accountStatsStore the {@link AccountStatsStore}.
   * @throws Exception
   */
  public StorageQuotaEnforcer(StorageQuotaConfig storageQuotaConfig, StorageQuotaSource storageQuotaSource,
      AccountStatsStore accountStatsStore) throws Exception {
    Objects.requireNonNull(accountStatsStore, "AccountStatsStore is null");
    this.metrics = new StorageQuotaServiceMetrics(new MetricRegistry());
    this.scheduler = Utils.newScheduler(1, STORAGE_QUOTA_SERVICE_PREFIX, false);
    this.config = storageQuotaConfig;
    this.storageQuotaSource = storageQuotaSource;
    this.storageUsageRefresher =
        new MySqlStorageUsageRefresher(accountStatsStore, this.scheduler, this.config, metrics);
  }

  StorageQuotaEnforcer(StorageQuotaConfig storageQuotaConfig, StorageQuotaSource storageQuotaSource,
      StorageUsageRefresher storageUsageRefresher) {
    this.metrics = new StorageQuotaServiceMetrics(new MetricRegistry());
    this.scheduler = null;
    this.config = storageQuotaConfig;
    this.storageQuotaSource = storageQuotaSource;
    this.storageUsageRefresher = storageUsageRefresher;
  }

  @Override
  public void init() throws Exception {
    initStorageUsage(storageUsageRefresher.getContainerStorageUsage());
    initStorageQuota(storageQuotaSource.getContainerQuota());
    registerListeners(storageQuotaSource, storageUsageRefresher);
  }

  @Override
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest, BlobInfo blobInfo,
      Map<QuotaName, Double> requestCostMap) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      return NO_QUOTA_VALUE_RECOMMENDATION;
    }
    if (!requestCostMap.containsKey(QuotaName.STORAGE_IN_GB)) {
      // No cost for the desired QuotaName, then just call recommend
      return recommend(restRequest);
    }

    // The cost is number of bytes in GB. Convert it back to raw number.
    long cost = (long) (requestCostMap.get(QuotaName.STORAGE_IN_GB).doubleValue() * BYTES_IN_GB);
    Pair<Long, Long> pair = charge(restRequest, cost);
    return recommendBasedOnQuotaAndUsage(pair);
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      return NO_QUOTA_VALUE_RECOMMENDATION;
    }
    Pair<Long, Long> pair = getQuotaAndUsage(restRequest);
    return recommendBasedOnQuotaAndUsage(pair);
  }

  /**
   * Return a {@link QuotaRecommendation} based on the given pair of quota and current usage.
   * @param pair The {@link Pair} of quota and current usage.
   * @return A {@link QuotaRecommendation}.
   */
  private QuotaRecommendation recommendBasedOnQuotaAndUsage(Pair<Long, Long> pair) {
    long quotaValue = pair.getFirst();
    long currentUsage = pair.getSecond();
    if (quotaValue == -1L) {
      // There is no quota set for the given account/container
      return NO_QUOTA_VALUE_RECOMMENDATION;
    }
    boolean shouldThrottle = currentUsage >= quotaValue;
    float usagePercentage = currentUsage >= quotaValue ? 100f : ((float) currentUsage) / quotaValue * 100f;
    return new QuotaRecommendation(shouldThrottle, usagePercentage, QuotaName.STORAGE_IN_GB,
        shouldThrottle ? HTTP_STATUS_THROTTLE : HTTP_STATUS_ALLOW, NO_RETRY);
  }

  /**
   * This {@link QuotaEnforcer} doesn't require any {@link QuotaSource} since it will fetch the quota internally.
   * @return a null QuotaSource.
   */
  @Override
  public QuotaSource getQuotaSource() {
    return storageQuotaSource;
  }

  @Override
  public void shutdown() {
    if (scheduler != null) {
      long schedulerTimeout = Math.max(config.refresherPollingIntervalMs, config.sourcePollingIntervalMs);
      Utils.shutDownExecutorService(scheduler, schedulerTimeout, TimeUnit.MILLISECONDS);
    }
    storageQuotaSource.shutdown();
  }

  void initStorageUsage(Map<String, Map<String, Long>> usage) {
    logger.info("Initializing storage usage for {} accounts", usage.size());
    storageUsage = new ConcurrentHashMap<>();
    initMap(usage, storageUsage, true);
  }

  Map<String, Map<String, Long>> getStorageUsage() {
    return Collections.unmodifiableMap(storageUsage);
  }

  void initStorageQuota(Map<String, Map<String, Long>> quota) {
    logger.info("Initializing storage quota for {} accounts");
    storageQuota = new HashMap<>();
    initMap(quota, storageQuota, false);
  }

  Map<String, Map<String, Long>> getStorageQuota() {
    return Collections.unmodifiableMap(storageQuota);
  }

  private void registerListeners(StorageQuotaSource storageQuotaSource, StorageUsageRefresher storageUsageRefresher) {
    logger.info("Register quota source and usage refresher listeners");
    storageQuotaSource.registerListener(getQuotaSourceListener());
    storageUsageRefresher.registerListener(getUsageRefresherListener());
    // Register the storage usage callback to source.
    storageQuotaSource.addStorageUsageCallback(this::getStorageUsage);
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
}

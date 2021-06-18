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
import com.github.ambry.account.Container;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
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
  private static final long BYTES_IN_GB = 1024 * 1024 * 1024;
  private static final long CHUNK_COST_IN_BYTES = 4 * 1024 * 1024;
  // The quota recommendation returned when there is no quota found for the given account/container.
  private static final QuotaRecommendation NO_QUOTA_VALUE_RECOMMENDATION =
      new QuotaRecommendation(false, 0.0f, QuotaName.STORAGE_IN_GB, HTTP_STATUS_ALLOW, NO_RETRY);
  protected final StorageUsageRefresher storageUsageRefresher;
  protected final QuotaSource quotaSource;
  protected final StorageQuotaConfig config;
  protected final StorageQuotaServiceMetrics metrics;
  protected final ScheduledExecutorService scheduler;
  protected volatile Map<String, Map<String, Long>> storageUsage;

  /**
   * Constructor to instantiate a new {@link StorageQuotaEnforcer}.
   * @param storageQuotaConfig the {@link StorageQuotaConfig}.
   * @param quotaSource the {@link QuotaSource} to get the quota.
   * @param accountStatsStore the {@link AccountStatsStore}.
   * @throws Exception
   */
  public StorageQuotaEnforcer(StorageQuotaConfig storageQuotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore) throws Exception {
    Objects.requireNonNull(accountStatsStore, "AccountStatsStore is null");
    this.metrics = new StorageQuotaServiceMetrics(new MetricRegistry());
    this.scheduler = Utils.newScheduler(1, STORAGE_QUOTA_SERVICE_PREFIX, false);
    this.config = storageQuotaConfig;
    this.quotaSource = quotaSource;
    this.storageUsageRefresher =
        new MySqlStorageUsageRefresher(accountStatsStore, this.scheduler, this.config, metrics);
  }

  /**
   * Constructor to instantiate a new {@link StorageQuotaEnforcer}.
   * @param storageQuotaConfig the {@link StorageQuotaConfig}.
   * @param quotaSource the {@link QuotaSource} to get the quota.
   * @param storageUsageRefresher the {@link StorageUsageRefresher} to refresh the storage usage for each container.
   * @throws Exception
   */
  StorageQuotaEnforcer(StorageQuotaConfig storageQuotaConfig, QuotaSource quotaSource,
      StorageUsageRefresher storageUsageRefresher) {
    this.metrics = new StorageQuotaServiceMetrics(new MetricRegistry());
    this.scheduler = null;
    this.config = storageQuotaConfig;
    this.quotaSource = quotaSource;
    this.storageUsageRefresher = storageUsageRefresher;
  }

  @Override
  public void init() throws Exception {
    initStorageUsage(storageUsageRefresher.getContainerStorageUsage());
    registerListeners();
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
  public QuotaRecommendation chargeAndRecommend(RestRequest restRequest) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      return NO_QUOTA_VALUE_RECOMMENDATION;
    }

    // The cost is number of bytes in GB for one chunk.
    Pair<Long, Long> pair = charge(restRequest, CHUNK_COST_IN_BYTES);
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
    boolean shouldThrottle = config.shouldThrottle && currentUsage >= quotaValue;
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
    return quotaSource;
  }

  @Override
  public void shutdown() {
    if (scheduler != null) {
      Utils.shutDownExecutorService(scheduler, config.refresherPollingIntervalMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Initialize the storage usage from the given map.
   * @param usage The map that contains storage usage for containers.
   */
  void initStorageUsage(Map<String, Map<String, Long>> usage) {
    logger.info("Initializing storage usage for {} accounts", usage.size());
    storageUsage = new ConcurrentHashMap<>();
    initMap(usage, storageUsage, true);
  }

  /**
   * Only for testing.
   * @return The unmodified version of storage usage.
   */
  Map<String, Map<String, Long>> getStorageUsage() {
    return Collections.unmodifiableMap(storageUsage);
  }

  /**
   * Register listeners to {@link StorageUsageRefresher}.
   */
  private void registerListeners() {
    logger.info("Register quota source and usage refresher listeners");
    storageUsageRefresher.registerListener(getUsageRefresherListener());
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
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      quotaValue = getQuotaValueForContainer(container);
      if (quotaValue != -1L) {
        currentUsage =
            storageUsage.computeIfAbsent(String.valueOf(container.getParentAccountId()), k -> new HashMap<>())
                .getOrDefault(String.valueOf(container.getId()), 0L);
      }
    } catch (Exception e) {
      logger.error("Failed to getQuotaAndUsage for RestRequest {}", restRequest, e);
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
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      quotaValue = getQuotaValueForContainer(container);
      if (quotaValue != -1L) {
        AtomicLong existingUsage = new AtomicLong();
        storageUsage.computeIfAbsent(String.valueOf(container.getParentAccountId()), k -> new ConcurrentHashMap<>())
            .compute(String.valueOf(container.getId()), (k, v) -> {
              existingUsage.set(v == null ? 0 : v);
              if (v == null) {
                return usage;
              }
              return v + usage;
            });
        usageAfterCharge = existingUsage.addAndGet(usage);
      }
    } catch (Exception e) {
      logger.error("Failed to charge for RestRequest {}", restRequest, e);
    }
    return new Pair<>(quotaValue, usageAfterCharge);
  }

  /**
   * Return the storage quota value (in bytes) for given {@link Container}.
   * @param container The {@link Container} to fetch quota value.
   * @return The storage quota value (in bytes)
   */
  protected long getQuotaValueForContainer(Container container) {
    Quota quota = quotaSource.getQuota(QuotaResource.fromContainer(container), QuotaName.STORAGE_IN_GB);
    if (quota != null) {
      return (long) quota.getQuotaValue() * BYTES_IN_GB;
    } else {
      return -1;
    }
  }
}

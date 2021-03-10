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
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * An {@link QuotaEnforcer} implementation on each container's storage usage. There are some overlapping code in this
 * class with {@link AmbryStorageQuotaService} since we are transitioning to a unified interface with Request Quota.
 *
 * This is a different from {@link StorageQuotaEnforcer} despite the similarity of names. It has nothing to do with
 * {@link StorageQuotaEnforcer} in fact.
 */
public class StorageQuotaAdaptorQuotaEnforcer implements QuotaEnforcer {
  private static final String STORAGE_QUOTA_SERVICE_PREFIX = "storage-quota-service";
  private static final int HTTP_STATUS_THROTTLE = 429;
  private static final int HTTP_STATUS_ALLOW = 200;
  private static final long NO_RETRY = -1L;
  private static final double BYTES_IN_GB = 1024 * 1024 * 1024;
  private final StorageUsageRefresher storageUsageRefresher;
  private final StorageQuotaSource storageQuotaSource;
  private final AmbryStorageQuotaEnforcer storageQuotaEnforcer;
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;
  private final StorageQuotaServiceMetrics metrics;

  // The quota recommendation returned when there is no quota found for the given account/container.
  private static final QuotaRecommendation NO_QUOTA_VALUE_RECOMMENDATION =
      new QuotaRecommendation(false, 0.0f, QuotaName.STORAGE_IN_GB, HTTP_STATUS_ALLOW, NO_RETRY);

  /**
   * Constructor to instantiate a new {@link StorageQuotaAdaptorQuotaEnforcer}.
   * @param storageQuotaConfig the {@link StorageQuotaConfig}.
   * @param accountStatsStore the {@link AccountStatsStore}.
   * @throws Exception
   */
  public StorageQuotaAdaptorQuotaEnforcer(StorageQuotaConfig storageQuotaConfig, AccountStatsStore accountStatsStore)
      throws Exception {
    Objects.requireNonNull(accountStatsStore, "AccountStatsStore is null");
    this.metrics = new StorageQuotaServiceMetrics(new MetricRegistry());
    this.scheduler = Utils.newScheduler(1, STORAGE_QUOTA_SERVICE_PREFIX, false);
    this.config = storageQuotaConfig;
    this.storageUsageRefresher =
        new MySqlStorageUsageRefresher(accountStatsStore, this.scheduler, this.config, metrics);
    this.storageQuotaSource =
        Utils.<StorageQuotaSourceFactory>getObj(config.sourceFactory, scheduler, config).getStorageQuotaSource();
    this.storageQuotaEnforcer = new AmbryStorageQuotaEnforcer(null, this.metrics);
  }

  @Override
  public void init() throws Exception {
    storageQuotaEnforcer.setQuotaMode(config.enforcerMode);
    storageQuotaEnforcer.initStorageUsage(storageUsageRefresher.getContainerStorageUsage());
    storageQuotaEnforcer.initStorageQuota(storageQuotaSource.getContainerQuota());
    storageQuotaEnforcer.registerListeners(storageQuotaSource, storageUsageRefresher);
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
    Pair<Long, Long> pair = storageQuotaEnforcer.charge(restRequest, cost);
    return recommendBasedOnQuotaAndUsage(pair);
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      return NO_QUOTA_VALUE_RECOMMENDATION;
    }
    Pair<Long, Long> pair = storageQuotaEnforcer.getQuotaAndUsage(restRequest);
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
    return null;
  }

  @Override
  public void shutdown() {
    if (scheduler != null) {
      long schedulerTimeout = Math.max(config.refresherPollingIntervalMs, config.sourcePollingIntervalMs);
      Utils.shutDownExecutorService(scheduler, schedulerTimeout, TimeUnit.MILLISECONDS);
    }
  }
}

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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
  private static final List<QuotaName> SUPPORTED_QUOTA_NAMES =
      Collections.unmodifiableList(Arrays.asList(QuotaName.STORAGE_IN_GB));
  private static final String STORAGE_QUOTA_SERVICE_PREFIX = "storage-quota-enforcer";
  private static final long NO_RETRY = -1L;
  private static final long BYTES_IN_GB = 1024 * 1024 * 1024;

  // The quota recommendation to return when we can ignore the storage quota restrain.
  private static final QuotaRecommendation IGNORE_STORAGE_QUOTA_RECOMMENDATION =
      new QuotaRecommendation(QuotaAction.ALLOW, 0.0f, QuotaName.STORAGE_IN_GB, NO_RETRY);

  // The quota recommendation to return when we reject the upload due to storage quota not found for
  // this account/container. Notice that QuotaAction is REJECT, but the percentage is 0.
  private static final QuotaRecommendation REJECT_DUE_TO_MISSING_QUOTA_RECOMMENDATION =
      new QuotaRecommendation(QuotaAction.REJECT, 0.0f, QuotaName.STORAGE_IN_GB, NO_RETRY);

  protected final StorageUsageRefresher storageUsageRefresher;
  protected final QuotaSource quotaSource;
  protected final StorageQuotaConfig config;
  protected final StorageQuotaServiceMetrics metrics;
  protected final ScheduledExecutorService scheduler;
  protected final AccountService accountService;
  protected volatile Map<QuotaResource, Long> storageUsages;

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
    if (!(quotaSource instanceof AccountServiceSupplier)) {
      throw new IllegalArgumentException("QuotaSource has to be able to return AccountService");
    }
    this.accountService = ((AccountServiceSupplier) quotaSource).getAccountService();
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
    if (!(quotaSource instanceof AccountServiceSupplier)) {
      throw new IllegalArgumentException("QuotaSource has to be able to return AccountService");
    }
    this.accountService = ((AccountServiceSupplier) quotaSource).getAccountService();
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
  public QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      return null;
    }
    if (!requestCostMap.containsKey(QuotaName.STORAGE_IN_GB)) {
      // No cost for the desired QuotaName, then just call recommend
      return null;
    }

    // The cost is number of bytes in GB. Convert it back to raw number.
    long cost = (long) (requestCostMap.get(QuotaName.STORAGE_IN_GB).doubleValue() * BYTES_IN_GB);
    Pair<Long, Long> pair = charge(restRequest, cost);
    return recommendBasedOnQuotaAndUsage(pair);
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) {
    if (!RestUtils.isUploadRequest(restRequest)) {
      // This is not an upload request, it doesn't change any storage footprint of this account/container.
      // Storage quota will not be enforced on this request.
      return IGNORE_STORAGE_QUOTA_RECOMMENDATION;
    }
    Pair<Long, Long> pair = getQuotaAndUsage(restRequest);
    return recommendBasedOnQuotaAndUsage(pair);
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) {
    return false;
  }

  @Override
  public List<QuotaName> supportedQuotaNames() {
    return SUPPORTED_QUOTA_NAMES;
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
   * Return a {@link QuotaRecommendation} based on the given pair of quota and current usage.
   * @param pair The {@link Pair} of quota and current usage.
   * @return A {@link QuotaRecommendation}.
   */
  QuotaRecommendation recommendBasedOnQuotaAndUsage(Pair<Long, Long> pair) {
    long quotaValue = pair.getFirst();
    long currentUsage = pair.getSecond();
    if (quotaValue == -1L) {
      // There is no quota set for the given account/container
      if (config.shouldRejectRequestWithoutQuota && config.shouldThrottle) {
        return REJECT_DUE_TO_MISSING_QUOTA_RECOMMENDATION;
      } else {
        // If we don't reject requests with missing storage quota, then just ignore it.
        return IGNORE_STORAGE_QUOTA_RECOMMENDATION;
      }
    }
    QuotaAction quotaAction = QuotaAction.ALLOW;
    if (config.shouldThrottle && currentUsage >= quotaValue) {
      quotaAction = QuotaAction.REJECT;
    }
    float usagePercentage = currentUsage >= quotaValue ? 100f : ((float) currentUsage) / quotaValue * 100f;
    return new QuotaRecommendation(quotaAction, usagePercentage, QuotaName.STORAGE_IN_GB, NO_RETRY);
  }

  /**
   * Initialize the storage usage from the given map.
   * @param usage The map that contains storage usage for containers.
   */
  void initStorageUsage(Map<String, Map<String, Long>> usage) {
    logger.info("Initializing storage usage for {} accounts", usage.size());
    storageUsages = new ConcurrentHashMap<>();
    initMap(usage, storageUsages);
  }

  /**
   * Only for testing.
   * @return The unmodified version of storage usages.
   */
  Map<QuotaResource, Long> getStorageUsages() {
    return Collections.unmodifiableMap(storageUsages);
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
      initMap(containerStorageUsage, this.storageUsages);
    };
  }

  /**
   * Initialize the map with another given map and replace the value in the map with the value from given map.
   * @param mapWithValue The given map used to initialize a different map.
   * @param mapToInit The storage usage map to be initialized.
   */
  private void initMap(Map<String, Map<String, Long>> mapWithValue, Map<QuotaResource, Long> mapToInit) {
    for (Map.Entry<String, Map<String, Long>> mapEntry : mapWithValue.entrySet()) {
      Account account = accountService.getAccountById(Short.valueOf(mapEntry.getKey()));
      if (account != null) {
        if (account.getQuotaResourceType() == QuotaResourceType.ACCOUNT) {
          // Put this in account storage usage
          long accountUsage = mapEntry.getValue().values().stream().mapToLong(Long::longValue).sum();
          mapToInit.put(QuotaResource.fromAccount(account), accountUsage);
        } else {
          for (Map.Entry<String, Long> innerMapEntry : mapEntry.getValue().entrySet()) {
            // Replace the value in the map anyway.
            Container container = account.getContainerById(Short.valueOf(innerMapEntry.getKey()));
            if (container != null) {
              mapToInit.put(QuotaResource.fromContainer(container), innerMapEntry.getValue());
            }
          }
        }
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
      Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      QuotaResource quotaResource =
          account.getQuotaResourceType() == QuotaResourceType.ACCOUNT ? QuotaResource.fromAccount(account)
              : QuotaResource.fromContainer(container);
      quotaValue = getQuotaValueForResource(quotaResource);
      if (quotaValue != -1L) {
        currentUsage = storageUsages.getOrDefault(quotaResource, 0L);
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
      Account account = RestUtils.getAccountFromArgs(restRequest.getArgs());
      Container container = RestUtils.getContainerFromArgs(restRequest.getArgs());
      QuotaResource quotaResource =
          account.getQuotaResourceType() == QuotaResourceType.ACCOUNT ? QuotaResource.fromAccount(account)
              : QuotaResource.fromContainer(container);
      quotaValue = getQuotaValueForResource(quotaResource);
      if (quotaValue != -1L) {
        AtomicLong existingUsage = new AtomicLong();
        storageUsages.compute(quotaResource, (k, v) -> {
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
   * Return the storage quota value (in bytes) for given {@link QuotaResource}.
   * @param resource The {@link QuotaResource} to fetch quota value.
   * @return The storage quota value (in bytes)
   */
  protected long getQuotaValueForResource(QuotaResource resource) {
    long quotaValue = -1;
    try {
      Quota quota = quotaSource.getQuota(resource, QuotaName.STORAGE_IN_GB);
      quotaValue = (long) quota.getQuotaValue() * BYTES_IN_GB;
    } catch (QuotaException quotaException) {
      logger.warn("Quota not found for resource id: {}, type: {}", resource.getResourceId(),
          resource.getQuotaResourceType());
    }
    return quotaValue;
  }

  /**
   * Interface to return {@link AccountService}.
   */
  public interface AccountServiceSupplier {

    /**
     * @return {@link AccountService}.
     */
    AccountService getAccountService();
  }
}

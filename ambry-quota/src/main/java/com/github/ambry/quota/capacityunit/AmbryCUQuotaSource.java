/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link QuotaSource} implementation that keeps the quota and usage values in memory, and treats Ambry frontend's
 * read and write bandwidth capacity as system resources.
 */
public class AmbryCUQuotaSource implements QuotaSource {
  static final String REFRESHER_THREAD_NAME_PREFIX = "inmem-quota-source-refresher";
  private static final EnumSet<QuotaName> SUPPORTED_QUOTA_NAMES =
      EnumSet.of(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT);
  private static final EnumSet<QuotaResourceType> SUPPORTED_QUOTA_RESOURCE_TYPES =
      EnumSet.of(QuotaResourceType.ACCOUNT, QuotaResourceType.CONTAINER);
  private static final Logger LOGGER = LoggerFactory.getLogger(AmbryCUQuotaSource.class);
  private static final long DEFAULT_RCU_FOR_NEW_RESOURCE = 0;
  private static final long DEFAULT_WCU_FOR_NEW_RESOURCE = 0;
  protected final CapacityUnit feQuota; // Ambry frontend's CU capacity.
  protected final AtomicReference<CapacityUnit> feUsage; // Ambry frontend's CU usage.
  protected final ConcurrentMap<String, CapacityUnit> cuQuota; // in memory quota for all resources.
  protected final ConcurrentMap<String, CapacityUnit> cuUsage; // in memory quota usage for all resources.
  private final ScheduledExecutorService usageRefresher;
  private final long aggregationWindowsInSecs;
  private final int feReadUsageAmplificationFactor;
  private final int feWriteUsageAmplificationFactor;
  private final AtomicBoolean isReady;
  private final QuotaMetrics quotaMetrics;

  /**
   * Constructor for {@link AmbryCUQuotaSource}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   * @throws IOException in case of any exception.
   */
  public AmbryCUQuotaSource(QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) throws IOException {
    feQuota = JsonCUQuotaDataProviderUtil.getFeCUCapacityFromJson(quotaConfig.frontendCUCapacityInJson);
    cuQuota = new ConcurrentHashMap<>(
        JsonCUQuotaDataProviderUtil.getCUQuotasFromJson(quotaConfig.resourceCUQuotaInJson, accountService));
    cuUsage = new ConcurrentHashMap<>();
    usageRefresher = Utils.newScheduler(1, REFRESHER_THREAD_NAME_PREFIX, true);
    aggregationWindowsInSecs = quotaConfig.cuQuotaAggregationWindowInSecs;
    isReady = new AtomicBoolean(false);
    feUsage = new AtomicReference<>(null);
    this.quotaMetrics = quotaMetrics;
    this.feReadUsageAmplificationFactor = routerConfig.routerGetRequestParallelism;
    this.feWriteUsageAmplificationFactor = routerConfig.routerPutRequestParallelism;
    quotaMetrics.setupFeUsageAmplificationFactorMetrics(feReadUsageAmplificationFactor,
        feWriteUsageAmplificationFactor);
  }

  @Override
  public synchronized void init() {
    if (isReady.get()) {
      LOGGER.warn("InMemoryCuQuotaSource is already initialized.");
      return;
    }
    feUsage.set(new CapacityUnit());
    cuQuota.keySet().forEach(key -> cuUsage.put(key, new CapacityUnit()));
    usageRefresher.scheduleAtFixedRate(this::resetQuotaUsage, aggregationWindowsInSecs, aggregationWindowsInSecs,
        TimeUnit.SECONDS);
    quotaMetrics.createMetricsForQuotaResources(new ArrayList<>(cuQuota.keySet()));
    isReady.set(true);
  }

  @Override
  public boolean isReady() {
    return isReady.get();
  }

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    checkSupported(quotaName, quotaResource);

    final String resourceId = quotaResource.getResourceId();
    assertResourceId(resourceId, false);
    return new Quota<>(quotaName, cuQuota.get(resourceId).getQuotaValue(quotaName), quotaResource);
  }

  @Override
  public float getUsage(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    checkSupported(quotaName, quotaResource);
    final String resourceId = quotaResource.getResourceId();
    assertResourceId(resourceId, true);
    CapacityUnit cu = cuUsage.get(resourceId);
    double usage = (cu != null) ? cu.getQuotaValue(quotaName) : 0;
    return QuotaUtils.getUsagePercentage(cuQuota.get(resourceId).getQuotaValue(quotaName), usage);
  }

  @Override
  public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) throws QuotaException {
    checkSupported(quotaName, quotaResource);
    final String resourceId = quotaResource.getResourceId();
    assertResourceId(resourceId, true);
    chargeSystemResourceUsage(quotaName, usageCost);
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      cuUsage.get(resourceId).incrementRcu((long) Math.ceil(usageCost));
    } else {
      cuUsage.get(resourceId).incrementWcu((long) Math.ceil(usageCost));
    }
  }

  @Override
  public float getSystemResourceUsage(QuotaName quotaName) {
    return QuotaUtils.getUsagePercentage(feQuota.getQuotaValue(quotaName), feUsage.get().getQuotaValue(quotaName));
  }

  @Override
  public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) {
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      feUsage.get().incrementRcu((long) Math.ceil(calculateFeUsageCost(quotaName, usageCost)));
    } else {
      feUsage.get().incrementWcu((long) Math.ceil(calculateFeUsageCost(quotaName, usageCost)));
    }
  }

  @Override
  public void updateNewQuotaResources(Collection<Account> accounts) {
    Collection<QuotaResource> quotaResources = QuotaUtils.getQuotaResourcesFromAccounts(accounts);
    synchronized (this) {
      quotaResources.forEach(quotaResource -> {
        cuQuota.putIfAbsent(quotaResource.getResourceId(),
            new CapacityUnit(DEFAULT_RCU_FOR_NEW_RESOURCE, DEFAULT_WCU_FOR_NEW_RESOURCE));
        cuUsage.putIfAbsent(quotaResource.getResourceId(),
            new CapacityUnit(DEFAULT_RCU_FOR_NEW_RESOURCE, DEFAULT_WCU_FOR_NEW_RESOURCE));
      });
      quotaMetrics.createMetricsForQuotaResources(
          quotaResources.stream().map(QuotaResource::getResourceId).collect(Collectors.toList()));
    }
  }

  @Override
  public void shutdown() {
    usageRefresher.shutdownNow();
    isReady.compareAndSet(true, false);
  }

  /**
   * @return A {@link Map} of quota resource id to {@link CapacityUnit} representing quota for all the resources that
   * this quota source knows about.
   */
  public Map<String, CapacityUnit> getAllQuota() {
    return Collections.unmodifiableMap(cuQuota);
  }

  /**
   * @return A {@link Map} of quota resource id to {@link CapacityUnit} representing usage for all the resources that
   * this quota source knows about.
   */
  public Map<String, CapacityUnit> getAllQuotaUsage() {
    return Collections.unmodifiableMap(cuUsage);
  }

  /**
   * Calculate the amplified frontend usage cost to serve the usageCost for a {@link QuotaResource}.
   * Frontend usage cost could get amplified because to serve one copy of data, frontend might need to simultaneously
   * read or write the data over network to multiple storage nodes. The amplification factor is determined by the configs
   * defined in {@link RouterConfig}.
   * @param quotaName {@link QuotaName} object.
   * @param usageCost the usage cost for {@link QuotaResource}.
   * @return amplified frontend usage cost.
   */
  private double calculateFeUsageCost(QuotaName quotaName, double usageCost) {
    return quotaName == QuotaName.READ_CAPACITY_UNIT ? usageCost * feReadUsageAmplificationFactor
        : usageCost * feWriteUsageAmplificationFactor;
  }

  /**
   * Atomically resets the usage of all the resources.
   */
  private synchronized void resetQuotaUsage() {
    cuUsage.replaceAll((k, v) -> new CapacityUnit());
    feUsage.set(new CapacityUnit());
  }

  /**
   * Checks if the specified {@link QuotaName} and {@link QuotaResourceType} are handled by this quota source.
   * @param quotaName {@link QuotaName} object.
   * @param quotaResource {@link QuotaResource} object.
   * @throws QuotaException in case of any exception.
   */
  private void checkSupported(QuotaName quotaName, QuotaResource quotaResource) throws QuotaException {
    if (!SUPPORTED_QUOTA_NAMES.contains(quotaName)) {
      throw new QuotaException("Unsupported quota name: " + quotaName.name(), false);
    }
    if (!SUPPORTED_QUOTA_RESOURCE_TYPES.contains(quotaResource.getQuotaResourceType())) {
      throw new QuotaException("Unsupported quota resource type: " + quotaResource.getQuotaResourceType(), false);
    }
  }

  /**
   * Asserts that the quota for the specified resourceId is present in this quota source. If assertForUsage is set to
   * true this method also asserts for the presence of usage information of the specified resourceId.
   * @param resourceId resource id to check.
   * @param assertForUsage if {@code true} then also assert for presence of usage information.
   * @throws QuotaException in case quota for the resource is not present in this quota source.
   */
  private void assertResourceId(String resourceId, boolean assertForUsage) throws QuotaException {
    if (!cuQuota.containsKey(resourceId)) {
      throw new QuotaException(String.format("Couldn't find quota for resource: %s", resourceId), true);
    }
    if (assertForUsage && !cuUsage.containsKey(resourceId)) {
      throw new QuotaException(String.format("Couldn't find usage information for resource: %s", resourceId), true);
    }
  }
}

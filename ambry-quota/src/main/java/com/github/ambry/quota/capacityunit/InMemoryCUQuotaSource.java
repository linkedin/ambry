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
import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link QuotaSource} implementation that keeps the quota and usage values in memory, and treats Ambry frontend's
 * Read and Write Capacity Units as system resources for handling quota exceeded requests.
 */
public class InMemoryCUQuotaSource implements QuotaSource {
  static final String REFRESHER_THREAD_NAME_PREFIX = "inmem-quota-source-refresher";
  private static final EnumSet<QuotaName> SUPPORTED_QUOTA_NAMES =
      EnumSet.of(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT);
  private static final EnumSet<QuotaResourceType> SUPPORTED_QUOTA_RESOURCE_TYPES =
      EnumSet.of(QuotaResourceType.ACCOUNT, QuotaResourceType.CONTAINER);
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryCUQuotaSource.class);
  private static final long DEFAULT_RCU_FOR_NEW_RESOURCE = 0;
  private static final long DEFAULT_WCU_FOR_NEW_RESOURCE = 0;
  protected final CapacityUnit feQuota; // Ambry frontend's CU capacity.
  private final Map<String, CapacityUnit> cuQuota; // in memory quota for all resources.
  private final Map<String, CapacityUnit> cuUsage; // in memory quota usage for all resources.
  private final ScheduledExecutorService usageRefresher;
  private final long aggregationWindowsInSecs;
  private final AtomicBoolean isReady;
  protected CapacityUnit feUsage; // Ambry frontend's CU usage.

  /**
   * Constructor for {@link InMemoryCUQuotaSource}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @throws IOException in case of any exception.
   */
  public InMemoryCUQuotaSource(QuotaConfig quotaConfig, AccountService accountService) throws IOException {
    feQuota = JsonCUQuotaDataProviderUtil.getFeCUCapacityFromJson(quotaConfig.frontendCUCapacityInJson);
    cuQuota = JsonCUQuotaDataProviderUtil.getCUQuotasFromJson(quotaConfig.resourceCUQuotaInJson, accountService);
    cuUsage = new HashMap<>();
    usageRefresher = Utils.newScheduler(1, REFRESHER_THREAD_NAME_PREFIX, true);
    aggregationWindowsInSecs = quotaConfig.cuQuotaAggregationWindowInSecs;
    isReady = new AtomicBoolean(false);
  }

  @Override
  public synchronized void init() {
    if (isReady.get()) {
      LOGGER.warn("InMemoryCuQuotaSource is already initialized.");
      return;
    }
    feUsage = new CapacityUnit();
    cuQuota.keySet().forEach(key -> cuUsage.put(key, new CapacityUnit()));
    usageRefresher.scheduleAtFixedRate(this::resetQuotaUsage, aggregationWindowsInSecs, aggregationWindowsInSecs,
        TimeUnit.SECONDS);
    isReady.set(true);
  }

  @Override
  public boolean isReady() {
    return isReady.get();
  }

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    checkSupported(quotaName, quotaResource);

    String resourceId = quotaResource.getResourceId();
    if (cuQuota.containsKey(resourceId)) {
      return new Quota<>(quotaName, cuQuota.get(resourceId).getQuotaValue(quotaName), quotaResource);
    }
    throw new QuotaException(String.format("Couldn't find quota for resource %s", quotaResource), true);
  }

  @Override
  public float getUsage(QuotaResource quotaResource, QuotaName quotaName) throws QuotaException {
    checkSupported(quotaName, quotaResource);
    String resourceId = quotaResource.getResourceId();
    if (!cuQuota.containsKey(resourceId)) {
      throw new QuotaException(String.format("Couldn't find quota for resource %s", quotaResource), true);
    }
    double usage = 0;
    if (cuUsage.containsKey(resourceId)) {
      usage = cuUsage.get(resourceId).getQuotaValue(quotaName);
    }
    return QuotaUtils.getUsagePercentage(cuQuota.get(resourceId).getQuotaValue(quotaName), usage);
  }

  @Override
  public void chargeUsage(QuotaResource quotaResource, QuotaName quotaName, double usageCost) throws QuotaException {
    checkSupported(quotaName, quotaResource);
    String resourceId = quotaResource.getResourceId();
    if (!cuQuota.containsKey(resourceId)) {
      throw new QuotaException(String.format("Couldn't find quota for resource %s", quotaResource), true);
    }
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      cuUsage.get(resourceId).incrementRcu((long) Math.ceil(usageCost));
    } else {
      cuUsage.get(resourceId).incrementWcu((long) Math.ceil(usageCost));
    }
  }

  @Override
  public float getSystemResourceUsage(QuotaName quotaName) throws QuotaException {
    return QuotaUtils.getUsagePercentage(feQuota.getQuotaValue(quotaName), feUsage.getQuotaValue(quotaName));
  }

  @Override
  public void chargeSystemResourceUsage(QuotaName quotaName, double usageCost) throws QuotaException {
    if (quotaName == QuotaName.READ_CAPACITY_UNIT) {
      feUsage.incrementRcu((long) Math.ceil(usageCost));
    } else {
      feUsage.incrementWcu((long) Math.ceil(usageCost));
    }
  }

  @Override
  public void updateNewQuotaResources(Collection<Account> accounts) {
    Collection<QuotaResource> quotaResources = QuotaUtils.getQuotaResourcesFromAccounts(accounts);
    quotaResources.forEach(quotaResource -> {
      cuQuota.putIfAbsent(quotaResource.getResourceId(),
          new CapacityUnit(DEFAULT_RCU_FOR_NEW_RESOURCE, DEFAULT_WCU_FOR_NEW_RESOURCE));
      cuUsage.putIfAbsent(quotaResource.getResourceId(),
          new CapacityUnit(DEFAULT_RCU_FOR_NEW_RESOURCE, DEFAULT_WCU_FOR_NEW_RESOURCE));
    });
  }

  @Override
  public void shutdown() throws InterruptedException {
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
   * Atomically resets the usage of all the resources.
   */
  private synchronized void resetQuotaUsage() {
    for (String resourceId : cuUsage.keySet()) {
      cuUsage.put(resourceId, new CapacityUnit());
    }
    feUsage.setWcu(0);
    feUsage.setRcu(0);
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
}

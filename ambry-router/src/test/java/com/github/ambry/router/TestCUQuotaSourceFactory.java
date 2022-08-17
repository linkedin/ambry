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
package com.github.ambry.router;

import com.github.ambry.account.AccountService;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSource;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory;
import com.github.ambry.quota.capacityunit.CapacityUnit;
import java.io.IOException;
import java.util.Map;


/**
 * A {@link QuotaSourceFactory} implementation to create {@link TestCUQuotaSource} object.
 */
public class TestCUQuotaSourceFactory implements QuotaSourceFactory {
  private final QuotaConfig quotaConfig;
  private final AccountService accountService;
  private final QuotaMetrics quotaMetrics;
  private final RouterConfig routerConfig;

  /**
   * Constructor for {@link AmbryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   */
  public TestCUQuotaSourceFactory(QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) {
    this.quotaConfig = quotaConfig;
    this.accountService = accountService;
    this.quotaMetrics = quotaMetrics;
    this.routerConfig = routerConfig;
  }

  @Override
  public QuotaSource getQuotaSource() {
    try {
      return new TestCUQuotaSource(quotaConfig, accountService, quotaMetrics, routerConfig);
    } catch (IOException ioException) {
      return null;
    }
  }
}

/**
 * A implementation of {@link AmbryCUQuotaSource} that makes quota and usage values available for test.
 */
class TestCUQuotaSource extends AmbryCUQuotaSource {

  /**
   * Constructor for {@link TestCUQuotaSource}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param accountService {@link AccountService} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   * @param routerConfig {@link RouterConfig} object.
   * @throws IOException in case of any exception.
   */
  public TestCUQuotaSource(QuotaConfig quotaConfig, AccountService accountService, QuotaMetrics quotaMetrics,
      RouterConfig routerConfig) throws IOException {
    super(quotaConfig, accountService, quotaMetrics, routerConfig);
  }

  /**
   * Set the frontend usage to the specified value.
   * @param newFeUsage new frontend usage values.
   */
  public void setFeUsage(CapacityUnit newFeUsage) {
    feUsage.get().setRcu(newFeUsage.getRcu());
    feUsage.get().setWcu(newFeUsage.getWcu());
  }

  /**
   * Get the quota for all resources.
   * @return A {@link Map} of quota resource ids to their {@link CapacityUnit} quotas.
   */
  public Map<String, CapacityUnit> getCuQuota() {
    return cuQuota;
  }

  /**
   * Get the usage for all resources.
   * @return A {@link Map} of quota resource ids to their {@link CapacityUnit} usage values.
   */
  public Map<String, CapacityUnit> getCuUsage() {
    return cuUsage;
  }
}

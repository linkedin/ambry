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

import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;
import java.io.IOException;


/**
 * An implementation of {@link QuotaEnforcerFactory} that creates and returns {@link AmbryCUQuotaEnforcer} object.
 */
public class AmbryCUQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final AmbryCUQuotaEnforcer ambryCUQuotaEnforcer;

  /**
   * Constructor for {@link AmbryCUQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore the {@link AccountStatsStore}.
   */
  public AmbryCUQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics) {
    ambryCUQuotaEnforcer = new AmbryCUQuotaEnforcer(quotaSource, quotaConfig, quotaMetrics);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return ambryCUQuotaEnforcer;
  }
}

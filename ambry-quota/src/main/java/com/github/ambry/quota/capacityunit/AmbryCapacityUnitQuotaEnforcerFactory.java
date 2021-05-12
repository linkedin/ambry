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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaEnforcerFactory;


/**
 * Implementation of {@link QuotaEnforcerFactory} that instantiates {@link AmbryCapacityUnitQuotaEnforcer}.
 */
public class AmbryCapacityUnitQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final QuotaEnforcer quotaEnforcer;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore the {@link AccountStatsStore}.
   */
  public AmbryCapacityUnitQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore) {
    quotaEnforcer = new AmbryCapacityUnitQuotaEnforcer(quotaSource);
  }

  @Override
  public QuotaEnforcer getRequestQuotaEnforcer() {
    return quotaEnforcer;
  }
}

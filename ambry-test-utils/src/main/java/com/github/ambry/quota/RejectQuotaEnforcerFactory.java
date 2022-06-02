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
package com.github.ambry.quota;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;


/**
 * Factory that instantiates {@link RejectRequestQuotaEnforcer} for tests.
 */
public class RejectQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final RejectRequestQuotaEnforcer rejectQuotaEnforcer;

  /**
   * Constructor for {@link RejectQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore the {@link AccountStatsStore}.
   */
  public RejectQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics) {
    this.rejectQuotaEnforcer = new RejectRequestQuotaEnforcer(quotaSource);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return rejectQuotaEnforcer;
  }
}

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

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaEnforcer;
import com.github.ambry.quota.capacityunit.AmbryCUQuotaSourceFactory;
import com.github.ambry.rest.RestRequest;


/**
 * A {@link QuotaEnforcerFactory} implementation to create {@link RejectingQuotaEnforcer} objects.
 */
public class RejectingQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final RejectingQuotaEnforcer rejectingQuotaEnforcer;

  /**
   * Constructor for {@link AmbryCUQuotaSourceFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore {@link AccountStatsStore} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   */
  public RejectingQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics) {
    rejectingQuotaEnforcer = new RejectingQuotaEnforcer(quotaSource, quotaConfig, quotaMetrics);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return rejectingQuotaEnforcer;
  }
}

/**
 * An {@link AmbryCUQuotaEnforcer} that rejects quota exceeded requests instead of delaying.
 */
class RejectingQuotaEnforcer extends AmbryCUQuotaEnforcer {
  public RejectingQuotaEnforcer(QuotaSource quotaSource, QuotaConfig quotaConfig, QuotaMetrics quotaMetrics) {
    super(quotaSource, quotaConfig, quotaMetrics);
  }

  /**
   * Build the {@link QuotaRecommendation} object from the specified usage and {@link QuotaName}.
   * @param usage percentage usage.
   * @param quotaName {@link QuotaName} object.
   * @param resourceId resource id of the resource for which check is being made.
   * @return QuotaRecommendation object.
   */
  protected QuotaRecommendation buildQuotaRecommendation(float usage, QuotaName quotaName, String resourceId) {
    QuotaAction quotaAction = (usage >= MAX_USAGE_PERCENTAGE_ALLOWED) ? QuotaAction.REJECT : QuotaAction.ALLOW;
    return new QuotaRecommendation(quotaAction, usage, quotaName,
        (quotaAction == QuotaAction.REJECT) ? throttleRetryAfterMs : QuotaRecommendation.NO_THROTTLE_RETRY_AFTER_MS);
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) {
    return false;
  }
}

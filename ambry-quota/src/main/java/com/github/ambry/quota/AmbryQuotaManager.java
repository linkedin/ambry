/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.utils.Utils;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * {@link QuotaManager} implementation to handle all the quota and quota enforcement for Ambry.
 */
public class AmbryQuotaManager implements QuotaManager {
  private final QuotaConfig quotaConfig;
  private final Set<QuotaEnforcer> quotaEnforcers;

  /**
   * Constructor for {@link AmbryQuotaManager}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @throws ReflectiveOperationException in case of any exception.
   */
  public AmbryQuotaManager(QuotaConfig quotaConfig) throws ReflectiveOperationException {
    this.quotaConfig = quotaConfig;
    quotaEnforcers = new HashSet<>();
    for (String enabledQuotaThrottler : this.quotaConfig.enabledQuotaEnforcers) {
      quotaEnforcers.add(Utils.getObj(enabledQuotaThrottler, quotaConfig));
    }
  }

  @Override
  public void init() {
    for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
      quotaEnforcer.init();
    }
  }

  @Override
  public boolean shouldThrottle(RequestCost requestCost, QuotaResource quotaResource, QuotaOperation quotaOperation,
      List<EnforcementRecommendation> enforcementRecommendations) {
    boolean shouldThrottle = true;
    Iterator<QuotaEnforcer> quotaEnforcerIterator = quotaEnforcers.iterator();
    while (quotaEnforcerIterator.hasNext()) {
      EnforcementRecommendation enforcementRecommendation =
          quotaEnforcerIterator.next().recommend(requestCost, quotaResource, quotaOperation);
      shouldThrottle = shouldThrottle && enforcementRecommendation.shouldThrottle();
      enforcementRecommendations.add(enforcementRecommendation);
    }
    return shouldThrottle;
  }

  @Override
  public boolean shouldThrottleAndCharge(RequestCost requestCost, QuotaResource quotaResource,
      QuotaOperation quotaOperation, List<EnforcementRecommendation> enforcementRecommendations) {
    boolean shouldThrottle = true;
    Iterator<QuotaEnforcer> quotaEnforcerIterator = quotaEnforcers.iterator();
    while (quotaEnforcerIterator.hasNext()) {
      EnforcementRecommendation enforcementRecommendation =
          quotaEnforcerIterator.next().chargeAndRecommend(requestCost, quotaResource, quotaOperation);
      shouldThrottle = shouldThrottle && enforcementRecommendation.shouldThrottle();
      enforcementRecommendations.add(enforcementRecommendation);
    }
    return shouldThrottle;
  }

  @Override
  public void shutdown() {
    for (QuotaEnforcer quotaEnforcer : quotaEnforcers) {
      quotaEnforcer.shutdown();
    }
  }
}

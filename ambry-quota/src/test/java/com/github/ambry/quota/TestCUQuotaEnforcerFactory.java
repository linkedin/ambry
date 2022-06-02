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
package com.github.ambry.quota;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.rest.RestRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * {@link QuotaEnforcerFactory} implementation for tests.
 */
public class TestCUQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final TestAmbryCUQuotaEnforcer ambryCUQuotaEnforcer;

  /**
   * Constructor for {@link TestCUQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore the {@link AccountStatsStore}.
   */
  public TestCUQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore, QuotaMetrics quotaMetrics) {
    ambryCUQuotaEnforcer = new TestAmbryCUQuotaEnforcer(quotaSource);
  }

  @Override
  public QuotaEnforcer getQuotaEnforcer() {
    return ambryCUQuotaEnforcer;
  }

  /**
   * A {@link SimpleQuotaRecommendationMergePolicy} that also keeps a count of number of {@link QuotaRecommendation}s it merged.s
   */
  static class TestQuotaRecommendationMergePolicy extends SimpleQuotaRecommendationMergePolicy {
    static int quotaRecommendationsCount = 0;

    /**
     * Constructor to create a {@link TestQuotaRecommendationMergePolicy}.
     * @param quotaConfig The {@link QuotaConfig}.
     */
    public TestQuotaRecommendationMergePolicy(QuotaConfig quotaConfig) {
      super(quotaConfig);
    }

    @Override
    public ThrottlingRecommendation mergeEnforcementRecommendations(List<QuotaRecommendation> quotaRecommendations) {
      quotaRecommendationsCount += quotaRecommendations.size();
      return super.mergeEnforcementRecommendations(quotaRecommendations);
    }
  }

  /**
   * An implementation of {@link QuotaEnforcer} for tests that keeps a count of number of times it methods are invoked.
   * This class can be setup to return specified values for {@link QuotaEnforcer} method calls.
   */
  static class TestAmbryCUQuotaEnforcer implements QuotaEnforcer {
    static final QuotaRecommendation ALLOW_QUOTA_RECOMMENDATION =
        new QuotaRecommendation(QuotaAction.ALLOW, 50, QuotaName.READ_CAPACITY_UNIT,
            QuotaRecommendation.NO_THROTTLE_RETRY_AFTER_MS);
    static final QuotaRecommendation REJECT_QUOTA_RECOMMENDATION =
        new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.READ_CAPACITY_UNIT, 10);
    static final QuotaRecommendation DELAY_QUOTA_RECOMMENDATION =
        new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.READ_CAPACITY_UNIT, 10);

    private final QuotaSource quotaSource;
    int initCallCount;
    int chargeCallCount;
    int recommendCallCount;
    int isQuotaExceedAllowedCallCount;
    int shutdownCallCount;
    QuotaRecommendation chargeReturnVal;
    QuotaRecommendation recommendReturnVal;
    boolean isQuotaExceedAllowedReturnVal;
    boolean throwException;

    /**
     * Constructor for {@link TestAmbryCUQuotaEnforcer}.
     * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
     */
    public TestAmbryCUQuotaEnforcer(QuotaSource quotaSource) {
      this.quotaSource = quotaSource;
    }

    @Override
    public void init() throws QuotaException {
      initCallCount++;
      if (throwException) {
        throw new QuotaException("exception in init", false);
      }
    }

    @Override
    public QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap)
        throws QuotaException {
      chargeCallCount++;
      if (throwException) {
        throw new QuotaException("exception in charge", false);
      }
      return chargeReturnVal;
    }

    @Override
    public QuotaRecommendation recommend(RestRequest restRequest) throws QuotaException {
      recommendCallCount++;
      if (throwException) {
        throw new QuotaException("exception in recommend", false);
      }
      return recommendReturnVal;
    }

    @Override
    public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
      isQuotaExceedAllowedCallCount++;
      if (throwException) {
        throw new QuotaException("exception in isQuotaExceedAllowed", false);
      }
      return isQuotaExceedAllowedReturnVal;
    }

    @Override
    public List<QuotaName> supportedQuotaNames() {
      return Collections.singletonList(QuotaName.READ_CAPACITY_UNIT);
    }

    @Override
    public QuotaSource getQuotaSource() {
      return quotaSource;
    }

    @Override
    public void shutdown() {
      shutdownCallCount++;
    }

    /**
     * Reset the accounting state to default values.
     */
    public void resetDefaults() {
      initCallCount = 0;
      chargeCallCount = 0;
      recommendCallCount = 0;
      isQuotaExceedAllowedCallCount = 0;
      shutdownCallCount = 0;
      chargeReturnVal = ALLOW_QUOTA_RECOMMENDATION;
      recommendReturnVal = ALLOW_QUOTA_RECOMMENDATION;
      isQuotaExceedAllowedReturnVal = false;
      throwException = false;
    }
  }
}

/*
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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link QuotaRecommendationMergePolicy} that merges a list of {@link QuotaRecommendation}s to
 * create a resultant {@link ThrottlingRecommendation}. This {@link ThrottlingRecommendation}
 * object will be used by rest of Ambry as the overall recommendation to ensure quota compliance of a request.
 *
 * This implementation merges the {@link QuotaRecommendation}s into the most severe {@link QuotaRecommendation}.
 * {@link QuotaRecommendation#quotaAction} is used to determine severity. For more details about severity and
 * ordering check {@link QuotaAction}.
 *
 * This Policy also respects the settings in the {@link QuotaConfig}. If the {@link QuotaConfig#requestThrottlingEnabled}
 * is false, it doesn't throttle on Capacity Unit quotas. If the {@link StorageQuotaConfig#shouldThrottle} is false, it
 * doesn't throttle on {@link QuotaName#STORAGE_IN_GB}.
 */
public class SimpleQuotaRecommendationMergePolicy implements QuotaRecommendationMergePolicy {
  static final long DEFAULT_RETRY_AFTER_MS = ThrottlingRecommendation.NO_RETRY_AFTER_MS;
  static final QuotaUsageLevel DEFAULT_QUOTA_USAGE_LEVEL = QuotaUsageLevel.HEALTHY;
  // Percentage usage level above which critical warning will be generated.
  static final int CRITICAL_USAGE_LEVEL_LIMIT = 95;
  // Percentage usage at or below this limit is healthy.
  private final int healthyUsageLevelLimit;
  private final QuotaConfig quotaConfig;

  /**
   * Constructor to create a {@link SimpleQuotaRecommendationMergePolicy}.
   * @param quotaConfig The {@link QuotaConfig}.
   */
  public SimpleQuotaRecommendationMergePolicy(QuotaConfig quotaConfig) {
    this.quotaConfig = quotaConfig;
    this.healthyUsageLevelLimit = quotaConfig.quotaUsageWarningThresholdInPercentage;
  }

  @Override
  public ThrottlingRecommendation mergeEnforcementRecommendations(List<QuotaRecommendation> quotaRecommendations) {
    QuotaAction mergedQuotaAction = QuotaAction.ALLOW;
    Map<QuotaName, Float> quotaUsagePercentage = new HashMap<>();
    long retryAfterMs = DEFAULT_RETRY_AFTER_MS;
    for (QuotaRecommendation recommendation : quotaRecommendations) {
      QuotaAction currentQuotaAction = recommendation.getQuotaAction();
      if (recommendation.getQuotaName() == QuotaName.READ_CAPACITY_UNIT
          || recommendation.getQuotaName() == QuotaName.WRITE_CAPACITY_UNIT) {
        currentQuotaAction = quotaConfig.requestThrottlingEnabled ? currentQuotaAction : QuotaAction.ALLOW;
      } else if (recommendation.getQuotaName() == QuotaName.STORAGE_IN_GB) {
        currentQuotaAction = quotaConfig.storageQuotaConfig.shouldThrottle ? currentQuotaAction : QuotaAction.ALLOW;
      }
      mergedQuotaAction = mergeQuotaAction(mergedQuotaAction, currentQuotaAction);
      quotaUsagePercentage.put(recommendation.getQuotaName(), recommendation.getQuotaUsagePercentage());
      retryAfterMs = Math.max(recommendation.getRetryAfterMs(), retryAfterMs);
    }
    QuotaUsageLevel quotaUsageLevel = quotaUsagePercentage.isEmpty() ? DEFAULT_QUOTA_USAGE_LEVEL
        : computeWarningLevel(Collections.max(quotaUsagePercentage.values()));
    return new ThrottlingRecommendation(mergedQuotaAction, quotaUsagePercentage, retryAfterMs, quotaUsageLevel);
  }

  /**
   * Compute the warning level from usage percentage.
   * @param usagePercentage usage of quota.
   * @return QuotaWarningLevel object.
   */
  private QuotaUsageLevel computeWarningLevel(float usagePercentage) {
    if (usagePercentage >= 100) {
      return QuotaUsageLevel.EXCEEDED;
    }
    if (usagePercentage >= CRITICAL_USAGE_LEVEL_LIMIT) {
      return QuotaUsageLevel.CRITICAL;
    }
    if (usagePercentage >= healthyUsageLevelLimit) {
      return QuotaUsageLevel.WARNING;
    }
    return QuotaUsageLevel.HEALTHY;
  }

  /**
   * Merge the {@link QuotaAction} objects into one by picking the more severe of the specified {@link QuotaAction}
   * objects. For more details about severity check {@link QuotaAction}.
   * @param quotaAction1 {@link QuotaAction} object.
   * @param quotaAction2 {@link QuotaAction} object.
   * @return QuotaAction which is the more severe {@link QuotaAction} in the pair.
   */
  private QuotaAction mergeQuotaAction(QuotaAction quotaAction1, QuotaAction quotaAction2) {
    if (quotaAction1.compareTo(quotaAction2) > 0) {
      return quotaAction1;
    }
    return quotaAction2;
  }
}

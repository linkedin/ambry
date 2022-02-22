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
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for @{@link SimpleQuotaRecommendationMergePolicy}.
 */
public class SimpleQuotaRecommendationMergePolicyTest {

  /** Test for {@link SimpleQuotaRecommendationMergePolicy#mergeEnforcementRecommendations}*/
  @Test
  public void testRecommend() {
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
    SimpleQuotaRecommendationMergePolicy simpleThrottlePolicy = new SimpleQuotaRecommendationMergePolicy(quotaConfig);

    // test for empty quota recommendation list
    ThrottlingRecommendation throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.emptyList());
    assertEquals(ThrottlingRecommendation.NO_RETRY_AFTER_MS, throttlingRecommendation.getRetryAfterMs());
    assertEquals(QuotaUsageLevel.HEALTHY, throttlingRecommendation.getQuotaUsageLevel());
    assertFalse(throttlingRecommendation.shouldThrottle());
    assertTrue(throttlingRecommendation.getQuotaUsagePercentage().isEmpty());

    // test for a single quota recommendation.
    QuotaRecommendation quotaRecommendation =
        new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.READ_CAPACITY_UNIT, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    // test for retry after interval
    List<QuotaRecommendation> quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 10));
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 1));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 10, throttlingRecommendation);

    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 12));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 12, throttlingRecommendation);

    // test for should throttle
    quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    // test for warning level
    quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 70, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.HEALTHY, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 70.0), 5, throttlingRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 81, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.WARNING, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT,
            (float) quotaConfig.quotaUsageWarningThresholdInPercentage + 1), 5, throttlingRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.ALLOW, 96, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.CRITICAL, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 96.0), 5, throttlingRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    // Test when the request throttling is disabled
    Properties prop = new Properties();
    prop.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, "false");
    simpleThrottlePolicy = new SimpleQuotaRecommendationMergePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    // test for a request quota recommendation.
    quotaRecommendation = new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.READ_CAPACITY_UNIT, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.STORAGE_IN_GB, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, 5, throttlingRecommendation);

    // Test when the storage quota throttling is disabled
    prop = new Properties();
    prop.setProperty(StorageQuotaConfig.SHOULD_THROTTLE, "false");
    simpleThrottlePolicy = new SimpleQuotaRecommendationMergePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    // test for a storage quota recommendation.
    quotaRecommendation = new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.STORAGE_IN_GB, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.STORAGE_IN_GB, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, 5, throttlingRecommendation);

    // Test when both quota throttling are disabled
    prop = new Properties();
    prop.setProperty(StorageQuotaConfig.SHOULD_THROTTLE, "false");
    prop.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, "false");
    simpleThrottlePolicy = new SimpleQuotaRecommendationMergePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    quotaRecommendation = new QuotaRecommendation(QuotaAction.DELAY, 101, QuotaName.READ_CAPACITY_UNIT, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendation = new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.WRITE_CAPACITY_UNIT, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.WRITE_CAPACITY_UNIT, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendation = new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.STORAGE_IN_GB, 5);
    throttlingRecommendation =
        simpleThrottlePolicy.mergeEnforcementRecommendations(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.STORAGE_IN_GB, (float) 101.0), 5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.READ_CAPACITY_UNIT, 5));
    quotaRecommendationList.add(new QuotaRecommendation(QuotaAction.REJECT, 101, QuotaName.WRITE_CAPACITY_UNIT, 5));
    throttlingRecommendation = simpleThrottlePolicy.mergeEnforcementRecommendations(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.WRITE_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, 5, throttlingRecommendation);
  }

  /**
   * Verify that the fields of specified {@link ThrottlingRecommendation} object are same as specified parameters.
   * @param quotaUsageLevel expected value for {@link QuotaUsageLevel}.
   * @param shouldThrottle expected value for throttle recommendation.
   * @param quotaUsageMap expected value for {@link Map} of quota usage.
   * @param retryAfterMs expected value for retry interval.
   * @param throttlingRecommendation {@link ThrottlingRecommendation} object to verify.
   */
  private void verifyThrottlingRecommendation(QuotaUsageLevel quotaUsageLevel, boolean shouldThrottle,
      Map<QuotaName, Float> quotaUsageMap, long retryAfterMs, ThrottlingRecommendation throttlingRecommendation) {
    assertEquals(quotaUsageLevel, throttlingRecommendation.getQuotaUsageLevel());
    assertEquals(shouldThrottle, throttlingRecommendation.shouldThrottle());
    assertEquals(quotaUsageMap.size(), throttlingRecommendation.getQuotaUsagePercentage().size());
    for (Map.Entry<QuotaName, Float> usageEntry : throttlingRecommendation.getQuotaUsagePercentage().entrySet()) {
      assertEquals(usageEntry.getValue(), quotaUsageMap.get(usageEntry.getKey()));
    }
    assertEquals(quotaUsageMap, throttlingRecommendation.getQuotaUsagePercentage());
    assertEquals(retryAfterMs, throttlingRecommendation.getRetryAfterMs());
  }
}

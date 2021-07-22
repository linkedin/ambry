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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test for @{@link MaxThrottlePolicy}.
 */
public class MaxThrottlePolicyTest {

  /** Test for {@link MaxThrottlePolicy#recommend}*/
  @Test
  public void testRecommend() {
    MaxThrottlePolicy maxThrottlePolicy =
        new MaxThrottlePolicy(new QuotaConfig(new VerifiableProperties(new Properties())));

    // test for empty quota recommendation list
    ThrottlingRecommendation throttlingRecommendation = maxThrottlePolicy.recommend(Collections.emptyList());
    assertEquals(ThrottlingRecommendation.NO_RETRY_AFTER_MS, throttlingRecommendation.getRetryAfterMs());
    assertEquals(QuotaUsageLevel.HEALTHY, throttlingRecommendation.getQuotaUsageLevel());
    assertEquals(HttpResponseStatus.OK.code(), throttlingRecommendation.getRecommendedHttpStatus());
    assertEquals(false, throttlingRecommendation.shouldThrottle());
    assertTrue(throttlingRecommendation.getQuotaUsagePercentage().isEmpty());

    // test for a single quota recommendation.
    QuotaRecommendation quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    // test for retry after interval
    List<QuotaRecommendation> quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            10));
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            1));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 10, throttlingRecommendation);

    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            12));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 12, throttlingRecommendation);

    // test for should throttle
    quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    // test for warning level
    quotaRecommendationList = new ArrayList<>();
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 70, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.HEALTHY, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 70.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 81, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.WARNING, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 81.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(false, 96, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.CRITICAL, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 96.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    // Test when the request throttling is disabled
    Properties prop = new Properties();
    prop.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, "false");
    maxThrottlePolicy = new MaxThrottlePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    // test for a request quota recommendation.
    quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.STORAGE_IN_GB, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    // Test when the storage quota throttling is disabled
    prop = new Properties();
    prop.setProperty(StorageQuotaConfig.SHOULD_THROTTLE, "false");
    maxThrottlePolicy = new MaxThrottlePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    // test for a storage quota recommendation.
    quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.STORAGE_IN_GB, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.STORAGE_IN_GB, (float) 101.0), HttpResponseStatus.TOO_MANY_REQUESTS.code(),
        5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, true, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    // Test when both quota throttling are disabled
    prop = new Properties();
    prop.setProperty(StorageQuotaConfig.SHOULD_THROTTLE, "false");
    prop.setProperty(QuotaConfig.REQUEST_THROTTLING_ENABLED, "false");
    maxThrottlePolicy = new MaxThrottlePolicy(new QuotaConfig(new VerifiableProperties(prop)));

    quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.READ_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.WRITE_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.WRITE_CAPACITY_UNIT, (float) 101.0),
        HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);

    quotaRecommendation =
        new QuotaRecommendation(true, 101, QuotaName.STORAGE_IN_GB, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5);
    throttlingRecommendation = maxThrottlePolicy.recommend(Collections.singletonList(quotaRecommendation));
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false,
        Collections.singletonMap(QuotaName.STORAGE_IN_GB, (float) 101.0), HttpResponseStatus.TOO_MANY_REQUESTS.code(),
        5, throttlingRecommendation);

    quotaRecommendationList.clear();
    quotaRecommendationList.add(quotaRecommendation);
    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.READ_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    quotaRecommendationList.add(
        new QuotaRecommendation(true, 101, QuotaName.WRITE_CAPACITY_UNIT, HttpResponseStatus.TOO_MANY_REQUESTS.code(),
            5));
    throttlingRecommendation = maxThrottlePolicy.recommend(quotaRecommendationList);
    verifyThrottlingRecommendation(QuotaUsageLevel.EXCEEDED, false, new HashMap<QuotaName, Float>() {
      {
        put(QuotaName.READ_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.WRITE_CAPACITY_UNIT, (float) 101.0);
        put(QuotaName.STORAGE_IN_GB, (float) 101.0);
      }
    }, HttpResponseStatus.TOO_MANY_REQUESTS.code(), 5, throttlingRecommendation);
  }

  /**
   * Verify that the fields of specified {@link ThrottlingRecommendation} object are same as specified parameters.
   * @param quotaUsageLevel expected value for {@link QuotaUsageLevel}.
   * @param shouldThrottle expected value for throttle recommendation.
   * @param quotaUsageMap expected value for {@link Map} of quota usage.
   * @param httpStatus expected value for http status.
   * @param retryAfterMs expected value for retry interval.
   * @param throttlingRecommendation {@link ThrottlingRecommendation} object to verify.
   */
  private void verifyThrottlingRecommendation(QuotaUsageLevel quotaUsageLevel, boolean shouldThrottle,
      Map<QuotaName, Float> quotaUsageMap, int httpStatus, long retryAfterMs,
      ThrottlingRecommendation throttlingRecommendation) {
    assertEquals(quotaUsageLevel, throttlingRecommendation.getQuotaUsageLevel());
    assertEquals(shouldThrottle, throttlingRecommendation.shouldThrottle());
    assertEquals(quotaUsageMap.size(), throttlingRecommendation.getQuotaUsagePercentage().size());
    for (Map.Entry<QuotaName, Float> usageEntry : throttlingRecommendation.getQuotaUsagePercentage().entrySet()) {
      assertEquals(usageEntry.getValue(), quotaUsageMap.get(usageEntry.getKey()));
    }
    assertTrue(quotaUsageMap.equals(throttlingRecommendation.getQuotaUsagePercentage()));
    assertEquals(httpStatus, throttlingRecommendation.getRecommendedHttpStatus());
    assertEquals(retryAfterMs, throttlingRecommendation.getRetryAfterMs());
  }
}

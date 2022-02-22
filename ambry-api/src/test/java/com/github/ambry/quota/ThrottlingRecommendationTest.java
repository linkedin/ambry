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

import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test of {@link ThrottlingRecommendation}.
 */
public class ThrottlingRecommendationTest {
  private static final Map<QuotaName, Float> USAGE_MAP = Collections.emptyMap();
  private static final long retryAfterMs = -1;
  private static final QuotaUsageLevel quotaUsageLevel = QuotaUsageLevel.EXCEEDED;

  @Test
  public void testShouldThrottle() {
    Assert.assertFalse(
        new ThrottlingRecommendation(QuotaAction.ALLOW, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldThrottle());
    Assert.assertTrue(
        new ThrottlingRecommendation(QuotaAction.REJECT, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldThrottle());
    Assert.assertTrue(
        new ThrottlingRecommendation(QuotaAction.DELAY, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldThrottle());
  }

  @Test
  public void testShouldDelay() {
    Assert.assertFalse(
        new ThrottlingRecommendation(QuotaAction.ALLOW, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldDelay());
    Assert.assertFalse(
        new ThrottlingRecommendation(QuotaAction.REJECT, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldDelay());
    Assert.assertTrue(
        new ThrottlingRecommendation(QuotaAction.DELAY, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldDelay());
  }

  @Test
  public void testShouldReject() {
    Assert.assertFalse(
        new ThrottlingRecommendation(QuotaAction.ALLOW, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldReject());
    Assert.assertTrue(
        new ThrottlingRecommendation(QuotaAction.REJECT, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldReject());
    Assert.assertFalse(
        new ThrottlingRecommendation(QuotaAction.DELAY, USAGE_MAP, retryAfterMs, quotaUsageLevel).shouldReject());
  }
}

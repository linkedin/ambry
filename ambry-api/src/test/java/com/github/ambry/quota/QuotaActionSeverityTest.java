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

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests that {@link QuotaAction#compareTo} returns correct ordering.
 */
public class QuotaActionSeverityTest {
  @Test
  public void severityOrderingTest() {
    // 1. test that Reject > Allow
    Assert.assertTrue(QuotaAction.REJECT.compareTo(QuotaAction.ALLOW) > 0);

    // 2. test that Reject > Delay
    Assert.assertTrue(QuotaAction.REJECT.compareTo(QuotaAction.DELAY) > 0);

    // 3. test that Delay > Allow
    Assert.assertTrue(QuotaAction.DELAY.compareTo(QuotaAction.ALLOW) > 0);

    // 4. test that Delay < Reject
    Assert.assertTrue(QuotaAction.DELAY.compareTo(QuotaAction.REJECT) < 0);

    // 5. test that Allow < Reject
    Assert.assertTrue(QuotaAction.ALLOW.compareTo(QuotaAction.REJECT) < 0);

    // 6. test that Allow < Delay
    Assert.assertTrue(QuotaAction.ALLOW.compareTo(QuotaAction.DELAY) < 0);
  }
}

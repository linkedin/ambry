/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.MockTime;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link DeleteTombstoneStats} and its builder.
 */
public class DeleteTombstoneStatsTest {
  @Test
  public void testBuilder() {
    MockTime time = new MockTime(System.currentTimeMillis());
    DeleteTombstoneStats.Builder builder = new DeleteTombstoneStats.Builder();
    builder.expiredCountInc();
    builder.expiredCountInc(10); // Expired count should be 11
    builder.expiredSizeInc(1000); // Expired size should be 1000
    builder.expiredSizeInc(2000); // Expired size should be 3000 now

    builder.permanentCountInc();
    builder.permanentCountInc(20); // permanent count should be 20
    long expectedPermanentSize = 0;
    for (int day : new int[]{20, 14, 10, 7, 5, 3, 2, 1}) {
      builder.permanentSizeInc(day * 100, time.milliseconds() - TimeUnit.DAYS.toMillis(day) + 100);
      expectedPermanentSize += day * 100;
    }
    // Do this one more time
    for (int day : new int[]{20, 14, 10, 7, 5, 3, 2, 1}) {
      builder.permanentSizeInc(day * 33, time.milliseconds() - TimeUnit.DAYS.toMillis(day) + 100);
      expectedPermanentSize += day * 33;
    }
    DeleteTombstoneStats stats = builder.build();
    Assert.assertEquals(11, stats.expiredCount);
    Assert.assertEquals(3000, stats.expiredSize);
    Assert.assertEquals(21, stats.permanentCount);
    Assert.assertEquals(expectedPermanentSize, stats.permanentSize);
    Assert.assertEquals(133 * 1, stats.permanentSizeOneDay);
    Assert.assertEquals(133 * 3, stats.permanentSizeTwoDays);
    Assert.assertEquals(133 * 6, stats.permanentSizeThreeDays);
    Assert.assertEquals(133 * 11, stats.permanentSizeFiveDays);
    Assert.assertEquals(133 * 18, stats.permanentSizeSevenDays);
    Assert.assertEquals(133 * 28, stats.permanentSizeTenDays);
    Assert.assertEquals(133 * 42, stats.permanentSizeFourteenDays);

    DeleteTombstoneStats another = builder.build();
    stats = stats.merge(another);
    Assert.assertEquals(11 * 2, stats.expiredCount);
    Assert.assertEquals(3000 * 2, stats.expiredSize);
    Assert.assertEquals(21 * 2, stats.permanentCount);
    Assert.assertEquals(expectedPermanentSize * 2, stats.permanentSize);
    Assert.assertEquals(133 * 1 * 2, stats.permanentSizeOneDay);
    Assert.assertEquals(133 * 3 * 2, stats.permanentSizeTwoDays);
    Assert.assertEquals(133 * 6 * 2, stats.permanentSizeThreeDays);
    Assert.assertEquals(133 * 11 * 2, stats.permanentSizeFiveDays);
    Assert.assertEquals(133 * 18 * 2, stats.permanentSizeSevenDays);
    Assert.assertEquals(133 * 28 * 2, stats.permanentSizeTenDays);
    Assert.assertEquals(133 * 42 * 2, stats.permanentSizeFourteenDays);
  }
}

/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.github.ambry.utils.Pair;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link Threshold}.
 */
public class ThresholdTest {
  private static final long ROUND_TRIP_TIME_THRESHOLD = 500;
  private static final long TIME_TO_FIRST_THRESHOLD = 400;
  private static final long AVERAGE_BANDWIDTH_THRESHOLD = 600;
  private Threshold threshold;

  public ThresholdTest() {
    Map<PerformanceIndex, Pair<Long, Boolean>> perfIndexAndCriteria = new HashMap<>();
    perfIndexAndCriteria.put(PerformanceIndex.RoundTripTime, new Pair<>(ROUND_TRIP_TIME_THRESHOLD, true));
    perfIndexAndCriteria.put(PerformanceIndex.TimeToFirstByte, new Pair<>(TIME_TO_FIRST_THRESHOLD, true));
    perfIndexAndCriteria.put(PerformanceIndex.AverageBandwidth, new Pair<>(AVERAGE_BANDWIDTH_THRESHOLD, false));
    threshold = new Threshold(perfIndexAndCriteria);
  }

  @Test
  public void checkThresholdsTest() {
    Map<PerformanceIndex, Long> dataToCheck = new HashMap<>();
    // test the exception due to no data to check
    try {
      threshold.checkThresholds(dataToCheck);
      fail("should fail due to empty input");
    } catch (Exception e) {
      assertTrue("Mismatch in exception type", e instanceof IllegalArgumentException);
    }
    // test that thresholds check succeeded
    dataToCheck.put(PerformanceIndex.RoundTripTime, 350L);
    dataToCheck.put(PerformanceIndex.TimeToFirstByte, 300L);
    dataToCheck.put(PerformanceIndex.AverageBandwidth, 700L);
    assertTrue("Check thresholds should succeed", threshold.checkThresholds(dataToCheck));

    // test that thresholds check failed
    for (PerformanceIndex perfIndex : EnumSet.allOf(PerformanceIndex.class)) {
      Map<PerformanceIndex, Long> dataToCheckDup = new HashMap<>(dataToCheck);
      if (perfIndex == PerformanceIndex.AverageBandwidth) {
        dataToCheckDup.put(perfIndex, dataToCheckDup.get(perfIndex) - 500L);
      } else {
        dataToCheckDup.put(perfIndex, dataToCheckDup.get(perfIndex) + 500L);
      }
      assertFalse("Check thresholds should fail", threshold.checkThresholds(dataToCheckDup));
    }
  }
}

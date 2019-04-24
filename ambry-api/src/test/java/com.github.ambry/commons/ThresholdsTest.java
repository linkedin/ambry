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

import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestMethod;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test {@link Thresholds}.
 */
public class ThresholdsTest {
  private static final long ROUND_TRIP_TIME_THRESHOLD = 500;
  private static final long TIME_TO_FIRST_THRESHOLD = 400;
  private static final long AVERAGE_BANDWIDTH_THRESHOLD = 600;
  private Thresholds thresholds;

  public ThresholdsTest() {
    Map<PerformanceIndex, Criteria> perfIndexAndCriteria = new HashMap<>();
    perfIndexAndCriteria.put(PerformanceIndex.RoundTripTime,
        new Criteria(ROUND_TRIP_TIME_THRESHOLD, Criteria.BoundType.UpperBound));
    perfIndexAndCriteria.put(PerformanceIndex.TimeToFirstByte,
        new Criteria(TIME_TO_FIRST_THRESHOLD, Criteria.BoundType.UpperBound));
    perfIndexAndCriteria.put(PerformanceIndex.AverageBandwidth,
        new Criteria(AVERAGE_BANDWIDTH_THRESHOLD, Criteria.BoundType.LowerBound));
    thresholds = new Thresholds(perfIndexAndCriteria);
  }

  /**
   * Test both success and failure of thresholds check. Also verified that invalid empty input can be captured.
   */
  @Test
  public void checkThresholdsTest() {
    Map<PerformanceIndex, Long> dataToCheck = new HashMap<>();
    // test the exception due to no data to check
    try {
      thresholds.checkThresholds(dataToCheck);
      fail("should fail due to empty input");
    } catch (Exception e) {
      assertTrue("Mismatch in exception type", e instanceof NullPointerException);
    }
    // test that thresholds check succeeded
    dataToCheck.put(PerformanceIndex.RoundTripTime, 350L);
    dataToCheck.put(PerformanceIndex.TimeToFirstByte, 300L);
    dataToCheck.put(PerformanceIndex.AverageBandwidth, 700L);
    assertTrue("Check thresholds should succeed", thresholds.checkThresholds(dataToCheck));

    // test that thresholds check failed
    for (PerformanceIndex perfIndex : EnumSet.allOf(PerformanceIndex.class)) {
      Map<PerformanceIndex, Long> dataToCheckDup = new HashMap<>(dataToCheck);
      if (perfIndex == PerformanceIndex.AverageBandwidth) {
        dataToCheckDup.put(perfIndex, dataToCheckDup.get(perfIndex) - 500L);
      } else {
        dataToCheckDup.put(perfIndex, dataToCheckDup.get(perfIndex) + 500L);
      }
      assertFalse("Check thresholds should fail", thresholds.checkThresholds(dataToCheckDup));
    }
  }

  /**
   * Test that {@link PerformanceConfig} can correctly populate thresholds with default value. This is to ensure JSONObject
   * can parse the long value from default strings.
   */
  @Test
  public void defaultPerformanceConfigTest() {
    // purposely use empty properties to make config use default value
    VerifiableProperties properties = new VerifiableProperties(new Properties());
    PerformanceConfig config = new PerformanceConfig(properties);
    for (RestMethod method : EnumSet.of(RestMethod.GET, RestMethod.DELETE, RestMethod.POST, RestMethod.HEAD,
        RestMethod.PUT)) {
      Criteria criteria = config.nonSuccessRequestThresholds.get(method).getCriteria(PerformanceIndex.RoundTripTime);
      assertEquals("Threshold value is not as expected", criteria.getThresholdValue(), Long.MAX_VALUE);
    }
    // verify default GET time to first byte is Long.MAX_VALUE
    assertEquals("Threshold value is not as expected", config.successGetTimeToFirstByteThreshold, Long.MAX_VALUE);
    // verify default GET/POST average bandwidth is 0
    assertEquals("Threshold value is not as expected", config.successGetAverageBandwidthThreshold, 0);
    assertEquals("Threshold value is not as expected", config.successPostAverageBandwidthThreshold, 0);
  }
}

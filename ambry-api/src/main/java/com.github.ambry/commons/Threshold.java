/**
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
import java.util.Map;


/**
 * A data structure holding various thresholds based on different criteria.
 */
public class Threshold {
  private final Map<PerformanceIndex, Pair<Long, Boolean>> perfIndexAndCriteria;

  /**
   * Threshold constructor
   * @param perfIndexAndCriteria a map that holds PerformanceIndex and its criteria. Criteria is in pair format where the
   *                             first value is predefined threshold and second value indicates whether measured value
   *                             should be lower than threshold or not.
   */
  public Threshold(Map<PerformanceIndex, Pair<Long, Boolean>> perfIndexAndCriteria) {
    this.perfIndexAndCriteria = perfIndexAndCriteria;
  }

  /**
   * Check if input data satisfy all thresholds.
   * @param dataToCheck a map whose key is {@link PerformanceIndex} and value is corresponding measured number.
   * @return {@code true} if thresholds are satisfied. {@code false} otherwise.
   */
  public boolean checkThresholds(Map<PerformanceIndex, Long> dataToCheck) {
    for (Map.Entry<PerformanceIndex, Pair<Long, Boolean>> indexAndCriteria : perfIndexAndCriteria.entrySet()) {
      PerformanceIndex perfIndex = indexAndCriteria.getKey();
      Pair<Long, Boolean> criteria = indexAndCriteria.getValue();
      if (!compareWithCriteria(dataToCheck.get(perfIndex), criteria)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare the measured value with predefined criteria.
   * @param valueToCheck the measured value to check.
   * @param criteria the predefined threshold and rule.
   * @return {@code true} if measured value meets criteria. {@code false} otherwise.
   */
  private boolean compareWithCriteria(Long valueToCheck, Pair<Long, Boolean> criteria) {
    if (valueToCheck == null) {
      throw new IllegalArgumentException("Input value is empty which cannot be applied to threshold check.");
    }
    boolean shouldBelowThreshold = criteria.getSecond();
    long threshold = criteria.getFirst();
    return shouldBelowThreshold ? valueToCheck < threshold : valueToCheck > threshold;
  }
}

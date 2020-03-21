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

import java.util.Map;


/**
 * A data structure holding various thresholds based on different criteria.
 */
public class Thresholds {
  private final Map<PerformanceIndex, Criteria> perfIndicesAndCriteria;

  /**
   * Thresholds constructor
   * @param perfIndicesAndCriteria a map that holds {@link PerformanceIndex} and its {@link Criteria}. Criteria is a
   *                               data structure that holds the bounds of threshold and helps to check if measured value
   *                               meets the requirement(threshold).
   */
  public Thresholds(Map<PerformanceIndex, Criteria> perfIndicesAndCriteria) {
    this.perfIndicesAndCriteria = perfIndicesAndCriteria;
  }

  /**
   * Check if input data satisfy all thresholds.
   * @param dataToCheck a map whose key is {@link PerformanceIndex} and value is corresponding measured number.
   * @return {@code true} if thresholds are satisfied. {@code false} otherwise.
   */
  public boolean checkThresholds(Map<PerformanceIndex, Long> dataToCheck) {
    for (Map.Entry<PerformanceIndex, Criteria> indicesAndCriteria : perfIndicesAndCriteria.entrySet()) {
      PerformanceIndex perfIndex = indicesAndCriteria.getKey();
      Criteria criteria = indicesAndCriteria.getValue();
      if (!criteria.meetRequirement(dataToCheck.get(perfIndex))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get the {@link Criteria} associated with the specified {@link PerformanceIndex}
   * @param performanceIndex to which the criteria is related
   * @return the criteria or null if it doesn't exist
   */
  public Criteria getCriteria(PerformanceIndex performanceIndex) {
    return perfIndicesAndCriteria.get(performanceIndex);
  }
}

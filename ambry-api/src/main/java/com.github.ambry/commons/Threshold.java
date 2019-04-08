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
public class Threshold {
  final Map<PerformanceIndex, Criteria> perfIndexAndCriteria;

  public Threshold(Map<PerformanceIndex, Criteria> map) {
    perfIndexAndCriteria = map;
  }

  /**
   * Check if input data satisfy all thresholds.
   * @param dataToCheck a map whose key is {@link PerformanceIndex} and value is corresponding measured number.
   * @return {@code true} if thresholds are satisfied. {@code false} otherwise.
   */
  public boolean checkThresholds(Map<PerformanceIndex, Long> dataToCheck) {
    for (Map.Entry<PerformanceIndex, Criteria> indexAndCriteria : perfIndexAndCriteria.entrySet()) {
      PerformanceIndex perfIndex = indexAndCriteria.getKey();
      Criteria ac = indexAndCriteria.getValue();
      if (!ac.apply(dataToCheck.get(perfIndex))) {
        return false;
      }
    }
    return true;
  }
}

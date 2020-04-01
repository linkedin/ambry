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

import java.util.Objects;


/**
 * Criteria is a data structure that holds predefined threshold value and its type (UpperBound or LowerBound).
 * To meet criteria, the measured value should be lower(higher) than the predefined threshold value if threshold is
 * defined as UpperBound(LowerBound).
 */
public class Criteria {
  public enum BoundType {
    UpperBound, LowerBound
  }

  private final long threshold;
  private final BoundType boundType;

  /**
   * Criteria ctor
   * @param threshold the predefined value of threshold
   * @param boundType specifies whether this threshold is UpperBound or LowerBound.
   */
  public Criteria(long threshold, BoundType boundType) {
    this.threshold = threshold;
    this.boundType = boundType;
  }

  /**
   * Check if measured value meets the predefined requirement(threshold).
   * @param value the measured value to check.
   * @return {@code true} if measured value meets requirement. {@code false} otherwise.
   * @throws NullPointerException when the input value is null.
   */
  boolean meetRequirement(Long value) throws NullPointerException {
    Objects.requireNonNull(value, "Input value is empty which cannot be applied to threshold check.");
    return boundType == BoundType.UpperBound ? value <= threshold : value >= threshold;
  }

  /**
   * @return the threshold value associated with this criteria
   */
  public long getThresholdValue() {
    return threshold;
  }
}

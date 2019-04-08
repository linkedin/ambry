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

/**
 * The criteria used to check if certain threshold is satisfied.
 */
public class Criteria {
  long threshold;
  boolean shouldBelowThreshold;

  public Criteria(long threshold, boolean shouldBelowThreshold) {
    this.threshold = threshold;
    this.shouldBelowThreshold = shouldBelowThreshold;
  }

  boolean apply(Long value) throws IllegalArgumentException {
    if (value == null) {
      throw new IllegalArgumentException("Input value is empty which cannot be applied to threshold check.");
    }
    return shouldBelowThreshold ? value < threshold : value > threshold;
  }
}

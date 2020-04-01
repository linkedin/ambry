/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import java.util.List;


/**
 * CostBenefitInfo Stores cost benefit information for a potential candidate to be compacted
 */
class CostBenefitInfo implements Comparable<CostBenefitInfo> {
  private final long cost;
  private final int benefit;
  private final Double costBenefitRatio;
  private final List<String> segmentsToCompact;

  CostBenefitInfo(List<String> segmentsToCompact, long cost, int benefit) {
    this.segmentsToCompact = segmentsToCompact;
    this.cost = cost;
    this.benefit = benefit;
    costBenefitRatio = benefit == 0 ? Double.MAX_VALUE : cost * (1.0 / benefit);
  }

  @Override
  public int compareTo(CostBenefitInfo that) {
    int compareToVal = costBenefitRatio.compareTo(that.costBenefitRatio);
    if (compareToVal != 0) {
      return compareToVal;
    }
    return Integer.compare(that.benefit, benefit);
  }

  /**
   * @return the cost associated with this {@link CostBenefitInfo}
   */
  long getCost() {
    return cost;
  }

  /**
   * @return the benefit associated with this {@link CostBenefitInfo}
   */
  int getBenefit() {
    return benefit;
  }

  /**
   * @return the cost benefit ratio associated with this {@link CostBenefitInfo}
   */
  Double getCostBenefitRatio() {
    return costBenefitRatio;
  }

  /**
   * @return the {@link List} of log segment names to compact
   */
  List<String> getSegmentsToCompact() {
    return segmentsToCompact;
  }

  @Override
  public String toString() {
    return "[CandidateInfo: Segments to compact " + segmentsToCompact + ", Cost " + cost + "," + "Benefit " + benefit
        + ", CostBenefitRatio " + costBenefitRatio + "]";
  }
}

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

/**
 * CostBenefitInfo Stores cost benefit information for a potential candidate to be compacted
 */
class CostBenefitInfo implements Comparable<CostBenefitInfo> {
  private final long cost;
  private final int benefit;
  private final Double costBenefitRatio;
  private final String firstLogSegmentName;
  private final String lastLogSegmentName;

  CostBenefitInfo(String firstLogSegmentName, String lastLogSegmentName, long cost, int benefit) {
    this.firstLogSegmentName = firstLogSegmentName;
    this.lastLogSegmentName = lastLogSegmentName;
    this.cost = cost;
    this.benefit = benefit;
    costBenefitRatio = benefit == 0 ? Double.MAX_VALUE : cost * 1.0 / benefit;
  }

  @Override
  public int compareTo(CostBenefitInfo that) {
    if (costBenefitRatio.compareTo(that.costBenefitRatio) != 0) {
      return costBenefitRatio.compareTo(that.costBenefitRatio);
    }
    if (benefit != that.benefit) {
      return benefit > that.benefit ? -1 : 1;
    }
    return 0;
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
   * @return the first log segment name associated with this {@link CostBenefitInfo}
   */
  String getFirstLogSegmentName() {
    return firstLogSegmentName;
  }

  /**
   * @return the last log segment name associated with this {@link CostBenefitInfo}
   */
  String getLastLogSegmentName() {
    return lastLogSegmentName;
  }

  @Override
  public String toString() {
    return "[CandidateInfo: First segment name " + firstLogSegmentName + ", Last segment name " + lastLogSegmentName
        + ", Cost " + cost + "," + "Benefit " + benefit + ", CostBenefitRatio " + costBenefitRatio + "]";
  }
}

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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.UtilsTest;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests {@link CostBenefitInfo}
 */
public class CostBenefitInfoTest {

  /**
   * Tests {@link CostBenefitInfo} for construction and getters
   */
  @Test
  public void testCostBenefitInfo() {
    for (int i = 0; i < 5; i++) {
      String firstSegmentName = UtilsTest.getRandomString(5);
      String lastSegmentName = UtilsTest.getRandomString(5);
      long cost = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE);
      int benefit = 1 + TestUtils.RANDOM.nextInt(Integer.MAX_VALUE);
      CostBenefitInfo actual = new CostBenefitInfo(firstSegmentName, lastSegmentName, cost, benefit);
      verifyCostBenfitInfo(actual, firstSegmentName, lastSegmentName, cost, benefit, cost * 1.0 / benefit);
    }
  }

  /**
   * Tests {@link CostBenefitInfo} for 0 benefit
   */
  @Test
  public void testCostBenefitInfoForZeroBenefit() {
    String firstSegmentName = UtilsTest.getRandomString(5);
    String lastSegmentName = UtilsTest.getRandomString(5);
    long cost = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE);
    int benefit = 0;
    CostBenefitInfo actual = new CostBenefitInfo(firstSegmentName, lastSegmentName, cost, benefit);
    verifyCostBenfitInfo(actual, firstSegmentName, lastSegmentName, cost, benefit, Double.MAX_VALUE);
  }

  /**
   * Tests {@link CostBenefitInfo} for comparisons
   */
  @Test
  public void testCostBenefitInfoComparison() {
    String firstSegmentName = UtilsTest.getRandomString(5);
    String lastSegmentName = UtilsTest.getRandomString(5);
    long cost = 1 + ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE - 1);
    int benefit = 1 + TestUtils.RANDOM.nextInt(Integer.MAX_VALUE - 1);
    CostBenefitInfo one = new CostBenefitInfo(firstSegmentName, lastSegmentName, cost, benefit);
    // generate a CostBenefitInfo with cost = 1 + one's cost
    compareAndTest(one, 1, 0, -1);
    // generate a CostBenefitInfo with cost = 1 - one's cost
    compareAndTest(one, -1, 0, 1);
    // generate a CostBenefitInfo with same cost as one
    compareAndTest(one, 0, 0, 0);
    // generate a CostBenefitInfo with benefit = 1 + one's benefit
    compareAndTest(one, 0, 1, 1);
    // generate a CostBenefitInfo with benefit = 1 - one's benefit
    compareAndTest(one, 0, -1, -1);

    // test a case where costBenefitRatio is same, but diff cost and benefit.
    cost = ThreadLocalRandom.current().nextLong(Integer.MAX_VALUE / 2);
    benefit = 2 + TestUtils.RANDOM.nextInt(Integer.MAX_VALUE / 2 - 2);
    if (cost % 2 != 0) {
      cost++;
    }
    if (benefit % 2 != 0) {
      benefit++;
    }
    one = new CostBenefitInfo(firstSegmentName, lastSegmentName, cost, benefit);
    // generate a CostBenefitInfo with cost = half of one and benefit = half of one. CostBenefit is same, but one'e
    // benefit is more
    compareAndTest(one, (cost / 2) * (-1), (benefit / 2) * (-1), -1);
  }

  /**
   * Compares the {@code actual} {@link CostBenefitInfo} with a new one that is generated based on the arguments passed
   * @param actual Actual {@link CostBenefitInfo} to be compared against
   * @param costDiff cost difference to generate the new {@link CostBenefitInfo} wrt {@code actual}
   * @param benefitDiff benefit difference to generate the new {@link CostBenefitInfo} wrt {@code actual}
   * @param comparisonValue expected comparison value
   */
  private void compareAndTest(CostBenefitInfo actual, long costDiff, int benefitDiff, int comparisonValue) {
    CostBenefitInfo newCostBenefitInfo =
        new CostBenefitInfo(actual.getFirstLogSegmentName(), actual.getLastLogSegmentName(),
            actual.getCost() + costDiff, actual.getBenefit() + benefitDiff);
    assertTrue("Cost Benefit info comparison mismatch ", actual.compareTo(newCostBenefitInfo) == comparisonValue);
    assertTrue("Cost Benefit info comparison mismatch ", newCostBenefitInfo.compareTo(actual) == comparisonValue * -1);
  }

  /**
   * Verifies {@link CostBenefitInfo} for expected values
   * @param actual {@link CostBenefitInfo} to be compared against
   * @param firstSegmentName expected first log segment name
   * @param lastSegmentName expected last log segment name
   * @param cost expected cost
   * @param benefit expected benefit
   * @param costBenefitRatio expected cost benefit ratio
   */
  private void verifyCostBenfitInfo(CostBenefitInfo actual, String firstSegmentName, String lastSegmentName, long cost,
      int benefit, double costBenefitRatio) {
    assertEquals("First log segment name mismatch ", firstSegmentName, actual.getFirstLogSegmentName());
    assertEquals("Last log segment name mismatch ", lastSegmentName, actual.getLastLogSegmentName());
    assertEquals("Cost mismatch ", cost, actual.getCost());
    assertEquals("Benefit mismatch ", benefit, actual.getBenefit());
    assertTrue("CostBenefitRatio mismatch ", actual.getCostBenefitRatio().compareTo(costBenefitRatio) == 0);
  }
}

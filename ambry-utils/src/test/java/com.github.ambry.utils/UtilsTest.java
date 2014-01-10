package com.github.ambry.utils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests Utils methods
 */
public class UtilsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testGetRandomLongException() {
    Utils.getRandomLong(0);
  }

  public void whpGetRandomLongRangeTest(int range, int draws) {
    // This test is probabilistic in nature if range is greater than one.
    // Make sure draws >> range for test to pass with high probability.
    int count[] = new int[range];
    for (int i = 0; i < draws; i++) {
      long r = Utils.getRandomLong(range);
      assertTrue(r >= 0);
      assertTrue(r < range);
      count[(int)r] = count[(int)r] + 1;
    }
    for (int i = 0; i < range; i++) {
      assertTrue(count[i] > 0);
    }
  }

  @Test
  public void testGetRandom() {
    whpGetRandomLongRangeTest(1, 1);
    whpGetRandomLongRangeTest(2, 1000);
    whpGetRandomLongRangeTest(3, 1000);
    whpGetRandomLongRangeTest(31, 100 * 1000);
    whpGetRandomLongRangeTest(99, 100 * 1000);
    whpGetRandomLongRangeTest(100, 100 * 1000);
  }
}
